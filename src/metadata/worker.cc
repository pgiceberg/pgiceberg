// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#include "metadata/protocol.h"
#include "metadata/shared.h"

extern "C" {
#include "access/xact.h"
#include "commands/extension.h"
#include "libpq/pqsignal.h"
#include "miscadmin.h"
#include "postmaster/bgworker.h"
#include "postmaster/interrupt.h"
#include "storage/ipc.h"
#include "storage/shm_mq.h"
#include "tcop/tcopprot.h"
#include "utils/guc.h"
#include "utils/memutils.h"
#include "utils/wait_event.h"
}

namespace pgiceberg::metadata {
namespace {
int WorkerIndex;
uint64 Generation;
struct Channel {
  uint64 request;
  MemoryContext context;
  dsm_segment* segment;
  shm_mq_handle* input;
  shm_mq_handle* output;
  char* reply;
  Size reply_size;
  bool received;
  bool sent;
};
Channel Channels[kConnectionSlots];

void CloseChannel(Channel* channel) {
  if (channel->segment) dsm_detach(channel->segment);
  if (channel->context) MemoryContextDelete(channel->context);
  *channel = {};
}

void WorkerExit(int, Datum) {
  LWLockAcquire(RegistryLock, LW_EXCLUSIVE);
  auto& slot = Registry->workers[WorkerIndex];
  if (slot.generation == Generation && slot.pid == MyProcPid) {
    slot.proc = nullptr;
    slot.pid = 0;
    if (slot.state != WorkerState::kAbsent) {
      slot.state = WorkerState::kStopped;
      slot.retry_at = GetCurrentTimestamp() + USECS_PER_SEC;
    }
    for (auto& connection : Registry->connections) {
      if (connection.active && connection.worker_slot == WorkerIndex &&
          connection.worker_generation == Generation) {
        WakeProcess(connection.proc, connection.pid);
        connection.active = false;
      }
    }
    WakeProcess(Registry->launcher, Registry->launcher_pid);
  }
  LWLockRelease(RegistryLock);
}

bool RefreshExtension() {
  LWLockAcquire(RegistryLock, LW_SHARED);
  uint64 changes = Registry->workers[WorkerIndex].extension_changes;
  LWLockRelease(RegistryLock);
  StartTransactionCommand();
  Oid extension = get_extension_oid("pgiceberg", true);
  CommitTransactionCommand();
  LWLockAcquire(RegistryLock, LW_EXCLUSIVE);
  auto& slot = Registry->workers[WorkerIndex];
  bool usable = slot.desired && slot.generation == Generation && OidIsValid(extension) &&
                (!OidIsValid(slot.extension) || slot.extension == extension);
  if (usable) {
    slot.extension = extension;
    slot.state = WorkerState::kReady;
  } else if (!OidIsValid(extension) && slot.extension_changes == changes) {
    slot.state = WorkerState::kAbsent;
    // The extension transaction hook normally wakes the launcher immediately.
    // A periodic retry covers missed notifications (e.g. prepared transactions).
    slot.retry_at = GetCurrentTimestamp() + 30 * USECS_PER_SEC;
  }
  LWLockRelease(RegistryLock);
  return usable;
}

void OpenChannel(Channel* channel, int index, const ConnectionSlot& connection) {
  channel->context =
      AllocSetContextCreate(TopMemoryContext, "metadata channel", ALLOCSET_DEFAULT_SIZES);
  MemoryContext previous = MemoryContextSwitchTo(channel->context);
  channel->segment = dsm_attach(connection.handle);
  if (channel->segment) {
    // Keep this mapping across the short catalog-check transactions.
    dsm_pin_mapping(channel->segment);
    if (dsm_segment_map_length(channel->segment) == kSegmentSize) {
      auto* base = static_cast<char*>(dsm_segment_address(channel->segment));
      auto* input = reinterpret_cast<shm_mq*>(base);
      auto* output = reinterpret_cast<shm_mq*>(base + kQueueSize);
      LWLockAcquire(RegistryLock, LW_SHARED);
      bool current = Registry->connections[index].active &&
                     Registry->connections[index].request == connection.request;
      if (current) {
        shm_mq_set_receiver(input, MyProc);
        shm_mq_set_sender(output, MyProc);
      }
      LWLockRelease(RegistryLock);
      if (!current) {
        MemoryContextSwitchTo(previous);
        CloseChannel(channel);
        return;
      }
      channel->input = shm_mq_attach(input, channel->segment, nullptr);
      channel->output = shm_mq_attach(output, channel->segment, nullptr);
      channel->request = connection.request;
    }
  }
  MemoryContextSwitchTo(previous);
  if (!channel->input) CloseChannel(channel);
}

void ProcessChannel(Channel* channel, const ConnectionSlot& connection) {
  if (!channel->received) {
    Size size;
    void* data;
    auto result = shm_mq_receive(channel->input, &size, &data, true);
    if (result == SHM_MQ_WOULD_BLOCK) return;
    if (result == SHM_MQ_DETACHED) {
      CloseChannel(channel);
      return;
    }
    MessageHeader header{};
    bool valid = DecodeHeader(data, size, &header) && header.database == MyDatabaseId &&
                 header.generation == Generation &&
                 header.request == connection.request &&
                 header.status == ReplyStatus::kOk &&
                 (header.kind == MessageKind::kEcho ||
                  (header.kind == MessageKind::kPing && header.payload_size == 0));
    MessageHeader reply{
        header.kind,  valid ? ReplyStatus::kOk : ReplyStatus::kInvalidRequest,
        MyDatabaseId, connection.request,
        Generation,   0};
    if (valid)
      reply.payload_size = header.kind == MessageKind::kPing ? 4 : header.payload_size;
    channel->reply_size = kHeaderSize + reply.payload_size;
    channel->reply =
        static_cast<char*>(MemoryContextAlloc(channel->context, channel->reply_size));
    EncodeHeader(channel->reply, reply);
    if (valid && header.kind == MessageKind::kPing)
      WriteUint32(channel->reply + kHeaderSize, MyProcPid);
    else if (reply.payload_size)
      memcpy(channel->reply + kHeaderSize, static_cast<char*>(data) + kHeaderSize,
             reply.payload_size);
    channel->received = true;
  }
  if (!channel->sent) {
    auto result =
        shm_mq_send(channel->output, channel->reply_size, channel->reply, true, true);
    if (result == SHM_MQ_DETACHED)
      CloseChannel(channel);
    else if (result == SHM_MQ_SUCCESS)
      channel->sent = true;
  }
}

void ProcessConnections() {
  for (int i = 0; i < MaxConnections; ++i) {
    CHECK_FOR_INTERRUPTS();
    LWLockAcquire(RegistryLock, LW_EXCLUSIVE);
    auto& shared = Registry->connections[i];
    if (shared.active && !ProcessAlive(shared.proc, shared.pid)) shared.active = false;
    ConnectionSlot connection = shared;
    LWLockRelease(RegistryLock);
    auto* channel = &Channels[i];
    bool ours = connection.active && connection.worker_slot == WorkerIndex &&
                connection.worker_generation == Generation;
    if (!ours || (channel->request && channel->request != connection.request))
      CloseChannel(channel);
    if (!ours) continue;
    // A detached client may disappear between publication and dsm_attach.
    // Isolate attachment/transport errors to this connection, not other callers.
    MemoryContext previous = CurrentMemoryContext;
    PG_TRY();
    {
      if (!channel->segment) OpenChannel(channel, i, connection);
      if (channel->segment) ProcessChannel(channel, connection);
    }
    PG_CATCH();
    {
      MemoryContextSwitchTo(previous);
      CloseChannel(channel);
      FlushErrorState();
    }
    PG_END_TRY();
    // Never reopen a detached or partially consumed queue: its endpoints are
    // single-assignment. The client will tear down its own mapping.
    if (!channel->segment) {
      LWLockAcquire(RegistryLock, LW_EXCLUSIVE);
      if (shared.active && shared.request == connection.request) {
        shared.active = false;
        WakeProcess(shared.proc, shared.pid);
      }
      LWLockRelease(RegistryLock);
    }
  }
}
}  // namespace
}  // namespace pgiceberg::metadata

extern "C" PGDLLEXPORT void pgiceberg_metadata_worker_main(Datum argument) {
  using namespace pgiceberg::metadata;
  pqsignal(SIGTERM, die);
  pqsignal(SIGHUP, SignalHandlerForConfigReload);
  BackgroundWorkerUnblockSignals();
  AttachRegistry();
  WorkerIndex = DatumGetInt32(argument);
  memcpy(&Generation, MyBgworkerEntry->bgw_extra, sizeof(Generation));
  if (WorkerIndex < 0 || WorkerIndex >= kWorkerSlots) proc_exit(1);
  LWLockAcquire(RegistryLock, LW_EXCLUSIVE);
  auto& slot = Registry->workers[WorkerIndex];
  bool valid = slot.desired && slot.generation == Generation && !slot.pid &&
               slot.state == WorkerState::kStarting;
  Oid database = slot.database;
  if (valid) {
    slot.proc = MyProc;
    slot.pid = MyProcPid;
  }
  LWLockRelease(RegistryLock);
  if (!valid) proc_exit(0);
  before_shmem_exit(WorkerExit, 0);
  // Only ping/echo are implemented. No client SQL or catalog mutations execute
  // with this bootstrap-superuser connection; future operations need ACL checks.
  BackgroundWorkerInitializeConnectionByOid(database, InvalidOid, 0);
  if (!RefreshExtension()) proc_exit(0);
  TimestampTz next_check = 0;
  for (;;) {
    ResetLatch(MyLatch);
    CHECK_FOR_INTERRUPTS();
    if (ConfigReloadPending) {
      ConfigReloadPending = false;
      ProcessConfigFile(PGC_SIGHUP);
    }
    if (GetCurrentTimestamp() >= next_check) {
      if (!RefreshExtension()) proc_exit(0);
      next_check = GetCurrentTimestamp() + 250000;
    }
    ProcessConnections();
    WaitLatch(MyLatch, WL_LATCH_SET | WL_TIMEOUT | WL_EXIT_ON_PM_DEATH, 100,
              PG_WAIT_EXTENSION);
  }
}
