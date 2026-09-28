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
#include "postgres.h"
#include "fmgr.h"
#include "funcapi.h"
#include "miscadmin.h"
#include "storage/shm_mq.h"
#include "utils/wait_event.h"
}

namespace pgiceberg::metadata {
namespace {
struct Request {
  dsm_segment* segment;
  shm_mq_handle* input;
  shm_mq_handle* output;
  int connection;
  int worker;
  uint64 id;
  uint64 generation;
  TimestampTz deadline;
};

void Cleanup(Request* request) {
  if (request->connection >= 0) {
    LWLockAcquire(RegistryLock, LW_EXCLUSIVE);
    auto& connection = Registry->connections[request->connection];
    if (connection.active && connection.request == request->id) connection.active = false;
    auto& worker = Registry->workers[request->worker];
    WakeProcess(worker.proc, worker.pid);
    LWLockRelease(RegistryLock);
    request->connection = -1;
  }
  if (request->segment) {
    dsm_detach(request->segment);
    request->segment = nullptr;
  }
}

void Wait(Request* request) {
  CHECK_FOR_INTERRUPTS();
  TimestampTz now = GetCurrentTimestamp();
  if (now >= request->deadline)
    ereport(ERROR, (errcode(ERRCODE_QUERY_CANCELED),
                    errmsg("pgiceberg metadata request timed out")));
  if (request->connection >= 0) {
    LWLockAcquire(RegistryLock, LW_SHARED);
    auto& worker = Registry->workers[request->worker];
    auto& connection = Registry->connections[request->connection];
    bool alive = worker.generation == request->generation &&
                 worker.state == WorkerState::kReady && worker.desired &&
                 ProcessAlive(worker.proc, worker.pid) && connection.active &&
                 connection.request == request->id;
    LWLockRelease(RegistryLock);
    if (!alive)
      ereport(ERROR, (errcode(ERRCODE_CONNECTION_FAILURE),
                      errmsg("pgiceberg metadata worker disconnected")));
  }
  long milliseconds =
      Min(50L, Max(1L, static_cast<long>((request->deadline - now) / 1000)));
  WaitLatch(MyLatch, WL_LATCH_SET | WL_TIMEOUT | WL_EXIT_ON_PM_DEATH, milliseconds,
            PG_WAIT_EXTENSION);
}

void Connect(Request* request) {
  for (;;) {
    ResetLatch(MyLatch);
    LWLockAcquire(RegistryLock, LW_EXCLUSIVE);
    bool configured = false;
    for (int i = 0; i < kWorkerSlots; ++i) {
      auto& worker = Registry->workers[i];
      if (worker.database != MyDatabaseId || !worker.desired) continue;
      configured = true;
      if (worker.state == WorkerState::kReady && ProcessAlive(worker.proc, worker.pid)) {
        for (int j = 0; j < MaxConnections; ++j) {
          auto& connection = Registry->connections[j];
          if (connection.active && ProcessAlive(connection.proc, connection.pid))
            continue;
          request->connection = j;
          request->worker = i;
          request->generation = worker.generation;
          request->id = ++Registry->next_request;
          connection = {true,
                        i,
                        worker.generation,
                        request->id,
                        MyProc,
                        MyProcPid,
                        dsm_segment_handle(request->segment)};
          WakeProcess(worker.proc, worker.pid);
          LWLockRelease(RegistryLock);
          return;
        }
        LWLockRelease(RegistryLock);
        ereport(ERROR,
                (errcode(ERRCODE_CONFIGURATION_LIMIT_EXCEEDED),
                 errmsg("pgiceberg metadata request limit reached"),
                 errhint("Retry later or increase pgiceberg.metadata_max_connections.")));
      }
      WakeProcess(Registry->launcher, Registry->launcher_pid);
    }
    LWLockRelease(RegistryLock);
    if (!configured)
      ereport(ERROR, (errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
                      errmsg("metadata worker is not enabled for this database"),
                      errhint("Add this database to pgiceberg.metadata_databases and "
                              "reload PostgreSQL.")));
    Wait(request);
  }
}

bytea* Call(MessageKind kind, const char* payload, Size payload_size,
            uint64* generation) {
  if (payload_size > kMaxPayload)
    ereport(ERROR, (errcode(ERRCODE_PROGRAM_LIMIT_EXCEEDED),
                    errmsg("metadata request payload exceeds %zu bytes", kMaxPayload)));
  AttachRegistry();
  // Heap-allocated state remains defined after PostgreSQL's error longjmp.
  auto* request = static_cast<Request*>(palloc0(sizeof(Request)));
  request->connection = -1;
  request->deadline = GetCurrentTimestamp() + static_cast<int64>(RequestTimeoutMs) * 1000;
  bytea* response = nullptr;
  PG_TRY();
  {
    request->segment = dsm_create(kSegmentSize, 0);
    auto* base = static_cast<char*>(dsm_segment_address(request->segment));
    auto* output = shm_mq_create(base, kQueueSize);
    auto* input = shm_mq_create(base + kQueueSize, kQueueSize);
    shm_mq_set_sender(output, MyProc);
    shm_mq_set_receiver(input, MyProc);
    request->output = shm_mq_attach(output, request->segment, nullptr);
    request->input = shm_mq_attach(input, request->segment, nullptr);
    Connect(request);
    auto* message = static_cast<char*>(palloc(kHeaderSize + payload_size));
    MessageHeader header{
        kind,        ReplyStatus::kOk,    MyDatabaseId,
        request->id, request->generation, static_cast<uint32>(payload_size)};
    EncodeHeader(message, header);
    if (payload_size) memcpy(message + kHeaderSize, payload, payload_size);
    for (;;) {
      ResetLatch(MyLatch);
      CHECK_FOR_INTERRUPTS();
      auto result =
          shm_mq_send(request->output, kHeaderSize + payload_size, message, true, true);
      if (result == SHM_MQ_SUCCESS) break;
      if (result == SHM_MQ_DETACHED)
        ereport(ERROR, (errmsg("metadata request queue detached")));
      Wait(request);
    }
    pfree(message);
    for (;;) {
      ResetLatch(MyLatch);
      CHECK_FOR_INTERRUPTS();
      Size size;
      void* data;
      auto result = shm_mq_receive(request->input, &size, &data, true);
      if (result == SHM_MQ_SUCCESS) {
        MessageHeader reply{};
        if (!DecodeHeader(data, size, &reply) || reply.kind != kind ||
            reply.status != ReplyStatus::kOk || reply.database != MyDatabaseId ||
            reply.request != request->id || reply.generation != request->generation ||
            (kind == MessageKind::kPing && reply.payload_size != 4) ||
            (kind == MessageKind::kEcho && reply.payload_size != payload_size))
          ereport(ERROR, (errmsg("invalid metadata worker response")));
        response = static_cast<bytea*>(palloc(VARHDRSZ + reply.payload_size));
        SET_VARSIZE(response, VARHDRSZ + reply.payload_size);
        memcpy(VARDATA(response), static_cast<char*>(data) + kHeaderSize,
               reply.payload_size);
        break;
      }
      if (result == SHM_MQ_DETACHED)
        ereport(ERROR, (errmsg("metadata response queue detached")));
      Wait(request);
    }
    *generation = request->generation;
    Cleanup(request);
  }
  PG_CATCH();
  {
    Cleanup(request);
    PG_RE_THROW();
  }
  PG_END_TRY();
  pfree(request);
  return response;
}
}  // namespace
}  // namespace pgiceberg::metadata

extern "C" {
PG_FUNCTION_INFO_V1(pgiceberg_metadata_worker_ping);
Datum pgiceberg_metadata_worker_ping(PG_FUNCTION_ARGS) {
  using namespace pgiceberg::metadata;
  uint64 generation;
  bytea* reply = Call(MessageKind::kPing, nullptr, 0, &generation);
  TupleDesc descriptor;
  if (get_call_result_type(fcinfo, nullptr, &descriptor) != TYPEFUNC_COMPOSITE)
    elog(ERROR, "metadata_worker_ping must return a composite type");
  BlessTupleDesc(descriptor);
  Datum values[] = {ObjectIdGetDatum(MyDatabaseId),
                    Int32GetDatum(static_cast<int32>(ReadUint32(VARDATA(reply)))),
                    Int64GetDatum(static_cast<int64>(generation)),
                    Int32GetDatum(kProtocolVersion)};
  bool nulls[] = {false, false, false, false};
  PG_RETURN_DATUM(HeapTupleGetDatum(heap_form_tuple(descriptor, values, nulls)));
}

PG_FUNCTION_INFO_V1(pgiceberg_metadata_worker_echo);
Datum pgiceberg_metadata_worker_echo(PG_FUNCTION_ARGS) {
  using namespace pgiceberg::metadata;
  bytea* payload = PG_GETARG_BYTEA_PP(0);
  uint64 generation;
  PG_RETURN_BYTEA_P(Call(MessageKind::kEcho, VARDATA_ANY(payload),
                         VARSIZE_ANY_EXHDR(payload), &generation));
}
}
