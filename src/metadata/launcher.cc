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

#include "metadata/shared.h"

extern "C" {
#include "access/xact.h"
#include "access/heapam.h"
#include "access/table.h"
#include "catalog/pg_database.h"
#include "libpq/pqsignal.h"
#include "miscadmin.h"
#include "postmaster/bgworker.h"
#include "postmaster/interrupt.h"
#include "storage/ipc.h"
#include "tcop/tcopprot.h"
#include "utils/guc.h"
#include "utils/memutils.h"
#include "utils/varlena.h"
#include "utils/wait_event.h"
}

namespace pgiceberg::metadata {
namespace {
void LauncherExit(int, Datum) {
  LWLockAcquire(RegistryLock, LW_EXCLUSIVE);
  if (Registry->launcher_pid == MyProcPid) {
    Registry->launcher = nullptr;
    Registry->launcher_pid = 0;
  }
  LWLockRelease(RegistryLock);
}

void Reconcile() {
  // Resolve names against the shared pg_database catalog. A database-less
  // launcher never scans another database's extension catalog or uses SPI.
  Oid databases[kWorkerSlots];
  int count = 0;
  StartTransactionCommand();
  char* copy = pstrdup(DatabaseNames);
  List* names = NIL;
  if (!SplitIdentifierString(copy, ',', &names))
    elog(ERROR, "invalid metadata database list");
  // A database-less backend cannot initialize local catalog/index caches.
  // Scan pg_database's heap like core's autovacuum launcher, without syscache.
  Relation relation = table_open(DatabaseRelationId, AccessShareLock);
  TableScanDesc scan = table_beginscan_catalog(relation, 0, nullptr);
  HeapTuple tuple;
  while ((tuple = heap_getnext(scan, ForwardScanDirection)) != nullptr) {
    auto* form = reinterpret_cast<Form_pg_database>(GETSTRUCT(tuple));
    if (!form->datallowconn || form->datistemplate) continue;
    ListCell* cell;
    foreach (cell, names) {
      if (strcmp(NameStr(form->datname), static_cast<char*>(lfirst(cell))) == 0) {
        if (count < MaxWorkers) databases[count++] = form->oid;
        break;
      }
    }
  }
  table_endscan(scan);
  table_close(relation, AccessShareLock);
  list_free(names);
  pfree(copy);
  CommitTransactionCommand();

  TimestampTz now = GetCurrentTimestamp();
  LWLockAcquire(RegistryLock, LW_EXCLUSIVE);
  for (auto& worker : Registry->workers) {
    worker.desired = false;
    for (int i = 0; i < count; ++i) worker.desired |= worker.database == databases[i];
    if (!worker.desired) WakeProcess(worker.proc, worker.pid);
    if (worker.pid && !ProcessAlive(worker.proc, worker.pid)) {
      worker.proc = nullptr;
      worker.pid = 0;
      worker.state = WorkerState::kStopped;
      worker.retry_at = now + USECS_PER_SEC;
    }
  }
  for (int i = 0; i < count; ++i) {
    bool found = false;
    for (auto& worker : Registry->workers) found |= worker.database == databases[i];
    if (found) continue;
    for (auto& worker : Registry->workers) {
      if (worker.desired || worker.pid || worker.state == WorkerState::kStarting)
        continue;
      worker = {};
      worker.database = databases[i];
      worker.desired = true;
      break;
    }
  }
  LWLockRelease(RegistryLock);

  for (int i = 0; i < kWorkerSlots; ++i) {
    CHECK_FOR_INTERRUPTS();
    LWLockAcquire(RegistryLock, LW_EXCLUSIVE);
    auto& slot = Registry->workers[i];
    // A starting child claims its generation before connecting. Expired
    // registrations are fenced out, also when a new launcher inherits them.
    if (slot.state == WorkerState::kStarting && !slot.pid && now >= slot.retry_at)
      slot.state = WorkerState::kStopped;
    if (!slot.desired || slot.pid || slot.state == WorkerState::kStarting ||
        now < slot.retry_at) {
      LWLockRelease(RegistryLock);
      continue;
    }
    int running = 0;
    for (const auto& worker : Registry->workers)
      if (worker.pid || worker.state == WorkerState::kStarting) ++running;
    if (running >= MaxWorkers) {
      LWLockRelease(RegistryLock);
      continue;
    }
    slot.state = WorkerState::kStarting;
    slot.generation = ++Registry->next_generation;
    slot.extension = InvalidOid;
    slot.retry_at = now + 10 * USECS_PER_SEC;
    uint64 generation = slot.generation;
    Oid database = slot.database;
    LWLockRelease(RegistryLock);

    BackgroundWorker worker{};
    worker.bgw_flags = BGWORKER_SHMEM_ACCESS | BGWORKER_BACKEND_DATABASE_CONNECTION;
    worker.bgw_start_time = BgWorkerStart_RecoveryFinished;
    worker.bgw_restart_time = BGW_NEVER_RESTART;
    snprintf(worker.bgw_library_name, BGW_MAXLEN, "pgiceberg");
    snprintf(worker.bgw_function_name, BGW_MAXLEN, "pgiceberg_metadata_worker_main");
    snprintf(worker.bgw_name, BGW_MAXLEN, "pgiceberg metadata %u", database);
    snprintf(worker.bgw_type, BGW_MAXLEN, "pgiceberg metadata worker");
    worker.bgw_main_arg = Int32GetDatum(i);
    memcpy(worker.bgw_extra, &generation, sizeof(generation));
    if (!RegisterDynamicBackgroundWorker(&worker, nullptr)) {
      LWLockAcquire(RegistryLock, LW_EXCLUSIVE);
      if (slot.generation == generation && !slot.pid) {
        slot.state = WorkerState::kStopped;
        slot.retry_at = now + USECS_PER_SEC;
      }
      LWLockRelease(RegistryLock);
      ereport(LOG,
              (errmsg("no background worker slot for pgiceberg database %u", database),
               errhint("Increase max_worker_processes.")));
    }
  }
}
}  // namespace
}  // namespace pgiceberg::metadata

extern "C" PGDLLEXPORT void pgiceberg_metadata_launcher_main(Datum) {
  using namespace pgiceberg::metadata;
  pqsignal(SIGTERM, die);
  pqsignal(SIGHUP, SignalHandlerForConfigReload);
  BackgroundWorkerUnblockSignals();
  AttachRegistry();
  before_shmem_exit(LauncherExit, 0);
  BackgroundWorkerInitializeConnection(nullptr, nullptr, 0);
  LWLockAcquire(RegistryLock, LW_EXCLUSIVE);
  Registry->launcher = MyProc;
  Registry->launcher_pid = MyProcPid;
  LWLockRelease(RegistryLock);
  for (;;) {
    ResetLatch(MyLatch);
    CHECK_FOR_INTERRUPTS();
    if (ConfigReloadPending) {
      ConfigReloadPending = false;
      ProcessConfigFile(PGC_SIGHUP);
    }
    Reconcile();
    WaitLatch(MyLatch, WL_LATCH_SET | WL_TIMEOUT | WL_EXIT_ON_PM_DEATH, 1000,
              PG_WAIT_EXTENSION);
  }
}
