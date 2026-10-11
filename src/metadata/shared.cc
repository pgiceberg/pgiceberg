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
#include "metadata/worker.h"

extern "C" {
#include "access/xact.h"
#include "catalog/objectaccess.h"
#include "catalog/pg_extension.h"
#include "miscadmin.h"
#include "postmaster/bgworker.h"
#include "storage/ipc.h"
#include "storage/shmem.h"
#include "utils/guc.h"
#include "utils/varlena.h"
}

namespace pgiceberg::metadata {
SharedState* Registry = nullptr;
LWLock* RegistryLock = nullptr;
int MaxWorkers = 8;
int MaxConnections = 64;
int RequestTimeoutMs = 5000;
char* DatabaseNames = nullptr;

namespace {
shmem_request_hook_type PreviousRequestHook = nullptr;
shmem_startup_hook_type PreviousStartupHook = nullptr;
object_access_hook_type PreviousObjectHook = nullptr;
bool ExtensionChanged = false;
bool Preloaded = false;

void RequestSharedMemory() {
  if (PreviousRequestHook) PreviousRequestHook();
  RequestAddinShmemSpace(sizeof(SharedState));
  RequestNamedLWLockTranche(kLockName, 1);
}

void InitializeSharedMemory() {
  if (PreviousStartupHook) PreviousStartupHook();
  bool found;
  LWLockAcquire(AddinShmemInitLock, LW_EXCLUSIVE);
  Registry = static_cast<SharedState*>(
      ShmemInitStruct(kSharedName, sizeof(SharedState), &found));
  if (!found) memset(Registry, 0, sizeof(SharedState));
  LWLockRelease(AddinShmemInitLock);
  RegistryLock = &GetNamedLWLockTranche(kLockName)[0].lock;
}

bool CheckDatabases(char** value, void**, GucSource) {
  char* copy = pstrdup(*value);
  List* names = NIL;
  bool valid =
      SplitIdentifierString(copy, ',', &names) && list_length(names) <= MaxWorkers;
  list_free(names);
  pfree(copy);
  if (!valid)
    GUC_check_errdetail(
        "Expected at most %d comma-separated database names "
        "(pgiceberg.metadata_max_workers).",
        MaxWorkers);
  return valid;
}

void ObjectAccess(ObjectAccessType access, Oid class_id, Oid object_id, int sub_id,
                  void* arg) {
  if (PreviousObjectHook) PreviousObjectHook(access, class_id, object_id, sub_id, arg);
  if (class_id == ExtensionRelationId &&
      (access == OAT_POST_CREATE || access == OAT_DROP))
    ExtensionChanged = true;
}

void ExtensionTransaction(XactEvent event, void*) {
  if (event != XACT_EVENT_COMMIT && event != XACT_EVENT_ABORT &&
      event != XACT_EVENT_PARALLEL_COMMIT && event != XACT_EVENT_PARALLEL_ABORT)
    return;
  if (!ExtensionChanged) return;
  ExtensionChanged = false;
  if (!Registry) return;
  // This notification only schedules a catalog recheck. Rollback and unrelated
  // extensions may cause harmless extra checks, including subtransaction aborts.
  LWLockAcquire(RegistryLock, LW_EXCLUSIVE);
  for (auto& worker : Registry->workers) {
    if (worker.database != MyDatabaseId || !worker.desired) continue;
    ++worker.extension_changes;
    if (worker.state == WorkerState::kAbsent) {
      worker.state = WorkerState::kStopped;
      worker.retry_at = 0;
    }
    WakeProcess(worker.proc, worker.pid);
  }
  WakeProcess(Registry->launcher, Registry->launcher_pid);
  LWLockRelease(RegistryLock);
}
}  // namespace

bool ProcessAlive(PGPROC* proc, int pid) { return proc && pid > 0 && proc->pid == pid; }

void WakeProcess(PGPROC* proc, int pid) {
  if (ProcessAlive(proc, pid)) SetLatch(&proc->procLatch);
}

void AttachRegistry() {
  if (!Preloaded)
    ereport(
        ERROR,
        (errmsg("pgiceberg metadata workers require shared_preload_libraries"),
         errhint("Add pgiceberg to shared_preload_libraries and restart PostgreSQL.")));
  if (!Registry) InitializeSharedMemory();
}

void RegisterMetadataWorkers() {
  DefineCustomIntVariable("pgiceberg.metadata_request_timeout_ms",
                          "Timeout for a metadata request, including worker startup.",
                          nullptr, &RequestTimeoutMs, 5000, 1, 600000, PGC_USERSET,
                          GUC_UNIT_MS, nullptr, nullptr, nullptr);
  if (!process_shared_preload_libraries_in_progress) return;
  DefineCustomIntVariable("pgiceberg.metadata_max_workers",
                          "Maximum number of database metadata workers.", nullptr,
                          &MaxWorkers, 8, 1, kWorkerSlots, PGC_POSTMASTER, 0, nullptr,
                          nullptr, nullptr);
  DefineCustomStringVariable(
      "pgiceberg.metadata_databases",
      "Databases served by metadata workers (comma-separated identifiers).", nullptr,
      &DatabaseNames, "", PGC_SIGHUP, GUC_LIST_INPUT, CheckDatabases, nullptr, nullptr);
  DefineCustomIntVariable(
      "pgiceberg.metadata_max_connections",
      "Maximum number of concurrent metadata requests across the instance.", nullptr,
      &MaxConnections, 64, 1, kConnectionSlots, PGC_POSTMASTER, 0, nullptr, nullptr,
      nullptr);
  Preloaded = true;
  PreviousRequestHook = shmem_request_hook;
  shmem_request_hook = RequestSharedMemory;
  PreviousStartupHook = shmem_startup_hook;
  shmem_startup_hook = InitializeSharedMemory;
  PreviousObjectHook = object_access_hook;
  object_access_hook = ObjectAccess;
  RegisterXactCallback(ExtensionTransaction, nullptr);

  BackgroundWorker worker{};
  worker.bgw_flags = BGWORKER_SHMEM_ACCESS | BGWORKER_BACKEND_DATABASE_CONNECTION;
  worker.bgw_start_time = BgWorkerStart_RecoveryFinished;
  worker.bgw_restart_time = 1;
  snprintf(worker.bgw_library_name, BGW_MAXLEN, "pgiceberg");
  snprintf(worker.bgw_function_name, BGW_MAXLEN, "pgiceberg_metadata_launcher_main");
  snprintf(worker.bgw_name, BGW_MAXLEN, "pgiceberg metadata launcher");
  snprintf(worker.bgw_type, BGW_MAXLEN, "pgiceberg metadata launcher");
  RegisterBackgroundWorker(&worker);
}
}  // namespace pgiceberg::metadata
