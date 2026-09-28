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

#pragma once

extern "C" {
#include "postgres.h"
#include "storage/dsm.h"
#include "storage/lwlock.h"
#include "storage/proc.h"
#include "utils/timestamp.h"
}

namespace pgiceberg::metadata {

constexpr int kWorkerSlots = 32;
constexpr int kConnectionSlots = 128;
constexpr Size kQueueSize = Size{16} * 1024;
constexpr Size kSegmentSize = 2 * kQueueSize;
constexpr const char* kLockName = "pgiceberg metadata";
constexpr const char* kSharedName = "pgiceberg metadata registry";

enum class WorkerState : uint8 { kStopped, kStarting, kReady, kAbsent };

struct WorkerSlot {
  Oid database;
  Oid extension;
  uint64 extension_changes;
  bool desired;
  WorkerState state;
  uint64 generation;
  PGPROC* proc;
  int pid;
  TimestampTz retry_at;
};

struct ConnectionSlot {
  bool active;
  int worker_slot;
  uint64 worker_generation;
  uint64 request;
  PGPROC* proc;
  int pid;
  dsm_handle handle;
};

struct SharedState {
  PGPROC* launcher;
  int launcher_pid;
  uint64 next_generation;
  uint64 next_request;
  WorkerSlot workers[kWorkerSlots];
  ConnectionSlot connections[kConnectionSlots];
};

extern SharedState* Registry;
extern LWLock* RegistryLock;
extern int MaxWorkers;
extern int MaxConnections;
extern int RequestTimeoutMs;
extern char* DatabaseNames;

// Callers hold RegistryLock while using a registered process identity.
void WakeProcess(PGPROC* proc, int pid);
bool ProcessAlive(PGPROC* proc, int pid);
void AttachRegistry();

}  // namespace pgiceberg::metadata
