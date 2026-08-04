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

#include "engine/modify_state.h"
#include "logical/logical.h"
#include "tableam/tableam.h"

#include <string>

#include <arrow/io/interfaces.h>
#include <arrow/util/thread_pool.h>
#include <iceberg/arrow/arrow_register.h>
#include <iceberg/avro/avro_register.h>
#include <iceberg/parquet/parquet_register.h>

extern "C" {
#include "postgres.h"
#include "fmgr.h"
#include "utils/guc.h"
#include "utils/guc_tables.h"
}

namespace {

constexpr int kArrowThreadPoolMaxCapacity = 1024;

int ArrowCpuThreadPoolCapacity = 0;
int ArrowIoThreadPoolCapacity = 0;
int DefaultArrowCpuThreadPoolCapacity = 0;
int DefaultArrowIoThreadPoolCapacity = 0;

void EnsureIcebergRegistrations() {
  static const bool registered = [] {
    iceberg::arrow::RegisterAll();
    iceberg::parquet::RegisterAll();
    iceberg::avro::RegisterAll();
    return true;
  }();
  (void)registered;
}

void CaptureArrowThreadPoolDefaults() {
  static const bool captured = [] {
    DefaultArrowCpuThreadPoolCapacity = arrow::GetCpuThreadPoolCapacity();
    DefaultArrowIoThreadPoolCapacity = arrow::io::GetIOThreadPoolCapacity();
    return true;
  }();
  (void)captured;
}

void WarnArrowThreadPoolCapacityError(const char* pool_name,
                                      const arrow::Status& status) {
  std::string message = status.ToString();
  ereport(WARNING, (errmsg("could not set Arrow %s thread pool capacity: %s", pool_name,
                           message.c_str())));
}

void ApplyArrowCpuThreadPoolCapacity(int configured_capacity) {
  CaptureArrowThreadPoolDefaults();
  int capacity =
      configured_capacity > 0 ? configured_capacity : DefaultArrowCpuThreadPoolCapacity;
  auto status = arrow::SetCpuThreadPoolCapacity(capacity);
  if (!status.ok()) {
    WarnArrowThreadPoolCapacityError("CPU", status);
  }
}

void ApplyArrowIoThreadPoolCapacity(int configured_capacity) {
  CaptureArrowThreadPoolDefaults();
  int capacity =
      configured_capacity > 0 ? configured_capacity : DefaultArrowIoThreadPoolCapacity;
  auto status = arrow::io::SetIOThreadPoolCapacity(capacity);
  if (!status.ok()) {
    WarnArrowThreadPoolCapacityError("I/O", status);
  }
}

void AssignArrowCpuThreadPoolCapacity(int new_value, void*) {
  ApplyArrowCpuThreadPoolCapacity(new_value);
}

void AssignArrowIoThreadPoolCapacity(int new_value, void*) {
  ApplyArrowIoThreadPoolCapacity(new_value);
}

void RegisterArrowThreadPoolGucs() {
  CaptureArrowThreadPoolDefaults();
  DefineCustomIntVariable(
      "pgiceberg.arrow_cpu_threads",
      "Sets the Arrow CPU thread pool capacity for this PostgreSQL backend.",
      "Set to 0 to keep Arrow's default CPU thread pool capacity.",
      &ArrowCpuThreadPoolCapacity, 0, 0, kArrowThreadPoolMaxCapacity, PGC_USERSET, 0,
      nullptr, AssignArrowCpuThreadPoolCapacity, nullptr);
  DefineCustomIntVariable(
      "pgiceberg.arrow_io_threads",
      "Sets the Arrow I/O thread pool capacity for this PostgreSQL backend.",
      "Set to 0 to keep Arrow's default I/O thread pool capacity.",
      &ArrowIoThreadPoolCapacity, 0, 0, kArrowThreadPoolMaxCapacity, PGC_USERSET, 0,
      nullptr, AssignArrowIoThreadPoolCapacity, nullptr);
  ApplyArrowCpuThreadPoolCapacity(ArrowCpuThreadPoolCapacity);
  ApplyArrowIoThreadPoolCapacity(ArrowIoThreadPoolCapacity);
}

bool PgIcebergGucsRegistered() {
  const struct config_generic* record =
      find_option("pgiceberg.arrow_cpu_threads", false, true, WARNING);
  return record != nullptr && (record->flags & GUC_CUSTOM_PLACEHOLDER) == 0;
}

}  // namespace

extern "C" {
PG_MODULE_MAGIC;

// Transaction callbacks must be registered at module load time, not from an
// FDW executor callback, because a backend can run multiple statements in one
// PostgreSQL transaction and all of them have to share the same pending
// Iceberg commit state.
void _PG_init(void) {
  static bool initialized = false;
  if (initialized || PgIcebergGucsRegistered()) {
    initialized = true;
    return;
  }
  initialized = true;

  EnsureIcebergRegistrations();
  RegisterArrowThreadPoolGucs();
  pgiceberg::engine::RegisterTransactionCallbacks();
  pgiceberg::tableam::RegisterTableAmHooks();
  pgiceberg::logical::RegisterLogicalWorker();
}
}
