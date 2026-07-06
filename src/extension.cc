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

#include "fdw/modify_state.h"
#include "tableam/tableam.h"

#include <iceberg/arrow/arrow_register.h>
#include <iceberg/avro/avro_register.h>
#include <iceberg/parquet/parquet_register.h>

namespace {

void EnsureIcebergRegistrations() {
  static const bool registered = [] {
    iceberg::arrow::RegisterAll();
    iceberg::parquet::RegisterAll();
    iceberg::avro::RegisterAll();
    return true;
  }();
  (void)registered;
}

}  // namespace

extern "C" {
#include "postgres.h"
#include "fmgr.h"

PG_MODULE_MAGIC;

// Transaction callbacks must be registered at module load time, not from an
// FDW executor callback, because a backend can run multiple statements in one
// PostgreSQL transaction and all of them have to share the same pending
// Iceberg commit state.
void _PG_init(void) {
  EnsureIcebergRegistrations();
  pgiceberg::fdw::RegisterTransactionCallbacks();
  pgiceberg::tableam::RegisterTableAmHooks();
}
}
