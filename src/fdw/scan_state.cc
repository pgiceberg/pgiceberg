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

#include "fdw/scan_state.h"

#include <memory>
#include <optional>
#include <vector>

#include <arrow/array.h>
#include <arrow/record_batch.h>
#include <iceberg/table.h>

#include "common/catalog.h"
#include "common/datum_convert.h"
#include "common/status.h"
#include "fdw/iceberg_scan.h"
#include "fdw/modify_state.h"

extern "C" {
#include "postgres.h"
#include "access/htup_details.h"
#include "executor/executor.h"
#include "utils/elog.h"
#include "utils/errcodes.h"
#include "utils/memutils.h"
#include "utils/rel.h"
}

namespace pgiceberg::fdw {
namespace {

Result<bool> LoadNextBatch(ScanState* state);

struct ColumnState {
  int batch_column_index = -1;
};

}  // namespace

struct ScanState {
  MemoryContextCallback* cleanup_callback = nullptr;
  std::shared_ptr<iceberg::Table> table;
  std::unique_ptr<IcebergScanCursor> cursor;
  std::shared_ptr<arrow::RecordBatch> batch;
  std::vector<std::optional<ColumnState>> columns;
  std::int64_t row = 0;
};

namespace {

Result<bool> LoadNextBatch(ScanState* state) {
  while (state->batch == nullptr || state->row >= state->batch->num_rows()) {
    std::shared_ptr<arrow::RecordBatch> batch;
    PGICEBERG_ASSIGN_OR_RETURN(auto has_batch, state->cursor->NextBatch(&batch));
    if (!has_batch) {
      state->batch.reset();
      return false;
    }
    state->batch = std::move(batch);
    state->row = 0;
  }
  return true;
}

}  // namespace

void DeleteScanState(void* arg) { delete static_cast<ScanState*>(arg); }

void RegisterMemoryContextCleanup(ScanState* state) {
  auto* callback = static_cast<MemoryContextCallback*>(
      MemoryContextAlloc(CurrentMemoryContext, sizeof(MemoryContextCallback)));
  callback->func = DeleteScanState;
  callback->arg = state;
  state->cleanup_callback = callback;
  MemoryContextRegisterResetCallback(CurrentMemoryContext, callback);
}

void DetachMemoryContextCleanup(ScanState* state) {
  if (state != nullptr && state->cleanup_callback != nullptr) {
    state->cleanup_callback->arg = nullptr;
    state->cleanup_callback = nullptr;
  }
}

Result<ScanState*> BeginScan(Relation relation, const Options& options) {
  auto state = std::make_unique<ScanState>();
  PGICEBERG_ASSIGN_OR_RETURN(auto catalog_options, ToCatalogOptions(options));
  PGICEBERG_ASSIGN_OR_RETURN(
      state->table,
      pgiceberg::LoadIcebergTable(catalog_options, RelationGetRelationName(relation)));
  // A scan can follow DML in the same PostgreSQL transaction.  Use the pending
  // transaction view when one exists so the FDW does not expose a weaker
  // read-your-writes rule than PostgreSQL users expect.
  PGICEBERG_ASSIGN_OR_RETURN(state->table,
                             ReadTableForCurrentTransaction(options, state->table));
  state->cursor = std::make_unique<IcebergScanCursor>(state->table);
  PGICEBERG_RETURN_NOT_OK(state->cursor->Init());

  TupleDesc desc = RelationGetDescr(relation);
  state->columns.resize(desc->natts);
  for (int i = 0; i < desc->natts; i++) {
    Form_pg_attribute attr = TupleDescAttr(desc, i);
    if (attr->attisdropped) {
      continue;
    }
    const int column_index =
        state->cursor->arrow_schema()->GetFieldIndex(NameStr(attr->attname));
    if (column_index < 0) {
      return std::unexpected(
          MakeError(ERRCODE_FDW_ERROR, std::string("column \"") + NameStr(attr->attname) +
                                           "\" does not exist in Parquet file"));
    }
    state->columns[i] = ColumnState{.batch_column_index = column_index};
  }

  RegisterMemoryContextCleanup(state.get());
  return state.release();
}

Result<TupleTableSlot*> IterateScan(ScanState* state, TupleTableSlot* slot) {
  ExecClearTuple(slot);
  if (state == nullptr) {
    return slot;
  }
  PGICEBERG_ASSIGN_OR_RETURN(auto has_batch, LoadNextBatch(state));
  if (!has_batch) {
    return slot;
  }

  TupleDesc desc = slot->tts_tupleDescriptor;
  for (int i = 0; i < desc->natts; i++) {
    slot->tts_isnull[i] = true;
    slot->tts_values[i] = static_cast<Datum>(0);
    auto& column = state->columns[i];
    if (!column.has_value()) {
      continue;
    }

    auto array = state->batch->column(column->batch_column_index);
    if (array == nullptr) {
      continue;
    }

    Form_pg_attribute attr = TupleDescAttr(desc, i);
    PGICEBERG_ASSIGN_OR_RETURN(
        slot->tts_values[i],
        pgiceberg::ConvertValue(*array, state->row, attr->atttypid, slot->tts_isnull[i]));
  }

  state->row++;
  ExecStoreVirtualTuple(slot);
  return slot;
}

void ReScan(ScanState* state) {
  if (state == nullptr) {
    return;
  }
  state->cursor->Reset();
  state->batch.reset();
  state->row = 0;
}

void EndScan(ScanState* state) {
  DetachMemoryContextCleanup(state);
  delete state;
}

}  // namespace pgiceberg::fdw
