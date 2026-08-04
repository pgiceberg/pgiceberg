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

#include "engine/scan_state.h"

#include <memory>
#include <optional>
#include <string>
#include <unordered_set>
#include <vector>

#include <arrow/array.h>
#include <arrow/record_batch.h>
#include <iceberg/table.h>

#include "common/catalog.h"
#include "common/datum_convert.h"
#include "common/pg_interrupt.h"
#include "common/status.h"
#include "engine/iceberg_scan.h"
#include "engine/modify_state.h"

extern "C" {
#include "postgres.h"
#include "access/htup_details.h"
#include "executor/executor.h"
#include "utils/elog.h"
#include "utils/errcodes.h"
#include "utils/memutils.h"
#include "utils/rel.h"
}

namespace pgiceberg::engine {
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
    PGICEBERG_RETURN_NOT_OK(pgiceberg::CheckForInterrupts());
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

std::vector<std::string> ProjectedColumnNames(TupleDesc desc,
                                              const std::vector<int>& attnums) {
  std::vector<std::string> names;
  names.reserve(attnums.size());
  for (const auto attnum : attnums) {
    if (attnum <= 0 || attnum > desc->natts) {
      continue;
    }
    Form_pg_attribute attr = TupleDescAttr(desc, attnum - 1);
    if (!attr->attisdropped) {
      names.emplace_back(NameStr(attr->attname));
    }
  }
  return names;
}

std::unordered_set<int> ProjectedAttributeSet(const std::vector<int>& attnums) {
  return std::unordered_set<int>(attnums.begin(), attnums.end());
}

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

Result<ScanState*> BeginScan(Relation relation, const Options& options,
                             const std::vector<int>& projected_attnums) {
  auto state = std::make_unique<ScanState>();
  PGICEBERG_ASSIGN_OR_RETURN(auto catalog_options, ToCatalogOptions(options));
  PGICEBERG_ASSIGN_OR_RETURN(
      state->table,
      pgiceberg::LoadIcebergTable(catalog_options, RelationGetRelationName(relation)));
  // Historical snapshot reads pin iceberg-cpp UseSnapshot and must not overlay
  // uncommitted PostgreSQL transaction state. Current-snapshot scans still honor
  // pending DML so the FDW keeps PostgreSQL read-your-writes semantics.
  if (!options.snapshot_id.has_value()) {
    PGICEBERG_ASSIGN_OR_RETURN(state->table,
                               ReadTableForCurrentTransaction(options, state->table));
  }
  TupleDesc desc = RelationGetDescr(relation);
  state->cursor = std::make_unique<IcebergScanCursor>(
      state->table, ProjectedColumnNames(desc, projected_attnums), options.snapshot_id);
  PGICEBERG_RETURN_NOT_OK(state->cursor->Init());

  auto projected = ProjectedAttributeSet(projected_attnums);
  state->columns.resize(desc->natts);
  for (int i = 0; i < desc->natts; i++) {
    Form_pg_attribute attr = TupleDescAttr(desc, i);
    if (attr->attisdropped || !projected.contains(i + 1)) {
      continue;
    }
    const int column_index =
        state->cursor->arrow_schema()->GetFieldIndex(NameStr(attr->attname));
    if (column_index < 0) {
      return std::unexpected(
          MakeError(ERRCODE_FDW_ERROR, std::string("column \"") + NameStr(attr->attname) +
                                           "\" does not exist in Iceberg table"));
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

}  // namespace pgiceberg::engine
