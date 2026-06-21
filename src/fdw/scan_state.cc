#include "fdw/scan_state.h"

#include <memory>
#include <optional>
#include <vector>

#include <arrow/array.h>
#include <arrow/array/util.h>
#include <arrow/c/bridge.h>
#include <arrow/table.h>
#include <iceberg/data/file_scan_task_reader.h>
#include <iceberg/schema_internal.h>
#include <iceberg/table.h>
#include <iceberg/table_scan.h>

#include "common/catalog.h"
#include "common/datum_convert.h"
#include "common/error.h"

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

struct ColumnState {
  std::shared_ptr<arrow::ChunkedArray> column;
  int chunk_index = 0;
  std::int64_t chunk_start = 0;
};

std::shared_ptr<arrow::Array> ArrayForRow(ColumnState& column, std::int64_t row,
                                          std::int64_t& offset) {
  while (column.chunk_index < column.column->num_chunks()) {
    auto chunk = column.column->chunk(column.chunk_index);
    const auto chunk_end = column.chunk_start + chunk->length();
    if (row < chunk_end) {
      offset = row - column.chunk_start;
      return chunk;
    }
    column.chunk_start = chunk_end;
    column.chunk_index++;
  }
  offset = 0;
  return nullptr;
}

}  // namespace

struct ScanState {
  MemoryContextCallback* cleanup_callback = nullptr;
  std::shared_ptr<arrow::Table> table;
  std::vector<std::optional<ColumnState>> columns;
  std::int64_t row = 0;
};

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

std::shared_ptr<arrow::Table> ReadIcebergTable(iceberg::Table& table) {
  auto schema = table.schema();
  if (!schema) {
    pgiceberg::ThrowIcebergError(schema.error());
  }

  std::vector<std::shared_ptr<iceberg::Schema>> schemas;
  auto all_schemas = table.schemas();
  if (all_schemas) {
    for (const auto& [_, candidate] : all_schemas->get()) {
      schemas.push_back(candidate);
    }
  }

  auto scan_builder = table.NewScan();
  if (!scan_builder) {
    pgiceberg::ThrowIcebergError(scan_builder.error());
  }
  auto scan = (*scan_builder)->Build();
  if (!scan) {
    pgiceberg::ThrowIcebergError(scan.error());
  }
  auto tasks = (*scan)->PlanFiles();
  if (!tasks) {
    pgiceberg::ThrowIcebergError(tasks.error());
  }

  auto reader = iceberg::FileScanTaskReader::Make(iceberg::FileScanTaskReader::Options{
      .io = table.io(),
      .table_schema = *schema,
      .schemas = std::move(schemas),
      .projected_schema = *schema,
  });
  if (!reader) {
    pgiceberg::ThrowIcebergError(reader.error());
  }

  std::vector<std::shared_ptr<arrow::Table>> tables;
  tables.reserve(tasks->size());
  for (const auto& task : *tasks) {
    auto stream = (*reader)->Open(*task);
    if (!stream) {
      pgiceberg::ThrowIcebergError(stream.error());
    }
    auto reader_ptr = pgiceberg::CheckArrowResult(
        arrow::ImportRecordBatchReader(&*stream), "import record batch reader");
    tables.push_back(pgiceberg::CheckArrowResult(
        arrow::Table::FromRecordBatchReader(reader_ptr.get()), "read Arrow table"));
  }

  if (tables.empty()) {
    ArrowSchema c_schema;
    auto status = iceberg::ToArrowSchema(**schema, &c_schema);
    if (!status) {
      pgiceberg::ThrowIcebergError(status.error());
    }
    auto arrow_schema = pgiceberg::CheckArrowResult(arrow::ImportSchema(&c_schema),
                                                    "import Arrow schema");
    std::vector<std::shared_ptr<arrow::ChunkedArray>> columns;
    columns.reserve(arrow_schema->num_fields());
    for (const auto& field : arrow_schema->fields()) {
      auto array = pgiceberg::CheckArrowResult(arrow::MakeArrayOfNull(field->type(), 0),
                                               "make null Arrow array");
      columns.push_back(std::make_shared<arrow::ChunkedArray>(array));
    }
    return arrow::Table::Make(arrow_schema, std::move(columns));
  }
  if (tables.size() == 1) {
    return tables.front();
  }
  return pgiceberg::CheckArrowResult(arrow::ConcatenateTables(tables),
                                     "concatenate Arrow tables");
}

ScanState* BeginScan(Relation relation, const Options& options) {
  auto state = std::make_unique<ScanState>();
  auto table = pgiceberg::LoadIcebergTable(ToCatalogOptions(options),
                                           RelationGetRelationName(relation));
  state->table = ReadIcebergTable(*table);

  TupleDesc desc = RelationGetDescr(relation);
  state->columns.resize(desc->natts);
  for (int i = 0; i < desc->natts; i++) {
    Form_pg_attribute attr = TupleDescAttr(desc, i);
    if (attr->attisdropped) {
      continue;
    }
    const int column_index =
        state->table->schema()->GetFieldIndex(NameStr(attr->attname));
    if (column_index < 0) {
      ereport(ERROR, (errcode(ERRCODE_FDW_ERROR),
                      errmsg("column \"%s\" does not exist in Parquet file",
                             NameStr(attr->attname))));
    }
    state->columns[i] = ColumnState{.column = state->table->column(column_index)};
  }

  RegisterMemoryContextCleanup(state.get());
  return state.release();
}

TupleTableSlot* IterateScan(ScanState* state, TupleTableSlot* slot) {
  ExecClearTuple(slot);
  if (state == nullptr || state->row >= state->table->num_rows()) {
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

    std::int64_t offset = 0;
    auto array = ArrayForRow(*column, state->row, offset);
    if (array == nullptr) {
      continue;
    }

    Form_pg_attribute attr = TupleDescAttr(desc, i);
    slot->tts_values[i] =
        pgiceberg::ConvertValue(*array, offset, attr->atttypid, slot->tts_isnull[i]);
  }

  state->row++;
  ExecStoreVirtualTuple(slot);
  return slot;
}

void ReScan(ScanState* state) {
  if (state == nullptr) {
    return;
  }
  state->row = 0;
  for (auto& column : state->columns) {
    if (!column.has_value()) {
      continue;
    }
    column->chunk_index = 0;
    column->chunk_start = 0;
  }
}

void EndScan(ScanState* state) {
  DetachMemoryContextCleanup(state);
  delete state;
}

}  // namespace pgiceberg::fdw
