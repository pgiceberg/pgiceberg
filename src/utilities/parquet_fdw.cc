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

#include <cstring>
#include <memory>
#include <numeric>
#include <string>
#include <utility>
#include <vector>

#include <arrow/api.h>
#include <arrow/io/file.h>
#include <parquet/arrow/reader.h>
#include <parquet/metadata.h>
#include <parquet/schema.h>

#include "common/datum_convert.h"
#include "common/fdw_path.h"
#include "common/pg_error.h"
#include "common/pg_interrupt.h"
#include "common/status.h"
#include "common/type_mapping.h"
#include "fdw/scan_projection.h"
#include "utilities/file_fdw_options.h"

extern "C" {
#include "postgres.h"
#include "commands/defrem.h"
#include "executor/executor.h"
#include "fmgr.h"
#include "foreign/fdwapi.h"
#include "foreign/foreign.h"
#include "lib/stringinfo.h"
#include "nodes/makefuncs.h"
#include "nodes/parsenodes.h"
#include "optimizer/pathnode.h"
#include "optimizer/planmain.h"
#include "optimizer/restrictinfo.h"
#include "storage/fd.h"
#include "utils/builtins.h"
#include "utils/errcodes.h"
#include "utils/rel.h"
}

namespace {

struct ColumnState {
  int field_index = -1;
};

pgiceberg::Result<std::shared_ptr<arrow::Schema>> ReadParquetSchema(
    const std::string& filename) {
  PGICEBERG_ASSIGN_OR_RETURN(
      auto input, pgiceberg::FromArrowResult(arrow::io::ReadableFile::Open(filename),
                                             "open Parquet file"));
  PGICEBERG_ASSIGN_OR_RETURN(
      auto reader, pgiceberg::FromArrowResult(
                       parquet::arrow::OpenFile(input, arrow::default_memory_pool()),
                       "open Parquet reader"));
  std::shared_ptr<arrow::Schema> schema;
  PGICEBERG_RETURN_NOT_OK(
      pgiceberg::FromArrowStatus(reader->GetSchema(&schema), "read Parquet schema"));
  return schema;
}

class ParquetCursor final {
 public:
  ParquetCursor(std::string filename, TupleDesc desc, std::vector<int> projected_attnums)
      : filename_(std::move(filename)),
        desc_(desc),
        projected_attnums_(std::move(projected_attnums)) {}

  pgiceberg::Status Init() { return Open(); }
  pgiceberg::Status ReScan() { return Open(); }

  pgiceberg::Result<TupleTableSlot*> Iterate(TupleTableSlot* slot) {
    ExecClearTuple(slot);
    PGICEBERG_ASSIGN_OR_RETURN(auto has_batch, LoadNextBatch());
    if (!has_batch) {
      return slot;
    }

    for (int i = 0; i < desc_->natts; i++) {
      slot->tts_isnull[i] = true;
      slot->tts_values[i] = static_cast<Datum>(0);
      const auto& column = columns_[i];
      if (column.field_index < 0) {
        continue;
      }

      auto array = batch_->column(column.field_index);
      Form_pg_attribute attr = TupleDescAttr(desc_, i);
      PGICEBERG_ASSIGN_OR_RETURN(
          slot->tts_values[i],
          pgiceberg::ConvertValue(*array, row_, attr->atttypid, slot->tts_isnull[i]));
    }

    row_++;
    ExecStoreVirtualTuple(slot);
    return slot;
  }

 private:
  pgiceberg::Status Open() {
    PGICEBERG_ASSIGN_OR_RETURN(
        auto input, pgiceberg::FromArrowResult(arrow::io::ReadableFile::Open(filename_),
                                               "open Parquet file"));
    PGICEBERG_ASSIGN_OR_RETURN(
        reader_, pgiceberg::FromArrowResult(
                     parquet::arrow::OpenFile(input, arrow::default_memory_pool()),
                     "open Parquet reader"));

    std::shared_ptr<arrow::Schema> schema;
    PGICEBERG_RETURN_NOT_OK(
        pgiceberg::FromArrowStatus(reader_->GetSchema(&schema), "read Parquet schema"));

    // GetRecordBatchReader() takes Parquet leaf-column indices, not top-level
    // field indices, and the two only coincide for flat schemas.  Group the
    // leaves under their root field so top-level selections stay correct when
    // the file also contains nested columns.
    const auto* parquet_schema = reader_->parquet_reader()->metadata()->schema();
    const auto* root_group = parquet_schema->group_node();
    std::vector<std::vector<int>> field_leaves(root_group->field_count());
    for (int leaf = 0; leaf < parquet_schema->num_columns(); leaf++) {
      const int root_field = root_group->FieldIndex(*parquet_schema->GetColumnRoot(leaf));
      field_leaves[root_field].push_back(leaf);
    }

    columns_.clear();
    columns_.resize(desc_->natts);
    std::vector<int> column_indices;
    int selected_fields = 0;
    for (const auto attnum : projected_attnums_) {
      if (attnum <= 0 || attnum > desc_->natts) {
        continue;
      }
      const int i = attnum - 1;
      Form_pg_attribute attr = TupleDescAttr(desc_, i);
      if (attr->attisdropped) {
        continue;
      }
      const int field_index = schema->GetFieldIndex(NameStr(attr->attname));
      if (field_index < 0) {
        return std::unexpected(pgiceberg::MakeError(
            ERRCODE_FDW_ERROR, std::string("column \"") + NameStr(attr->attname) +
                                   "\" does not exist in Parquet file"));
      }
      // The reader emits one batch column per selected root field, ordered by
      // first appearance in column_indices, so count fields rather than leaves.
      columns_[i] = ColumnState{.field_index = selected_fields++};
      column_indices.insert(column_indices.end(), field_leaves[field_index].begin(),
                            field_leaves[field_index].end());
    }

    std::vector<int> row_groups(reader_->num_row_groups());
    std::iota(row_groups.begin(), row_groups.end(), 0);
    PGICEBERG_ASSIGN_OR_RETURN(
        batch_reader_, pgiceberg::FromArrowResult(
                           reader_->GetRecordBatchReader(row_groups, column_indices),
                           "create Parquet batch reader"));

    batch_.reset();
    row_ = 0;
    return pgiceberg::Ok();
  }

  pgiceberg::Result<bool> LoadNextBatch() {
    while (batch_ == nullptr || row_ >= batch_->num_rows()) {
      PGICEBERG_RETURN_NOT_OK(pgiceberg::CheckForInterrupts());
      std::shared_ptr<arrow::RecordBatch> batch;
      PGICEBERG_RETURN_NOT_OK(pgiceberg::FromArrowStatus(batch_reader_->ReadNext(&batch),
                                                         "read Parquet record batch"));
      if (batch == nullptr) {
        batch_.reset();
        return false;
      }
      batch_ = std::move(batch);
      row_ = 0;
    }
    return true;
  }

  std::string filename_;
  TupleDesc desc_;
  std::vector<int> projected_attnums_;
  std::unique_ptr<parquet::arrow::FileReader> reader_;
  std::unique_ptr<arrow::RecordBatchReader> batch_reader_;
  std::shared_ptr<arrow::RecordBatch> batch_;
  std::vector<ColumnState> columns_;
  std::int64_t row_ = 0;
};

struct ParquetScanState {
  MemoryContextCallback* cleanup_callback = nullptr;
  std::unique_ptr<ParquetCursor> cursor;
};

void DeleteParquetScanState(void* arg) { delete static_cast<ParquetScanState*>(arg); }

void RegisterMemoryContextCleanup(ParquetScanState* state) {
  auto* callback = static_cast<MemoryContextCallback*>(
      MemoryContextAlloc(CurrentMemoryContext, sizeof(MemoryContextCallback)));
  callback->func = DeleteParquetScanState;
  callback->arg = state;
  state->cleanup_callback = callback;
  MemoryContextRegisterResetCallback(CurrentMemoryContext, callback);
}

void DetachMemoryContextCleanup(ParquetScanState* state) {
  if (state != nullptr && state->cleanup_callback != nullptr) {
    state->cleanup_callback->arg = nullptr;
    state->cleanup_callback = nullptr;
  }
}

pgiceberg::Status GetForeignRelSizeImpl(RelOptInfo* baserel, Oid relation_oid) {
  baserel->rows = 1000;
  auto options = pgiceberg::FileFdwOptionsForForeignTable(relation_oid);
  PGICEBERG_RETURN_NOT_OK(
      pgiceberg::ValidateFileFdwOptions(options, "pgiceberg_parquet"));
  PGICEBERG_ASSIGN_OR_RETURN(
      auto input,
      pgiceberg::FromArrowResult(arrow::io::ReadableFile::Open(options.filename),
                                 "open Parquet file"));
  PGICEBERG_ASSIGN_OR_RETURN(
      auto reader, pgiceberg::FromArrowResult(
                       parquet::arrow::OpenFile(input, arrow::default_memory_pool()),
                       "open Parquet reader"));
  baserel->rows = static_cast<double>(reader->parquet_reader()->metadata()->num_rows());
  return pgiceberg::Ok();
}

void GetForeignRelSize(PlannerInfo*, RelOptInfo* baserel, Oid relation_oid) {
  pgiceberg::PgStatusGuard(
      [&]() { return GetForeignRelSizeImpl(baserel, relation_oid); });
}

void GetForeignPaths(PlannerInfo* root, RelOptInfo* baserel, Oid) {
  add_path(baserel, reinterpret_cast<Path*>(
                        pgiceberg::CreateSimpleForeignScanPath(root, baserel)));
}

ForeignScan* GetForeignPlan(PlannerInfo*, RelOptInfo* baserel, Oid, ForeignPath*,
                            List* tlist, List* scan_clauses, Plan* outer_plan) {
  scan_clauses = extract_actual_clauses(scan_clauses, false);
  auto* fdw_private =
      pgiceberg::fdw::BuildFdwScanProjectionPrivate(baserel, tlist, scan_clauses);
  return make_foreignscan(tlist, scan_clauses, baserel->relid, NIL, fdw_private, NIL, NIL,
                          outer_plan);
}

int IsForeignRelUpdatable(Relation) { return 0; }

pgiceberg::Status BeginForeignScanImpl(ForeignScanState* node, int eflags) {
  if (eflags & EXEC_FLAG_EXPLAIN_ONLY) {
    return pgiceberg::Ok();
  }

  Relation relation = node->ss.ss_currentRelation;
  auto options = pgiceberg::FileFdwOptionsForForeignTable(RelationGetRelid(relation));
  PGICEBERG_RETURN_NOT_OK(
      pgiceberg::ValidateFileFdwOptions(options, "pgiceberg_parquet"));

  auto state = std::make_unique<ParquetScanState>();
  auto* plan = castNode(ForeignScan, node->ss.ps.plan);
  auto cursor =
      std::make_unique<ParquetCursor>(options.filename, RelationGetDescr(relation),
                                      pgiceberg::fdw::FdwScanProjectionFromPlan(plan));
  PGICEBERG_RETURN_NOT_OK(cursor->Init());
  state->cursor = std::move(cursor);

  RegisterMemoryContextCleanup(state.get());
  node->fdw_state = state.release();
  return pgiceberg::Ok();
}

void BeginForeignScan(ForeignScanState* node, int eflags) {
  pgiceberg::PgStatusGuard([&]() { return BeginForeignScanImpl(node, eflags); });
}

pgiceberg::Result<TupleTableSlot*> IterateForeignScanImpl(ForeignScanState* node) {
  auto* state = static_cast<ParquetScanState*>(node->fdw_state);
  if (state == nullptr) {
    ExecClearTuple(node->ss.ss_ScanTupleSlot);
    return node->ss.ss_ScanTupleSlot;
  }
  return state->cursor->Iterate(node->ss.ss_ScanTupleSlot);
}

TupleTableSlot* IterateForeignScan(ForeignScanState* node) {
  return pgiceberg::PgResultGuard([&]() { return IterateForeignScanImpl(node); });
}

void ReScanForeignScan(ForeignScanState* node) {
  auto* state = static_cast<ParquetScanState*>(node->fdw_state);
  if (state == nullptr) {
    return;
  }
  pgiceberg::PgStatusGuard([&]() { return state->cursor->ReScan(); });
}

void EndForeignScan(ForeignScanState* node) {
  auto* state = static_cast<ParquetScanState*>(node->fdw_state);
  DetachMemoryContextCleanup(state);
  delete state;
  node->fdw_state = nullptr;
}

pgiceberg::Status AppendImportColumns(StringInfo sql, const std::string& filename) {
  PGICEBERG_ASSIGN_OR_RETURN(auto schema, ReadParquetSchema(filename));
  for (int i = 0; i < schema->num_fields(); i++) {
    const auto& field = schema->field(i);
    auto sql_type = pgiceberg::ArrowTypeToSql(*field->type());
    if (sql_type.empty()) {
      return std::unexpected(pgiceberg::MakeError(
          ERRCODE_FEATURE_NOT_SUPPORTED,
          "unsupported Parquet column type for import: " + field->type()->ToString()));
    }
    if (i > 0) {
      appendStringInfoString(sql, ", ");
    }
    appendStringInfo(sql, "%s %s", quote_identifier(field->name().c_str()),
                     sql_type.c_str());
  }
  return pgiceberg::Ok();
}

pgiceberg::Result<List*> ImportForeignSchemaImpl(ImportForeignSchemaStmt* stmt,
                                                 Oid server_oid) {
  List* commands = NIL;
  auto import_dir = pgiceberg::DirectoryForImport(server_oid, stmt->remote_schema);
  DIR* dir = AllocateDir(import_dir.c_str());
  if (dir == nullptr) {
    return std::unexpected(pgiceberg::MakeError(
        ERRCODE_IO_ERROR,
        std::string("failed to open directory \"") + import_dir + "\""));
  }

  struct dirent* entry = nullptr;
  while ((entry = ReadDir(dir, import_dir.c_str())) != nullptr) {
    std::string filename(entry->d_name);
    if (!pgiceberg::EndsWith(filename, ".parquet")) {
      continue;
    }

    auto table_name = pgiceberg::BasenameWithoutExtension(filename, ".parquet");
    if (!pgiceberg::ImportFilterMatches(stmt, table_name)) {
      continue;
    }

    std::string path = import_dir;
    path.append("/").append(filename);
    StringInfo sql = makeStringInfo();
    appendStringInfo(sql, "CREATE FOREIGN TABLE %s.%s (",
                     quote_identifier(stmt->local_schema),
                     quote_identifier(table_name.c_str()));
    auto status = AppendImportColumns(sql, path);
    if (!status) {
      FreeDir(dir);
      return std::unexpected(status.error());
    }
    appendStringInfo(sql, ") SERVER %s OPTIONS (filename %s)",
                     quote_identifier(stmt->server_name),
                     quote_literal_cstr(path.c_str()));
    commands = lappend(commands, sql->data);
  }

  FreeDir(dir);
  return commands;
}

List* ImportForeignSchema(ImportForeignSchemaStmt* stmt, Oid server_oid) {
  return pgiceberg::PgResultGuard(
      [&]() { return ImportForeignSchemaImpl(stmt, server_oid); });
}

pgiceberg::Result<Datum> ParquetFdwHandlerImpl() {
  FdwRoutine* routine = makeNode(FdwRoutine);
  routine->GetForeignRelSize = GetForeignRelSize;
  routine->GetForeignPaths = GetForeignPaths;
  routine->GetForeignPlan = GetForeignPlan;
  routine->BeginForeignScan = BeginForeignScan;
  routine->IterateForeignScan = IterateForeignScan;
  routine->ReScanForeignScan = ReScanForeignScan;
  routine->EndForeignScan = EndForeignScan;
  routine->IsForeignRelUpdatable = IsForeignRelUpdatable;
  routine->ImportForeignSchema = ImportForeignSchema;
  return PointerGetDatum(routine);
}

}  // namespace

extern "C" {
PG_FUNCTION_INFO_V1(pgiceberg_parquet_fdw_handler);
PG_FUNCTION_INFO_V1(pgiceberg_parquet_fdw_validator);

Datum pgiceberg_parquet_fdw_handler(PG_FUNCTION_ARGS) {
  return pgiceberg::PgResultGuard([]() { return ParquetFdwHandlerImpl(); });
}

Datum pgiceberg_parquet_fdw_validator(PG_FUNCTION_ARGS) {
  Datum raw_options = PG_GETARG_DATUM(0);
  Oid catalog = PG_GETARG_OID(1);
  pgiceberg::PgStatusGuard([=]() {
    return pgiceberg::ValidateFileFdwUtilityOptions(raw_options, catalog,
                                                    "pgiceberg_parquet");
  });
  PG_RETURN_VOID();
}

}  // extern "C"
