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

#include <algorithm>
#include <cstring>
#include <memory>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

#include <arrow/api.h>
#include <arrow/io/file.h>
#include <parquet/arrow/reader.h>

#include "common/datum_convert.h"
#include "common/pg_error.h"
#include "common/status.h"

extern "C" {
#include "postgres.h"
#include "access/reloptions.h"
#include "catalog/pg_foreign_server_d.h"
#include "catalog/pg_foreign_table_d.h"
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

struct FileOptions {
  std::string dirname;
  std::string filename;
};

struct ColumnState {
  int field_index = -1;
};

bool EndsWith(std::string_view value, std::string_view suffix) {
  return value.size() >= suffix.size() &&
         value.substr(value.size() - suffix.size()) == suffix;
}

std::string BasenameWithoutExtension(std::string_view name, std::string_view extension) {
  const auto dot = name.size() - extension.size();
  return std::string(name.substr(0, dot));
}

void ApplyOption(FileOptions& options, DefElem* def) {
  if (std::strcmp(def->defname, "filename") == 0) {
    options.filename = defGetString(def);
  } else if (std::strcmp(def->defname, "dirname") == 0) {
    options.dirname = defGetString(def);
  }
}

void ApplyOptions(FileOptions& options, List* option_list) {
  ListCell* cell = nullptr;
  foreach (cell, option_list) {
    ApplyOption(options, static_cast<DefElem*>(lfirst(cell)));
  }
}

FileOptions OptionsForForeignTable(Oid relation_oid) {
  FileOptions options;
  ForeignTable* table = GetForeignTable(relation_oid);
  ForeignServer* server = GetForeignServer(table->serverid);
  ApplyOptions(options, server->options);
  ApplyOptions(options, table->options);
  return options;
}

FileOptions OptionsForServer(Oid server_oid) {
  FileOptions options;
  ForeignServer* server = GetForeignServer(server_oid);
  ApplyOptions(options, server->options);
  return options;
}

pgiceberg::Status ValidateFileOptions(const FileOptions& options,
                                      std::string_view fdw_name) {
  if (options.filename.empty()) {
    return std::unexpected(
        pgiceberg::MakeError(ERRCODE_FDW_DYNAMIC_PARAMETER_VALUE_NEEDED,
                             std::string(fdw_name) + " option \"filename\" is required"));
  }
  return pgiceberg::Ok();
}

void ValidateUtilityOptions(PG_FUNCTION_ARGS, const char* fdw_name) {
  pgiceberg::PgStatusGuard([&]() -> pgiceberg::Status {
    List* options = untransformRelOptions(PG_GETARG_DATUM(0));
    Oid catalog = PG_GETARG_OID(1);
    FileOptions parsed_options;

    ListCell* cell = nullptr;
    foreach (cell, options) {
      auto* def = static_cast<DefElem*>(lfirst(cell));
      const bool valid_option = (catalog == ForeignTableRelationId &&
                                 std::strcmp(def->defname, "filename") == 0) ||
                                (catalog == ForeignServerRelationId &&
                                 std::strcmp(def->defname, "dirname") == 0);
      if (!valid_option) {
        return std::unexpected(pgiceberg::MakeError(
            ERRCODE_FDW_INVALID_OPTION_NAME,
            std::string("invalid ") + fdw_name + " option \"" + def->defname + "\"",
            "Valid options are: dirname on servers, filename on foreign tables."));
      }
      ApplyOption(parsed_options, def);
    }

    if (catalog == ForeignTableRelationId) {
      PGICEBERG_RETURN_NOT_OK(ValidateFileOptions(parsed_options, fdw_name));
    }
    return pgiceberg::Ok();
  });
}

bool ImportFilterMatches(ImportForeignSchemaStmt* stmt, const std::string& table_name) {
  if (stmt->list_type == FDW_IMPORT_SCHEMA_ALL) {
    return true;
  }

  bool listed = false;
  ListCell* cell = nullptr;
  foreach (cell, stmt->table_list) {
    auto* range = static_cast<RangeVar*>(lfirst(cell));
    if (table_name == range->relname) {
      listed = true;
      break;
    }
  }

  if (stmt->list_type == FDW_IMPORT_SCHEMA_LIMIT_TO) {
    return listed;
  }
  if (stmt->list_type == FDW_IMPORT_SCHEMA_EXCEPT) {
    return !listed;
  }
  return true;
}

std::string DirectoryForImport(Oid server_oid, const char* remote_schema) {
  auto options = OptionsForServer(server_oid);
  if (remote_schema == nullptr || std::strlen(remote_schema) == 0 ||
      options.dirname.empty() || remote_schema[0] == '/') {
    return remote_schema == nullptr ? std::string{} : std::string(remote_schema);
  }
  if (std::strcmp(remote_schema, ".") == 0) {
    return options.dirname;
  }
  return options.dirname + "/" + remote_schema;
}

std::string ArrowTypeSql(const arrow::DataType& type) {
  switch (type.id()) {
    case arrow::Type::BOOL:
      return "boolean";
    case arrow::Type::INT8:
    case arrow::Type::INT16:
    case arrow::Type::UINT8:
      return "smallint";
    case arrow::Type::INT32:
    case arrow::Type::UINT16:
      return "integer";
    case arrow::Type::INT64:
    case arrow::Type::UINT32:
      return "bigint";
    case arrow::Type::FLOAT:
      return "real";
    case arrow::Type::DOUBLE:
      return "double precision";
    case arrow::Type::STRING:
    case arrow::Type::LARGE_STRING:
      return "text";
    case arrow::Type::DATE32:
      return "date";
    case arrow::Type::TIMESTAMP: {
      const auto& timestamp = static_cast<const arrow::TimestampType&>(type);
      return timestamp.timezone().empty() ? "timestamp" : "timestamptz";
    }
    default:
      return {};
  }
}

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
  ParquetCursor(std::string filename, TupleDesc desc)
      : filename_(std::move(filename)), desc_(desc) {}

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
    PGICEBERG_ASSIGN_OR_RETURN(batch_reader_,
                               pgiceberg::FromArrowResult(reader_->GetRecordBatchReader(),
                                                          "create Parquet batch reader"));

    columns_.clear();
    columns_.resize(desc_->natts);
    for (int i = 0; i < desc_->natts; i++) {
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
      columns_[i] = ColumnState{.field_index = field_index};
    }

    batch_.reset();
    row_ = 0;
    return pgiceberg::Ok();
  }

  pgiceberg::Result<bool> LoadNextBatch() {
    while (batch_ == nullptr || row_ >= batch_->num_rows()) {
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

void GetForeignRelSize(PlannerInfo*, RelOptInfo* baserel, Oid) { baserel->rows = 1000; }

ForeignPath* CreateForeignScanPath(PlannerInfo* root, RelOptInfo* baserel) {
  const auto rows = baserel->rows;
  const auto total_cost = std::max(1.0, rows);
#if PG_VERSION_NUM >= 180000
  return create_foreignscan_path(root, baserel, nullptr, rows, 0, 0, total_cost, NIL,
                                 nullptr, nullptr, NIL, NIL);
#elif PG_VERSION_NUM >= 170000
  return create_foreignscan_path(root, baserel, nullptr, rows, 0, total_cost, NIL,
                                 nullptr, nullptr, NIL, NIL);
#else
  return create_foreignscan_path(root, baserel, nullptr, rows, 0, total_cost, NIL,
                                 nullptr, nullptr, NIL);
#endif
}

void GetForeignPaths(PlannerInfo* root, RelOptInfo* baserel, Oid) {
  add_path(baserel, reinterpret_cast<Path*>(CreateForeignScanPath(root, baserel)));
}

ForeignScan* GetForeignPlan(PlannerInfo*, RelOptInfo* baserel, Oid, ForeignPath*,
                            List* tlist, List* scan_clauses, Plan* outer_plan) {
  scan_clauses = extract_actual_clauses(scan_clauses, false);
  return make_foreignscan(tlist, scan_clauses, baserel->relid, NIL, NIL, NIL, NIL,
                          outer_plan);
}

int IsForeignRelUpdatable(Relation) { return 0; }

pgiceberg::Status BeginForeignScanImpl(ForeignScanState* node, int eflags) {
  if (eflags & EXEC_FLAG_EXPLAIN_ONLY) {
    return pgiceberg::Ok();
  }

  Relation relation = node->ss.ss_currentRelation;
  auto options = OptionsForForeignTable(RelationGetRelid(relation));
  PGICEBERG_RETURN_NOT_OK(ValidateFileOptions(options, "pgiceberg_parquet"));

  auto state = std::make_unique<ParquetScanState>();
  auto cursor =
      std::make_unique<ParquetCursor>(options.filename, RelationGetDescr(relation));
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
    auto sql_type = ArrowTypeSql(*field->type());
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
  auto import_dir = DirectoryForImport(server_oid, stmt->remote_schema);
  DIR* dir = AllocateDir(import_dir.c_str());
  if (dir == nullptr) {
    return std::unexpected(pgiceberg::MakeError(
        ERRCODE_IO_ERROR,
        std::string("failed to open directory \"") + import_dir + "\""));
  }

  struct dirent* entry = nullptr;
  while ((entry = ReadDir(dir, import_dir.c_str())) != nullptr) {
    std::string filename(entry->d_name);
    if (!EndsWith(filename, ".parquet")) {
      continue;
    }

    auto table_name = BasenameWithoutExtension(filename, ".parquet");
    if (!ImportFilterMatches(stmt, table_name)) {
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

}  // namespace

extern "C" {
PG_FUNCTION_INFO_V1(pgiceberg_parquet_fdw_handler);
PG_FUNCTION_INFO_V1(pgiceberg_parquet_fdw_validator);

Datum pgiceberg_parquet_fdw_handler(PG_FUNCTION_ARGS) {
  return pgiceberg::PgGuard([]() -> Datum {
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
    PG_RETURN_POINTER(routine);
  });
}

Datum pgiceberg_parquet_fdw_validator(PG_FUNCTION_ARGS) {
  ValidateUtilityOptions(fcinfo, "pgiceberg_parquet");
  PG_RETURN_VOID();
}

}  // extern "C"
