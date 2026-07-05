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
#include <cstdint>
#include <cstring>
#include <memory>
#include <optional>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

#include <avro/DataFile.hh>
#include <avro/Generic.hh>

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
#include "utils/date.h"
#include "utils/errcodes.h"
#include "utils/timestamp.h"
#include "utils/rel.h"
}

namespace {

constexpr std::int64_t kPostgresUnixEpochOffsetMicros = 946684800000000LL;

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

std::optional<avro::NodePtr> NullableAvroValueNode(const avro::NodePtr& node) {
  if (node->type() != avro::AVRO_UNION) {
    return node;
  }

  std::optional<avro::NodePtr> value_node;
  for (std::size_t i = 0; i < node->leaves(); i++) {
    const auto& leaf = node->leafAt(i);
    if (leaf->type() == avro::AVRO_NULL) {
      continue;
    }
    if (value_node.has_value()) {
      return std::nullopt;
    }
    value_node = leaf;
  }
  return value_node;
}

std::string AvroTypeSql(const avro::NodePtr& original_node) {
  auto node = NullableAvroValueNode(original_node);
  if (!node.has_value()) {
    return {};
  }

  const auto logical_type = (*node)->logicalType().type();
  switch ((*node)->type()) {
    case avro::AVRO_BOOL:
      return "boolean";
    case avro::AVRO_INT:
      return logical_type == avro::LogicalType::DATE ? "date" : "integer";
    case avro::AVRO_LONG:
      switch (logical_type) {
        case avro::LogicalType::TIMESTAMP_MILLIS:
        case avro::LogicalType::TIMESTAMP_MICROS:
        case avro::LogicalType::TIMESTAMP_NANOS:
          return "timestamptz";
        case avro::LogicalType::LOCAL_TIMESTAMP_MILLIS:
        case avro::LogicalType::LOCAL_TIMESTAMP_MICROS:
        case avro::LogicalType::LOCAL_TIMESTAMP_NANOS:
          return "timestamp";
        default:
          return "bigint";
      }
    case avro::AVRO_FLOAT:
      return "real";
    case avro::AVRO_DOUBLE:
      return "double precision";
    case avro::AVRO_STRING:
    case avro::AVRO_ENUM:
      return "text";
    default:
      return {};
  }
}

pgiceberg::Result<avro::ValidSchema> ReadAvroSchema(const std::string& filename) {
  try {
    avro::DataFileReader<avro::GenericDatum> reader(filename.c_str());
    return reader.dataSchema();
  } catch (const std::exception& error) {
    return std::unexpected(pgiceberg::MakeError(
        ERRCODE_FDW_ERROR, std::string("open Avro file: ") + error.what()));
  }
}

Datum AvroTimestampDatum(std::int64_t value, avro::LogicalType::Type logical_type,
                         Oid pg_type) {
  switch (logical_type) {
    case avro::LogicalType::TIMESTAMP_MILLIS:
    case avro::LogicalType::LOCAL_TIMESTAMP_MILLIS:
      value *= 1000LL;
      break;
    case avro::LogicalType::TIMESTAMP_MICROS:
    case avro::LogicalType::LOCAL_TIMESTAMP_MICROS:
      break;
    case avro::LogicalType::TIMESTAMP_NANOS:
    case avro::LogicalType::LOCAL_TIMESTAMP_NANOS:
      value /= 1000LL;
      break;
    default:
      break;
  }
  value -= kPostgresUnixEpochOffsetMicros;
  return pg_type == TIMESTAMPOID ? TimestampGetDatum(value) : TimestampTzGetDatum(value);
}

pgiceberg::Result<Datum> ConvertAvroValue(const avro::GenericDatum& datum, Oid pg_type,
                                          bool& is_null) {
  if (datum.type() == avro::AVRO_NULL) {
    is_null = true;
    return static_cast<Datum>(0);
  }
  is_null = false;

  switch (pg_type) {
    case INT2OID:
      if (datum.type() == avro::AVRO_INT) {
        return Int16GetDatum(static_cast<int16>(datum.value<int32_t>()));
      }
      if (datum.type() == avro::AVRO_LONG) {
        return Int16GetDatum(static_cast<int16>(datum.value<int64_t>()));
      }
      break;
    case INT4OID:
      if (datum.type() == avro::AVRO_INT) return Int32GetDatum(datum.value<int32_t>());
      if (datum.type() == avro::AVRO_LONG) {
        return Int32GetDatum(static_cast<int32>(datum.value<int64_t>()));
      }
      break;
    case INT8OID:
      if (datum.type() == avro::AVRO_INT) return Int64GetDatum(datum.value<int32_t>());
      if (datum.type() == avro::AVRO_LONG) return Int64GetDatum(datum.value<int64_t>());
      break;
    case FLOAT4OID:
      if (datum.type() == avro::AVRO_FLOAT) return Float4GetDatum(datum.value<float>());
      if (datum.type() == avro::AVRO_DOUBLE) {
        return Float4GetDatum(static_cast<float4>(datum.value<double>()));
      }
      break;
    case FLOAT8OID:
      if (datum.type() == avro::AVRO_FLOAT) return Float8GetDatum(datum.value<float>());
      if (datum.type() == avro::AVRO_DOUBLE) return Float8GetDatum(datum.value<double>());
      break;
    case TEXTOID:
    case VARCHAROID:
    case BPCHAROID:
      if (datum.type() == avro::AVRO_STRING) {
        return CStringGetTextDatum(datum.value<std::string>().c_str());
      }
      if (datum.type() == avro::AVRO_ENUM) {
        return CStringGetTextDatum(datum.value<avro::GenericEnum>().symbol().c_str());
      }
      break;
    case BOOLOID:
      if (datum.type() == avro::AVRO_BOOL) return BoolGetDatum(datum.value<bool>());
      break;
    case DATEOID:
      if (datum.type() == avro::AVRO_INT) {
        return DateADTGetDatum(datum.value<int32_t>() - POSTGRES_EPOCH_JDATE +
                               UNIX_EPOCH_JDATE);
      }
      break;
    case TIMESTAMPOID:
    case TIMESTAMPTZOID:
      if (datum.type() == avro::AVRO_LONG) {
        return AvroTimestampDatum(datum.value<int64_t>(), datum.logicalType().type(),
                                  pg_type);
      }
      break;
    default:
      break;
  }

  return std::unexpected(
      pgiceberg::MakeError(ERRCODE_FEATURE_NOT_SUPPORTED,
                           "unsupported pgiceberg column conversion from Avro type " +
                               avro::toString(datum.type())));
}

class AvroCursor final {
 public:
  AvroCursor(std::string filename, TupleDesc desc)
      : filename_(std::move(filename)), desc_(desc) {}

  pgiceberg::Status Init() { return Open(); }
  pgiceberg::Status ReScan() { return Open(); }

  pgiceberg::Result<TupleTableSlot*> Iterate(TupleTableSlot* slot) {
    ExecClearTuple(slot);
    avro::GenericDatum datum(root_);
    if (!reader_->read(datum)) {
      return slot;
    }
    if (datum.type() != avro::AVRO_RECORD) {
      return std::unexpected(pgiceberg::MakeError(
          ERRCODE_FDW_ERROR, "Avro file root schema must be a record"));
    }

    const auto& record = datum.value<avro::GenericRecord>();
    for (int i = 0; i < desc_->natts; i++) {
      slot->tts_isnull[i] = true;
      slot->tts_values[i] = static_cast<Datum>(0);
      if (!columns_[i].has_value()) {
        continue;
      }

      Form_pg_attribute attr = TupleDescAttr(desc_, i);
      PGICEBERG_ASSIGN_OR_RETURN(
          slot->tts_values[i], ConvertAvroValue(record.fieldAt(columns_[i]->field_index),
                                                attr->atttypid, slot->tts_isnull[i]));
    }

    ExecStoreVirtualTuple(slot);
    return slot;
  }

 private:
  pgiceberg::Status Open() {
    try {
      reader_ =
          std::make_unique<avro::DataFileReader<avro::GenericDatum>>(filename_.c_str());
    } catch (const std::exception& error) {
      return std::unexpected(pgiceberg::MakeError(
          ERRCODE_FDW_ERROR, std::string("open Avro file: ") + error.what()));
    }

    root_ = reader_->dataSchema().root();
    if (root_->type() != avro::AVRO_RECORD) {
      return std::unexpected(pgiceberg::MakeError(
          ERRCODE_FDW_ERROR, "Avro file root schema must be a record"));
    }

    columns_.clear();
    columns_.resize(desc_->natts);
    for (int i = 0; i < desc_->natts; i++) {
      Form_pg_attribute attr = TupleDescAttr(desc_, i);
      if (attr->attisdropped) {
        continue;
      }

      std::size_t field_index = 0;
      if (!root_->nameIndex(NameStr(attr->attname), field_index)) {
        return std::unexpected(pgiceberg::MakeError(
            ERRCODE_FDW_ERROR, std::string("column \"") + NameStr(attr->attname) +
                                   "\" does not exist in Avro file"));
      }
      columns_[i] = ColumnState{.field_index = static_cast<int>(field_index)};
    }
    return pgiceberg::Ok();
  }

  std::string filename_;
  TupleDesc desc_;
  std::unique_ptr<avro::DataFileReader<avro::GenericDatum>> reader_;
  avro::NodePtr root_;
  std::vector<std::optional<ColumnState>> columns_;
};

struct AvroScanState {
  MemoryContextCallback* cleanup_callback = nullptr;
  std::unique_ptr<AvroCursor> cursor;
};

void DeleteAvroScanState(void* arg) { delete static_cast<AvroScanState*>(arg); }

void RegisterMemoryContextCleanup(AvroScanState* state) {
  auto* callback = static_cast<MemoryContextCallback*>(
      MemoryContextAlloc(CurrentMemoryContext, sizeof(MemoryContextCallback)));
  callback->func = DeleteAvroScanState;
  callback->arg = state;
  state->cleanup_callback = callback;
  MemoryContextRegisterResetCallback(CurrentMemoryContext, callback);
}

void DetachMemoryContextCleanup(AvroScanState* state) {
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
  PGICEBERG_RETURN_NOT_OK(ValidateFileOptions(options, "pgiceberg_avro"));

  auto state = std::make_unique<AvroScanState>();
  auto cursor =
      std::make_unique<AvroCursor>(options.filename, RelationGetDescr(relation));
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
  auto* state = static_cast<AvroScanState*>(node->fdw_state);
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
  auto* state = static_cast<AvroScanState*>(node->fdw_state);
  if (state == nullptr) {
    return;
  }
  pgiceberg::PgStatusGuard([&]() { return state->cursor->ReScan(); });
}

void EndForeignScan(ForeignScanState* node) {
  auto* state = static_cast<AvroScanState*>(node->fdw_state);
  DetachMemoryContextCleanup(state);
  delete state;
  node->fdw_state = nullptr;
}

pgiceberg::Status AppendImportColumns(StringInfo sql, const std::string& filename) {
  PGICEBERG_ASSIGN_OR_RETURN(auto schema, ReadAvroSchema(filename));
  auto root = schema.root();
  if (root->type() != avro::AVRO_RECORD) {
    return std::unexpected(pgiceberg::MakeError(
        ERRCODE_FDW_ERROR, "Avro file root schema must be a record"));
  }

  for (std::size_t i = 0; i < root->leaves(); i++) {
    const auto& field = root->leafAt(i);
    auto sql_type = AvroTypeSql(field);
    if (sql_type.empty()) {
      return std::unexpected(
          pgiceberg::MakeError(ERRCODE_FEATURE_NOT_SUPPORTED,
                               std::string("unsupported Avro column type for import: ") +
                                   avro::toString(field->type())));
    }
    if (i > 0) {
      appendStringInfoString(sql, ", ");
    }
    appendStringInfo(sql, "%s %s", quote_identifier(root->nameAt(i).c_str()),
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
    if (!EndsWith(filename, ".avro")) {
      continue;
    }

    auto table_name = BasenameWithoutExtension(filename, ".avro");
    if (!ImportFilterMatches(stmt, table_name)) {
      continue;
    }

    std::string path = import_dir + "/" + filename;
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
PG_FUNCTION_INFO_V1(pgiceberg_avro_fdw_handler);
PG_FUNCTION_INFO_V1(pgiceberg_avro_fdw_validator);

Datum pgiceberg_avro_fdw_handler(PG_FUNCTION_ARGS) {
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

Datum pgiceberg_avro_fdw_validator(PG_FUNCTION_ARGS) {
  ValidateUtilityOptions(fcinfo, "pgiceberg_avro");
  PG_RETURN_VOID();
}

}  // extern "C"
