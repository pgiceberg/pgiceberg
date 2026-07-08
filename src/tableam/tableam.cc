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

#include "tableam/tableam.h"

#include <algorithm>
#include <cstring>
#include <filesystem>
#include <memory>
#include <optional>
#include <sstream>
#include <string>
#include <vector>

#include <iceberg/arrow/arrow_io_internal.h>
#include <iceberg/catalog/sql/sql_catalog.h>
#include <iceberg/partition_spec.h>
#include <iceberg/schema.h>
#include <iceberg/schema_field.h>
#include <iceberg/sort_order.h>
#include <iceberg/table.h>
#include <iceberg/table_properties.h>
#include <iceberg/table_identifier.h>
#include <iceberg/transaction.h>
#include <iceberg/update/update_properties.h>

#include "common/catalog.h"
#include "common/pg_error.h"
#include "common/pg_relation.h"
#include "common/status.h"
#include "common/type_mapping.h"
#include "fdw/modify_state.h"
#include "fdw/options.h"
#include "fdw/scan_state.h"

extern "C" {
#include "postgres.h"
#include "access/multixact.h"
#include "access/table.h"
#include "access/relscan.h"
#include "access/tableam.h"
#include "access/xact.h"
#include "catalog/namespace.h"
#include "catalog/pg_am_d.h"
#include "catalog/pg_type_d.h"
#include "commands/defrem.h"
#include "executor/executor.h"
#include "executor/tuptable.h"
#include "fmgr.h"
#include "miscadmin.h"
#include "nodes/makefuncs.h"
#include "nodes/parsenodes.h"
#include "nodes/pg_list.h"
#include "tcop/utility.h"
#include "utils/builtins.h"
#include "utils/errcodes.h"
#include "utils/guc.h"
#include "utils/lsyscache.h"
#include "utils/rel.h"
#include "utils/snapmgr.h"
#include "utils/varlena.h"
#include "executor/spi.h"
}

namespace pgiceberg::tableam {
namespace {

constexpr const char* kIcebergTableAmName = "iceberg";

char* DefaultCatalog = nullptr;
char* DefaultNamespace = nullptr;
int DefaultFormatVersion = 2;
ProcessUtility_hook_type PreviousProcessUtilityHook = nullptr;

struct SpiConnection {
  bool connected = false;

  Status Connect() {
    if (SPI_connect() != SPI_OK_CONNECT) {
      return std::unexpected(
          MakeError(ERRCODE_INTERNAL_ERROR, "could not connect to PostgreSQL SPI"));
    }
    connected = true;
    return Ok();
  }

  ~SpiConnection() {
    if (connected) {
      SPI_finish();
    }
  }
};

struct ActiveSnapshotGuard {
  bool pushed = false;

  ActiveSnapshotGuard() {
    PushActiveSnapshot(GetTransactionSnapshot());
    pushed = true;
  }

  ~ActiveSnapshotGuard() {
    if (pushed) {
      PopActiveSnapshot();
    }
  }
};

struct IcebergScanDesc {
  TableScanDescData base;
  pgiceberg::fdw::ScanState* state = nullptr;
};

struct Binding {
  Oid relid = InvalidOid;
  std::string catalog;
  std::string name_space;
  std::string table_name;
};

struct NativeCreateOptions {
  std::string catalog;
  std::string name_space = "default";
  std::string table_name;
  int format_version = 2;
};

enum class PendingCatalogChangeKind {
  kCreate,
  kDrop,
};

struct PendingCatalogChange {
  PendingCatalogChangeKind kind = PendingCatalogChangeKind::kCreate;
  SubTransactionId subtransaction_id = InvalidSubTransactionId;
  Binding binding;
  int format_version = 2;
};

std::vector<PendingCatalogChange>& PendingCatalogChanges() {
  static auto* changes = new std::vector<PendingCatalogChange>();
  return *changes;
}

std::vector<std::string> SplitNamespace(const std::string& name_space) {
  std::vector<std::string> levels;
  std::stringstream input(name_space);
  std::string level;
  while (std::getline(input, level, '.')) {
    if (!level.empty()) {
      levels.push_back(level);
    }
  }
  if (levels.empty()) {
    levels.push_back("default");
  }
  return levels;
}

Status ValidateFormatVersion(int format_version) {
  if (format_version == 2 || format_version == 3) {
    return Ok();
  }
  return std::unexpected(
      MakeError(ERRCODE_INVALID_PARAMETER_VALUE,
                "unsupported Iceberg format version " + std::to_string(format_version),
                "Iceberg format version 1 is not supported; valid values are 2 and 3."));
}

Result<std::shared_ptr<iceberg::sql::SqlCatalog>> CreateSqlCatalog(
    const CatalogOptions& options) {
  if (options.catalog_uri.empty()) {
    return std::unexpected(
        MakeError(ERRCODE_FDW_INVALID_OPTION_NAME, "pgiceberg catalog_uri is required"));
  }
  if (options.warehouse.empty()) {
    return std::unexpected(
        MakeError(ERRCODE_FDW_INVALID_OPTION_NAME, "pgiceberg warehouse is required"));
  }

  std::shared_ptr<iceberg::FileIO> file_io(
      iceberg::arrow::ArrowFileSystemFileIO::MakeLocalFileIO().release());
  iceberg::sql::SqlCatalogConfig config{
      .name = options.catalog_name,
      .uri = options.catalog_uri,
      .warehouse_location = options.warehouse,
      .max_connections = 1,
  };

  if (options.catalog_type == "sqlite") {
    return FromIcebergResult(iceberg::sql::SqlCatalog::MakeSqliteCatalog(config, file_io),
                             "create SQLite catalog");
  }
  if (options.catalog_type == "sql") {
    return FromIcebergResult(
        iceberg::sql::SqlCatalog::MakePostgreSqlCatalog(config, file_io),
        "create PostgreSQL catalog");
  }
  return std::unexpected(MakeError(
      ERRCODE_FEATURE_NOT_SUPPORTED,
      "pgiceberg table access method currently supports only catalog_type 'sql' "
      "or 'sqlite'"));
}

Result<std::shared_ptr<iceberg::Schema>> SchemaFromRelation(Relation relation) {
  TupleDesc desc = RelationGetDescr(relation);
  std::vector<iceberg::SchemaField> fields;
  fields.reserve(desc->natts);
  int field_id = 1;
  for (int i = 0; i < desc->natts; i++) {
    Form_pg_attribute attr = TupleDescAttr(desc, i);
    if (attr->attisdropped) {
      continue;
    }
    PGICEBERG_ASSIGN_OR_RETURN(
        auto type, PostgresTypeToIcebergType(attr->atttypid, attr->atttypmod));
    if (attr->attnotnull) {
      fields.push_back(
          iceberg::SchemaField::MakeRequired(field_id, NameStr(attr->attname), type));
    } else {
      fields.push_back(
          iceberg::SchemaField::MakeOptional(field_id, NameStr(attr->attname), type));
    }
    field_id++;
  }
  if (fields.empty()) {
    return std::unexpected(
        MakeError(ERRCODE_INVALID_TABLE_DEFINITION,
                  "pgiceberg iceberg tables require at least one column"));
  }
  return std::make_shared<iceberg::Schema>(std::move(fields), 1);
}

std::vector<int> AllTableColumns(Relation relation) {
  TupleDesc desc = RelationGetDescr(relation);
  std::vector<int> attnums;
  attnums.reserve(desc->natts);
  for (int i = 0; i < desc->natts; i++) {
    Form_pg_attribute attr = TupleDescAttr(desc, i);
    if (!attr->attisdropped) {
      attnums.push_back(i + 1);
    }
  }
  return attnums;
}

iceberg::TableIdentifier TableIdentifierForBinding(const Binding& binding) {
  return iceberg::TableIdentifier{
      .ns = iceberg::Namespace{.levels = SplitNamespace(binding.name_space)},
      .name = binding.table_name};
}

Status CreateIcebergCatalogTable(const Binding& binding, Relation relation,
                                 int format_version) {
  PGICEBERG_ASSIGN_OR_RETURN(auto options, LoadCatalogOptions(binding.catalog));
  PGICEBERG_ASSIGN_OR_RETURN(auto catalog, CreateSqlCatalog(options));
  const auto ident = TableIdentifierForBinding(binding);
  const auto& ns = ident.ns;
  PGICEBERG_ASSIGN_OR_RETURN(
      auto ns_exists,
      FromIcebergResult(catalog->NamespaceExists(ns), "check Iceberg namespace"));
  if (!ns_exists) {
    PGICEBERG_RETURN_NOT_OK(
        FromIcebergStatus(catalog->CreateNamespace(ns, {}), "create Iceberg namespace"));
  }

  PGICEBERG_ASSIGN_OR_RETURN(
      auto table_exists,
      FromIcebergResult(catalog->TableExists(ident), "check Iceberg table"));
  if (table_exists) {
    return std::unexpected(
        MakeError(ERRCODE_DUPLICATE_TABLE, "Iceberg table \"" + binding.name_space + "." +
                                               binding.table_name + "\" already exists"));
  }

  PGICEBERG_ASSIGN_OR_RETURN(auto schema, SchemaFromRelation(relation));
  const auto table_location =
      std::filesystem::path(options.warehouse) / binding.name_space / binding.table_name;
  std::filesystem::create_directories(table_location / "metadata");
  PGICEBERG_ASSIGN_OR_RETURN(
      auto staged_table,
      FromIcebergResult(catalog->StageCreateTable(
                            ident, schema, iceberg::PartitionSpec::Unpartitioned(),
                            iceberg::SortOrder::Unsorted(), table_location.string(),
                            {{"write.parquet.compression-codec", "uncompressed"}}),
                        "stage create Iceberg table"));
  PGICEBERG_ASSIGN_OR_RETURN(auto properties_update,
                             FromIcebergResult(staged_table->NewUpdateProperties(),
                                               "create table properties update"));
  PGICEBERG_RETURN_NOT_OK(
      FromIcebergStatus(properties_update
                            ->Set(iceberg::TableProperties::kFormatVersion.key(),
                                  std::to_string(format_version))
                            .Commit(),
                        "set table format version"));
  PGICEBERG_ASSIGN_OR_RETURN(
      auto table, FromIcebergResult(staged_table->Commit(), "create Iceberg table"));
  (void)table;
  return Ok();
}

Status DropIcebergCatalogTable(const Binding& binding) {
  PGICEBERG_ASSIGN_OR_RETURN(auto options, LoadCatalogOptions(binding.catalog));
  PGICEBERG_ASSIGN_OR_RETURN(auto catalog, CreateSqlCatalog(options));
  const auto ident = TableIdentifierForBinding(binding);
  PGICEBERG_ASSIGN_OR_RETURN(
      auto table_exists,
      FromIcebergResult(catalog->TableExists(ident), "check Iceberg table"));
  if (!table_exists) {
    return Ok();
  }
  return FromIcebergStatus(catalog->DropTable(ident, false), "drop Iceberg table");
}

Status InsertBinding(const Binding& binding) {
  SpiConnection spi;
  PGICEBERG_RETURN_NOT_OK(spi.Connect());
  const char* sql =
      "INSERT INTO pgiceberg.table_bindings "
      "(relid, catalog, namespace, table_name, created_at, updated_at) "
      "VALUES ($1, $2, $3, $4, now(), now())";
  Oid argtypes[] = {OIDOID, TEXTOID, TEXTOID, TEXTOID};
  Datum values[] = {
      ObjectIdGetDatum(binding.relid),
      CStringGetTextDatum(binding.catalog.c_str()),
      CStringGetTextDatum(binding.name_space.c_str()),
      CStringGetTextDatum(binding.table_name.c_str()),
  };
  const int result = SPI_execute_with_args(sql, 4, argtypes, values, nullptr, false, 1);
  if (result != SPI_OK_INSERT) {
    return std::unexpected(
        MakeError(ERRCODE_INTERNAL_ERROR, "could not insert pgiceberg table binding"));
  }
  return Ok();
}

Result<std::optional<Binding>> LoadBindingIfExists(Oid relid) {
  SpiConnection spi;
  PGICEBERG_RETURN_NOT_OK(spi.Connect());
  const char* sql =
      "SELECT catalog, namespace, table_name "
      "FROM pgiceberg.table_bindings WHERE relid = $1";
  Oid argtypes[] = {OIDOID};
  Datum values[] = {ObjectIdGetDatum(relid)};
  const int result = SPI_execute_with_args(sql, 1, argtypes, values, nullptr, true, 1);
  if (result != SPI_OK_SELECT) {
    return std::unexpected(
        MakeError(ERRCODE_INTERNAL_ERROR, "could not read pgiceberg table binding"));
  }
  if (SPI_processed == 0) {
    return std::nullopt;
  }

  HeapTuple tuple = SPI_tuptable->vals[0];
  TupleDesc desc = SPI_tuptable->tupdesc;
  auto text_column = [&](int column_number) -> Result<std::string> {
    bool is_null = false;
    Datum value = SPI_getbinval(tuple, desc, column_number, &is_null);
    if (is_null) {
      return std::unexpected(MakeError(ERRCODE_NULL_VALUE_NOT_ALLOWED,
                                       "pgiceberg table binding contains NULL"));
    }
    return std::string(TextDatumGetCString(value));
  };

  Binding binding{.relid = relid};
  PGICEBERG_ASSIGN_OR_RETURN(binding.catalog, text_column(1));
  PGICEBERG_ASSIGN_OR_RETURN(binding.name_space, text_column(2));
  PGICEBERG_ASSIGN_OR_RETURN(binding.table_name, text_column(3));
  return binding;
}

Status DeleteBindings(const std::vector<Oid>& relids) {
  if (relids.empty()) {
    return Ok();
  }
  SpiConnection spi;
  PGICEBERG_RETURN_NOT_OK(spi.Connect());
  for (Oid relid : relids) {
    const char* sql = "DELETE FROM pgiceberg.table_bindings WHERE relid = $1";
    Oid argtypes[] = {OIDOID};
    Datum values[] = {ObjectIdGetDatum(relid)};
    const int result = SPI_execute_with_args(sql, 1, argtypes, values, nullptr, false, 0);
    if (result != SPI_OK_DELETE) {
      return std::unexpected(
          MakeError(ERRCODE_INTERNAL_ERROR, "could not delete pgiceberg table binding"));
    }
  }
  return Ok();
}

Result<Binding> LoadBinding(Relation relation) {
  const Oid relid = RelationGetRelid(relation);
  PGICEBERG_ASSIGN_OR_RETURN(auto binding, LoadBindingIfExists(relid));
  if (!binding.has_value()) {
    return std::unexpected(MakeError(
        ERRCODE_UNDEFINED_OBJECT,
        "pgiceberg table binding does not exist for relation " + std::to_string(relid)));
  }
  return *binding;
}

Result<fdw::Options> OptionsFromBinding(Relation relation) {
  PGICEBERG_ASSIGN_OR_RETURN(auto binding, LoadBinding(relation));
  fdw::Options options;
  options.catalog = binding.catalog;
  options.name_space = binding.name_space;
  options.table = binding.table_name;
  return options;
}

bool IsIcebergAccessMethodName(const char* access_method) {
  return access_method != nullptr && std::strcmp(access_method, kIcebergTableAmName) == 0;
}

bool IsIcebergCreateStmt(CreateStmt* stmt) {
  if (stmt->accessMethod != nullptr) {
    return IsIcebergAccessMethodName(stmt->accessMethod);
  }
  return IsIcebergAccessMethodName(default_table_access_method);
}

bool IsNativeCreateOption(const char* name) {
  return std::strcmp(name, "catalog") == 0 || std::strcmp(name, "namespace") == 0 ||
         std::strcmp(name, "table") == 0 || std::strcmp(name, "table_name") == 0 ||
         std::strcmp(name, "format_version") == 0;
}

Status CheckDuplicateOption(bool& seen, const char* name) {
  if (seen) {
    return std::unexpected(MakeError(
        ERRCODE_SYNTAX_ERROR,
        std::string("pgiceberg option \"") + name + "\" specified more than once"));
  }
  seen = true;
  return Ok();
}

Result<NativeCreateOptions> ParseAndRemoveNativeCreateOptions(CreateStmt* stmt) {
  NativeCreateOptions options{
      .catalog = DefaultCatalog == nullptr ? "" : DefaultCatalog,
      .name_space = DefaultNamespace == nullptr || DefaultNamespace[0] == '\0'
                        ? "default"
                        : DefaultNamespace,
      .format_version = DefaultFormatVersion,
  };

  bool seen_catalog = false;
  bool seen_namespace = false;
  bool seen_table = false;
  bool seen_format_version = false;
  List* remaining = NIL;

  // TODO: Replace this ProcessUtility preprocessing with real table AM
  // reloptions once PostgreSQL exposes reloption validation for table access
  // methods.  Until then, remove pgiceberg-private options before core
  // reloptions validation sees them.
  ListCell* cell = nullptr;
  foreach (cell, stmt->options) {
    auto* def = static_cast<DefElem*>(lfirst(cell));
    if (!IsNativeCreateOption(def->defname)) {
      remaining = lappend(remaining, def);
      continue;
    }

    if (std::strcmp(def->defname, "catalog") == 0) {
      PGICEBERG_RETURN_NOT_OK(CheckDuplicateOption(seen_catalog, def->defname));
      options.catalog = defGetString(def);
    } else if (std::strcmp(def->defname, "namespace") == 0) {
      PGICEBERG_RETURN_NOT_OK(CheckDuplicateOption(seen_namespace, def->defname));
      options.name_space = defGetString(def);
    } else if (std::strcmp(def->defname, "table") == 0 ||
               std::strcmp(def->defname, "table_name") == 0) {
      PGICEBERG_RETURN_NOT_OK(CheckDuplicateOption(seen_table, def->defname));
      options.table_name = defGetString(def);
    } else if (std::strcmp(def->defname, "format_version") == 0) {
      PGICEBERG_RETURN_NOT_OK(CheckDuplicateOption(seen_format_version, def->defname));
      options.format_version = defGetInt32(def);
    }
  }

  stmt->options = remaining;
  return options;
}

bool IsIcebergCreateTableAsStmt(CreateTableAsStmt* stmt) {
  if (stmt->objtype != OBJECT_TABLE || stmt->into == nullptr) {
    return false;
  }
  if (stmt->into->accessMethod != nullptr) {
    return IsIcebergAccessMethodName(stmt->into->accessMethod);
  }
  return IsIcebergAccessMethodName(default_table_access_method);
}

bool RelationUsesIcebergTableAm(Oid relid) {
  Oid iceberg_am = get_table_am_oid(kIcebergTableAmName, true);
  return OidIsValid(iceberg_am) && get_rel_relam(relid) == iceberg_am;
}

void ErrorUnsupported(const char* operation) {
  ereport(ERROR,
          (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
           errmsg("pgiceberg table access method does not support %s", operation)));
}

void ValidateCreateStmt(CreateStmt* stmt, const NativeCreateOptions& options) {
  if (stmt->if_not_exists) {
    ErrorUnsupported("CREATE TABLE IF NOT EXISTS");
  }
  if (stmt->relation->relpersistence != RELPERSISTENCE_PERMANENT) {
    ErrorUnsupported("temporary or unlogged tables");
  }
  if (stmt->inhRelations != NIL || stmt->partbound != nullptr ||
      stmt->partspec != nullptr) {
    ErrorUnsupported("inheritance or partitioning");
  }
  if (stmt->ofTypename != nullptr) {
    ErrorUnsupported("typed tables");
  }
  if (options.catalog.empty()) {
    ereport(
        ERROR,
        (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
         errmsg("pgiceberg catalog must be specified before creating an iceberg table"),
         errhint("Register a catalog with pgiceberg.add_catalog(...) and run SET "
                 "pgiceberg.default_catalog = '<name>', or use WITH (catalog = "
                 "'<name>').")));
  }
  PgStatusGuard([&]() { return ValidateFormatVersion(options.format_version); });
}

Status QueueCreateTable(CreateStmt* stmt, const NativeCreateOptions& create_options) {
  Oid relid = RangeVarGetRelid(stmt->relation, NoLock, false);
  Relation relation = table_open(relid, AccessShareLock);
  RelationLockGuard relation_guard(relation, AccessShareLock);

  Binding binding{
      .relid = relid,
      .catalog = create_options.catalog,
      .name_space =
          create_options.name_space.empty() ? "default" : create_options.name_space,
      .table_name = create_options.table_name.empty() ? RelationGetRelationName(relation)
                                                      : create_options.table_name,
  };
  PendingCatalogChanges().push_back(PendingCatalogChange{
      .kind = PendingCatalogChangeKind::kCreate,
      .subtransaction_id = GetCurrentSubTransactionId(),
      .binding = std::move(binding),
      .format_version = create_options.format_version,
  });
  return Ok();
}

Status CommitCreateTable(const Binding& binding, int format_version) {
  Relation relation = table_open(binding.relid, AccessShareLock);
  RelationLockGuard relation_guard(relation, AccessShareLock);

  PGICEBERG_RETURN_NOT_OK(CreateIcebergCatalogTable(binding, relation, format_version));
  return InsertBinding(binding);
}

std::vector<Oid> DropRelationOids(DropStmt* stmt) {
  std::vector<Oid> relids;
  if (stmt->removeType != OBJECT_TABLE) {
    return relids;
  }
  ListCell* cell = nullptr;
  foreach (cell, stmt->objects) {
    auto* names = static_cast<List*>(lfirst(cell));
    RangeVar* relation = makeRangeVarFromNameList(names);
    Oid relid = RangeVarGetRelid(relation, NoLock, true);
    if (OidIsValid(relid)) {
      relids.push_back(relid);
    }
  }
  return relids;
}

std::vector<Binding> LoadExistingBindings(const std::vector<Oid>& relids) {
  std::vector<Binding> bindings;
  for (Oid relid : relids) {
    if (!RelationUsesIcebergTableAm(relid)) {
      continue;
    }
    // Tables created in the current transaction have a queued create instead
    // of a binding row (the row is only inserted at pre-commit), so a missing
    // binding is not an error here.
    auto binding = PgResultGuard([&]() { return LoadBindingIfExists(relid); });
    if (binding.has_value()) {
      bindings.push_back(std::move(*binding));
    }
  }
  return bindings;
}

void RemovePendingCreates(const std::vector<Oid>& relids) {
  auto& changes = PendingCatalogChanges();
  std::erase_if(changes, [&](const PendingCatalogChange& change) {
    return change.kind == PendingCatalogChangeKind::kCreate &&
           std::find(relids.begin(), relids.end(), change.binding.relid) != relids.end();
  });
}

void QueueDropTables(std::vector<Binding> bindings) {
  for (auto& binding : bindings) {
    PendingCatalogChanges().push_back(PendingCatalogChange{
        .kind = PendingCatalogChangeKind::kDrop,
        .subtransaction_id = GetCurrentSubTransactionId(),
        .binding = std::move(binding),
    });
  }
}

void ValidateTruncateStmt(TruncateStmt* stmt) {
  ListCell* cell = nullptr;
  foreach (cell, stmt->relations) {
    auto* relation = reinterpret_cast<RangeVar*>(lfirst(cell));
    Oid relid = RangeVarGetRelid(relation, NoLock, true);
    if (OidIsValid(relid) && RelationUsesIcebergTableAm(relid)) {
      ErrorUnsupported("TRUNCATE");
    }
  }
}

void ValidateAlterTableStmt(AlterTableStmt* stmt) {
  if (stmt->objtype != OBJECT_TABLE || stmt->relation == nullptr) {
    return;
  }
  Oid relid = RangeVarGetRelid(stmt->relation, NoLock, stmt->missing_ok);
  if (OidIsValid(relid) && RelationUsesIcebergTableAm(relid)) {
    ErrorUnsupported("ALTER TABLE");
  }
}

Status CommitPendingCatalogChanges() {
  ActiveSnapshotGuard snapshot;
  for (const auto& change : PendingCatalogChanges()) {
    switch (change.kind) {
      case PendingCatalogChangeKind::kCreate:
        PGICEBERG_RETURN_NOT_OK(CommitCreateTable(change.binding, change.format_version));
        break;
      case PendingCatalogChangeKind::kDrop:
        PGICEBERG_RETURN_NOT_OK(DropIcebergCatalogTable(change.binding));
        break;
    }
  }
  PendingCatalogChanges().clear();
  return Ok();
}

Status XactCallbackImpl(XactEvent event) {
  switch (event) {
    case XACT_EVENT_PRE_COMMIT:
      return CommitPendingCatalogChanges();
    case XACT_EVENT_PRE_PREPARE:
      if (!PendingCatalogChanges().empty()) {
        return std::unexpected(MakeError(
            ERRCODE_FEATURE_NOT_SUPPORTED,
            "pgiceberg table access method does not support prepared transactions"));
      }
      break;
    case XACT_EVENT_ABORT:
    case XACT_EVENT_PARALLEL_ABORT:
      PendingCatalogChanges().clear();
      break;
    default:
      break;
  }
  return Ok();
}

void XactCallback(XactEvent event, void*) {
  PgStatusGuard([&]() { return XactCallbackImpl(event); });
}

Status SubXactCallbackImpl(SubXactEvent event, SubTransactionId my_subid,
                           SubTransactionId parent_subid) {
  auto& changes = PendingCatalogChanges();
  switch (event) {
    case SUBXACT_EVENT_COMMIT_SUB:
      for (auto& change : changes) {
        if (change.subtransaction_id == my_subid) {
          change.subtransaction_id = parent_subid;
        }
      }
      break;
    case SUBXACT_EVENT_ABORT_SUB:
      std::erase_if(changes, [my_subid](const PendingCatalogChange& change) {
        return change.subtransaction_id == my_subid;
      });
      break;
    default:
      break;
  }
  return Ok();
}

void SubXactCallback(SubXactEvent event, SubTransactionId my_subid,
                     SubTransactionId parent_subid, void*) {
  PgStatusGuard([&]() { return SubXactCallbackImpl(event, my_subid, parent_subid); });
}

void PgIcebergProcessUtility(PlannedStmt* pstmt, const char* query_string,
                             bool read_only_tree, ProcessUtilityContext context,
                             ParamListInfo params, QueryEnvironment* query_env,
                             DestReceiver* dest, QueryCompletion* qc) {
  Node* utility = pstmt->utilityStmt;
  CreateStmt* iceberg_create = nullptr;
  NativeCreateOptions iceberg_create_options;
  std::vector<Oid> drop_relids;
  std::vector<Binding> drop_bindings;
  if (IsA(utility, CreateStmt)) {
    auto* stmt = reinterpret_cast<CreateStmt*>(utility);
    if (IsIcebergCreateStmt(stmt)) {
      // ParseAndRemoveNativeCreateOptions mutates stmt->options.  A read-only
      // tree comes from the plan cache and must stay intact for later
      // executions, so mutate a copy and execute that instead.
      if (read_only_tree) {
        pstmt = castNode(PlannedStmt, copyObjectImpl(pstmt));
        read_only_tree = false;
        stmt = castNode(CreateStmt, pstmt->utilityStmt);
      }
      iceberg_create_options = PgResultGuard([&]() -> Result<NativeCreateOptions> {
        return ParseAndRemoveNativeCreateOptions(stmt);
      });
      ValidateCreateStmt(stmt, iceberg_create_options);
      iceberg_create = stmt;
    }
  } else if (IsA(utility, CreateTableAsStmt)) {
    if (IsIcebergCreateTableAsStmt(reinterpret_cast<CreateTableAsStmt*>(utility))) {
      ErrorUnsupported("CREATE TABLE AS");
    }
  } else if (IsA(utility, AlterTableStmt)) {
    ValidateAlterTableStmt(reinterpret_cast<AlterTableStmt*>(utility));
  } else if (IsA(utility, DropStmt)) {
    drop_relids = DropRelationOids(reinterpret_cast<DropStmt*>(utility));
    drop_bindings = LoadExistingBindings(drop_relids);
  } else if (IsA(utility, TruncateStmt)) {
    ValidateTruncateStmt(reinterpret_cast<TruncateStmt*>(utility));
  }

  if (PreviousProcessUtilityHook != nullptr) {
    PreviousProcessUtilityHook(pstmt, query_string, read_only_tree, context, params,
                               query_env, dest, qc);
  } else {
    standard_ProcessUtility(pstmt, query_string, read_only_tree, context, params,
                            query_env, dest, qc);
  }

  if (iceberg_create != nullptr) {
    PgStatusGuard(
        [&]() { return QueueCreateTable(iceberg_create, iceberg_create_options); });
    CommandCounterIncrement();
  }
  if (!drop_relids.empty()) {
    RemovePendingCreates(drop_relids);
    QueueDropTables(std::move(drop_bindings));
    PgStatusGuard([&]() { return DeleteBindings(drop_relids); });
  }
}

const TupleTableSlotOps* IcebergSlotCallbacks(Relation) { return &TTSOpsVirtual; }

TableScanDesc IcebergScanBegin(Relation rel, Snapshot snapshot, int nkeys,
                               ScanKeyData* key, ParallelTableScanDesc pscan,
                               uint32 flags) {
  if (pscan != nullptr) {
    ErrorUnsupported("parallel scans");
  }
  if (nkeys != 0) {
    ErrorUnsupported("scan keys");
  }
  auto* scan = static_cast<IcebergScanDesc*>(palloc0(sizeof(IcebergScanDesc)));
  scan->base.rs_rd = rel;
  scan->base.rs_snapshot = snapshot;
  scan->base.rs_nkeys = nkeys;
  scan->base.rs_key = key;
  scan->base.rs_flags = flags;
  PgStatusGuard([&]() -> Status {
    PGICEBERG_ASSIGN_OR_RETURN(auto options, OptionsFromBinding(rel));
    PGICEBERG_ASSIGN_OR_RETURN(scan->state,
                               fdw::BeginScan(rel, options, AllTableColumns(rel)));
    return Ok();
  });
  return reinterpret_cast<TableScanDesc>(scan);
}

void IcebergScanEnd(TableScanDesc sscan) {
  auto* scan = reinterpret_cast<IcebergScanDesc*>(sscan);
  fdw::EndScan(scan->state);
  pfree(scan);
}

void IcebergScanRescan(TableScanDesc sscan, ScanKeyData* key, bool, bool, bool, bool) {
  if (key != nullptr) {
    ErrorUnsupported("scan keys");
  }
  auto* scan = reinterpret_cast<IcebergScanDesc*>(sscan);
  fdw::ReScan(scan->state);
}

bool IcebergScanGetNextSlot(TableScanDesc sscan, ScanDirection direction,
                            TupleTableSlot* slot) {
  if (!ScanDirectionIsForward(direction)) {
    ErrorUnsupported("backward scans");
  }
  auto* scan = reinterpret_cast<IcebergScanDesc*>(sscan);
  return PgResultGuard([&]() -> Result<bool> {
    PGICEBERG_ASSIGN_OR_RETURN(auto result, fdw::IterateScan(scan->state, slot));
    return !TupIsNull(result);
  });
}

Size IcebergParallelScanEstimate(Relation) { return sizeof(ParallelTableScanDescData); }

Size IcebergParallelScanInitialize(Relation, ParallelTableScanDesc) {
  ErrorUnsupported("parallel scans");
  return 0;
}

void IcebergParallelScanReinitialize(Relation, ParallelTableScanDesc) {}

IndexFetchTableData* IcebergIndexFetchBegin(Relation rel) {
  auto* data = static_cast<IndexFetchTableData*>(palloc0(sizeof(IndexFetchTableData)));
  data->rel = rel;
  return data;
}

void IcebergIndexFetchReset(IndexFetchTableData*) {}

void IcebergIndexFetchEnd(IndexFetchTableData* data) { pfree(data); }

bool IcebergIndexFetchTuple(IndexFetchTableData*, ItemPointer, Snapshot, TupleTableSlot*,
                            bool*, bool*) {
  ErrorUnsupported("index tuple fetch");
  return false;
}

bool IcebergTupleFetchRowVersion(Relation, ItemPointer, Snapshot, TupleTableSlot*) {
  ErrorUnsupported("tuple fetch by TID");
  return false;
}

bool IcebergTupleTidValid(TableScanDesc, ItemPointer) { return false; }

void IcebergTupleGetLatestTid(TableScanDesc, ItemPointer) {
  ErrorUnsupported("TID lookup");
}

bool IcebergTupleSatisfiesSnapshot(Relation, TupleTableSlot*, Snapshot) { return true; }

TransactionId IcebergIndexDeleteTuples(Relation, TM_IndexDeleteOp*) {
  ErrorUnsupported("index tuple deletion");
  return InvalidTransactionId;
}

void IcebergTupleInsert(Relation rel, TupleTableSlot* slot, CommandId, int,
                        BulkInsertStateData*) {
  TupleTableSlot* slots[] = {slot};
  PgStatusGuard([&]() -> Status {
    PGICEBERG_ASSIGN_OR_RETURN(auto options, OptionsFromBinding(rel));
    return fdw::AppendSlots(rel, options, slots, 1);
  });
  ItemPointerSetInvalid(&slot->tts_tid);
}

void IcebergTupleInsertSpeculative(Relation, TupleTableSlot*, CommandId, int,
                                   BulkInsertStateData*, uint32) {
  ErrorUnsupported("speculative insert");
}

void IcebergTupleCompleteSpeculative(Relation, TupleTableSlot*, uint32, bool) {
  ErrorUnsupported("speculative insert");
}

void IcebergMultiInsert(Relation rel, TupleTableSlot** slots, int nslots, CommandId, int,
                        BulkInsertStateData*) {
  PgStatusGuard([&]() -> Status {
    PGICEBERG_ASSIGN_OR_RETURN(auto options, OptionsFromBinding(rel));
    return fdw::AppendSlots(rel, options, slots, nslots);
  });
  for (int i = 0; i < nslots; i++) {
    ItemPointerSetInvalid(&slots[i]->tts_tid);
  }
}

TM_Result IcebergTupleDelete(Relation, ItemPointer, CommandId, Snapshot, Snapshot, bool,
                             TM_FailureData*, bool) {
  ErrorUnsupported("DELETE");
  return TM_Ok;
}

TM_Result IcebergTupleUpdate(Relation, ItemPointer, TupleTableSlot*, CommandId, Snapshot,
                             Snapshot, bool, TM_FailureData*, LockTupleMode*,
                             TU_UpdateIndexes*) {
  ErrorUnsupported("UPDATE");
  return TM_Ok;
}

TM_Result IcebergTupleLock(Relation, ItemPointer, Snapshot, TupleTableSlot*, CommandId,
                           LockTupleMode, LockWaitPolicy, uint8, TM_FailureData*) {
  ErrorUnsupported("row locking");
  return TM_Ok;
}

void IcebergRelationSetNewFileLocator(Relation, const RelFileLocator*, char,
                                      TransactionId* freeze_xid, MultiXactId* min_multi) {
  *freeze_xid = InvalidTransactionId;
  *min_multi = InvalidMultiXactId;
}

void IcebergRelationNontransactionalTruncate(Relation) { ErrorUnsupported("TRUNCATE"); }

void IcebergRelationCopyData(Relation, const RelFileLocator*) {
  ErrorUnsupported("relation storage copy");
}

void IcebergRelationCopyForCluster(Relation, Relation, Relation, bool, TransactionId,
                                   TransactionId*, MultiXactId*, double*, double*,
                                   double*) {
  ErrorUnsupported("CLUSTER or VACUUM FULL");
}

void IcebergRelationVacuum(Relation, VacuumParams*, BufferAccessStrategy) {
  ErrorUnsupported("VACUUM");
}

#if PG_VERSION_NUM >= 170000
bool IcebergScanAnalyzeNextBlock(TableScanDesc, ReadStream*) { return false; }
#else
bool IcebergScanAnalyzeNextBlock(TableScanDesc, BlockNumber, BufferAccessStrategy) {
  return false;
}
#endif

bool IcebergScanAnalyzeNextTuple(TableScanDesc, TransactionId, double*, double*,
                                 TupleTableSlot*) {
  return false;
}

double IcebergIndexBuildRangeScan(Relation, Relation, IndexInfo*, bool, bool, bool,
                                  BlockNumber, BlockNumber, IndexBuildCallback, void*,
                                  TableScanDesc) {
  ErrorUnsupported("CREATE INDEX");
  return 0;
}

void IcebergIndexValidateScan(Relation, Relation, IndexInfo*, Snapshot,
                              ValidateIndexState*) {
  ErrorUnsupported("index validation");
}

uint64 IcebergRelationSize(Relation, ForkNumber) { return 0; }

bool IcebergRelationNeedsToastTable(Relation) { return false; }

void IcebergRelationEstimateSize(Relation, int32*, BlockNumber* pages, double* tuples,
                                 double* allvisfrac) {
  *pages = 1;
  *tuples = 1000;
  *allvisfrac = 0;
}

bool IcebergScanSampleNextBlock(TableScanDesc, SampleScanState*) {
  ErrorUnsupported("TABLESAMPLE");
  return false;
}

bool IcebergScanSampleNextTuple(TableScanDesc, SampleScanState*, TupleTableSlot*) {
  ErrorUnsupported("TABLESAMPLE");
  return false;
}

const TableAmRoutine IcebergTableAmRoutine = {
    .type = T_TableAmRoutine,
    .slot_callbacks = IcebergSlotCallbacks,
    .scan_begin = IcebergScanBegin,
    .scan_end = IcebergScanEnd,
    .scan_rescan = IcebergScanRescan,
    .scan_getnextslot = IcebergScanGetNextSlot,
    .parallelscan_estimate = IcebergParallelScanEstimate,
    .parallelscan_initialize = IcebergParallelScanInitialize,
    .parallelscan_reinitialize = IcebergParallelScanReinitialize,
    .index_fetch_begin = IcebergIndexFetchBegin,
    .index_fetch_reset = IcebergIndexFetchReset,
    .index_fetch_end = IcebergIndexFetchEnd,
    .index_fetch_tuple = IcebergIndexFetchTuple,
    .tuple_fetch_row_version = IcebergTupleFetchRowVersion,
    .tuple_tid_valid = IcebergTupleTidValid,
    .tuple_get_latest_tid = IcebergTupleGetLatestTid,
    .tuple_satisfies_snapshot = IcebergTupleSatisfiesSnapshot,
    .index_delete_tuples = IcebergIndexDeleteTuples,
    .tuple_insert = IcebergTupleInsert,
    .tuple_insert_speculative = IcebergTupleInsertSpeculative,
    .tuple_complete_speculative = IcebergTupleCompleteSpeculative,
    .multi_insert = IcebergMultiInsert,
    .tuple_delete = IcebergTupleDelete,
    .tuple_update = IcebergTupleUpdate,
    .tuple_lock = IcebergTupleLock,
    .relation_set_new_filelocator = IcebergRelationSetNewFileLocator,
    .relation_nontransactional_truncate = IcebergRelationNontransactionalTruncate,
    .relation_copy_data = IcebergRelationCopyData,
    .relation_copy_for_cluster = IcebergRelationCopyForCluster,
    .relation_vacuum = IcebergRelationVacuum,
    .scan_analyze_next_block = IcebergScanAnalyzeNextBlock,
    .scan_analyze_next_tuple = IcebergScanAnalyzeNextTuple,
    .index_build_range_scan = IcebergIndexBuildRangeScan,
    .index_validate_scan = IcebergIndexValidateScan,
    .relation_size = IcebergRelationSize,
    .relation_needs_toast_table = IcebergRelationNeedsToastTable,
    .relation_estimate_size = IcebergRelationEstimateSize,
    .scan_sample_next_block = IcebergScanSampleNextBlock,
    .scan_sample_next_tuple = IcebergScanSampleNextTuple,
};

}  // namespace

void RegisterTableAmHooks() {
  DefineCustomStringVariable(
      "pgiceberg.default_catalog", "Catalog name used by CREATE TABLE ... USING iceberg.",
      nullptr, &DefaultCatalog, "", PGC_USERSET, 0, nullptr, nullptr, nullptr);
  DefineCustomStringVariable("pgiceberg.default_namespace",
                             "Iceberg namespace used by CREATE TABLE ... USING iceberg.",
                             nullptr, &DefaultNamespace, "default", PGC_USERSET, 0,
                             nullptr, nullptr, nullptr);
  DefineCustomIntVariable(
      "pgiceberg.default_format_version",
      "Iceberg format version used by CREATE TABLE ... USING iceberg.", nullptr,
      &DefaultFormatVersion, 2, 2, 3, PGC_USERSET, 0, nullptr, nullptr, nullptr);
  PreviousProcessUtilityHook = ProcessUtility_hook;
  ProcessUtility_hook = PgIcebergProcessUtility;
  RegisterXactCallback(XactCallback, nullptr);
  RegisterSubXactCallback(SubXactCallback, nullptr);
}

}  // namespace pgiceberg::tableam

extern "C" {
PG_FUNCTION_INFO_V1(pgiceberg_table_am_handler);

Datum pgiceberg_table_am_handler(PG_FUNCTION_ARGS) {
  PG_RETURN_POINTER(&pgiceberg::tableam::IcebergTableAmRoutine);
}
}
