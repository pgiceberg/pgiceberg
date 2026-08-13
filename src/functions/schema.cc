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

#include <memory>
#include <string>
#include <utility>
#include <vector>

#include <iceberg/schema.h>
#include <iceberg/table.h>
#include <iceberg/type.h>
#include <iceberg/update/update_schema.h>

#include "common/catalog.h"
#include "common/fcinfo.h"
#include "common/pg_error.h"
#include "common/pg_relation.h"
#include "common/schema_binding.h"
#include "common/status.h"
#include "common/type_mapping.h"
#include "engine/options.h"

extern "C" {
#include "postgres.h"
#include "access/table.h"
#include "catalog/pg_class.h"
#include "catalog/pg_type_d.h"
#include "executor/spi.h"
#include "fmgr.h"
#include "miscadmin.h"
#include "utils/acl.h"
#include "utils/array.h"
#include "utils/builtins.h"
#include "utils/errcodes.h"
#include "utils/jsonb.h"
#include "utils/lsyscache.h"
#include "utils/rel.h"
}

namespace {

pgiceberg::Result<std::vector<std::string>> TextArrayArg(FunctionCallInfo fcinfo,
                                                         int argno) {
  if (PG_ARGISNULL(argno)) {
    return std::vector<std::string>{};
  }
  ArrayType* array = PG_GETARG_ARRAYTYPE_P(argno);
  if (ARR_ELEMTYPE(array) != TEXTOID) {
    return std::unexpected(
        pgiceberg::MakeError(ERRCODE_DATATYPE_MISMATCH, "expected a text[] argument"));
  }
  int16 element_len = 0;
  bool element_byval = false;
  char element_align = 0;
  get_typlenbyvalalign(TEXTOID, &element_len, &element_byval, &element_align);
  Datum* values = nullptr;
  bool* nulls = nullptr;
  int count = 0;
  deconstruct_array(array, TEXTOID, element_len, element_byval, element_align, &values,
                    &nulls, &count);
  std::vector<std::string> result;
  result.reserve(count);
  for (int i = 0; i < count; i++) {
    if (nulls[i]) {
      return std::unexpected(pgiceberg::MakeError(
          ERRCODE_NULL_VALUE_NOT_ALLOWED, "text[] argument must not contain NULL"));
    }
    result.emplace_back(TextDatumGetCString(values[i]));
  }
  return result;
}

pgiceberg::Result<std::vector<Oid>> RegtypeArrayArg(FunctionCallInfo fcinfo, int argno) {
  if (PG_ARGISNULL(argno)) {
    return std::vector<Oid>{};
  }
  ArrayType* array = PG_GETARG_ARRAYTYPE_P(argno);
  Oid element_type = ARR_ELEMTYPE(array);
  if (element_type != OIDOID && element_type != REGTYPEOID) {
    return std::unexpected(
        pgiceberg::MakeError(ERRCODE_DATATYPE_MISMATCH, "expected a regtype[] argument"));
  }
  int16 element_len = 0;
  bool element_byval = false;
  char element_align = 0;
  get_typlenbyvalalign(element_type, &element_len, &element_byval, &element_align);
  Datum* values = nullptr;
  bool* nulls = nullptr;
  int count = 0;
  deconstruct_array(array, element_type, element_len, element_byval, element_align,
                    &values, &nulls, &count);
  std::vector<Oid> result;
  result.reserve(count);
  for (int i = 0; i < count; i++) {
    if (nulls[i]) {
      return std::unexpected(pgiceberg::MakeError(
          ERRCODE_NULL_VALUE_NOT_ALLOWED, "regtype[] argument must not contain NULL"));
    }
    result.push_back(DatumGetObjectId(values[i]));
  }
  return result;
}

pgiceberg::Result<pgiceberg::engine::Options> OptionsForRelation(Relation relation) {
  const Oid relid = RelationGetRelid(relation);
  if (relation->rd_rel->relkind == RELKIND_FOREIGN_TABLE) {
    return pgiceberg::engine::OptionsForForeignTable(relid,
                                                     RelationGetRelationName(relation));
  }

  const int connect_rc = SPI_connect();
  const bool started_here = (connect_rc == SPI_OK_CONNECT);
  if (!started_here && connect_rc != SPI_ERROR_CONNECT) {
    return std::unexpected(pgiceberg::MakeError(ERRCODE_INTERNAL_ERROR,
                                                "could not connect to PostgreSQL SPI"));
  }
  auto finish_if_started = [started_here]() {
    if (started_here) {
      SPI_finish();
    }
  };

  const char* sql =
      "SELECT catalog, namespace, table_name FROM pgiceberg.table_bindings "
      "WHERE relid = $1";
  Oid argtypes[] = {OIDOID};
  Datum values[] = {ObjectIdGetDatum(relid)};
  const int result = SPI_execute_with_args(sql, 1, argtypes, values, nullptr, true, 1);
  if (result != SPI_OK_SELECT) {
    finish_if_started();
    return std::unexpected(
        pgiceberg::MakeError(ERRCODE_INTERNAL_ERROR, "could not read table binding"));
  }
  if (SPI_processed == 0) {
    finish_if_started();
    return std::unexpected(pgiceberg::MakeError(
        ERRCODE_UNDEFINED_OBJECT,
        "relation is not a pgiceberg foreign table or iceberg table"));
  }

  HeapTuple tuple = SPI_tuptable->vals[0];
  TupleDesc desc = SPI_tuptable->tupdesc;
  auto text_column = [&](int column_number) -> pgiceberg::Result<std::string> {
    bool is_null = false;
    Datum value = SPI_getbinval(tuple, desc, column_number, &is_null);
    if (is_null) {
      return std::unexpected(pgiceberg::MakeError(
          ERRCODE_NULL_VALUE_NOT_ALLOWED, "pgiceberg table binding contains NULL"));
    }
    return std::string(TextDatumGetCString(value));
  };
  pgiceberg::engine::Options options;
  PGICEBERG_ASSIGN_OR_RETURN(options.catalog, text_column(1));
  PGICEBERG_ASSIGN_OR_RETURN(options.name_space, text_column(2));
  PGICEBERG_ASSIGN_OR_RETURN(options.table, text_column(3));
  finish_if_started();
  return options;
}

pgiceberg::Result<std::shared_ptr<iceberg::Schema>> LoadRelationIcebergSchema(
    Relation relation) {
  PGICEBERG_ASSIGN_OR_RETURN(auto options, OptionsForRelation(relation));
  PGICEBERG_ASSIGN_OR_RETURN(auto catalog_options,
                             pgiceberg::engine::ToCatalogOptions(options));
  PGICEBERG_ASSIGN_OR_RETURN(
      auto table,
      pgiceberg::LoadIcebergTable(catalog_options, RelationGetRelationName(relation)));
  return pgiceberg::FromIcebergResult(table->schema(), "load schema");
}

pgiceberg::Result<std::vector<pgiceberg::SchemaChange>> DiffRelationSchema(
    Relation relation) {
  PGICEBERG_ASSIGN_OR_RETURN(auto local, pgiceberg::LoadLocalColumns(relation));
  PGICEBERG_ASSIGN_OR_RETURN(auto schema, LoadRelationIcebergSchema(relation));
  PGICEBERG_ASSIGN_OR_RETURN(auto binding, pgiceberg::BindSchema(local, *schema));
  return pgiceberg::DiffSchema(binding);
}

pgiceberg::Status ExecuteSql(const std::string& sql) {
  const int connect_rc = SPI_connect();
  const bool started_here = (connect_rc == SPI_OK_CONNECT);
  if (!started_here && connect_rc != SPI_ERROR_CONNECT) {
    return std::unexpected(pgiceberg::MakeError(ERRCODE_INTERNAL_ERROR,
                                                "could not connect to PostgreSQL SPI"));
  }
  const int result = SPI_execute(sql.c_str(), false, 0);
  if (started_here) {
    SPI_finish();
  }
  if (result < 0) {
    return std::unexpected(pgiceberg::MakeError(
        ERRCODE_INTERNAL_ERROR, "could not apply schema refresh SQL: " + sql));
  }
  return pgiceberg::Ok();
}

pgiceberg::Result<std::string> QualifiedRelationName(Relation relation) {
  const char* nspname = get_namespace_name(RelationGetNamespace(relation));
  if (nspname == nullptr) {
    return std::unexpected(pgiceberg::MakeError(ERRCODE_UNDEFINED_SCHEMA,
                                                "could not resolve relation schema"));
  }
  return std::string(
      quote_qualified_identifier(nspname, RelationGetRelationName(relation)));
}

pgiceberg::Status RefreshForeignTable(const std::string& qualified,
                                      const std::vector<pgiceberg::SchemaChange>& changes,
                                      const pgiceberg::SchemaBinding& binding) {
  auto column_name_after_rename = [](const pgiceberg::BoundField& field) {
    return field.renamed ? field.iceberg_name : field.local_name;
  };

  for (const auto& change : changes) {
    if (change.kind == pgiceberg::SchemaChangeKind::kDropped &&
        change.local_column.has_value()) {
      std::string sql = "ALTER FOREIGN TABLE " + qualified + " DROP COLUMN " +
                        quote_identifier(change.local_column->c_str());
      PGICEBERG_RETURN_NOT_OK(ExecuteSql(sql));
    }
  }

  for (const auto& change : changes) {
    if (change.kind == pgiceberg::SchemaChangeKind::kRenamed &&
        change.local_column.has_value() && change.iceberg_name.has_value()) {
      std::string sql = "ALTER FOREIGN TABLE " + qualified + " RENAME COLUMN " +
                        quote_identifier(change.local_column->c_str()) + " TO " +
                        quote_identifier(change.iceberg_name->c_str());
      PGICEBERG_RETURN_NOT_OK(ExecuteSql(sql));
    }
  }

  for (const auto& field : binding.fields) {
    if (field.attnum == InvalidAttrNumber || field.readable) {
      continue;
    }
    PGICEBERG_ASSIGN_OR_RETURN(auto sql_type,
                               pgiceberg::IcebergTypeToSql(*field.iceberg_type));
    const auto column_name = column_name_after_rename(field);
    std::string sql = "ALTER FOREIGN TABLE " + qualified + " ALTER COLUMN " +
                      quote_identifier(column_name.c_str()) + " TYPE " + sql_type;
    PGICEBERG_RETURN_NOT_OK(ExecuteSql(sql));
  }

  for (const auto& field : binding.fields) {
    if (field.attnum == InvalidAttrNumber) {
      continue;
    }
    const auto column_name = column_name_after_rename(field);
    const char* action = field.had_field_id ? "SET" : "ADD";
    std::string sql = "ALTER FOREIGN TABLE " + qualified + " ALTER COLUMN " +
                      quote_identifier(column_name.c_str()) + " OPTIONS (" + action +
                      " " + pgiceberg::kFieldIdOption + " '" +
                      std::to_string(field.field_id) + "')";
    PGICEBERG_RETURN_NOT_OK(ExecuteSql(sql));
  }

  for (const auto& change : changes) {
    if (change.kind != pgiceberg::SchemaChangeKind::kAdded ||
        !change.iceberg_name.has_value() || !change.iceberg_type.has_value() ||
        !change.iceberg_field_id.has_value()) {
      continue;
    }
    std::string sql = "ALTER FOREIGN TABLE " + qualified + " ADD COLUMN " +
                      quote_identifier(change.iceberg_name->c_str()) + " " +
                      *change.iceberg_type + " OPTIONS (" + pgiceberg::kFieldIdOption +
                      " '" + std::to_string(*change.iceberg_field_id) + "')";
    PGICEBERG_RETURN_NOT_OK(ExecuteSql(sql));
  }
  return pgiceberg::Ok();
}

pgiceberg::Status EnsureRelationSelect(Oid relid) {
  if (pg_class_aclcheck(relid, GetSessionUserId(), ACL_SELECT) != ACLCHECK_OK) {
    return std::unexpected(
        pgiceberg::MakeError(ERRCODE_INSUFFICIENT_PRIVILEGE,
                             "permission denied for relation " + std::to_string(relid)));
  }
  return pgiceberg::Ok();
}

pgiceberg::Status EnsureRelationOwner(Oid relid) {
  if (!pg_class_ownercheck(relid, GetSessionUserId())) {
    return std::unexpected(
        pgiceberg::MakeError(ERRCODE_INSUFFICIENT_PRIVILEGE,
                             "must be owner of relation " + std::to_string(relid)));
  }
  return pgiceberg::Ok();
}

}  // namespace

extern "C" {
PG_FUNCTION_INFO_V1(pgiceberg_schema_diff_json);
PG_FUNCTION_INFO_V1(pgiceberg_refresh_schema);
PG_FUNCTION_INFO_V1(pgiceberg_update_schema);

Datum pgiceberg_schema_diff_json(PG_FUNCTION_ARGS) {
  return pgiceberg::PgResultGuard([&]() -> pgiceberg::Result<Datum> {
    const Oid relid = PG_GETARG_OID(0);
    PGICEBERG_RETURN_NOT_OK(EnsureRelationSelect(relid));
    Relation relation = table_open(relid, AccessShareLock);
    pgiceberg::RelationLockGuard guard(relation, AccessShareLock);
    PGICEBERG_ASSIGN_OR_RETURN(auto changes, DiffRelationSchema(relation));
    const auto json = pgiceberg::SchemaChangesToJson(changes);
    return DirectFunctionCall1(jsonb_in, CStringGetDatum(json.c_str()));
  });
}

Datum pgiceberg_refresh_schema(PG_FUNCTION_ARGS) {
  pgiceberg::PgStatusGuard([&]() -> pgiceberg::Status {
    const Oid relid = PG_GETARG_OID(0);
    PGICEBERG_RETURN_NOT_OK(EnsureRelationOwner(relid));
    pgiceberg::SchemaBinding binding;
    std::vector<pgiceberg::SchemaChange> changes;
    std::string qualified;
    {
      Relation relation = table_open(relid, AccessShareLock);
      pgiceberg::RelationLockGuard guard(relation, AccessShareLock);
      if (relation->rd_rel->relkind != RELKIND_FOREIGN_TABLE) {
        return std::unexpected(pgiceberg::MakeError(
            ERRCODE_FEATURE_NOT_SUPPORTED,
            "pgiceberg.refresh_schema currently supports foreign tables",
            "Native iceberg tables keep field ids in pgiceberg.column_bindings; "
            "ALTER TABLE schema evolution is not implemented yet."));
      }
      PGICEBERG_ASSIGN_OR_RETURN(auto local, pgiceberg::LoadLocalColumns(relation));
      PGICEBERG_ASSIGN_OR_RETURN(auto schema, LoadRelationIcebergSchema(relation));
      PGICEBERG_ASSIGN_OR_RETURN(binding, pgiceberg::BindSchema(local, *schema));
      PGICEBERG_ASSIGN_OR_RETURN(changes, pgiceberg::DiffSchema(binding));
      PGICEBERG_ASSIGN_OR_RETURN(qualified, QualifiedRelationName(relation));
    }
    return RefreshForeignTable(qualified, changes, binding);
  });
  PG_RETURN_VOID();
}

Datum pgiceberg_update_schema(PG_FUNCTION_ARGS) {
  pgiceberg::PgStatusGuard([&]() -> pgiceberg::Status {
    PGICEBERG_ASSIGN_OR_RETURN(
        auto options, pgiceberg::LoadCatalogOptions(pgiceberg::TextArg(fcinfo, 0)));
    options.name_space = pgiceberg::TextArg(fcinfo, 1);
    options.table = pgiceberg::TextArg(fcinfo, 2);
    PGICEBERG_ASSIGN_OR_RETURN(auto add_names, TextArrayArg(fcinfo, 3));
    PGICEBERG_ASSIGN_OR_RETURN(auto add_types, RegtypeArrayArg(fcinfo, 4));
    PGICEBERG_ASSIGN_OR_RETURN(auto drop_names, TextArrayArg(fcinfo, 5));
    PGICEBERG_ASSIGN_OR_RETURN(auto rename_from, TextArrayArg(fcinfo, 6));
    PGICEBERG_ASSIGN_OR_RETURN(auto rename_to, TextArrayArg(fcinfo, 7));

    if (add_names.size() != add_types.size()) {
      return std::unexpected(
          pgiceberg::MakeError(ERRCODE_ARRAY_SUBSCRIPT_ERROR,
                               "add_names and add_types must have the same length"));
    }
    if (rename_from.size() != rename_to.size()) {
      return std::unexpected(
          pgiceberg::MakeError(ERRCODE_ARRAY_SUBSCRIPT_ERROR,
                               "rename_from and rename_to must have the same length"));
    }

    PGICEBERG_ASSIGN_OR_RETURN(
        auto table, pgiceberg::LoadIcebergTable(options, options.table.c_str()));
    PGICEBERG_ASSIGN_OR_RETURN(
        auto update, pgiceberg::FromIcebergResult(table->NewUpdateSchema(),
                                                  "create Iceberg schema update"));
    for (std::size_t i = 0; i < add_names.size(); i++) {
      PGICEBERG_ASSIGN_OR_RETURN(auto type,
                                 pgiceberg::PostgresTypeToIcebergType(add_types[i]));
      update->AddColumn(add_names[i], type);
    }
    for (const auto& name : drop_names) {
      update->DeleteColumn(name);
    }
    for (std::size_t i = 0; i < rename_from.size(); i++) {
      update->RenameColumn(rename_from[i], rename_to[i]);
    }
    return pgiceberg::FromIcebergStatus(update->Commit(), "commit Iceberg schema update");
  });
  PG_RETURN_VOID();
}

}  // extern "C"
