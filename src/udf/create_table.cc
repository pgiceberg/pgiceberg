#include <iceberg/arrow/arrow_io_internal.h>
#include <iceberg/catalog/sql/sql_catalog.h>
#include <iceberg/partition_spec.h>
#include <iceberg/schema.h>
#include <iceberg/schema_field.h>
#include <iceberg/sort_order.h>
#include <iceberg/table_properties.h>
#include <iceberg/table_identifier.h>
#include <iceberg/transaction.h>
#include <iceberg/update/update_properties.h>

#include <filesystem>
#include <memory>
#include <sstream>
#include <string>
#include <utility>
#include <vector>

extern "C" {
#include "postgres.h"
#include "catalog/pg_type_d.h"
#include "fmgr.h"
#include "utils/array.h"
#include "utils/builtins.h"
#include "utils/errcodes.h"
#include "utils/lsyscache.h"
}

#include "common/pg_error.h"
#include "common/status.h"
#include "common/type_mapping.h"

namespace {

std::string TextArg(FunctionCallInfo fcinfo, int argno) {
  return text_to_cstring(PG_GETARG_TEXT_PP(argno));
}

pgiceberg::Result<std::vector<Datum>> ArrayDatums(ArrayType* array, Oid expected_type,
                                                  const char* argument_name) {
  Oid element_type = ARR_ELEMTYPE(array);
  if (element_type != expected_type) {
    return std::unexpected(pgiceberg::MakeError(
        ERRCODE_DATATYPE_MISMATCH, std::string("pgiceberg.create_table argument \"") +
                                       argument_name +
                                       "\" has wrong array element type"));
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

  std::vector<Datum> result;
  result.reserve(count);
  for (int i = 0; i < count; i++) {
    if (nulls[i]) {
      return std::unexpected(
          pgiceberg::MakeError(ERRCODE_NULL_VALUE_NOT_ALLOWED,
                               std::string("pgiceberg.create_table argument \"") +
                                   argument_name + "\" must not contain NULL"));
    }
    result.push_back(values[i]);
  }
  return result;
}

pgiceberg::Result<std::vector<std::string>> TextArrayArg(FunctionCallInfo fcinfo,
                                                         int argno,
                                                         const char* argument_name) {
  PGICEBERG_ASSIGN_OR_RETURN(
      auto values, ArrayDatums(PG_GETARG_ARRAYTYPE_P(argno), TEXTOID, argument_name));
  std::vector<std::string> result;
  result.reserve(values.size());
  for (Datum value : values) {
    result.emplace_back(TextDatumGetCString(value));
  }
  return result;
}

pgiceberg::Result<std::vector<Oid>> OidArrayArg(FunctionCallInfo fcinfo, int argno,
                                                const char* argument_name) {
  ArrayType* array = PG_GETARG_ARRAYTYPE_P(argno);
  Oid element_type = ARR_ELEMTYPE(array);
  if (element_type != OIDOID && element_type != REGTYPEOID) {
    return std::unexpected(pgiceberg::MakeError(
        ERRCODE_DATATYPE_MISMATCH, std::string("pgiceberg.create_table argument \"") +
                                       argument_name +
                                       "\" has wrong array element type"));
  }

  PGICEBERG_ASSIGN_OR_RETURN(auto values,
                             ArrayDatums(array, element_type, argument_name));
  std::vector<Oid> result;
  result.reserve(values.size());
  for (Datum value : values) {
    result.push_back(DatumGetObjectId(value));
  }
  return result;
}

pgiceberg::Result<std::vector<bool>> BoolArrayArg(FunctionCallInfo fcinfo, int argno,
                                                  const char* argument_name) {
  PGICEBERG_ASSIGN_OR_RETURN(
      auto values, ArrayDatums(PG_GETARG_ARRAYTYPE_P(argno), BOOLOID, argument_name));
  std::vector<bool> result;
  result.reserve(values.size());
  for (Datum value : values) {
    result.push_back(DatumGetBool(value));
  }
  return result;
}

pgiceberg::Result<std::shared_ptr<iceberg::Schema>> BuildSchema(
    const std::vector<std::string>& column_names, const std::vector<Oid>& column_types,
    const std::vector<bool>& column_required) {
  if (column_names.empty()) {
    return std::unexpected(
        pgiceberg::MakeError(ERRCODE_INVALID_PARAMETER_VALUE,
                             "pgiceberg.create_table requires at least one column"));
  }
  if (column_names.size() != column_types.size() ||
      column_names.size() != column_required.size()) {
    return std::unexpected(pgiceberg::MakeError(
        ERRCODE_ARRAY_SUBSCRIPT_ERROR,
        "pgiceberg.create_table column_names, column_types, and column_required must "
        "have the same length"));
  }

  std::vector<iceberg::SchemaField> fields;
  fields.reserve(column_names.size());
  for (std::size_t i = 0; i < column_names.size(); i++) {
    PGICEBERG_ASSIGN_OR_RETURN(auto type,
                               pgiceberg::PostgresTypeToIcebergType(column_types[i]));
    const int field_id = static_cast<int>(i + 1);
    if (column_required[i]) {
      fields.push_back(
          iceberg::SchemaField::MakeRequired(field_id, column_names[i], type));
    } else {
      fields.push_back(
          iceberg::SchemaField::MakeOptional(field_id, column_names[i], type));
    }
  }
  return std::make_shared<iceberg::Schema>(std::move(fields), 1);
}

pgiceberg::Status ValidateFormatVersion(int32_t format_version) {
  if (format_version == 2 || format_version == 3) {
    return {};
  }
  return std::unexpected(pgiceberg::MakeError(
      ERRCODE_INVALID_PARAMETER_VALUE,
      "unsupported pgiceberg.create_table format_version " +
          std::to_string(format_version),
      "Iceberg format version 1 is not supported; valid values are 2 and 3."));
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

pgiceberg::Result<std::shared_ptr<iceberg::sql::SqlCatalog>> CreateCatalog(
    const std::string& catalog_type, const std::string& catalog_uri,
    const std::string& warehouse, const std::string& catalog_name) {
  std::shared_ptr<iceberg::FileIO> file_io(
      iceberg::arrow::ArrowFileSystemFileIO::MakeLocalFileIO().release());
  iceberg::sql::SqlCatalogConfig config{
      .name = catalog_name,
      .uri = catalog_uri,
      .warehouse_location = warehouse,
      .max_connections = 1,
  };

  if (catalog_type == "sqlite") {
    return pgiceberg::FromIcebergResult(
        iceberg::sql::SqlCatalog::MakeSqliteCatalog(config, file_io),
        "create SQLite catalog");
  }
  if (catalog_type == "sql") {
    return pgiceberg::FromIcebergResult(
        iceberg::sql::SqlCatalog::MakePostgreSqlCatalog(config, file_io),
        "create PostgreSQL catalog");
  }

  return std::unexpected(
      pgiceberg::MakeError(ERRCODE_FDW_INVALID_ATTRIBUTE_VALUE,
                           "invalid pgiceberg catalog_type \"" + catalog_type + "\"",
                           "Valid catalog_type values are: sql, sqlite."));
}

}  // namespace

extern "C" {
PG_FUNCTION_INFO_V1(pgiceberg_create_table);
}

extern "C" Datum pgiceberg_create_table(PG_FUNCTION_ARGS) {
  return pgiceberg::PgResultGuard([&]() -> pgiceberg::Result<Datum> {
    const std::string catalog_type = TextArg(fcinfo, 0);
    const std::string catalog_uri = TextArg(fcinfo, 1);
    const std::string warehouse = TextArg(fcinfo, 2);
    const std::string name_space = TextArg(fcinfo, 3);
    const std::string table_name = TextArg(fcinfo, 4);
    PGICEBERG_ASSIGN_OR_RETURN(auto column_names,
                               TextArrayArg(fcinfo, 5, "column_names"));
    PGICEBERG_ASSIGN_OR_RETURN(auto column_types, OidArrayArg(fcinfo, 6, "column_types"));
    PGICEBERG_ASSIGN_OR_RETURN(auto column_required,
                               BoolArrayArg(fcinfo, 7, "column_required"));
    const bool drop_if_exists = PG_GETARG_BOOL(8);
    const std::string catalog_name = TextArg(fcinfo, 9);
    const int32_t format_version = PG_GETARG_INT32(10);
    PGICEBERG_RETURN_NOT_OK(ValidateFormatVersion(format_version));

    PGICEBERG_ASSIGN_OR_RETURN(
        auto catalog, CreateCatalog(catalog_type, catalog_uri, warehouse, catalog_name));
    iceberg::Namespace ns{.levels = SplitNamespace(name_space)};
    PGICEBERG_ASSIGN_OR_RETURN(
        auto ns_exists,
        pgiceberg::FromIcebergResult(catalog->NamespaceExists(ns), "check namespace"));
    if (!ns_exists) {
      PGICEBERG_RETURN_NOT_OK(pgiceberg::FromIcebergStatus(
          catalog->CreateNamespace(ns, {}), "create namespace"));
    }

    iceberg::TableIdentifier ident{.ns = ns, .name = table_name};
    PGICEBERG_ASSIGN_OR_RETURN(
        auto table_exists,
        pgiceberg::FromIcebergResult(catalog->TableExists(ident), "check table"));
    if (table_exists) {
      if (!drop_if_exists) {
        return std::unexpected(pgiceberg::MakeError(
            ERRCODE_DUPLICATE_TABLE,
            "Iceberg table \"" + name_space + "." + table_name + "\" already exists"));
      }
      PGICEBERG_RETURN_NOT_OK(
          pgiceberg::FromIcebergStatus(catalog->DropTable(ident, false), "drop table"));
    }

    const auto table_location =
        std::filesystem::path(warehouse) / name_space / table_name;
    std::filesystem::create_directories(table_location / "metadata");
    PGICEBERG_ASSIGN_OR_RETURN(auto schema,
                               BuildSchema(column_names, column_types, column_required));
    PGICEBERG_ASSIGN_OR_RETURN(
        auto table, pgiceberg::FromIcebergResult(
                        catalog->StageCreateTable(
                            ident, schema, iceberg::PartitionSpec::Unpartitioned(),
                            iceberg::SortOrder::Unsorted(), table_location.string(),
                            {{"write.parquet.compression-codec", "uncompressed"}}),
                        "stage create table"));
    PGICEBERG_ASSIGN_OR_RETURN(
        auto properties_update,
        pgiceberg::FromIcebergResult(table->NewUpdateProperties(),
                                     "create table properties update"));
    PGICEBERG_RETURN_NOT_OK(pgiceberg::FromIcebergStatus(
        properties_update
            ->Set(iceberg::TableProperties::kFormatVersion.key(),
                  std::to_string(format_version))
            .Commit(),
        "set table format version"));
    PGICEBERG_ASSIGN_OR_RETURN(auto created_table, pgiceberg::FromIcebergResult(
                                                       table->Commit(), "create table"));
    (void)created_table;
    (void)table;

    return static_cast<Datum>(0);
  });
}
