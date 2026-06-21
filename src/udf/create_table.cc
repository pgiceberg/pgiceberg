#include <iceberg/arrow/arrow_io_internal.h>
#include <iceberg/catalog/sql/sql_catalog.h>
#include <iceberg/partition_spec.h>
#include <iceberg/schema.h>
#include <iceberg/schema_field.h>
#include <iceberg/sort_order.h>
#include <iceberg/table_identifier.h>

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

#include "common/error.h"
#include "common/pg_error.h"
#include "common/type_mapping.h"

namespace {

std::string TextArg(FunctionCallInfo fcinfo, int argno) {
  return text_to_cstring(PG_GETARG_TEXT_PP(argno));
}

std::vector<Datum> ArrayDatums(ArrayType* array, Oid expected_type,
                               const char* argument_name) {
  Oid element_type = ARR_ELEMTYPE(array);
  if (element_type != expected_type) {
    throw pgiceberg::PgError(ERRCODE_DATATYPE_MISMATCH,
                             std::string("pgiceberg.create_table argument \"") +
                                 argument_name + "\" has wrong array element type");
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
      throw pgiceberg::PgError(ERRCODE_NULL_VALUE_NOT_ALLOWED,
                               std::string("pgiceberg.create_table argument \"") +
                                   argument_name + "\" must not contain NULL");
    }
    result.push_back(values[i]);
  }
  return result;
}

std::vector<std::string> TextArrayArg(FunctionCallInfo fcinfo, int argno,
                                      const char* argument_name) {
  auto values = ArrayDatums(PG_GETARG_ARRAYTYPE_P(argno), TEXTOID, argument_name);
  std::vector<std::string> result;
  result.reserve(values.size());
  for (Datum value : values) {
    result.emplace_back(TextDatumGetCString(value));
  }
  return result;
}

std::vector<Oid> OidArrayArg(FunctionCallInfo fcinfo, int argno,
                             const char* argument_name) {
  ArrayType* array = PG_GETARG_ARRAYTYPE_P(argno);
  Oid element_type = ARR_ELEMTYPE(array);
  if (element_type != OIDOID && element_type != REGTYPEOID) {
    throw pgiceberg::PgError(ERRCODE_DATATYPE_MISMATCH,
                             std::string("pgiceberg.create_table argument \"") +
                                 argument_name + "\" has wrong array element type");
  }

  auto values = ArrayDatums(array, element_type, argument_name);
  std::vector<Oid> result;
  result.reserve(values.size());
  for (Datum value : values) {
    result.push_back(DatumGetObjectId(value));
  }
  return result;
}

std::vector<bool> BoolArrayArg(FunctionCallInfo fcinfo, int argno,
                               const char* argument_name) {
  auto values = ArrayDatums(PG_GETARG_ARRAYTYPE_P(argno), BOOLOID, argument_name);
  std::vector<bool> result;
  result.reserve(values.size());
  for (Datum value : values) {
    result.push_back(DatumGetBool(value));
  }
  return result;
}

std::shared_ptr<iceberg::Schema> BuildSchema(const std::vector<std::string>& column_names,
                                             const std::vector<Oid>& column_types,
                                             const std::vector<bool>& column_required) {
  if (column_names.empty()) {
    throw pgiceberg::PgError(ERRCODE_INVALID_PARAMETER_VALUE,
                             "pgiceberg.create_table requires at least one "
                             "column");
  }
  if (column_names.size() != column_types.size() ||
      column_names.size() != column_required.size()) {
    throw pgiceberg::PgError(ERRCODE_ARRAY_SUBSCRIPT_ERROR,
                             "pgiceberg.create_table column_names, column_types, and "
                             "column_required must have the same length");
  }

  std::vector<iceberg::SchemaField> fields;
  fields.reserve(column_names.size());
  for (std::size_t i = 0; i < column_names.size(); i++) {
    auto type = pgiceberg::PostgresTypeToIcebergType(column_types[i]);
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

std::shared_ptr<iceberg::sql::SqlCatalog> CreateCatalog(const std::string& catalog_type,
                                                        const std::string& catalog_uri,
                                                        const std::string& warehouse,
                                                        const std::string& catalog_name) {
  std::shared_ptr<iceberg::FileIO> file_io(
      iceberg::arrow::ArrowFileSystemFileIO::MakeLocalFileIO().release());
  iceberg::sql::SqlCatalogConfig config{
      .name = catalog_name,
      .uri = catalog_uri,
      .warehouse_location = warehouse,
      .max_connections = 1,
  };

  if (catalog_type == "sqlite") {
    return pgiceberg::CheckIcebergResult(
        iceberg::sql::SqlCatalog::MakeSqliteCatalog(config, file_io),
        "create SQLite catalog");
  }
  if (catalog_type == "sql") {
    return pgiceberg::CheckIcebergResult(
        iceberg::sql::SqlCatalog::MakePostgreSqlCatalog(config, file_io),
        "create PostgreSQL catalog");
  }

  throw pgiceberg::PgError(ERRCODE_FDW_INVALID_ATTRIBUTE_VALUE,
                           "invalid pgiceberg catalog_type \"" + catalog_type + "\"",
                           "Valid catalog_type values are: sql, sqlite.");
}

}  // namespace

extern "C" {
PG_FUNCTION_INFO_V1(pgiceberg_create_table);
}

extern "C" Datum pgiceberg_create_table(PG_FUNCTION_ARGS) {
  return pgiceberg::PgGuard([&]() -> Datum {
    const std::string catalog_type = TextArg(fcinfo, 0);
    const std::string catalog_uri = TextArg(fcinfo, 1);
    const std::string warehouse = TextArg(fcinfo, 2);
    const std::string name_space = TextArg(fcinfo, 3);
    const std::string table_name = TextArg(fcinfo, 4);
    const auto column_names = TextArrayArg(fcinfo, 5, "column_names");
    const auto column_types = OidArrayArg(fcinfo, 6, "column_types");
    const auto column_required = BoolArrayArg(fcinfo, 7, "column_required");
    const bool drop_if_exists = PG_GETARG_BOOL(8);
    const std::string catalog_name = TextArg(fcinfo, 9);

    auto catalog = CreateCatalog(catalog_type, catalog_uri, warehouse, catalog_name);
    iceberg::Namespace ns{.levels = SplitNamespace(name_space)};
    auto ns_exists =
        pgiceberg::CheckIcebergResult(catalog->NamespaceExists(ns), "check namespace");
    if (!ns_exists) {
      pgiceberg::CheckIcebergStatus(catalog->CreateNamespace(ns, {}), "create namespace");
    }

    iceberg::TableIdentifier ident{.ns = ns, .name = table_name};
    auto table_exists =
        pgiceberg::CheckIcebergResult(catalog->TableExists(ident), "check table");
    if (table_exists) {
      if (!drop_if_exists) {
        throw pgiceberg::PgError(
            ERRCODE_DUPLICATE_TABLE,
            "Iceberg table \"" + name_space + "." + table_name + "\" already exists");
      }
      pgiceberg::CheckIcebergStatus(catalog->DropTable(ident, false), "drop table");
    }

    const auto table_location =
        std::filesystem::path(warehouse) / name_space / table_name;
    std::filesystem::create_directories(table_location / "metadata");
    pgiceberg::CheckIcebergResult(
        catalog->CreateTable(ident,
                             BuildSchema(column_names, column_types, column_required),
                             iceberg::PartitionSpec::Unpartitioned(),
                             iceberg::SortOrder::Unsorted(), table_location.string(),
                             {{"write.parquet.compression-codec", "uncompressed"}}),
        "create table");

    PG_RETURN_VOID();
  });
}
