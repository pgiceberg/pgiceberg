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

#include <iceberg/schema.h>
#include <iceberg/schema_field.h>

#include <memory>
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

#include "common/catalog.h"
#include "common/fcinfo.h"
#include "common/pg_error.h"
#include "common/status.h"
#include "common/type_mapping.h"

namespace {

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

}  // namespace

extern "C" {
PG_FUNCTION_INFO_V1(pgiceberg_create_table);

Datum pgiceberg_create_table(PG_FUNCTION_ARGS) {
  return pgiceberg::PgResultGuard([&]() -> pgiceberg::Result<Datum> {
    PGICEBERG_ASSIGN_OR_RETURN(
        auto options, pgiceberg::LoadCatalogOptions(pgiceberg::TextArg(fcinfo, 0)));
    options.name_space = pgiceberg::TextArg(fcinfo, 1);
    options.table = pgiceberg::TextArg(fcinfo, 2);
    PGICEBERG_ASSIGN_OR_RETURN(auto column_names,
                               TextArrayArg(fcinfo, 3, "column_names"));
    PGICEBERG_ASSIGN_OR_RETURN(auto column_types, OidArrayArg(fcinfo, 4, "column_types"));
    PGICEBERG_ASSIGN_OR_RETURN(auto column_required,
                               BoolArrayArg(fcinfo, 5, "column_required"));
    const bool drop_if_exists = PG_GETARG_BOOL(6);
    const int32_t format_version = PG_GETARG_INT32(7);

    PGICEBERG_ASSIGN_OR_RETURN(auto schema,
                               BuildSchema(column_names, column_types, column_required));
    PGICEBERG_ASSIGN_OR_RETURN(
        auto created_table,
        pgiceberg::CreateUnpartitionedIcebergTable(options, std::move(schema),
                                                   format_version, drop_if_exists));
    (void)created_table;

    return static_cast<Datum>(0);
  });
}

}  // extern "C"
