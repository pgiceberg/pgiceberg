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

#include "common/schema_binding.h"

#include <charconv>
#include <cstdio>
#include <cstring>
#include <sstream>
#include <system_error>
#include <unordered_map>
#include <unordered_set>
#include <utility>
#include <variant>

#include <arrow/array/builder_base.h>
#include <arrow/type.h>
#include <arrow/util/key_value_metadata.h>
#include <iceberg/constants.h>
#include <iceberg/expression/literal.h>
#include <iceberg/schema.h>
#include <iceberg/schema_field.h>
#include <iceberg/type.h>
#include <iceberg/util/decimal.h>
#include <iceberg/util/uuid.h>

#include "common/constants.h"
#include "common/datum_convert.h"
#include "common/status.h"
#include "common/type_mapping.h"

extern "C" {
#include "postgres.h"
#include "access/htup_details.h"
#include "catalog/pg_class.h"
#include "catalog/pg_type_d.h"
#include "commands/defrem.h"
#include "executor/spi.h"
#include "foreign/foreign.h"
#include "nodes/pg_list.h"
#include "utils/builtins.h"
#include "utils/date.h"
#include "utils/errcodes.h"
#include "utils/lsyscache.h"
#include "utils/numeric.h"
#include "utils/rel.h"
#include "utils/timestamp.h"
#include "utils/uuid.h"
}

namespace pgiceberg {
namespace {

iceberg::TypeId CanonicalTypeIdImpl(iceberg::TypeId type_id) {
  switch (type_id) {
    case iceberg::TypeId::kTimestampNs:
      return iceberg::TypeId::kTimestamp;
    case iceberg::TypeId::kTimestampTzNs:
      return iceberg::TypeId::kTimestampTz;
    default:
      return type_id;
  }
}

bool TypesMatch(const iceberg::Type& iceberg_type, const iceberg::Type& postgres_mapped) {
  if (CanonicalTypeIdImpl(iceberg_type.type_id()) !=
      CanonicalTypeIdImpl(postgres_mapped.type_id())) {
    return false;
  }
  if (iceberg_type.type_id() == iceberg::TypeId::kDecimal &&
      postgres_mapped.type_id() == iceberg::TypeId::kDecimal) {
    const auto& left = static_cast<const iceberg::DecimalType&>(iceberg_type);
    const auto& right = static_cast<const iceberg::DecimalType&>(postgres_mapped);
    return left.precision() == right.precision() && left.scale() == right.scale();
  }
  return true;
}

bool IsWideningPromotion(iceberg::TypeId from, iceberg::TypeId to) {
  if (from == to) {
    return true;
  }
  if (from == iceberg::TypeId::kInt && to == iceberg::TypeId::kLong) {
    return true;
  }
  if (from == iceberg::TypeId::kFloat && to == iceberg::TypeId::kDouble) {
    return true;
  }
  return CanonicalTypeIdImpl(from) == CanonicalTypeIdImpl(to);
}

std::optional<iceberg::TypeId> PostgresMappedTypeId(Oid pg_type) {
  switch (pg_type) {
    case BOOLOID:
      return iceberg::TypeId::kBoolean;
    case INT2OID:
    case INT4OID:
      return iceberg::TypeId::kInt;
    case INT8OID:
      return iceberg::TypeId::kLong;
    case FLOAT4OID:
      return iceberg::TypeId::kFloat;
    case FLOAT8OID:
      return iceberg::TypeId::kDouble;
    case NUMERICOID:
      return iceberg::TypeId::kDecimal;
    case BYTEAOID:
      return iceberg::TypeId::kBinary;
    case UUIDOID:
      return iceberg::TypeId::kUuid;
    case DATEOID:
      return iceberg::TypeId::kDate;
    case TIMEOID:
      return iceberg::TypeId::kTime;
    case TIMESTAMPOID:
      return iceberg::TypeId::kTimestamp;
    case TIMESTAMPTZOID:
      return iceberg::TypeId::kTimestampTz;
    case TEXTOID:
    case VARCHAROID:
    case BPCHAROID:
      return iceberg::TypeId::kString;
    default:
      return std::nullopt;
  }
}

Oid IcebergTypeToPgOid(const iceberg::Type& type) {
  switch (CanonicalTypeIdImpl(type.type_id())) {
    case iceberg::TypeId::kBoolean:
      return BOOLOID;
    case iceberg::TypeId::kInt:
      return INT4OID;
    case iceberg::TypeId::kLong:
      return INT8OID;
    case iceberg::TypeId::kFloat:
      return FLOAT4OID;
    case iceberg::TypeId::kDouble:
      return FLOAT8OID;
    case iceberg::TypeId::kDecimal:
      return NUMERICOID;
    case iceberg::TypeId::kString:
      return TEXTOID;
    case iceberg::TypeId::kUuid:
      return UUIDOID;
    case iceberg::TypeId::kDate:
      return DATEOID;
    case iceberg::TypeId::kTime:
      return TIMEOID;
    case iceberg::TypeId::kTimestamp:
      return TIMESTAMPOID;
    case iceberg::TypeId::kTimestampTz:
      return TIMESTAMPTZOID;
    case iceberg::TypeId::kBinary:
    case iceberg::TypeId::kFixed:
      return BYTEAOID;
    default:
      return InvalidOid;
  }
}

std::optional<int32_t> ParseFieldIdValue(const char* value) {
  if (value == nullptr || value[0] == '\0') {
    return std::nullopt;
  }
  int32_t field_id = 0;
  const char* end = value + std::strlen(value);
  auto [ptr, ec] = std::from_chars(value, end, field_id);
  if (ec != std::errc{} || ptr != end || field_id <= 0) {
    return std::nullopt;
  }
  return field_id;
}

Result<std::unordered_map<AttrNumber, int32_t>> LoadTableAmFieldIds(Oid relid) {
  std::unordered_map<AttrNumber, int32_t> field_ids;
  const int connect_rc = SPI_connect();
  const bool started_here = (connect_rc == SPI_OK_CONNECT);
  if (!started_here && connect_rc != SPI_ERROR_CONNECT) {
    return std::unexpected(
        MakeError(ERRCODE_INTERNAL_ERROR, "could not connect to PostgreSQL SPI"));
  }

  auto finish_if_started = [started_here]() {
    if (started_here) {
      SPI_finish();
    }
  };

  const char* sql =
      "SELECT attnum, field_id FROM pgiceberg.column_bindings WHERE relid = $1";
  Oid argtypes[] = {OIDOID};
  Datum values[] = {ObjectIdGetDatum(relid)};
  const int result = SPI_execute_with_args(sql, 1, argtypes, values, nullptr, true, 0);
  if (result != SPI_OK_SELECT) {
    finish_if_started();
    return std::unexpected(
        MakeError(ERRCODE_INTERNAL_ERROR, "could not read pgiceberg column bindings"));
  }

  for (uint64 i = 0; i < SPI_processed; i++) {
    HeapTuple tuple = SPI_tuptable->vals[i];
    TupleDesc desc = SPI_tuptable->tupdesc;
    bool attnum_null = false;
    bool field_id_null = false;
    Datum attnum = SPI_getbinval(tuple, desc, 1, &attnum_null);
    Datum field_id = SPI_getbinval(tuple, desc, 2, &field_id_null);
    if (attnum_null || field_id_null) {
      finish_if_started();
      return std::unexpected(MakeError(ERRCODE_NULL_VALUE_NOT_ALLOWED,
                                       "pgiceberg column binding contains NULL"));
    }
    field_ids[DatumGetInt16(attnum)] = DatumGetInt32(field_id);
  }

  finish_if_started();
  return field_ids;
}

std::string LocalTypeName(Oid pg_type, int32 typmod) {
  if (pg_type == TIMESTAMPOID) {
    return "timestamp";
  }
  if (pg_type == TIMESTAMPTZOID) {
    return "timestamptz";
  }
  if (pg_type == TIMEOID) {
    return "time";
  }
  if (pg_type == VARCHAROID || pg_type == BPCHAROID) {
    return "text";
  }
  if (pg_type == NUMERICOID) {
    auto mapped = PostgresTypeToIcebergType(pg_type, typmod);
    if (mapped.has_value()) {
      auto sql = IcebergTypeToSql(**mapped);
      if (sql.has_value()) {
        return *sql;
      }
    }
  }
  char* name = format_type_be(pg_type);
  return name == nullptr ? std::string{} : std::string(name);
}

Result<Datum> LiteralToDatum(const iceberg::Literal& literal, Oid pg_type) {
  if (literal.IsNull() || literal.IsAboveMax() || literal.IsBelowMin()) {
    return std::unexpected(
        MakeError(ERRCODE_INVALID_PARAMETER_VALUE,
                  "Iceberg default value cannot be applied to PostgreSQL"));
  }

  const auto& value = literal.value();
  switch (literal.type()->type_id()) {
    case iceberg::TypeId::kBoolean:
      return BoolGetDatum(std::get<bool>(value));
    case iceberg::TypeId::kInt:
      if (pg_type == INT2OID) {
        return Int16GetDatum(static_cast<int16>(std::get<int32_t>(value)));
      }
      if (pg_type == INT8OID) {
        return Int64GetDatum(static_cast<int64>(std::get<int32_t>(value)));
      }
      return Int32GetDatum(std::get<int32_t>(value));
    case iceberg::TypeId::kLong:
      return Int64GetDatum(std::get<int64_t>(value));
    case iceberg::TypeId::kFloat:
      if (pg_type == FLOAT8OID) {
        return Float8GetDatum(static_cast<float8>(std::get<float>(value)));
      }
      return Float4GetDatum(std::get<float>(value));
    case iceberg::TypeId::kDouble:
      return Float8GetDatum(std::get<double>(value));
    case iceberg::TypeId::kString: {
      const auto& text = std::get<std::string>(value);
      return CStringGetTextDatum(text.c_str());
    }
    case iceberg::TypeId::kBinary:
    case iceberg::TypeId::kFixed: {
      const auto& bytes = std::get<std::vector<uint8_t>>(value);
      const auto size = bytes.size();
      auto* result = static_cast<bytea*>(palloc(VARHDRSZ + size));
      SET_VARSIZE(result, VARHDRSZ + size);
      if (size > 0) {
        std::memcpy(VARDATA(result), bytes.data(), size);
      }
      return PointerGetDatum(result);
    }
    case iceberg::TypeId::kUuid: {
      const auto& uuid = std::get<iceberg::Uuid>(value);
      auto* result = reinterpret_cast<pg_uuid_t*>(palloc(sizeof(pg_uuid_t)));
      const auto bytes = uuid.bytes();
      std::memcpy(result->data, bytes.data(), UUID_LEN);
      return UUIDPGetDatum(result);
    }
    case iceberg::TypeId::kDate: {
      const int32 days_since_unix = std::get<int32_t>(value);
      return DateADTGetDatum(days_since_unix - POSTGRES_EPOCH_JDATE + UNIX_EPOCH_JDATE);
    }
    case iceberg::TypeId::kTime:
      return TimeADTGetDatum(std::get<int64_t>(value));
    case iceberg::TypeId::kTimestamp:
    case iceberg::TypeId::kTimestampNs: {
      auto micros = std::get<int64_t>(value);
      if (literal.type()->type_id() == iceberg::TypeId::kTimestampNs) {
        micros /= 1000;
      }
      return TimestampGetDatum(micros - kPostgresUnixEpochOffsetMicros);
    }
    case iceberg::TypeId::kTimestampTz:
    case iceberg::TypeId::kTimestampTzNs: {
      auto micros = std::get<int64_t>(value);
      if (literal.type()->type_id() == iceberg::TypeId::kTimestampTzNs) {
        micros /= 1000;
      }
      return TimestampTzGetDatum(micros - kPostgresUnixEpochOffsetMicros);
    }
    case iceberg::TypeId::kDecimal: {
      const auto& decimal_type =
          static_cast<const iceberg::DecimalType&>(*literal.type());
      const auto& decimal = std::get<iceberg::Decimal>(value);
      PGICEBERG_ASSIGN_OR_RETURN(auto numeric_string,
                                 FromIcebergResult(decimal.ToString(decimal_type.scale()),
                                                   "format Iceberg decimal default"));
      return DirectFunctionCall3(numeric_in, CStringGetDatum(numeric_string.c_str()),
                                 ObjectIdGetDatum(InvalidOid), Int32GetDatum(-1));
    }
    default:
      break;
  }
  (void)pg_type;
  return std::unexpected(
      MakeError(ERRCODE_FEATURE_NOT_SUPPORTED,
                "unsupported Iceberg default type " + literal.type()->ToString()));
}

void AddChange(std::vector<SchemaChange>* changes, SchemaChange change) {
  changes->push_back(std::move(change));
}

}  // namespace

bool IsValidColumnOption(const char* name) {
  return name != nullptr && std::strcmp(name, kFieldIdOption) == 0;
}

Result<int32_t> ParseFieldIdOption(const char* value) {
  auto field_id = ParseFieldIdValue(value);
  if (!field_id.has_value()) {
    return std::unexpected(MakeError(ERRCODE_FDW_INVALID_ATTRIBUTE_VALUE,
                                     std::string("invalid pgiceberg field_id \"") +
                                         (value == nullptr ? "" : value) + "\"",
                                     "field_id must be a positive Iceberg field id."));
  }
  return *field_id;
}

Status ValidateFieldIdOption(const char* value) {
  PGICEBERG_ASSIGN_OR_RETURN(auto ignored, ParseFieldIdOption(value));
  (void)ignored;
  return Ok();
}

std::optional<int32_t> FieldIdFromOptionList(List* options) {
  if (options == nullptr) {
    return std::nullopt;
  }
  ListCell* cell = nullptr;
  foreach (cell, options) {
    auto* def = static_cast<DefElem*>(lfirst(cell));
    if (std::strcmp(def->defname, kFieldIdOption) == 0) {
      return ParseFieldIdValue(defGetString(def));
    }
  }
  return std::nullopt;
}

std::optional<int32_t> ArrowFieldId(const arrow::Field& field) {
  const auto metadata = field.metadata();
  if (metadata == nullptr) {
    return std::nullopt;
  }
  auto result = metadata->Get(iceberg::kParquetFieldIdKey);
  if (!result.ok()) {
    return std::nullopt;
  }
  return ParseFieldIdValue(result->c_str());
}

int ArrowFieldIndexById(const arrow::Schema& schema, int32_t field_id) {
  for (int i = 0; i < schema.num_fields(); i++) {
    auto id = ArrowFieldId(*schema.field(i));
    if (id.has_value() && *id == field_id) {
      return i;
    }
  }
  return -1;
}

iceberg::TypeId CanonicalTypeId(const iceberg::Type& type) {
  return CanonicalTypeIdImpl(type.type_id());
}

bool IcebergTypeReadableAs(const iceberg::Type& type, Oid pg_type, int32) {
  auto local = PostgresMappedTypeId(pg_type);
  if (!local.has_value()) {
    return false;
  }
  const auto remote = CanonicalTypeIdImpl(type.type_id());
  if (remote == iceberg::TypeId::kDecimal && *local == iceberg::TypeId::kDecimal) {
    return true;
  }
  if (remote == iceberg::TypeId::kFixed && *local == iceberg::TypeId::kBinary) {
    return true;
  }
  return IsWideningPromotion(remote, *local) || remote == *local ||
         (*local == iceberg::TypeId::kLong && remote == iceberg::TypeId::kInt) ||
         (*local == iceberg::TypeId::kDouble && remote == iceberg::TypeId::kFloat);
}

bool PostgresTypeWritableAs(Oid pg_type, int32, const iceberg::Type& type) {
  auto local = PostgresMappedTypeId(pg_type);
  if (!local.has_value()) {
    return false;
  }
  const auto remote = CanonicalTypeIdImpl(type.type_id());
  if (remote == iceberg::TypeId::kDecimal && *local == iceberg::TypeId::kDecimal) {
    return true;
  }
  if (remote == iceberg::TypeId::kFixed && *local == iceberg::TypeId::kBinary) {
    return true;
  }
  return IsWideningPromotion(*local, remote);
}

Result<std::vector<LocalColumn>> LoadLocalColumns(Relation relation) {
  TupleDesc desc = RelationGetDescr(relation);
  const Oid relid = RelationGetRelid(relation);
  const bool is_foreign = relation->rd_rel->relkind == RELKIND_FOREIGN_TABLE;
  std::unordered_map<AttrNumber, int32_t> tableam_field_ids;
  if (!is_foreign) {
    PGICEBERG_ASSIGN_OR_RETURN(tableam_field_ids, LoadTableAmFieldIds(relid));
  }

  std::vector<LocalColumn> columns;
  columns.reserve(desc->natts);
  for (int i = 0; i < desc->natts; i++) {
    Form_pg_attribute attr = TupleDescAttr(desc, i);
    if (attr->attisdropped) {
      continue;
    }
    LocalColumn column;
    column.attnum = attr->attnum;
    column.name = NameStr(attr->attname);
    column.pg_type = attr->atttypid;
    column.typmod = attr->atttypmod;
    column.not_null = attr->attnotnull;
    if (is_foreign) {
      column.field_id =
          FieldIdFromOptionList(GetForeignColumnOptions(relid, attr->attnum));
    } else {
      auto it = tableam_field_ids.find(attr->attnum);
      if (it != tableam_field_ids.end()) {
        column.field_id = it->second;
      }
    }
    columns.push_back(std::move(column));
  }
  return columns;
}

Result<SchemaBinding> BindSchema(const std::vector<LocalColumn>& local,
                                 const iceberg::Schema& schema) {
  SchemaBinding binding;
  std::unordered_map<int32_t, const LocalColumn*> by_field_id;
  std::unordered_map<std::string, const LocalColumn*> by_name;
  std::unordered_set<AttrNumber> matched;

  for (const auto& column : local) {
    if (column.field_id.has_value()) {
      if (!by_field_id.emplace(*column.field_id, &column).second) {
        return std::unexpected(
            MakeError(ERRCODE_DUPLICATE_OBJECT,
                      "duplicate Iceberg field_id " + std::to_string(*column.field_id) +
                          " on local columns",
                      "Each PostgreSQL column must map to a unique Iceberg field id."));
      }
    } else {
      by_name.emplace(column.name, &column);
    }
  }

  for (const auto& field : schema.fields()) {
    BoundField bound;
    bound.field_id = field.field_id();
    bound.iceberg_name = std::string(field.name());
    bound.iceberg_type = field.type();
    bound.optional = field.optional();
    bound.write_default = field.write_default();
    bound.initial_default = field.initial_default();

    const LocalColumn* column = nullptr;
    auto by_id = by_field_id.find(field.field_id());
    if (by_id != by_field_id.end()) {
      column = by_id->second;
    } else {
      auto by_name_it = by_name.find(std::string(field.name()));
      if (by_name_it != by_name.end()) {
        column = by_name_it->second;
      }
    }

    if (column != nullptr) {
      matched.insert(column->attnum);
      bound.attnum = column->attnum;
      bound.local_name = column->name;
      bound.pg_type = column->pg_type;
      bound.typmod = column->typmod;
      bound.had_field_id = column->field_id.has_value();
      bound.renamed = bound.local_name != bound.iceberg_name;
      auto mapped = PostgresTypeToIcebergType(column->pg_type, column->typmod);
      bound.type_changed = !mapped.has_value() || !TypesMatch(*field.type(), **mapped);
      bound.readable =
          IcebergTypeReadableAs(*field.type(), column->pg_type, column->typmod);
      bound.writable =
          PostgresTypeWritableAs(column->pg_type, column->typmod, *field.type());
      if (!bound.readable) {
        bound.incompatibility = "Iceberg field \"" + bound.iceberg_name + "\" (id " +
                                std::to_string(bound.field_id) +
                                ") is not readable as PostgreSQL type " +
                                LocalTypeName(column->pg_type, column->typmod);
      } else if (!bound.writable) {
        bound.incompatibility = "PostgreSQL column \"" + bound.local_name +
                                "\" cannot be written as Iceberg field \"" +
                                bound.iceberg_name + "\"";
      }
    }
    binding.fields.push_back(std::move(bound));
  }

  for (const auto& column : local) {
    if (!matched.contains(column.attnum)) {
      binding.unmatched_local.push_back(column);
    }
  }
  return binding;
}

Result<std::vector<SchemaChange>> DiffSchema(const SchemaBinding& binding) {
  std::vector<SchemaChange> changes;
  for (const auto& field : binding.fields) {
    if (field.attnum == InvalidAttrNumber) {
      auto sql_type = IcebergTypeToSql(*field.iceberg_type);
      AddChange(
          &changes,
          SchemaChange{
              .kind = SchemaChangeKind::kAdded,
              .iceberg_field_id = field.field_id,
              .iceberg_name = field.iceberg_name,
              .iceberg_type =
                  sql_type.has_value() ? *sql_type : field.iceberg_type->ToString(),
              .detail = "Iceberg added field id " + std::to_string(field.field_id)});
      continue;
    }

    SchemaChange change;
    change.local_column = field.local_name;
    change.local_type = LocalTypeName(field.pg_type, field.typmod);
    change.iceberg_field_id = field.field_id;
    change.iceberg_name = field.iceberg_name;
    auto sql_type = IcebergTypeToSql(*field.iceberg_type);
    change.iceberg_type =
        sql_type.has_value() ? *sql_type : field.iceberg_type->ToString();

    if (!field.readable) {
      change.kind = SchemaChangeKind::kIncompatible;
      change.detail = field.incompatibility;
      AddChange(&changes, std::move(change));
      continue;
    }
    if (field.type_changed) {
      change.kind = SchemaChangeKind::kTypeChanged;
      change.detail = "local type " + *change.local_type + " differs from Iceberg type " +
                      *change.iceberg_type;
      AddChange(&changes, change);
    }
    if (field.renamed) {
      change.kind = SchemaChangeKind::kRenamed;
      change.detail = "local column \"" + field.local_name +
                      "\" maps to Iceberg field \"" + field.iceberg_name + "\" by id " +
                      std::to_string(field.field_id);
      AddChange(&changes, std::move(change));
    }
  }

  for (const auto& column : binding.unmatched_local) {
    if (!column.field_id.has_value()) {
      AddChange(&changes,
                SchemaChange{.kind = SchemaChangeKind::kMissingFieldId,
                             .local_column = column.name,
                             .local_type = LocalTypeName(column.pg_type, column.typmod),
                             .detail = "local column \"" + column.name +
                                       "\" has no Iceberg field_id"});
      continue;
    }
    AddChange(&changes,
              SchemaChange{.kind = SchemaChangeKind::kDropped,
                           .local_column = column.name,
                           .local_type = LocalTypeName(column.pg_type, column.typmod),
                           .iceberg_field_id = column.field_id,
                           .detail = "Iceberg dropped field id " +
                                     std::to_string(*column.field_id)});
  }
  return changes;
}

Status CheckScanCompatible(const SchemaBinding& binding,
                           const std::vector<int>& projected_attnums) {
  std::unordered_set<int> projected(projected_attnums.begin(), projected_attnums.end());
  for (const auto& field : binding.fields) {
    if (field.attnum == InvalidAttrNumber || !projected.contains(field.attnum)) {
      continue;
    }
    if (!field.readable) {
      return std::unexpected(MakeError(ERRCODE_FDW_INCONSISTENT_DESCRIPTOR_INFORMATION,
                                       field.incompatibility,
                                       "Call pgiceberg.refresh_schema() to update the "
                                       "local column type, or ALTER the column."));
    }
  }
  return Ok();
}

Status CheckWriteCompatible(const SchemaBinding& binding) {
  for (const auto& field : binding.fields) {
    if (field.attnum != InvalidAttrNumber) {
      if (!field.writable) {
        return std::unexpected(MakeError(
            ERRCODE_FDW_INCONSISTENT_DESCRIPTOR_INFORMATION, field.incompatibility,
            "Call pgiceberg.refresh_schema() to update the local column type."));
      }
      continue;
    }
    if (!field.optional && field.write_default == nullptr) {
      return std::unexpected(MakeError(
          ERRCODE_FDW_ERROR,
          "required Iceberg field \"" + field.iceberg_name + "\" (id " +
              std::to_string(field.field_id) +
              ") is missing from the local table and has no write-default",
          "Add the column locally with pgiceberg.refresh_schema(), or evolve the "
          "Iceberg schema to make the field optional or give it a write-default."));
    }
  }
  return Ok();
}

Result<std::string> ProjectedIcebergName(const LocalColumn& column,
                                         const iceberg::Schema& schema) {
  if (column.field_id.has_value()) {
    PGICEBERG_ASSIGN_OR_RETURN(auto field,
                               FromIcebergResult(schema.FindFieldById(*column.field_id),
                                                 "resolve Iceberg field id"));
    if (!field.has_value()) {
      return std::unexpected(MakeError(
          ERRCODE_FDW_ERROR,
          "column \"" + column.name + "\" field_id " + std::to_string(*column.field_id) +
              " does not exist in Iceberg table",
          "Call pgiceberg.refresh_schema() to drop stale local columns."));
    }
    return std::string(field->get().name());
  }
  return column.name;
}

Result<std::vector<std::string>> ProjectedIcebergNames(
    const std::vector<LocalColumn>& local, const std::vector<int>& attnums,
    const iceberg::Schema& schema) {
  std::unordered_map<AttrNumber, const LocalColumn*> by_attnum;
  for (const auto& column : local) {
    by_attnum[column.attnum] = &column;
  }

  std::vector<std::string> names;
  names.reserve(attnums.size());
  for (const auto attnum : attnums) {
    auto it = by_attnum.find(static_cast<AttrNumber>(attnum));
    if (it == by_attnum.end()) {
      continue;
    }
    if (it->second->field_id.has_value()) {
      auto field = schema.FindFieldById(*it->second->field_id);
      if (!field.has_value() || !field->has_value()) {
        continue;
      }
      names.emplace_back(field->value().get().name());
      continue;
    }
    names.push_back(it->second->name);
  }
  return names;
}

Result<int> BatchColumnIndex(const arrow::Schema& schema, const LocalColumn& column) {
  if (column.field_id.has_value()) {
    const int by_id = ArrowFieldIndexById(schema, *column.field_id);
    if (by_id >= 0) {
      return by_id;
    }
  }
  const int by_name = schema.GetFieldIndex(column.name);
  if (by_name >= 0) {
    return by_name;
  }
  if (column.field_id.has_value()) {
    return -1;
  }
  return std::unexpected(MakeError(
      ERRCODE_FDW_ERROR,
      std::string("column \"") + column.name + "\" does not exist in Iceberg table"));
}

Status AppendLiteral(arrow::ArrayBuilder& builder, const iceberg::Literal& literal,
                     Oid pg_type, const arrow::DataType& arrow_type) {
  if (literal.IsNull()) {
    return FromArrowStatus(builder.AppendNull(), "append Iceberg default NULL");
  }
  const Oid target_type =
      pg_type == InvalidOid ? IcebergTypeToPgOid(*literal.type()) : pg_type;
  PGICEBERG_ASSIGN_OR_RETURN(auto datum, LiteralToDatum(literal, target_type));
  return AppendDatum(builder, datum, target_type, false, arrow_type);
}

const char* SchemaChangeKindName(SchemaChangeKind kind) {
  switch (kind) {
    case SchemaChangeKind::kAdded:
      return "added";
    case SchemaChangeKind::kDropped:
      return "dropped";
    case SchemaChangeKind::kRenamed:
      return "renamed";
    case SchemaChangeKind::kTypeChanged:
      return "type_changed";
    case SchemaChangeKind::kIncompatible:
      return "incompatible";
    case SchemaChangeKind::kMissingFieldId:
      return "missing_field_id";
  }
  return "unknown";
}

std::string JsonEscape(std::string_view value) {
  std::string escaped;
  escaped.reserve(value.size());
  for (const unsigned char ch : value) {
    switch (ch) {
      case '"':
        escaped += "\\\"";
        break;
      case '\\':
        escaped += "\\\\";
        break;
      case '\n':
        escaped += "\\n";
        break;
      case '\r':
        escaped += "\\r";
        break;
      case '\t':
        escaped += "\\t";
        break;
      default:
        if (ch < 0x20) {
          char buf[8];
          snprintf(buf, sizeof(buf), "\\u%04x", ch);
          escaped += buf;
        } else {
          escaped.push_back(static_cast<char>(ch));
        }
        break;
    }
  }
  return escaped;
}

std::string SchemaChangesToJson(const std::vector<SchemaChange>& changes) {
  std::ostringstream json;
  json << "[";
  for (std::size_t i = 0; i < changes.size(); i++) {
    const auto& change = changes[i];
    if (i > 0) {
      json << ",";
    }
    json << "{\"change\":\"" << SchemaChangeKindName(change.kind) << "\"";
    auto emit_string = [&](const char* key, const std::optional<std::string>& value) {
      json << ",\"" << key << "\":";
      if (!value.has_value()) {
        json << "null";
      } else {
        json << "\"" << JsonEscape(*value) << "\"";
      }
    };
    emit_string("local_column", change.local_column);
    emit_string("local_type", change.local_type);
    json << ",\"iceberg_field_id\":";
    if (change.iceberg_field_id.has_value()) {
      json << *change.iceberg_field_id;
    } else {
      json << "null";
    }
    emit_string("iceberg_name", change.iceberg_name);
    emit_string("iceberg_type", change.iceberg_type);
    json << ",\"detail\":\"" << JsonEscape(change.detail) << "\"}";
  }
  json << "]";
  return json.str();
}

Status InsertColumnBindings(Relation relation) {
  TupleDesc desc = RelationGetDescr(relation);
  const Oid relid = RelationGetRelid(relation);
  const int connect_rc = SPI_connect();
  const bool started_here = (connect_rc == SPI_OK_CONNECT);
  if (!started_here && connect_rc != SPI_ERROR_CONNECT) {
    return std::unexpected(
        MakeError(ERRCODE_INTERNAL_ERROR, "could not connect to PostgreSQL SPI"));
  }
  auto finish_if_started = [started_here]() {
    if (started_here) {
      SPI_finish();
    }
  };

  int32_t field_id = 1;
  for (int i = 0; i < desc->natts; i++) {
    Form_pg_attribute attr = TupleDescAttr(desc, i);
    if (attr->attisdropped) {
      continue;
    }
    const char* sql =
        "INSERT INTO pgiceberg.column_bindings (relid, attnum, field_id) "
        "VALUES ($1, $2, $3)";
    Oid argtypes[] = {OIDOID, INT2OID, INT4OID};
    Datum values[] = {ObjectIdGetDatum(relid), Int16GetDatum(attr->attnum),
                      Int32GetDatum(field_id)};
    const int result = SPI_execute_with_args(sql, 3, argtypes, values, nullptr, false, 1);
    if (result != SPI_OK_INSERT) {
      finish_if_started();
      return std::unexpected(
          MakeError(ERRCODE_INTERNAL_ERROR, "could not insert pgiceberg column binding"));
    }
    field_id++;
  }
  finish_if_started();
  return Ok();
}

Result<std::optional<int32_t>> LoadBoundFieldId(Oid relid, AttrNumber attnum) {
  PGICEBERG_ASSIGN_OR_RETURN(auto field_ids, LoadTableAmFieldIds(relid));
  auto it = field_ids.find(attnum);
  if (it == field_ids.end()) {
    return std::nullopt;
  }
  return it->second;
}

}  // namespace pgiceberg
