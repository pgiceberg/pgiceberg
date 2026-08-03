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

#include "common/type_mapping.h"

#include <cstdint>
#include <sstream>

#include <arrow/type.h>
#include <iceberg/type.h>

extern "C" {
#include "catalog/pg_type_d.h"
#include "utils/builtins.h"
}

namespace pgiceberg {
namespace {

constexpr int32 kDefaultDecimalPrecision = 38;
constexpr int32 kDefaultDecimalScale = 0;
constexpr int32 kMaxIcebergDecimalPrecision = 38;

struct DecimalTypmod {
  int32 precision = kDefaultDecimalPrecision;
  int32 scale = kDefaultDecimalScale;
};

Result<DecimalTypmod> DecodeNumericTypmod(int32 typmod) {
  if (typmod < static_cast<int32>(VARHDRSZ)) {
    return DecimalTypmod{};
  }

  const int32 payload = typmod - static_cast<int32>(VARHDRSZ);
  const int32 precision = (payload >> 16) & 0xffff;
  const int32 scale = static_cast<int16_t>(payload & 0xffff);
  if (precision <= 0 || precision > kMaxIcebergDecimalPrecision) {
    return std::unexpected(
        MakeError(ERRCODE_INVALID_PARAMETER_VALUE,
                  "PostgreSQL numeric precision is not supported for Iceberg decimal: " +
                      std::to_string(precision)));
  }
  if (scale < 0) {
    return std::unexpected(MakeError(
        ERRCODE_INVALID_PARAMETER_VALUE,
        "PostgreSQL numeric negative scale is not supported for Iceberg decimal: " +
            std::to_string(scale)));
  }
  return DecimalTypmod{.precision = precision, .scale = scale};
}

std::string DecimalSql(const iceberg::Type& type) {
  const auto& decimal = static_cast<const iceberg::DecimalType&>(type);
  std::ostringstream sql;
  sql << "numeric(" << decimal.precision() << ", " << decimal.scale() << ")";
  return sql.str();
}

}  // namespace

Result<std::shared_ptr<iceberg::Type>> PostgresTypeToIcebergType(Oid pg_type,
                                                                 int32 typmod) {
  switch (pg_type) {
    case BOOLOID:
      return iceberg::boolean();
    case INT2OID:
    case INT4OID:
      return iceberg::int32();
    case INT8OID:
      return iceberg::int64();
    case FLOAT4OID:
      return iceberg::float32();
    case FLOAT8OID:
      return iceberg::float64();
    case NUMERICOID: {
      PGICEBERG_ASSIGN_OR_RETURN(auto decimal, DecodeNumericTypmod(typmod));
      return iceberg::decimal(decimal.precision, decimal.scale);
    }
    case BYTEAOID:
      return iceberg::binary();
    case UUIDOID:
      return iceberg::uuid();
    case DATEOID:
      return iceberg::date();
    case TIMEOID:
      return iceberg::time();
    case TIMESTAMPOID:
      return iceberg::timestamp();
    case TIMESTAMPTZOID:
      return iceberg::timestamp_tz();
    case TEXTOID:
    case VARCHAROID:
    case BPCHAROID:
      return iceberg::string();
    default:
      return std::unexpected(
          MakeError(ERRCODE_FEATURE_NOT_SUPPORTED,
                    std::string("PostgreSQL type ") + format_type_be(pg_type) +
                        " is not supported for Iceberg schema mapping"));
  }
}

std::string IcebergTypeToSql(const iceberg::Type& type) {
  switch (type.type_id()) {
    case iceberg::TypeId::kBoolean:
      return "boolean";
    case iceberg::TypeId::kInt:
      return "integer";
    case iceberg::TypeId::kLong:
      return "bigint";
    case iceberg::TypeId::kFloat:
      return "real";
    case iceberg::TypeId::kDouble:
      return "double precision";
    case iceberg::TypeId::kDecimal:
      return DecimalSql(type);
    case iceberg::TypeId::kString:
      return "text";
    case iceberg::TypeId::kUuid:
      return "uuid";
    case iceberg::TypeId::kDate:
      return "date";
    case iceberg::TypeId::kTime:
      return "time";
    case iceberg::TypeId::kTimestamp:
    case iceberg::TypeId::kTimestampNs:
      return "timestamp";
    case iceberg::TypeId::kTimestampTz:
    case iceberg::TypeId::kTimestampTzNs:
      return "timestamptz";
    case iceberg::TypeId::kBinary:
    case iceberg::TypeId::kFixed:
      return "bytea";
    default:
      return "text";
  }
}

std::string ArrowTypeToSql(const arrow::DataType& type) {
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

}  // namespace pgiceberg
