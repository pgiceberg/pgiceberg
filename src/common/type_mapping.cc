#include "common/type_mapping.h"

#include <cstdint>
#include <sstream>
#include <stdexcept>

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

DecimalTypmod DecodeNumericTypmod(int32 typmod) {
  if (typmod < static_cast<int32>(VARHDRSZ)) {
    return {};
  }

  const int32 payload = typmod - static_cast<int32>(VARHDRSZ);
  const int32 precision = (payload >> 16) & 0xffff;
  const int32 scale = static_cast<int16_t>(payload & 0xffff);
  if (precision <= 0 || precision > kMaxIcebergDecimalPrecision) {
    throw std::runtime_error(
        "PostgreSQL numeric precision is not supported "
        "for Iceberg decimal: " +
        std::to_string(precision));
  }
  if (scale < 0) {
    throw std::runtime_error(
        "PostgreSQL numeric negative scale is not "
        "supported for Iceberg decimal: " +
        std::to_string(scale));
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

std::shared_ptr<iceberg::Type> PostgresTypeToIcebergType(Oid pg_type, int32 typmod) {
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
      const auto decimal = DecodeNumericTypmod(typmod);
      return iceberg::decimal(decimal.precision, decimal.scale);
    }
    case DATEOID:
      return iceberg::date();
    case TIMESTAMPOID:
      return iceberg::timestamp();
    case TIMESTAMPTZOID:
      return iceberg::timestamp_tz();
    case TEXTOID:
    case VARCHAROID:
    case BPCHAROID:
      return iceberg::string();
    default:
      throw std::runtime_error(std::string("PostgreSQL type ") + format_type_be(pg_type) +
                               " is not supported for Iceberg schema mapping");
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
    case iceberg::TypeId::kUuid:
      return "text";
    case iceberg::TypeId::kDate:
      return "date";
    case iceberg::TypeId::kTime:
      return "time";
    case iceberg::TypeId::kTimestamp:
      return "timestamp";
    case iceberg::TypeId::kTimestampTz:
      return "timestamptz";
    case iceberg::TypeId::kBinary:
    case iceberg::TypeId::kFixed:
      return "bytea";
    default:
      return "text";
  }
}

}  // namespace pgiceberg
