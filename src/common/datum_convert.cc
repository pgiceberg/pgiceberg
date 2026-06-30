#include "common/datum_convert.h"

#include <cstring>
#include <memory>
#include <optional>
#include <string>

#include <arrow/array.h>
#include <arrow/array/builder_base.h>
#include <arrow/scalar.h>

#include "common/status.h"

extern "C" {
#include "postgres.h"
#include "catalog/pg_type_d.h"
#include "utils/builtins.h"
#include "utils/date.h"
#include "utils/timestamp.h"
}

namespace pgiceberg {
namespace {

constexpr std::int64_t kPostgresUnixEpochOffsetMicros = 946684800000000LL;

std::optional<std::int64_t> IntegerValue(const arrow::Array& array, std::int64_t offset) {
  switch (array.type_id()) {
    case arrow::Type::INT8:
      return static_cast<const arrow::Int8Array&>(array).Value(offset);
    case arrow::Type::INT16:
      return static_cast<const arrow::Int16Array&>(array).Value(offset);
    case arrow::Type::INT32:
      return static_cast<const arrow::Int32Array&>(array).Value(offset);
    case arrow::Type::INT64:
      return static_cast<const arrow::Int64Array&>(array).Value(offset);
    case arrow::Type::UINT8:
      return static_cast<const arrow::UInt8Array&>(array).Value(offset);
    case arrow::Type::UINT16:
      return static_cast<const arrow::UInt16Array&>(array).Value(offset);
    case arrow::Type::UINT32:
      return static_cast<std::int64_t>(
          static_cast<const arrow::UInt32Array&>(array).Value(offset));
    case arrow::Type::UINT64:
      return static_cast<std::int64_t>(
          static_cast<const arrow::UInt64Array&>(array).Value(offset));
    default:
      return std::nullopt;
  }
}

std::optional<double> FloatingValue(const arrow::Array& array, std::int64_t offset) {
  switch (array.type_id()) {
    case arrow::Type::FLOAT:
      return static_cast<const arrow::FloatArray&>(array).Value(offset);
    case arrow::Type::DOUBLE:
      return static_cast<const arrow::DoubleArray&>(array).Value(offset);
    default:
      if (auto integer = IntegerValue(array, offset); integer.has_value()) {
        return static_cast<double>(*integer);
      }
      return std::nullopt;
  }
}

std::optional<std::string> StringValue(const arrow::Array& array, std::int64_t offset) {
  switch (array.type_id()) {
    case arrow::Type::STRING:
      return static_cast<const arrow::StringArray&>(array).GetString(offset);
    case arrow::Type::LARGE_STRING:
      return static_cast<const arrow::LargeStringArray&>(array).GetString(offset);
    default:
      return std::nullopt;
  }
}

std::int64_t TimestampMicros(const arrow::Array& array, std::int64_t offset) {
  auto timestamp_type = std::static_pointer_cast<arrow::TimestampType>(array.type());
  std::int64_t value = static_cast<const arrow::TimestampArray&>(array).Value(offset);
  switch (timestamp_type->unit()) {
    case arrow::TimeUnit::SECOND:
      value *= 1000000LL;
      break;
    case arrow::TimeUnit::MILLI:
      value *= 1000LL;
      break;
    case arrow::TimeUnit::MICRO:
      break;
    case arrow::TimeUnit::NANO:
      value /= 1000LL;
      break;
  }
  return value - kPostgresUnixEpochOffsetMicros;
}

}  // namespace

Result<Datum> ConvertValue(const arrow::Array& array, std::int64_t offset, Oid pg_type,
                           bool& is_null) {
  if (array.IsNull(offset)) {
    is_null = true;
    return static_cast<Datum>(0);
  }

  is_null = false;
  switch (pg_type) {
    case INT2OID:
      if (auto value = IntegerValue(array, offset); value.has_value()) {
        return Int16GetDatum(static_cast<int16>(*value));
      }
      break;
    case INT4OID:
      if (auto value = IntegerValue(array, offset); value.has_value()) {
        return Int32GetDatum(static_cast<int32>(*value));
      }
      break;
    case INT8OID:
      if (auto value = IntegerValue(array, offset); value.has_value()) {
        return Int64GetDatum(static_cast<int64>(*value));
      }
      break;
    case FLOAT4OID:
      if (auto value = FloatingValue(array, offset); value.has_value()) {
        return Float4GetDatum(static_cast<float4>(*value));
      }
      break;
    case FLOAT8OID:
      if (auto value = FloatingValue(array, offset); value.has_value()) {
        return Float8GetDatum(static_cast<float8>(*value));
      }
      break;
    case TEXTOID:
    case VARCHAROID:
    case BPCHAROID:
      if (auto value = StringValue(array, offset); value.has_value()) {
        return CStringGetTextDatum(value->c_str());
      }
      break;
    case BOOLOID:
      if (array.type_id() == arrow::Type::BOOL) {
        return BoolGetDatum(static_cast<const arrow::BooleanArray&>(array).Value(offset));
      }
      break;
    case DATEOID:
      if (array.type_id() == arrow::Type::DATE32) {
        const int32 days_since_unix =
            static_cast<const arrow::Date32Array&>(array).Value(offset);
        return DateADTGetDatum(days_since_unix - POSTGRES_EPOCH_JDATE + UNIX_EPOCH_JDATE);
      }
      break;
    case TIMESTAMPOID:
    case TIMESTAMPTZOID:
      if (array.type_id() == arrow::Type::TIMESTAMP) {
        const auto value = TimestampMicros(array, offset);
        return (pg_type == TIMESTAMPOID) ? TimestampGetDatum(value)
                                         : TimestampTzGetDatum(value);
      }
      break;
    default:
      break;
  }

  return std::unexpected(
      MakeError(ERRCODE_FEATURE_NOT_SUPPORTED,
                "unsupported pgiceberg column conversion from Arrow type " +
                    array.type()->ToString()));
}

Result<std::shared_ptr<arrow::Scalar>> ScalarFromDatum(Datum datum,
                                                       const arrow::DataType& type) {
  switch (type.id()) {
    case arrow::Type::BOOL:
      return std::make_shared<arrow::BooleanScalar>(DatumGetBool(datum));
    case arrow::Type::INT16:
      return std::make_shared<arrow::Int16Scalar>(DatumGetInt16(datum));
    case arrow::Type::INT32:
    case arrow::Type::DATE32: {
      if (type.id() == arrow::Type::DATE32) {
        const auto days =
            DatumGetDateADT(datum) + POSTGRES_EPOCH_JDATE - UNIX_EPOCH_JDATE;
        return std::make_shared<arrow::Date32Scalar>(days);
      }
      return std::make_shared<arrow::Int32Scalar>(DatumGetInt32(datum));
    }
    case arrow::Type::INT64:
      return std::make_shared<arrow::Int64Scalar>(DatumGetInt64(datum));
    case arrow::Type::FLOAT:
      return std::make_shared<arrow::FloatScalar>(DatumGetFloat4(datum));
    case arrow::Type::DOUBLE:
      return std::make_shared<arrow::DoubleScalar>(DatumGetFloat8(datum));
    case arrow::Type::STRING:
    case arrow::Type::LARGE_STRING: {
      char* text_value = TextDatumGetCString(datum);
      return std::make_shared<arrow::StringScalar>(std::string(text_value));
    }
    case arrow::Type::TIMESTAMP: {
      const auto micros = DatumGetTimestamp(datum) + kPostgresUnixEpochOffsetMicros;
      return std::make_shared<arrow::TimestampScalar>(
          micros, std::static_pointer_cast<arrow::TimestampType>(
                      std::const_pointer_cast<arrow::DataType>(type.shared_from_this())));
    }
    default:
      return std::unexpected(
          MakeError(ERRCODE_FEATURE_NOT_SUPPORTED,
                    "unsupported pgiceberg INSERT Arrow type " + type.ToString()));
  }
}

Status AppendDatum(arrow::ArrayBuilder& builder, Datum value, bool is_null,
                   const arrow::DataType& type) {
  if (is_null) {
    return FromArrowStatus(builder.AppendNull(), "append Arrow NULL");
  }
  PGICEBERG_ASSIGN_OR_RETURN(auto scalar, ScalarFromDatum(value, type));
  return FromArrowStatus(builder.AppendScalar(*scalar), "append Arrow scalar");
}

Result<bool> DatumEquals(Datum left, Datum right, Oid pg_type) {
  switch (pg_type) {
    case BOOLOID:
      return DatumGetBool(left) == DatumGetBool(right);
    case INT2OID:
      return DatumGetInt16(left) == DatumGetInt16(right);
    case INT4OID:
      return DatumGetInt32(left) == DatumGetInt32(right);
    case INT8OID:
      return DatumGetInt64(left) == DatumGetInt64(right);
    case FLOAT4OID:
      return DatumGetFloat4(left) == DatumGetFloat4(right);
    case FLOAT8OID:
      return DatumGetFloat8(left) == DatumGetFloat8(right);
    case DATEOID:
      return DatumGetDateADT(left) == DatumGetDateADT(right);
    case TIMESTAMPOID:
      return DatumGetTimestamp(left) == DatumGetTimestamp(right);
    case TIMESTAMPTZOID:
      return DatumGetTimestampTz(left) == DatumGetTimestampTz(right);
    case TEXTOID:
    case VARCHAROID:
    case BPCHAROID: {
      char* left_text = TextDatumGetCString(left);
      char* right_text = TextDatumGetCString(right);
      return std::strcmp(left_text, right_text) == 0;
    }
    default:
      return std::unexpected(MakeError(ERRCODE_FEATURE_NOT_SUPPORTED,
                                       "unsupported pgiceberg row identity type"));
  }
}

}  // namespace pgiceberg
