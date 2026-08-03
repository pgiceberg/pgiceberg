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

#include "common/datum_convert.h"

#include <cstring>
#include <limits>
#include <memory>
#include <optional>
#include <string>
#include <string_view>

#include <arrow/array.h>
#include <arrow/array/builder_base.h>
#include <arrow/buffer.h>
#include <arrow/extension_type.h>
#include <arrow/scalar.h>
#include <arrow/type.h>
#include <arrow/util/decimal.h>

#include "common/status.h"

extern "C" {
#include "postgres.h"
#include "catalog/pg_type_d.h"
#include "utils/fmgrprotos.h"
#include "utils/builtins.h"
#include "utils/date.h"
#include "utils/numeric.h"
#include "utils/timestamp.h"
#include "utils/uuid.h"
#include "varatt.h"
}

namespace pgiceberg {
namespace {

Result<std::optional<std::int64_t>> IntegerValue(const arrow::Array& array,
                                                 std::int64_t offset) {
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
    case arrow::Type::UINT64: {
      const auto value = static_cast<const arrow::UInt64Array&>(array).Value(offset);
      if (value > static_cast<std::uint64_t>(std::numeric_limits<std::int64_t>::max())) {
        return std::unexpected(MakeError(
            ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE,
            "Arrow unsigned integer value is out of range for PostgreSQL bigint"));
      }
      return static_cast<std::int64_t>(value);
    }
    default:
      return std::nullopt;
  }
}

template <typename T>
Result<T> CheckedIntegerCast(std::int64_t value, std::string_view type_name) {
  if (value < static_cast<std::int64_t>(std::numeric_limits<T>::min()) ||
      value > static_cast<std::int64_t>(std::numeric_limits<T>::max())) {
    return std::unexpected(MakeError(
        ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE,
        "Arrow integer value is out of range for PostgreSQL " + std::string(type_name)));
  }
  return static_cast<T>(value);
}

Result<std::optional<double>> FloatingValue(const arrow::Array& array,
                                            std::int64_t offset) {
  switch (array.type_id()) {
    case arrow::Type::FLOAT:
      return static_cast<const arrow::FloatArray&>(array).Value(offset);
    case arrow::Type::DOUBLE:
      return static_cast<const arrow::DoubleArray&>(array).Value(offset);
    case arrow::Type::UINT64:
      return static_cast<double>(
          static_cast<const arrow::UInt64Array&>(array).Value(offset));
    default:
      PGICEBERG_ASSIGN_OR_RETURN(auto integer, IntegerValue(array, offset));
      if (integer.has_value()) {
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

std::optional<std::string> DecimalValue(const arrow::Array& array, std::int64_t offset) {
  switch (array.type_id()) {
    case arrow::Type::DECIMAL32:
      return static_cast<const arrow::Decimal32Array&>(array).FormatValue(offset);
    case arrow::Type::DECIMAL64:
      return static_cast<const arrow::Decimal64Array&>(array).FormatValue(offset);
    case arrow::Type::DECIMAL128:
      return static_cast<const arrow::Decimal128Array&>(array).FormatValue(offset);
    case arrow::Type::DECIMAL256:
      return static_cast<const arrow::Decimal256Array&>(array).FormatValue(offset);
    default:
      return std::nullopt;
  }
}

std::optional<std::string_view> BinaryValue(const arrow::Array& array,
                                            std::int64_t offset) {
  switch (array.type_id()) {
    case arrow::Type::BINARY:
      return static_cast<const arrow::BinaryArray&>(array).GetView(offset);
    case arrow::Type::LARGE_BINARY:
      return static_cast<const arrow::LargeBinaryArray&>(array).GetView(offset);
    case arrow::Type::FIXED_SIZE_BINARY:
      return static_cast<const arrow::FixedSizeBinaryArray&>(array).GetView(offset);
    case arrow::Type::EXTENSION:
      return BinaryValue(*static_cast<const arrow::ExtensionArray&>(array).storage(),
                         offset);
    default:
      return std::nullopt;
  }
}

Datum ByteaGetDatum(std::string_view value) {
  auto* result = reinterpret_cast<bytea*>(palloc(VARHDRSZ + value.size()));
  SET_VARSIZE(result, VARHDRSZ + value.size());
  std::memcpy(VARDATA(result), value.data(), value.size());
  return PointerGetDatum(result);
}

Result<Datum> UuidGetDatum(std::string_view value) {
  if (value.size() != UUID_LEN) {
    return std::unexpected(MakeError(ERRCODE_INVALID_PARAMETER_VALUE,
                                     "Arrow fixed-size binary UUID must be 16 bytes"));
  }
  auto* result = reinterpret_cast<pg_uuid_t*>(palloc(sizeof(pg_uuid_t)));
  std::memcpy(result->data, value.data(), UUID_LEN);
  return UUIDPGetDatum(result);
}

Datum NumericDatumFromString(std::string_view value) {
  std::string value_string(value);
  return DirectFunctionCall3(numeric_in, CStringGetDatum(value_string.c_str()),
                             ObjectIdGetDatum(InvalidOid), Int32GetDatum(-1));
}

std::string NumericToString(Datum value) {
  auto* numeric_string = DatumGetCString(DirectFunctionCall1(numeric_out, value));
  return std::string(numeric_string);
}

std::int64_t ScaleToMicros(std::int64_t value, arrow::TimeUnit::type unit) {
  switch (unit) {
    case arrow::TimeUnit::SECOND:
      return value * 1000000LL;
    case arrow::TimeUnit::MILLI:
      return value * 1000LL;
    case arrow::TimeUnit::MICRO:
      return value;
    case arrow::TimeUnit::NANO:
      return value / 1000LL;
  }
  return value;
}

std::int64_t ScaleFromMicros(std::int64_t value, arrow::TimeUnit::type unit) {
  switch (unit) {
    case arrow::TimeUnit::SECOND:
      return value / 1000000LL;
    case arrow::TimeUnit::MILLI:
      return value / 1000LL;
    case arrow::TimeUnit::MICRO:
      return value;
    case arrow::TimeUnit::NANO:
      return value * 1000LL;
  }
  return value;
}

std::int64_t TimestampMicros(const arrow::Array& array, std::int64_t offset) {
  auto timestamp_type = std::static_pointer_cast<arrow::TimestampType>(array.type());
  const auto value = static_cast<const arrow::TimestampArray&>(array).Value(offset);
  return ScaleToMicros(value, timestamp_type->unit()) - kPostgresUnixEpochOffsetMicros;
}

std::optional<std::int64_t> TimeMicros(const arrow::Array& array, std::int64_t offset) {
  switch (array.type_id()) {
    case arrow::Type::TIME32: {
      auto time_type = std::static_pointer_cast<arrow::Time32Type>(array.type());
      return ScaleToMicros(static_cast<const arrow::Time32Array&>(array).Value(offset),
                           time_type->unit());
    }
    case arrow::Type::TIME64: {
      auto time_type = std::static_pointer_cast<arrow::Time64Type>(array.type());
      return ScaleToMicros(static_cast<const arrow::Time64Array&>(array).Value(offset),
                           time_type->unit());
    }
    default:
      return std::nullopt;
  }
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
    case INT2OID: {
      PGICEBERG_ASSIGN_OR_RETURN(auto value, IntegerValue(array, offset));
      if (value.has_value()) {
        PGICEBERG_ASSIGN_OR_RETURN(auto checked,
                                   CheckedIntegerCast<int16>(*value, "smallint"));
        return Int16GetDatum(checked);
      }
      break;
    }
    case INT4OID: {
      PGICEBERG_ASSIGN_OR_RETURN(auto value, IntegerValue(array, offset));
      if (value.has_value()) {
        PGICEBERG_ASSIGN_OR_RETURN(auto checked,
                                   CheckedIntegerCast<int32>(*value, "integer"));
        return Int32GetDatum(checked);
      }
      break;
    }
    case INT8OID: {
      PGICEBERG_ASSIGN_OR_RETURN(auto value, IntegerValue(array, offset));
      if (value.has_value()) {
        return Int64GetDatum(static_cast<int64>(*value));
      }
      break;
    }
    case FLOAT4OID: {
      PGICEBERG_ASSIGN_OR_RETURN(auto value, FloatingValue(array, offset));
      if (value.has_value()) {
        return Float4GetDatum(static_cast<float4>(*value));
      }
      break;
    }
    case FLOAT8OID: {
      PGICEBERG_ASSIGN_OR_RETURN(auto value, FloatingValue(array, offset));
      if (value.has_value()) {
        return Float8GetDatum(static_cast<float8>(*value));
      }
      break;
    }
    case NUMERICOID:
      if (auto value = DecimalValue(array, offset); value.has_value()) {
        return NumericDatumFromString(*value);
      }
      break;
    case TEXTOID:
    case VARCHAROID:
    case BPCHAROID:
      if (auto value = StringValue(array, offset); value.has_value()) {
        return CStringGetTextDatum(value->c_str());
      }
      break;
    case BYTEAOID:
      if (auto value = BinaryValue(array, offset); value.has_value()) {
        return ByteaGetDatum(*value);
      }
      break;
    case UUIDOID:
      if (auto value = BinaryValue(array, offset); value.has_value()) {
        return UuidGetDatum(*value);
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
    case TIMEOID:
      if (auto value = TimeMicros(array, offset); value.has_value()) {
        return TimeADTGetDatum(*value);
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

Result<std::shared_ptr<arrow::Scalar>> ScalarFromDatum(Datum datum, Oid pg_type,
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
    case arrow::Type::DECIMAL128: {
      if (pg_type != NUMERICOID) {
        break;
      }
      const auto& decimal_type = static_cast<const arrow::DecimalType&>(type);
      const auto numeric_string = NumericToString(datum);
      arrow::Decimal128 decimal;
      int32 precision = 0;
      int32 scale = 0;
      PGICEBERG_RETURN_NOT_OK(FromArrowStatus(
          arrow::Decimal128::FromString(numeric_string, &decimal, &precision, &scale),
          "convert PostgreSQL numeric to Arrow decimal"));
      if (scale != decimal_type.scale()) {
        PGICEBERG_ASSIGN_OR_RETURN(
            decimal, FromArrowResult(decimal.Rescale(scale, decimal_type.scale()),
                                     "rescale PostgreSQL numeric to Arrow decimal"));
      }
      if (!decimal.FitsInPrecision(decimal_type.precision())) {
        return std::unexpected(
            MakeError(ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE,
                      "PostgreSQL numeric value is out of range for Arrow decimal"));
      }
      return std::make_shared<arrow::Decimal128Scalar>(
          decimal,
          std::static_pointer_cast<arrow::Decimal128Type>(
              std::const_pointer_cast<arrow::DataType>(type.shared_from_this())));
    }
    case arrow::Type::FLOAT:
      return std::make_shared<arrow::FloatScalar>(DatumGetFloat4(datum));
    case arrow::Type::DOUBLE:
      return std::make_shared<arrow::DoubleScalar>(DatumGetFloat8(datum));
    case arrow::Type::BINARY:
    case arrow::Type::LARGE_BINARY:
    case arrow::Type::FIXED_SIZE_BINARY: {
      std::string value;
      if (pg_type == BYTEAOID) {
        auto* bytea_value = DatumGetByteaPP(datum);
        value.assign(VARDATA_ANY(bytea_value), VARSIZE_ANY_EXHDR(bytea_value));
      } else if (pg_type == UUIDOID) {
        const auto* uuid = DatumGetUUIDP(datum);
        value.assign(reinterpret_cast<const char*>(uuid->data), UUID_LEN);
      } else {
        break;
      }
      if (type.id() == arrow::Type::FIXED_SIZE_BINARY) {
        const auto& fixed_type = static_cast<const arrow::FixedSizeBinaryType&>(type);
        if (value.size() != static_cast<std::size_t>(fixed_type.byte_width())) {
          return std::unexpected(MakeError(
              ERRCODE_STRING_DATA_LENGTH_MISMATCH,
              "PostgreSQL binary value length does not match Arrow fixed-size binary"));
        }
        return std::make_shared<arrow::FixedSizeBinaryScalar>(
            arrow::Buffer::FromString(std::move(value)),
            std::static_pointer_cast<arrow::FixedSizeBinaryType>(
                std::const_pointer_cast<arrow::DataType>(type.shared_from_this())));
      }
      if (type.id() == arrow::Type::LARGE_BINARY) {
        return std::make_shared<arrow::LargeBinaryScalar>(
            arrow::Buffer::FromString(std::move(value)),
            std::static_pointer_cast<arrow::LargeBinaryType>(
                std::const_pointer_cast<arrow::DataType>(type.shared_from_this())));
      }
      return std::make_shared<arrow::BinaryScalar>(
          arrow::Buffer::FromString(std::move(value)),
          std::static_pointer_cast<arrow::BinaryType>(
              std::const_pointer_cast<arrow::DataType>(type.shared_from_this())));
    }
    case arrow::Type::STRING:
    case arrow::Type::LARGE_STRING: {
      char* text_value = TextDatumGetCString(datum);
      return std::make_shared<arrow::StringScalar>(std::string(text_value));
    }
    case arrow::Type::TIME32: {
      const auto& time_type = static_cast<const arrow::Time32Type&>(type);
      return std::make_shared<arrow::Time32Scalar>(
          static_cast<int32>(ScaleFromMicros(DatumGetTimeADT(datum), time_type.unit())),
          std::static_pointer_cast<arrow::Time32Type>(
              std::const_pointer_cast<arrow::DataType>(type.shared_from_this())));
    }
    case arrow::Type::TIME64: {
      const auto& time_type = static_cast<const arrow::Time64Type&>(type);
      return std::make_shared<arrow::Time64Scalar>(
          ScaleFromMicros(DatumGetTimeADT(datum), time_type.unit()),
          std::static_pointer_cast<arrow::Time64Type>(
              std::const_pointer_cast<arrow::DataType>(type.shared_from_this())));
    }
    case arrow::Type::TIMESTAMP: {
      const auto& timestamp_type = static_cast<const arrow::TimestampType&>(type);
      const auto micros = DatumGetTimestamp(datum) + kPostgresUnixEpochOffsetMicros;
      return std::make_shared<arrow::TimestampScalar>(
          ScaleFromMicros(micros, timestamp_type.unit()),
          std::static_pointer_cast<arrow::TimestampType>(
              std::const_pointer_cast<arrow::DataType>(type.shared_from_this())));
    }
    default:
      return std::unexpected(
          MakeError(ERRCODE_FEATURE_NOT_SUPPORTED,
                    "unsupported pgiceberg INSERT Arrow type " + type.ToString()));
  }

  return std::unexpected(
      MakeError(ERRCODE_FEATURE_NOT_SUPPORTED,
                "unsupported pgiceberg INSERT conversion from PostgreSQL type " +
                    std::to_string(pg_type) + " to Arrow type " + type.ToString()));
}

Status AppendDatum(arrow::ArrayBuilder& builder, Datum value, Oid pg_type, bool is_null,
                   const arrow::DataType& type) {
  if (is_null) {
    return FromArrowStatus(builder.AppendNull(), "append Arrow NULL");
  }
  PGICEBERG_ASSIGN_OR_RETURN(auto scalar, ScalarFromDatum(value, pg_type, type));
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
    case NUMERICOID:
      return DatumGetBool(DirectFunctionCall2(numeric_eq, left, right));
    case FLOAT4OID:
      return DatumGetFloat4(left) == DatumGetFloat4(right);
    case FLOAT8OID:
      return DatumGetFloat8(left) == DatumGetFloat8(right);
    case DATEOID:
      return DatumGetDateADT(left) == DatumGetDateADT(right);
    case TIMEOID:
      return DatumGetTimeADT(left) == DatumGetTimeADT(right);
    case TIMESTAMPOID:
      return DatumGetTimestamp(left) == DatumGetTimestamp(right);
    case TIMESTAMPTZOID:
      return DatumGetTimestampTz(left) == DatumGetTimestampTz(right);
    case BYTEAOID: {
      auto* left_bytes = DatumGetByteaPP(left);
      auto* right_bytes = DatumGetByteaPP(right);
      const auto left_size = VARSIZE_ANY_EXHDR(left_bytes);
      const auto right_size = VARSIZE_ANY_EXHDR(right_bytes);
      return left_size == right_size &&
             std::memcmp(VARDATA_ANY(left_bytes), VARDATA_ANY(right_bytes), left_size) ==
                 0;
    }
    case UUIDOID: {
      const auto* left_uuid = DatumGetUUIDP(left);
      const auto* right_uuid = DatumGetUUIDP(right);
      return std::memcmp(left_uuid->data, right_uuid->data, UUID_LEN) == 0;
    }
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
