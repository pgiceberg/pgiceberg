#pragma once

#include <cstdint>
#include <memory>

extern "C" {
#include "postgres.h"
}

#include "common/status.h"

namespace arrow {
class Array;
class ArrayBuilder;
class DataType;
class Scalar;
}  // namespace arrow

namespace pgiceberg {

Result<Datum> ConvertValue(const arrow::Array& array, std::int64_t offset, Oid pg_type,
                           bool& is_null);

Result<std::shared_ptr<arrow::Scalar>> ScalarFromDatum(Datum value,
                                                       const arrow::DataType& type);

Status AppendDatum(arrow::ArrayBuilder& builder, Datum value, bool is_null,
                   const arrow::DataType& type);

Result<bool> DatumEquals(Datum left, Datum right, Oid pg_type);

}  // namespace pgiceberg
