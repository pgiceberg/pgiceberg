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

Result<std::shared_ptr<arrow::Scalar>> ScalarFromDatum(Datum value, Oid pg_type,
                                                       const arrow::DataType& type);

Status AppendDatum(arrow::ArrayBuilder& builder, Datum value, Oid pg_type, bool is_null,
                   const arrow::DataType& type);

Result<bool> DatumEquals(Datum left, Datum right, Oid pg_type);

}  // namespace pgiceberg
