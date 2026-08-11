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

#include <memory>
#include <string>

#include <arrow/type_fwd.h>
#include <iceberg/type_fwd.h>

#include "common/status.h"

extern "C" {
#include "postgres.h"
}

namespace pgiceberg {

Result<std::shared_ptr<iceberg::Type>> PostgresTypeToIcebergType(Oid pg_type,
                                                                 int32 typmod = -1);

Result<std::string> IcebergTypeToSql(const iceberg::Type& type);
std::string ArrowTypeToSql(const arrow::DataType& type);

}  // namespace pgiceberg
