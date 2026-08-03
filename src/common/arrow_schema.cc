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

#include "common/arrow_schema.h"

#include <arrow/c/bridge.h>
#include <iceberg/schema.h>
#include <iceberg/schema_internal.h>

namespace pgiceberg {

Result<std::shared_ptr<arrow::Schema>> ArrowSchemaFor(const iceberg::Schema& schema) {
  ArrowSchema c_schema;
  PGICEBERG_RETURN_NOT_OK(FromIcebergStatus(iceberg::ToArrowSchema(schema, &c_schema),
                                            "convert Iceberg schema to Arrow"));
  return FromArrowResult(arrow::ImportSchema(&c_schema), "import Arrow schema");
}

}  // namespace pgiceberg
