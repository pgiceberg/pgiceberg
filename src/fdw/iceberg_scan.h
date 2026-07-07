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

#include <cstddef>
#include <memory>
#include <optional>
#include <string>
#include <vector>

#include <arrow/record_batch.h>
#include <arrow/type_fwd.h>
#include <iceberg/arrow_c_data.h>
#include <iceberg/type_fwd.h>

#include "common/status.h"

namespace iceberg {
class FileScanTaskReader;
}  // namespace iceberg

namespace pgiceberg::fdw {

class IcebergScanCursor {
 public:
  explicit IcebergScanCursor(
      std::shared_ptr<iceberg::Table> table,
      std::optional<std::vector<std::string>> selected_columns = std::nullopt);
  ~IcebergScanCursor();

  IcebergScanCursor(const IcebergScanCursor&) = delete;
  IcebergScanCursor& operator=(const IcebergScanCursor&) = delete;

  const std::shared_ptr<arrow::Schema>& arrow_schema() const { return arrow_schema_; }
  const std::vector<std::shared_ptr<iceberg::DataFile>>& data_files() const {
    return data_files_;
  }

  Status Init();
  Result<bool> NextBatch(std::shared_ptr<arrow::RecordBatch>* batch);
  void Reset();

 private:
  Result<bool> OpenCurrentTask();

  std::shared_ptr<iceberg::Table> table_;
  std::optional<std::vector<std::string>> selected_columns_;
  std::shared_ptr<arrow::Schema> arrow_schema_;
  std::vector<std::shared_ptr<iceberg::FileScanTask>> tasks_;
  std::vector<std::shared_ptr<iceberg::DataFile>> data_files_;
  std::unique_ptr<iceberg::FileScanTaskReader> task_reader_;
  std::size_t task_index_ = 0;
  std::optional<ArrowArrayStream> current_stream_;
  std::shared_ptr<arrow::RecordBatchReader> batch_reader_;
};

}  // namespace pgiceberg::fdw
