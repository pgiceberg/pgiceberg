#pragma once

#include <cstddef>
#include <memory>
#include <optional>
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
  explicit IcebergScanCursor(std::shared_ptr<iceberg::Table> table);
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
  std::shared_ptr<arrow::Schema> arrow_schema_;
  std::vector<std::shared_ptr<iceberg::FileScanTask>> tasks_;
  std::vector<std::shared_ptr<iceberg::DataFile>> data_files_;
  std::unique_ptr<iceberg::FileScanTaskReader> task_reader_;
  std::size_t task_index_ = 0;
  std::optional<ArrowArrayStream> current_stream_;
  std::shared_ptr<arrow::RecordBatchReader> batch_reader_;
};

}  // namespace pgiceberg::fdw
