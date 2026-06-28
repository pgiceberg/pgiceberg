#include "fdw/iceberg_scan.h"

#include <utility>

#include <arrow/c/bridge.h>
#include <arrow/record_batch.h>
#include <iceberg/data/file_scan_task_reader.h>
#include <iceberg/schema_internal.h>
#include <iceberg/table.h>
#include <iceberg/table_scan.h>

#include "common/error.h"

namespace pgiceberg::fdw {

namespace {

std::shared_ptr<arrow::Schema> ArrowSchemaFor(const iceberg::Schema& schema) {
  ArrowSchema c_schema;
  auto status = iceberg::ToArrowSchema(schema, &c_schema);
  if (!status) {
    pgiceberg::ThrowIcebergError(status.error());
  }
  return pgiceberg::CheckArrowResult(arrow::ImportSchema(&c_schema),
                                     "import Arrow schema");
}

}  // namespace

IcebergScanCursor::IcebergScanCursor(std::shared_ptr<iceberg::Table> table)
    : table_(std::move(table)) {
  auto schema = table_->schema();
  if (!schema) {
    pgiceberg::ThrowIcebergError(schema.error());
  }
  arrow_schema_ = ArrowSchemaFor(**schema);

  std::vector<std::shared_ptr<iceberg::Schema>> schemas;
  auto all_schemas = table_->schemas();
  if (all_schemas) {
    for (const auto& [_, candidate] : all_schemas->get()) {
      schemas.push_back(candidate);
    }
  }

  auto scan_builder = table_->NewScan();
  if (!scan_builder) {
    pgiceberg::ThrowIcebergError(scan_builder.error());
  }
  auto scan = (*scan_builder)->Build();
  if (!scan) {
    pgiceberg::ThrowIcebergError(scan.error());
  }
  auto tasks = (*scan)->PlanFiles();
  if (!tasks) {
    pgiceberg::ThrowIcebergError(tasks.error());
  }
  tasks_ = std::move(*tasks);

  data_files_.reserve(tasks_.size());
  for (const auto& task : tasks_) {
    data_files_.push_back(task->data_file());
  }

  auto reader = iceberg::FileScanTaskReader::Make(iceberg::FileScanTaskReader::Options{
      .io = table_->io(),
      .table_schema = *schema,
      .schemas = std::move(schemas),
      .projected_schema = *schema,
      .properties = table_->properties().configs(),
  });
  if (!reader) {
    pgiceberg::ThrowIcebergError(reader.error());
  }
  task_reader_ = std::move(*reader);
}

IcebergScanCursor::~IcebergScanCursor() { Reset(); }

bool IcebergScanCursor::OpenCurrentTask() {
  if (task_index_ >= tasks_.size()) {
    return false;
  }

  auto stream = task_reader_->Open(*tasks_[task_index_]);
  if (!stream) {
    pgiceberg::ThrowIcebergError(stream.error());
  }
  current_stream_ = *stream;
  batch_reader_ = pgiceberg::CheckArrowResult(
      arrow::ImportRecordBatchReader(&*current_stream_), "import record batch reader");
  return true;
}

bool IcebergScanCursor::NextBatch(std::shared_ptr<arrow::RecordBatch>* batch) {
  *batch = nullptr;
  while (true) {
    if (batch_reader_ == nullptr && !OpenCurrentTask()) {
      return false;
    }

    auto next =
        pgiceberg::CheckArrowResult(batch_reader_->Next(), "read Arrow record batch");
    if (next != nullptr) {
      *batch = std::move(next);
      return true;
    }

    batch_reader_.reset();
    current_stream_.reset();
    task_index_++;
  }
}

void IcebergScanCursor::Reset() {
  batch_reader_.reset();
  current_stream_.reset();
  task_index_ = 0;
}

}  // namespace pgiceberg::fdw
