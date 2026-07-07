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

#include "fdw/iceberg_scan.h"

#include <utility>

#include <arrow/c/bridge.h>
#include <arrow/record_batch.h>
#include <iceberg/data/file_scan_task_reader.h>
#include <iceberg/schema.h>
#include <iceberg/schema_internal.h>
#include <iceberg/table.h>
#include <iceberg/table_scan.h>

#include "common/pg_interrupt.h"
#include "common/status.h"

namespace pgiceberg::fdw {

namespace {

Result<std::shared_ptr<arrow::Schema>> ArrowSchemaFor(const iceberg::Schema& schema) {
  ArrowSchema c_schema;
  PGICEBERG_RETURN_NOT_OK(FromIcebergStatus(iceberg::ToArrowSchema(schema, &c_schema),
                                            "convert Iceberg schema to Arrow"));
  return FromArrowResult(arrow::ImportSchema(&c_schema), "import Arrow schema");
}

}  // namespace

IcebergScanCursor::IcebergScanCursor(
    std::shared_ptr<iceberg::Table> table,
    std::optional<std::vector<std::string>> selected_columns)
    : table_(std::move(table)), selected_columns_(std::move(selected_columns)) {}

Status IcebergScanCursor::Init() {
  PGICEBERG_ASSIGN_OR_RETURN(auto schema,
                             FromIcebergResult(table_->schema(), "load schema"));

  std::vector<std::shared_ptr<iceberg::Schema>> schemas;
  auto all_schemas = table_->schemas();
  if (all_schemas) {
    for (const auto& [_, candidate] : all_schemas->get()) {
      schemas.push_back(candidate);
    }
  }

  PGICEBERG_ASSIGN_OR_RETURN(auto scan_builder,
                             FromIcebergResult(table_->NewScan(), "create table scan"));
  if (selected_columns_.has_value()) {
    if (selected_columns_->empty()) {
      scan_builder->Project(iceberg::Schema::EmptySchema());
    } else {
      scan_builder->Select(*selected_columns_);
    }
  }
  PGICEBERG_ASSIGN_OR_RETURN(
      auto scan, FromIcebergResult(scan_builder->Build(), "build table scan"));
  PGICEBERG_ASSIGN_OR_RETURN(auto projected_schema,
                             FromIcebergResult(scan->schema(), "load scan schema"));
  PGICEBERG_ASSIGN_OR_RETURN(arrow_schema_, ArrowSchemaFor(*projected_schema));
  PGICEBERG_ASSIGN_OR_RETURN(tasks_,
                             FromIcebergResult(scan->PlanFiles(), "plan scan files"));

  data_files_.reserve(tasks_.size());
  for (const auto& task : tasks_) {
    data_files_.push_back(task->data_file());
  }

  auto reader = iceberg::FileScanTaskReader::Make(iceberg::FileScanTaskReader::Options{
      .io = table_->io(),
      .table_schema = schema,
      .schemas = std::move(schemas),
      .projected_schema = std::move(projected_schema),
      .properties = table_->properties().configs(),
  });
  PGICEBERG_ASSIGN_OR_RETURN(
      task_reader_, FromIcebergResult(std::move(reader), "create scan task reader"));
  return Ok();
}

IcebergScanCursor::~IcebergScanCursor() { Reset(); }

Result<bool> IcebergScanCursor::OpenCurrentTask() {
  if (task_index_ >= tasks_.size()) {
    return false;
  }

  PGICEBERG_ASSIGN_OR_RETURN(
      current_stream_,
      FromIcebergResult(task_reader_->Open(*tasks_[task_index_]), "open scan task"));
  PGICEBERG_ASSIGN_OR_RETURN(
      batch_reader_, FromArrowResult(arrow::ImportRecordBatchReader(&*current_stream_),
                                     "import record batch reader"));
  return true;
}

Result<bool> IcebergScanCursor::NextBatch(std::shared_ptr<arrow::RecordBatch>* batch) {
  *batch = nullptr;
  while (true) {
    PGICEBERG_RETURN_NOT_OK(pgiceberg::CheckForInterrupts());
    if (batch_reader_ == nullptr) {
      PGICEBERG_ASSIGN_OR_RETURN(auto opened, OpenCurrentTask());
      if (!opened) {
        return false;
      }
    }

    PGICEBERG_ASSIGN_OR_RETURN(
        auto next, FromArrowResult(batch_reader_->Next(), "read Arrow record batch"));
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
