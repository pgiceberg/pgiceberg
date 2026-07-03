#include "fdw/modify_state.h"

#include <unistd.h>

#include <algorithm>
#include <chrono>
#include <filesystem>
#include <memory>
#include <optional>
#include <sstream>
#include <string>
#include <utility>
#include <vector>

#include <arrow/array.h>
#include <arrow/array/builder_base.h>
#include <arrow/c/bridge.h>
#include <arrow/record_batch.h>
#include <iceberg/data/data_writer.h>
#include <iceberg/file_io.h>
#include <iceberg/file_format.h>
#include <iceberg/manifest/manifest_entry.h>
#include <iceberg/partition_spec.h>
#include <iceberg/schema.h>
#include <iceberg/schema_internal.h>
#include <iceberg/table.h>
#include <iceberg/table_metadata.h>
#include <iceberg/transaction.h>
#include <iceberg/update/fast_append.h>
#include <iceberg/update/overwrite_files.h>

#include "common/catalog.h"
#include "common/datum_convert.h"
#include "common/pg_error.h"
#include "common/status.h"
#include "fdw/iceberg_scan.h"

extern "C" {
#include "postgres.h"
#include "access/htup_details.h"
#include "access/xact.h"
#include "executor/executor.h"
#include "nodes/execnodes.h"
#include "nodes/plannodes.h"
#include "utils/datum.h"
#include "utils/memutils.h"
#include "utils/rel.h"
}

namespace pgiceberg::fdw {
namespace {

// UPDATE/DELETE rewrite whole Iceberg data files today.  Keep the Arrow batch
// bounded so a large rewrite does not also require materializing the whole
// replacement file in backend memory.
constexpr std::int64_t kDmlRewriteBatchRows = 8192;

// Iceberg metadata updates need to be replayable.  A PostgreSQL subtransaction
// can abort after we have already applied an update to the in-memory Iceberg
// transaction, and iceberg-cpp does not provide a way to subtract one update
// from that transaction.  Replaying the surviving changes into a new
// transaction keeps PostgreSQL subtransaction semantics intact.
//
// Data files are tracked separately because they are written outside
// PostgreSQL storage and therefore need best-effort cleanup on abort.
class PendingIcebergChange {
 public:
  virtual ~PendingIcebergChange() = default;
  virtual Status Apply(iceberg::Transaction& transaction) = 0;
  virtual std::vector<std::string> NewDataFilePaths() const = 0;
};

class PendingAppendChange final : public PendingIcebergChange {
 public:
  explicit PendingAppendChange(std::shared_ptr<iceberg::DataFile> data_file)
      : data_file_(std::move(data_file)) {}

  Status Apply(iceberg::Transaction& transaction) override {
    if (data_file_ == nullptr) {
      return Ok();
    }
    PGICEBERG_ASSIGN_OR_RETURN(auto append, FromIcebergResult(transaction.NewFastAppend(),
                                                              "create append update"));
    append->AppendFile(data_file_);
    return FromIcebergStatus(append->Commit(), "apply pending Iceberg append");
  }

  std::vector<std::string> NewDataFilePaths() const override {
    if (data_file_ == nullptr) {
      return {};
    }
    return {data_file_->file_path};
  }

 private:
  std::shared_ptr<iceberg::DataFile> data_file_;
};

class PendingOverwriteChange final : public PendingIcebergChange {
 public:
  PendingOverwriteChange(std::vector<std::shared_ptr<iceberg::DataFile>> deleted_files,
                         std::shared_ptr<iceberg::DataFile> replacement_file)
      : deleted_files_(std::move(deleted_files)),
        replacement_file_(std::move(replacement_file)) {}

  Status Apply(iceberg::Transaction& transaction) override {
    PGICEBERG_ASSIGN_OR_RETURN(
        auto overwrite,
        FromIcebergResult(transaction.NewOverwrite(), "create overwrite update"));
    for (const auto& file : deleted_files_) {
      overwrite->DeleteFile(file);
    }
    if (replacement_file_ != nullptr) {
      overwrite->AddFile(replacement_file_);
    }
    return FromIcebergStatus(overwrite->Commit(), "apply pending Iceberg overwrite");
  }

  std::vector<std::string> NewDataFilePaths() const override {
    if (replacement_file_ == nullptr) {
      return {};
    }
    return {replacement_file_->file_path};
  }

 private:
  std::vector<std::shared_ptr<iceberg::DataFile>> deleted_files_;
  std::shared_ptr<iceberg::DataFile> replacement_file_;
};

// A pending change is tagged with the current PostgreSQL subtransaction so an
// abort can discard only the work done inside that savepoint.
struct PendingModifyChange {
  SubTransactionId subtransaction_id = InvalidSubTransactionId;
  std::unique_ptr<PendingIcebergChange> change;
};

// Pending table state is backend-local transaction state.  The Iceberg
// transaction is updated eagerly so later statements in the same PostgreSQL
// transaction can read their own writes, but its catalog commit is delayed
// until PostgreSQL reaches PRE_COMMIT.
struct PendingTableChange {
  std::string key;
  std::shared_ptr<iceberg::Table> base_table;
  std::shared_ptr<iceberg::Transaction> transaction;
  std::vector<PendingModifyChange> changes;
  bool committed = false;
  bool commit_state_unknown = false;
};

// LoadIcebergTable returns a fresh Table object for each executor entry, so
// pointer identity would split pending work from later statements.  Length
// prefixes keep the option tuple unambiguous without depending on a character
// that catalog names cannot contain.
std::string PendingTableKey(const Options& options) {
  std::string key;
  const std::string_view parts[] = {options.catalog_type, options.catalog_uri,
                                    options.warehouse,    options.catalog_name,
                                    options.name_space,   options.table};
  for (auto part : parts) {
    key += std::to_string(part.size());
    key += ':';
    key.append(part);
  }
  return key;
}

// Keep the queue outside executor state.  EndModify is statement-scoped, while
// the decision to publish or discard Iceberg metadata is PostgreSQL
// transaction-scoped.
std::vector<PendingTableChange>& PendingTableChanges() {
  static auto* changes = new std::vector<PendingTableChange>();
  return *changes;
}

PendingTableChange* FindPendingTableChange(const std::string& key) {
  auto& changes = PendingTableChanges();
  auto it =
      std::find_if(changes.begin(), changes.end(), [&](const PendingTableChange& change) {
        return change.key == key && !change.committed;
      });
  if (it == changes.end()) {
    return nullptr;
  }
  return &*it;
}

// Rebuild after a subtransaction abort rather than trying to mutate the old
// Iceberg transaction.  That makes abort handling depend only on the list of
// surviving logical changes, not on iceberg-cpp internals.
Status RebuildPendingTransaction(PendingTableChange& table_change) {
  PGICEBERG_ASSIGN_OR_RETURN(auto transaction,
                             FromIcebergResult(table_change.base_table->NewTransaction(),
                                               "create Iceberg transaction"));
  for (auto& change : table_change.changes) {
    PGICEBERG_RETURN_NOT_OK(change.change->Apply(*transaction));
  }
  table_change.transaction = std::move(transaction);
  return Ok();
}

Result<PendingTableChange*> EnsurePendingTableChange(
    const Options& options, std::shared_ptr<iceberg::Table> table) {
  auto key = PendingTableKey(options);
  if (auto* pending = FindPendingTableChange(key); pending != nullptr) {
    return pending;
  }

  PGICEBERG_ASSIGN_OR_RETURN(
      auto transaction,
      FromIcebergResult(table->NewTransaction(), "create Iceberg transaction"));
  auto& changes = PendingTableChanges();
  changes.push_back(PendingTableChange{
      .key = std::move(key),
      .base_table = std::move(table),
      .transaction = std::move(transaction),
      .changes = {},
      .committed = false,
      .commit_state_unknown = false,
  });
  return &changes.back();
}

Result<std::shared_ptr<iceberg::Table>> StaticTableForPendingChange(
    const PendingTableChange& table_change) {
  // Scans in the same PostgreSQL transaction should see earlier FDW writes.
  // A StaticTable over the pending transaction metadata gives the scan a
  // snapshot with those uncommitted Iceberg metadata updates, while the catalog
  // still remains unchanged until PRE_COMMIT.
  auto metadata = std::shared_ptr<iceberg::TableMetadata>(
      table_change.transaction,
      const_cast<iceberg::TableMetadata*>(&table_change.transaction->current()));
  return FromIcebergResult(
      iceberg::StaticTable::Make(
          table_change.base_table->name(), std::move(metadata),
          std::string(table_change.base_table->metadata_file_location()),
          table_change.base_table->io()),
      "create Iceberg transaction read table");
}

std::vector<std::string> NewDataFilePaths(const PendingTableChange& table_change) {
  std::vector<std::string> paths;
  for (const auto& change : table_change.changes) {
    auto change_paths = change.change->NewDataFilePaths();
    paths.insert(paths.end(), change_paths.begin(), change_paths.end());
  }
  return paths;
}

std::vector<std::string> NewDataFilePaths(
    const std::shared_ptr<iceberg::DataFile>& data_file) {
  if (data_file == nullptr) {
    return {};
  }
  return {data_file->file_path};
}

void CleanupDataFiles(const std::shared_ptr<iceberg::Table>& table,
                      const std::vector<std::string>& paths) {
  if (table == nullptr || paths.empty()) {
    return;
  }
  // Parquet files are written before PostgreSQL knows whether the transaction
  // will commit.  Deletion is necessarily best effort because the files live in
  // the Iceberg table's file IO, outside PostgreSQL rollback machinery.
  auto status = FromIcebergStatus(table->io()->DeleteFiles(paths),
                                  "delete aborted Iceberg data files");
  if (!status) {
    elog(WARNING, "%s", status.error().message().c_str());
  }
}

void ClearPendingTableChanges(bool cleanup_data_files) {
  if (cleanup_data_files) {
    for (const auto& table_change : PendingTableChanges()) {
      if (!table_change.committed && !table_change.commit_state_unknown) {
        CleanupDataFiles(table_change.base_table, NewDataFilePaths(table_change));
      }
    }
  }
  PendingTableChanges().clear();
}

Status QueuePendingModifyChange(PendingTableChange& table_change,
                                std::unique_ptr<PendingIcebergChange> change) {
  auto* pending_change = change.get();
  table_change.changes.push_back(PendingModifyChange{
      .subtransaction_id = GetCurrentSubTransactionId(), .change = std::move(change)});
  // Apply now, but do not publish to the catalog yet.  Applying now gives later
  // statements read-your-writes behavior; delaying the transaction commit lets
  // PostgreSQL abort discard the Iceberg metadata update.
  auto status = pending_change->Apply(*table_change.transaction);
  if (!status) {
    CleanupDataFiles(table_change.base_table, pending_change->NewDataFilePaths());
    table_change.changes.pop_back();
    return std::unexpected(status.error());
  }
  return Ok();
}

Status CommitPendingModifyChanges() {
  // PRE_COMMIT is the latest point where an ERROR can still abort the
  // PostgreSQL transaction.  Commit Iceberg here so a catalog failure does not
  // leave PostgreSQL thinking the statement succeeded.
  for (auto& table_change : PendingTableChanges()) {
    if (table_change.committed || table_change.changes.empty()) {
      continue;
    }
    auto commit_result = table_change.transaction->Commit();
    if (!commit_result) {
      if (commit_result.error().kind == iceberg::ErrorKind::kCommitStateUnknown) {
        // Once iceberg-cpp reports an unknown commit state, deleting newly
        // written files could corrupt a commit that actually reached the
        // catalog.  Leave the files in place and surface the commit error.
        table_change.commit_state_unknown = true;
      }
      return std::unexpected(
          MakePgError(commit_result.error(), "commit pending Iceberg transaction"));
    }
    auto committed_table = std::move(commit_result).value();
    table_change.base_table = std::move(committed_table);
    table_change.changes.clear();
    table_change.committed = true;
  }
  ClearPendingTableChanges(false);
  return Ok();
}

void XactCallback(XactEvent event, void*) {
  try {
    switch (event) {
      case XACT_EVENT_PRE_COMMIT: {
        auto status = CommitPendingModifyChanges();
        if (!status) {
          pgiceberg::ReportError(status.error());
        }
        break;
      }
      case XACT_EVENT_PRE_PREPARE:
        if (!PendingTableChanges().empty()) {
          // Prepared transactions would require durable pending Iceberg change
          // state across backend exit and crash recovery.  This queue is only
          // backend-local, so fail before PostgreSQL prepares.
          ereport(ERROR,
                  (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                   errmsg("pgiceberg DML does not support prepared transactions")));
        }
        break;
      case XACT_EVENT_ABORT:
      case XACT_EVENT_PARALLEL_ABORT:
        ClearPendingTableChanges(true);
        break;
      default:
        break;
    }
  } catch (...) {
    pgiceberg::ReportCurrentException();
  }
}

void SubXactCallback(SubXactEvent event, SubTransactionId my_subid,
                     SubTransactionId parent_subid, void*) {
  try {
    auto& table_changes = PendingTableChanges();
    switch (event) {
      case SUBXACT_EVENT_COMMIT_SUB:
        for (auto& table_change : table_changes) {
          for (auto& change : table_change.changes) {
            if (change.subtransaction_id == my_subid) {
              // PostgreSQL promotes a committed subtransaction's effects to
              // its parent.  The Iceberg metadata update must follow that same
              // ownership so a later parent abort can still find it.
              change.subtransaction_id = parent_subid;
            }
          }
        }
        break;
      case SUBXACT_EVENT_ABORT_SUB:
        for (auto& table_change : table_changes) {
          std::vector<std::string> aborted_paths;
          for (const auto& change : table_change.changes) {
            if (change.subtransaction_id == my_subid) {
              auto paths = change.change->NewDataFilePaths();
              aborted_paths.insert(aborted_paths.end(), paths.begin(), paths.end());
            }
          }
          CleanupDataFiles(table_change.base_table, aborted_paths);
          std::erase_if(table_change.changes,
                        [my_subid](const PendingModifyChange& change) {
                          return change.subtransaction_id == my_subid;
                        });
          if (!table_change.changes.empty()) {
            // The transaction already saw the aborted subtransaction's Iceberg
            // update.  Replaying only the survivors is simpler and safer than
            // depending on update-specific undo support.
            auto status = RebuildPendingTransaction(table_change);
            if (!status) {
              pgiceberg::ReportError(status.error());
            }
          }
        }
        std::erase_if(table_changes, [](const PendingTableChange& table_change) {
          return table_change.changes.empty();
        });
        break;
      default:
        break;
    }
  } catch (...) {
    pgiceberg::ReportCurrentException();
  }
}

// Rows copied out of executor slots must own their Datums.  PostgreSQL is free
// to reuse slot storage between callbacks, while UPDATE/DELETE matching happens
// later when the old Iceberg files are rewritten.
struct Value {
  Datum datum = static_cast<Datum>(0);
  bool is_null = true;
};

using Row = std::vector<Value>;

Result<std::shared_ptr<arrow::Schema>> ArrowSchemaFor(const iceberg::Schema& schema) {
  ArrowSchema c_schema;
  PGICEBERG_RETURN_NOT_OK(FromIcebergStatus(iceberg::ToArrowSchema(schema, &c_schema),
                                            "convert Iceberg schema to Arrow"));
  return FromArrowResult(arrow::ImportSchema(&c_schema), "import Arrow schema");
}

std::string DataFilePath(const iceberg::Table& table) {
  auto now = std::chrono::system_clock::now().time_since_epoch().count();
  std::ostringstream name;
  name << table.location() << "/data/pgiceberg-" << getpid() << "-" << now << ".parquet";

  if (!table.location().starts_with("s3://") && !table.location().starts_with("gs://")) {
    std::filesystem::create_directories(
        std::filesystem::path(std::string(table.location())) / "data");
  }

  return name.str();
}

Result<std::vector<std::unique_ptr<arrow::ArrayBuilder>>> MakeBuilders(
    const arrow::Schema& schema) {
  std::vector<std::unique_ptr<arrow::ArrayBuilder>> builders;
  builders.reserve(schema.num_fields());
  for (int i = 0; i < schema.num_fields(); i++) {
    PGICEBERG_ASSIGN_OR_RETURN(
        auto builder, FromArrowResult(arrow::MakeBuilder(schema.field(i)->type(),
                                                         arrow::default_memory_pool()),
                                      "make Arrow builder"));
    builders.push_back(std::move(builder));
  }
  return builders;
}

Status AppendValue(arrow::ArrayBuilder& builder, const Value& value,
                   const arrow::DataType& type) {
  return pgiceberg::AppendDatum(builder, value.datum, value.is_null, type);
}

Row CopyRowFromSlot(TupleTableSlot* slot, TupleDesc desc) {
  Row row(desc->natts);
  slot_getallattrs(slot);
  for (int i = 0; i < desc->natts; i++) {
    Form_pg_attribute attr = TupleDescAttr(desc, i);
    if (attr->attisdropped || slot->tts_isnull[i]) {
      continue;
    }
    row[i] = Value{.datum = datumCopy(slot->tts_values[i], attr->attbyval, attr->attlen),
                   .is_null = false};
  }
  return row;
}

Row CopyRowFromHeapTupleDatum(Datum tuple_datum, TupleDesc desc) {
  HeapTupleHeader header = DatumGetHeapTupleHeader(tuple_datum);
  HeapTupleData tuple;
  tuple.t_len = HeapTupleHeaderGetDatumLength(header);
  ItemPointerSetInvalid(&tuple.t_self);
  tuple.t_tableOid = InvalidOid;
  tuple.t_data = header;

  std::vector<Datum> values(desc->natts);
  auto nulls = std::make_unique<bool[]>(desc->natts);
  heap_deform_tuple(&tuple, desc, values.data(), nulls.get());

  Row row(desc->natts);
  for (int i = 0; i < desc->natts; i++) {
    Form_pg_attribute attr = TupleDescAttr(desc, i);
    if (attr->attisdropped || nulls[i]) {
      continue;
    }
    row[i] = Value{.datum = datumCopy(values[i], attr->attbyval, attr->attlen),
                   .is_null = false};
  }
  return row;
}

TupleTableSlot* StoreRowInSlot(const Row& row, TupleTableSlot* slot, TupleDesc desc) {
  ExecClearTuple(slot);
  for (int i = 0; i < desc->natts; i++) {
    slot->tts_values[i] = static_cast<Datum>(0);
    slot->tts_isnull[i] = true;
    Form_pg_attribute attr = TupleDescAttr(desc, i);
    if (attr->attisdropped || row[i].is_null) {
      continue;
    }
    slot->tts_values[i] = row[i].datum;
    slot->tts_isnull[i] = false;
  }
  ExecStoreVirtualTuple(slot);
  return slot;
}

Result<bool> RowEquals(const Row& left, const Row& right, TupleDesc desc) {
  if (left.size() != right.size()) {
    return false;
  }
  for (int i = 0; i < desc->natts; i++) {
    Form_pg_attribute attr = TupleDescAttr(desc, i);
    if (attr->attisdropped) {
      continue;
    }
    if (left[i].is_null != right[i].is_null) {
      return false;
    }
    if (!left[i].is_null) {
      PGICEBERG_ASSIGN_OR_RETURN(
          auto equal,
          pgiceberg::DatumEquals(left[i].datum, right[i].datum, attr->atttypid));
      if (!equal) {
        return false;
      }
    }
  }
  return true;
}

Status AppendRow(std::vector<std::unique_ptr<arrow::ArrayBuilder>>& builders,
                 const arrow::Schema& schema, const std::vector<int>& attr_numbers,
                 const Row& row) {
  for (int i = 0; i < schema.num_fields(); i++) {
    PGICEBERG_RETURN_NOT_OK(
        AppendValue(*builders[i], row[attr_numbers[i] - 1], *schema.field(i)->type()));
  }
  return Ok();
}

Result<std::shared_ptr<iceberg::DataFile>> WriteRows(
    iceberg::Table& table, const std::shared_ptr<iceberg::Schema>& iceberg_schema,
    const std::shared_ptr<iceberg::PartitionSpec>& spec,
    const std::shared_ptr<arrow::Schema>& arrow_schema,
    std::vector<std::unique_ptr<arrow::ArrayBuilder>> builders, std::int64_t rows) {
  if (rows == 0) {
    return nullptr;
  }

  std::vector<std::shared_ptr<arrow::Array>> arrays;
  arrays.reserve(builders.size());
  for (auto& builder : builders) {
    PGICEBERG_ASSIGN_OR_RETURN(auto array,
                               FromArrowResult(builder->Finish(), "finish Arrow array"));
    arrays.push_back(std::move(array));
  }

  auto batch = arrow::RecordBatch::Make(arrow_schema, rows, arrays);
  ArrowArray c_array;
  PGICEBERG_RETURN_NOT_OK(FromArrowStatus(arrow::ExportRecordBatch(*batch, &c_array),
                                          "export Arrow record batch"));

  PGICEBERG_ASSIGN_OR_RETURN(
      auto writer, FromIcebergResult(iceberg::DataWriter::Make(iceberg::DataWriterOptions{
                                         .path = DataFilePath(table),
                                         .schema = iceberg_schema,
                                         .spec = spec,
                                         .format = iceberg::FileFormatType::kParquet,
                                         .io = table.io(),
                                         .properties = table.properties().configs(),
                                     }),
                                     "create data writer"));
  PGICEBERG_RETURN_NOT_OK(FromIcebergStatus(writer->Write(&c_array), "write data file"));
  PGICEBERG_RETURN_NOT_OK(FromIcebergStatus(writer->Close(), "close data writer"));
  PGICEBERG_ASSIGN_OR_RETURN(
      auto metadata, FromIcebergResult(writer->Metadata(), "read writer metadata"));
  if (metadata.data_files.empty()) {
    return nullptr;
  }
  return metadata.data_files.front();
}

class RowBatchWriter {
 public:
  static Result<std::unique_ptr<RowBatchWriter>> Make(
      iceberg::Table& table, std::shared_ptr<iceberg::Schema> iceberg_schema,
      std::shared_ptr<iceberg::PartitionSpec> spec,
      std::shared_ptr<arrow::Schema> arrow_schema) {
    PGICEBERG_ASSIGN_OR_RETURN(auto builders, MakeBuilders(*arrow_schema));
    return std::unique_ptr<RowBatchWriter>(
        new RowBatchWriter(table, std::move(iceberg_schema), std::move(spec),
                           std::move(arrow_schema), std::move(builders)));
  }

  Status Append(const std::vector<int>& attr_numbers, const Row& row) {
    PGICEBERG_RETURN_NOT_OK(EnsureWriter());
    PGICEBERG_RETURN_NOT_OK(AppendRow(builders_, *arrow_schema_, attr_numbers, row));
    rows_in_batch_++;
    if (rows_in_batch_ >= kDmlRewriteBatchRows) {
      PGICEBERG_RETURN_NOT_OK(Flush());
    }
    return Ok();
  }

  Result<std::shared_ptr<iceberg::DataFile>> Finish() {
    PGICEBERG_RETURN_NOT_OK(Flush());
    if (writer_ == nullptr) {
      return nullptr;
    }
    PGICEBERG_RETURN_NOT_OK(FromIcebergStatus(writer_->Close(), "close data writer"));
    PGICEBERG_ASSIGN_OR_RETURN(
        auto metadata, FromIcebergResult(writer_->Metadata(), "read writer metadata"));
    if (metadata.data_files.empty()) {
      return nullptr;
    }
    return metadata.data_files.front();
  }

 private:
  RowBatchWriter(iceberg::Table& table, std::shared_ptr<iceberg::Schema> iceberg_schema,
                 std::shared_ptr<iceberg::PartitionSpec> spec,
                 std::shared_ptr<arrow::Schema> arrow_schema,
                 std::vector<std::unique_ptr<arrow::ArrayBuilder>> builders)
      : table_(table),
        iceberg_schema_(std::move(iceberg_schema)),
        spec_(std::move(spec)),
        arrow_schema_(std::move(arrow_schema)),
        builders_(std::move(builders)) {}

  Status EnsureWriter() {
    if (writer_ != nullptr) {
      return Ok();
    }
    PGICEBERG_ASSIGN_OR_RETURN(
        auto writer,
        FromIcebergResult(iceberg::DataWriter::Make(iceberg::DataWriterOptions{
                              .path = DataFilePath(table_),
                              .schema = iceberg_schema_,
                              .spec = spec_,
                              .format = iceberg::FileFormatType::kParquet,
                              .io = table_.io(),
                              .properties = table_.properties().configs(),
                          }),
                          "create data writer"));
    writer_ = std::move(writer);
    return Ok();
  }

  Status Flush() {
    if (rows_in_batch_ == 0) {
      return Ok();
    }

    std::vector<std::shared_ptr<arrow::Array>> arrays;
    arrays.reserve(builders_.size());
    for (auto& builder : builders_) {
      PGICEBERG_ASSIGN_OR_RETURN(
          auto array, FromArrowResult(builder->Finish(), "finish Arrow array"));
      arrays.push_back(std::move(array));
    }

    auto batch = arrow::RecordBatch::Make(arrow_schema_, rows_in_batch_, arrays);
    ArrowArray c_array;
    PGICEBERG_RETURN_NOT_OK(FromArrowStatus(arrow::ExportRecordBatch(*batch, &c_array),
                                            "export Arrow record batch"));
    PGICEBERG_RETURN_NOT_OK(
        FromIcebergStatus(writer_->Write(&c_array), "write data file"));

    PGICEBERG_ASSIGN_OR_RETURN(builders_, MakeBuilders(*arrow_schema_));
    rows_in_batch_ = 0;
    return Ok();
  }

  iceberg::Table& table_;
  std::shared_ptr<iceberg::Schema> iceberg_schema_;
  std::shared_ptr<iceberg::PartitionSpec> spec_;
  std::shared_ptr<arrow::Schema> arrow_schema_;
  std::vector<std::unique_ptr<arrow::ArrayBuilder>> builders_;
  std::unique_ptr<iceberg::DataWriter> writer_;
  std::int64_t rows_in_batch_ = 0;
};

Result<Row> RowFromRecordBatch(const arrow::RecordBatch& batch, std::int64_t row_index,
                               TupleDesc desc) {
  Row row(desc->natts);
  for (int i = 0; i < desc->natts; i++) {
    Form_pg_attribute attr = TupleDescAttr(desc, i);
    if (attr->attisdropped) {
      continue;
    }
    const int column_index = batch.schema()->GetFieldIndex(NameStr(attr->attname));
    if (column_index < 0) {
      return std::unexpected(
          MakeError(ERRCODE_FDW_ERROR, "column does not exist in Iceberg table"));
    }

    auto column = batch.column(column_index);
    bool is_null = true;
    PGICEBERG_ASSIGN_OR_RETURN(
        Datum datum,
        pgiceberg::ConvertValue(*column, row_index, attr->atttypid, is_null));
    if (is_null) {
      continue;
    }
    row[i] =
        Value{.datum = datumCopy(datum, attr->attbyval, attr->attlen), .is_null = false};
  }
  return row;
}

}  // namespace

void RegisterTransactionCallbacks() {
  // PostgreSQL calls extension _PG_init once per backend, which is the right
  // lifetime for the pending queue.  Per-statement FDW callbacks cannot see
  // the final transaction outcome.
  RegisterXactCallback(XactCallback, nullptr);
  RegisterSubXactCallback(SubXactCallback, nullptr);
}

Result<std::shared_ptr<iceberg::Table>> ReadTableForCurrentTransaction(
    const Options& options, std::shared_ptr<iceberg::Table> table) {
  auto* pending = FindPendingTableChange(PendingTableKey(options));
  if (pending == nullptr) {
    return table;
  }
  return StaticTableForPendingChange(*pending);
}

// ModifyState is statement-local executor state.  It owns transient row and
// Arrow builder state, but never owns the final Iceberg commit decision; that
// belongs to the backend-local pending queue above.
struct ModifyState {
  MemoryContextCallback* cleanup_callback = nullptr;
  CmdType operation = CMD_UNKNOWN;
  Options options;
  std::shared_ptr<iceberg::Table> table;
  std::shared_ptr<iceberg::Table> read_table;
  std::shared_ptr<iceberg::Schema> iceberg_schema;
  std::shared_ptr<iceberg::PartitionSpec> spec;
  std::shared_ptr<arrow::Schema> arrow_schema;
  std::vector<std::unique_ptr<arrow::ArrayBuilder>> builders;
  std::vector<int> attr_numbers;
  TupleDesc tuple_desc = nullptr;
  AttrNumber wholerow_attno = InvalidAttrNumber;
  std::vector<Row> old_rows;
  std::vector<Row> new_rows;
  std::int64_t rows = 0;
};

void DeleteModifyState(void* arg) { delete static_cast<ModifyState*>(arg); }

void RegisterMemoryContextCleanup(ModifyState* state) {
  // FDW EndForeignModify is not guaranteed after an ERROR.  Register with the
  // current memory context so C++ state is released with PostgreSQL executor
  // cleanup even on error paths.
  auto* callback = static_cast<MemoryContextCallback*>(
      MemoryContextAlloc(CurrentMemoryContext, sizeof(MemoryContextCallback)));
  callback->func = DeleteModifyState;
  callback->arg = state;
  state->cleanup_callback = callback;
  MemoryContextRegisterResetCallback(CurrentMemoryContext, callback);
}

void DetachMemoryContextCleanup(ModifyState* state) {
  if (state != nullptr && state->cleanup_callback != nullptr) {
    state->cleanup_callback->arg = nullptr;
    state->cleanup_callback = nullptr;
  }
}

Result<ModifyState*> BeginModify(ModifyTableState* mtstate, ResultRelInfo* rinfo,
                                 const Options& options) {
  auto state = std::make_unique<ModifyState>();
  state->operation = mtstate->operation;
  state->options = options;
  Relation relation = rinfo->ri_RelationDesc;
  state->tuple_desc = RelationGetDescr(relation);
  PGICEBERG_ASSIGN_OR_RETURN(
      state->table, pgiceberg::LoadIcebergTable(ToCatalogOptions(options),
                                                RelationGetRelationName(relation)));
  PGICEBERG_ASSIGN_OR_RETURN(state->read_table,
                             ReadTableForCurrentTransaction(options, state->table));

  PGICEBERG_ASSIGN_OR_RETURN(
      state->iceberg_schema,
      FromIcebergResult(state->read_table->schema(), "load schema"));
  PGICEBERG_ASSIGN_OR_RETURN(state->arrow_schema, ArrowSchemaFor(*state->iceberg_schema));

  PGICEBERG_ASSIGN_OR_RETURN(
      state->spec, FromIcebergResult(state->read_table->spec(), "load partition spec"));
  if (!state->spec->fields().empty()) {
    return std::unexpected(
        MakeError(ERRCODE_FEATURE_NOT_SUPPORTED,
                  "pgiceberg DML currently supports only unpartitioned Iceberg tables"));
  }

  for (int i = 0; i < state->arrow_schema->num_fields(); i++) {
    const auto& field = state->arrow_schema->field(i);
    auto attr_number = InvalidAttrNumber;
    for (int j = 0; j < state->tuple_desc->natts; j++) {
      Form_pg_attribute attr = TupleDescAttr(state->tuple_desc, j);
      if (!attr->attisdropped && field->name() == NameStr(attr->attname)) {
        attr_number = attr->attnum;
        break;
      }
    }
    if (attr_number == InvalidAttrNumber) {
      return std::unexpected(MakeError(
          ERRCODE_FDW_ERROR,
          "Iceberg field \"" + field->name() + "\" does not exist in foreign table"));
    }
    state->attr_numbers.push_back(attr_number);
  }
  PGICEBERG_ASSIGN_OR_RETURN(state->builders, MakeBuilders(*state->arrow_schema));

  if (state->operation == CMD_UPDATE || state->operation == CMD_DELETE) {
    auto* subplan = outerPlanState(mtstate)->plan;
    state->wholerow_attno = ExecFindJunkAttributeInTlist(subplan->targetlist, "wholerow");
    if (!AttributeNumberIsValid(state->wholerow_attno)) {
      return std::unexpected(
          MakeError(ERRCODE_FDW_ERROR, "could not find junk wholerow column"));
    }
  }

  RegisterMemoryContextCleanup(state.get());
  return state.release();
}

Result<TupleTableSlot*> ExecInsert(ModifyState* state, TupleTableSlot* slot) {
  PGICEBERG_RETURN_NOT_OK(AppendRow(state->builders, *state->arrow_schema,
                                    state->attr_numbers,
                                    CopyRowFromSlot(slot, state->tuple_desc)));
  state->rows++;
  return slot;
}

Result<TupleTableSlot*> ExecUpdate(ModifyState* state, TupleTableSlot* slot,
                                   TupleTableSlot* plan_slot) {
  bool is_null = false;
  Datum wholerow = ExecGetJunkAttribute(plan_slot, state->wholerow_attno, &is_null);
  if (is_null) {
    return std::unexpected(MakeError(ERRCODE_FDW_ERROR, "wholerow is NULL"));
  }
  state->old_rows.push_back(CopyRowFromHeapTupleDatum(wholerow, state->tuple_desc));
  state->new_rows.push_back(CopyRowFromSlot(slot, state->tuple_desc));
  state->rows++;
  return slot;
}

Result<TupleTableSlot*> ExecDelete(ModifyState* state, TupleTableSlot* slot,
                                   TupleTableSlot* plan_slot) {
  bool is_null = false;
  Datum wholerow = ExecGetJunkAttribute(plan_slot, state->wholerow_attno, &is_null);
  if (is_null) {
    return std::unexpected(MakeError(ERRCODE_FDW_ERROR, "wholerow is NULL"));
  }
  Row old_row = CopyRowFromHeapTupleDatum(wholerow, state->tuple_desc);
  state->old_rows.push_back(old_row);
  state->rows++;
  return StoreRowInSlot(old_row, slot, state->tuple_desc);
}

Status EndModify(ModifyState* state) {
  DetachMemoryContextCleanup(state);
  std::unique_ptr<ModifyState> guard(state);
  if (state == nullptr || state->rows == 0) {
    return Ok();
  }

  if (state->operation == CMD_INSERT) {
    PGICEBERG_ASSIGN_OR_RETURN(
        auto data_file,
        WriteRows(*state->table, state->iceberg_schema, state->spec, state->arrow_schema,
                  std::move(state->builders), state->rows));
    auto pending_table_result = EnsurePendingTableChange(state->options, state->table);
    if (!pending_table_result) {
      CleanupDataFiles(state->table, NewDataFilePaths(data_file));
      return std::unexpected(pending_table_result.error());
    }
    PGICEBERG_RETURN_NOT_OK(QueuePendingModifyChange(
        **pending_table_result,
        std::make_unique<PendingAppendChange>(std::move(data_file))));
    return Ok();
  }

  // UPDATE and DELETE rewrite the files visible to this statement.  read_table
  // may already include earlier pending writes from the same PostgreSQL
  // transaction, so the rewrite preserves read-your-writes across statements.
  IcebergScanCursor current(state->read_table);
  PGICEBERG_RETURN_NOT_OK(current.Init());
  PGICEBERG_ASSIGN_OR_RETURN(auto rewrite_writer,
                             RowBatchWriter::Make(*state->table, state->iceberg_schema,
                                                  state->spec, state->arrow_schema));
  std::vector<bool> used_changes(state->old_rows.size(), false);

  std::shared_ptr<arrow::RecordBatch> batch;
  while (true) {
    PGICEBERG_ASSIGN_OR_RETURN(auto has_batch, current.NextBatch(&batch));
    if (!has_batch) {
      break;
    }
    for (std::int64_t row_index = 0; row_index < batch->num_rows(); row_index++) {
      PGICEBERG_ASSIGN_OR_RETURN(
          Row current_row, RowFromRecordBatch(*batch, row_index, state->tuple_desc));

      std::optional<std::size_t> matched_change;
      for (std::size_t i = 0; i < state->old_rows.size(); i++) {
        PGICEBERG_ASSIGN_OR_RETURN(
            auto rows_equal,
            RowEquals(current_row, state->old_rows[i], state->tuple_desc));
        if (!used_changes[i] && rows_equal) {
          matched_change = i;
          used_changes[i] = true;
          break;
        }
      }

      if (!matched_change.has_value()) {
        PGICEBERG_RETURN_NOT_OK(rewrite_writer->Append(state->attr_numbers, current_row));
      } else if (state->operation == CMD_UPDATE) {
        PGICEBERG_RETURN_NOT_OK(rewrite_writer->Append(state->attr_numbers,
                                                       state->new_rows[*matched_change]));
      }
    }
  }

  PGICEBERG_ASSIGN_OR_RETURN(auto replacement, rewrite_writer->Finish());
  auto pending_table_result = EnsurePendingTableChange(state->options, state->table);
  if (!pending_table_result) {
    CleanupDataFiles(state->table, NewDataFilePaths(replacement));
    return std::unexpected(pending_table_result.error());
  }
  PGICEBERG_RETURN_NOT_OK(QueuePendingModifyChange(
      **pending_table_result, std::make_unique<PendingOverwriteChange>(
                                  current.data_files(), std::move(replacement))));
  return Ok();
}

}  // namespace pgiceberg::fdw
