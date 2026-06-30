#include "fdw/modify_state.h"

#include <unistd.h>

#include <chrono>
#include <cstring>
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
#include <arrow/scalar.h>
#include <iceberg/data/data_writer.h>
#include <iceberg/file_format.h>
#include <iceberg/partition_spec.h>
#include <iceberg/schema.h>
#include <iceberg/schema_internal.h>
#include <iceberg/table.h>
#include <iceberg/transaction.h>
#include <iceberg/update/fast_append.h>
#include <iceberg/update/overwrite_files.h>

#include "common/catalog.h"
#include "common/datum_convert.h"
#include "common/status.h"
#include "fdw/iceberg_scan.h"

extern "C" {
#include "postgres.h"
#include "access/htup_details.h"
#include "catalog/pg_type_d.h"
#include "executor/executor.h"
#include "nodes/execnodes.h"
#include "nodes/plannodes.h"
#include "utils/builtins.h"
#include "utils/date.h"
#include "utils/datum.h"
#include "utils/memutils.h"
#include "utils/rel.h"
#include "utils/timestamp.h"
}

namespace pgiceberg::fdw {
namespace {

constexpr std::int64_t kPostgresUnixEpochOffsetMicros = 946684800000000LL;
constexpr std::int64_t kDmlRewriteBatchRows = 8192;

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

Result<std::shared_ptr<arrow::Scalar>> ScalarFromDatum(Datum value,
                                                       const arrow::DataType& type) {
  switch (type.id()) {
    case arrow::Type::BOOL:
      return std::make_shared<arrow::BooleanScalar>(DatumGetBool(value));
    case arrow::Type::INT16:
      return std::make_shared<arrow::Int16Scalar>(DatumGetInt16(value));
    case arrow::Type::INT32:
    case arrow::Type::DATE32: {
      if (type.id() == arrow::Type::DATE32) {
        const auto days =
            DatumGetDateADT(value) + POSTGRES_EPOCH_JDATE - UNIX_EPOCH_JDATE;
        return std::make_shared<arrow::Date32Scalar>(days);
      }
      return std::make_shared<arrow::Int32Scalar>(DatumGetInt32(value));
    }
    case arrow::Type::INT64:
      return std::make_shared<arrow::Int64Scalar>(DatumGetInt64(value));
    case arrow::Type::FLOAT:
      return std::make_shared<arrow::FloatScalar>(DatumGetFloat4(value));
    case arrow::Type::DOUBLE:
      return std::make_shared<arrow::DoubleScalar>(DatumGetFloat8(value));
    case arrow::Type::STRING:
    case arrow::Type::LARGE_STRING: {
      char* text_value = TextDatumGetCString(value);
      return std::make_shared<arrow::StringScalar>(std::string(text_value));
    }
    case arrow::Type::TIMESTAMP: {
      const auto micros = DatumGetTimestamp(value) + kPostgresUnixEpochOffsetMicros;
      return std::make_shared<arrow::TimestampScalar>(
          micros, std::static_pointer_cast<arrow::TimestampType>(
                      std::const_pointer_cast<arrow::DataType>(type.shared_from_this())));
    }
    default:
      return std::unexpected(
          MakeError(ERRCODE_FEATURE_NOT_SUPPORTED,
                    "unsupported pgiceberg INSERT Arrow type " + type.ToString()));
  }
}

Status AppendValue(arrow::ArrayBuilder& builder, const Value& value,
                   const arrow::DataType& type) {
  if (value.is_null) {
    return FromArrowStatus(builder.AppendNull(), "append Arrow NULL");
  }
  PGICEBERG_ASSIGN_OR_RETURN(auto scalar, ScalarFromDatum(value.datum, type));
  return FromArrowStatus(builder.AppendScalar(*scalar), "append Arrow scalar");
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

Result<bool> DatumEquals(Datum left, Datum right, Oid type) {
  switch (type) {
    case BOOLOID:
      return DatumGetBool(left) == DatumGetBool(right);
    case INT2OID:
      return DatumGetInt16(left) == DatumGetInt16(right);
    case INT4OID:
      return DatumGetInt32(left) == DatumGetInt32(right);
    case INT8OID:
      return DatumGetInt64(left) == DatumGetInt64(right);
    case FLOAT4OID:
      return DatumGetFloat4(left) == DatumGetFloat4(right);
    case FLOAT8OID:
      return DatumGetFloat8(left) == DatumGetFloat8(right);
    case DATEOID:
      return DatumGetDateADT(left) == DatumGetDateADT(right);
    case TIMESTAMPOID:
      return DatumGetTimestamp(left) == DatumGetTimestamp(right);
    case TIMESTAMPTZOID:
      return DatumGetTimestampTz(left) == DatumGetTimestampTz(right);
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
          auto equal, DatumEquals(left[i].datum, right[i].datum, attr->atttypid));
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

struct ModifyState {
  MemoryContextCallback* cleanup_callback = nullptr;
  CmdType operation = CMD_UNKNOWN;
  std::shared_ptr<iceberg::Table> table;
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
  Relation relation = rinfo->ri_RelationDesc;
  state->tuple_desc = RelationGetDescr(relation);
  PGICEBERG_ASSIGN_OR_RETURN(
      state->table, pgiceberg::LoadIcebergTable(ToCatalogOptions(options),
                                                RelationGetRelationName(relation)));

  PGICEBERG_ASSIGN_OR_RETURN(state->iceberg_schema,
                             FromIcebergResult(state->table->schema(), "load schema"));
  PGICEBERG_ASSIGN_OR_RETURN(state->arrow_schema, ArrowSchemaFor(*state->iceberg_schema));

  PGICEBERG_ASSIGN_OR_RETURN(
      state->spec, FromIcebergResult(state->table->spec(), "load partition spec"));
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
    PGICEBERG_ASSIGN_OR_RETURN(
        auto append,
        FromIcebergResult(state->table->NewFastAppend(), "create append update"));
    if (data_file != nullptr) {
      append->AppendFile(data_file);
    }
    PGICEBERG_RETURN_NOT_OK(FromIcebergStatus(append->Commit(), "commit append"));
    return Ok();
  }

  IcebergScanCursor current(state->table);
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
  PGICEBERG_ASSIGN_OR_RETURN(
      auto overwrite,
      FromIcebergResult(state->table->NewOverwrite(), "create overwrite update"));
  for (const auto& file : current.data_files()) {
    overwrite->DeleteFile(file);
  }
  if (replacement != nullptr) {
    overwrite->AddFile(replacement);
  }
  PGICEBERG_RETURN_NOT_OK(FromIcebergStatus(overwrite->Commit(), "commit overwrite"));
  return Ok();
}

}  // namespace pgiceberg::fdw
