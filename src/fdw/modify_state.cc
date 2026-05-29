#include "fdw/modify_state.h"

#include <unistd.h>

#include <chrono>
#include <cstring>
#include <filesystem>
#include <memory>
#include <optional>
#include <sstream>
#include <stdexcept>
#include <string>
#include <utility>
#include <vector>

#include <arrow/array.h>
#include <arrow/array/builder_base.h>
#include <arrow/c/bridge.h>
#include <arrow/record_batch.h>
#include <arrow/scalar.h>
#include <arrow/table.h>
#include <iceberg/data/data_writer.h>
#include <iceberg/data/file_scan_task_reader.h>
#include <iceberg/file_format.h>
#include <iceberg/partition_spec.h>
#include <iceberg/schema.h>
#include <iceberg/schema_internal.h>
#include <iceberg/table.h>
#include <iceberg/table_scan.h>
#include <iceberg/transaction.h>
#include <iceberg/update/fast_append.h>
#include <iceberg/update/merging_snapshot_update.h>

#include "common/catalog.h"
#include "common/datum_convert.h"
#include "common/error.h"

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

struct Value {
  Datum datum = static_cast<Datum>(0);
  bool is_null = true;
};

using Row = std::vector<Value>;

struct CurrentTableData {
  std::shared_ptr<arrow::Table> table;
  std::vector<std::shared_ptr<iceberg::DataFile>> data_files;
};

class OverwriteFiles final : public iceberg::MergingSnapshotUpdate {
 public:
  static iceberg::Result<std::shared_ptr<OverwriteFiles>> Make(
      const std::shared_ptr<iceberg::Table>& table) {
    auto ctx_result =
        iceberg::TransactionContext::Make(table, iceberg::TransactionKind::kUpdate);
    if (!ctx_result) {
      return std::unexpected<iceberg::Error>(ctx_result.error());
    }
    return std::shared_ptr<OverwriteFiles>(
        new OverwriteFiles(std::string(table->name().name), std::move(*ctx_result)));
  }

  std::string operation() override { return iceberg::DataOperation::kOverwrite; }

  iceberg::Status AddFile(std::shared_ptr<iceberg::DataFile> file) {
    return AddDataFile(std::move(file));
  }

  iceberg::Status RemoveFile(std::shared_ptr<iceberg::DataFile> file) {
    return DeleteDataFile(std::move(file));
  }

 private:
  OverwriteFiles(std::string table_name, std::shared_ptr<iceberg::TransactionContext> ctx)
      : iceberg::MergingSnapshotUpdate(std::move(table_name), std::move(ctx)) {}
};

std::shared_ptr<arrow::Schema> ArrowSchemaFor(const iceberg::Schema& schema) {
  ArrowSchema c_schema;
  auto status = iceberg::ToArrowSchema(schema, &c_schema);
  if (!status) {
    pgiceberg::ThrowIcebergError(status.error());
  }
  return pgiceberg::CheckArrowResult(arrow::ImportSchema(&c_schema),
                                     "import Arrow schema");
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

std::vector<std::unique_ptr<arrow::ArrayBuilder>> MakeBuilders(
    const arrow::Schema& schema) {
  std::vector<std::unique_ptr<arrow::ArrayBuilder>> builders;
  builders.reserve(schema.num_fields());
  for (int i = 0; i < schema.num_fields(); i++) {
    auto builder =
        arrow::MakeBuilder(schema.field(i)->type(), arrow::default_memory_pool());
    builders.push_back(
        pgiceberg::CheckArrowResult(std::move(builder), "make Arrow builder"));
  }
  return builders;
}

std::shared_ptr<arrow::Scalar> ScalarFromDatum(Datum value, const arrow::DataType& type) {
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
      throw std::runtime_error("unsupported pgiceberg INSERT Arrow type " +
                               type.ToString());
  }
}

void AppendValue(arrow::ArrayBuilder& builder, const Value& value,
                 const arrow::DataType& type) {
  if (value.is_null) {
    pgiceberg::CheckArrowStatus(builder.AppendNull(), "append Arrow NULL");
    return;
  }
  auto scalar = ScalarFromDatum(value.datum, type);
  pgiceberg::CheckArrowStatus(builder.AppendScalar(*scalar), "append Arrow scalar");
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

bool DatumEquals(Datum left, Datum right, Oid type) {
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
      throw std::runtime_error("unsupported pgiceberg row identity type");
  }
}

bool RowEquals(const Row& left, const Row& right, TupleDesc desc) {
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
    if (!left[i].is_null && !DatumEquals(left[i].datum, right[i].datum, attr->atttypid)) {
      return false;
    }
  }
  return true;
}

void AppendRow(std::vector<std::unique_ptr<arrow::ArrayBuilder>>& builders,
               const arrow::Schema& schema, const std::vector<int>& attr_numbers,
               const Row& row) {
  for (int i = 0; i < schema.num_fields(); i++) {
    AppendValue(*builders[i], row[attr_numbers[i] - 1], *schema.field(i)->type());
  }
}

std::shared_ptr<iceberg::DataFile> WriteRows(
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
    arrays.push_back(
        pgiceberg::CheckArrowResult(builder->Finish(), "finish Arrow array"));
  }

  auto batch = arrow::RecordBatch::Make(arrow_schema, rows, arrays);
  ArrowArray c_array;
  pgiceberg::CheckArrowStatus(arrow::ExportRecordBatch(*batch, &c_array),
                              "export Arrow record batch");

  auto writer = iceberg::DataWriter::Make(iceberg::DataWriterOptions{
      .path = DataFilePath(table),
      .schema = iceberg_schema,
      .spec = spec,
      .format = iceberg::FileFormatType::kParquet,
      .io = table.io(),
      .properties = table.properties().configs(),
  });
  if (!writer) {
    pgiceberg::ThrowIcebergError(writer.error());
  }
  auto write_status = (*writer)->Write(&c_array);
  if (!write_status) {
    pgiceberg::ThrowIcebergError(write_status.error());
  }
  auto close_status = (*writer)->Close();
  if (!close_status) {
    pgiceberg::ThrowIcebergError(close_status.error());
  }
  auto metadata = (*writer)->Metadata();
  if (!metadata) {
    pgiceberg::ThrowIcebergError(metadata.error());
  }
  if (metadata->data_files.empty()) {
    return nullptr;
  }
  return metadata->data_files.front();
}

CurrentTableData ReadCurrentTable(iceberg::Table& table) {
  auto schema = table.schema();
  if (!schema) {
    pgiceberg::ThrowIcebergError(schema.error());
  }

  std::vector<std::shared_ptr<iceberg::Schema>> schemas;
  auto all_schemas = table.schemas();
  if (all_schemas) {
    for (const auto& [_, candidate] : all_schemas->get()) {
      schemas.push_back(candidate);
    }
  }

  auto scan_builder = table.NewScan();
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

  auto reader = iceberg::FileScanTaskReader::Make(iceberg::FileScanTaskReader::Options{
      .io = table.io(),
      .table_schema = *schema,
      .schemas = std::move(schemas),
      .projected_schema = *schema,
  });
  if (!reader) {
    pgiceberg::ThrowIcebergError(reader.error());
  }

  CurrentTableData result;
  std::vector<std::shared_ptr<arrow::Table>> tables;
  tables.reserve(tasks->size());
  result.data_files.reserve(tasks->size());
  for (const auto& task : *tasks) {
    result.data_files.push_back(task->data_file());
    auto stream = (*reader)->Open(*task);
    if (!stream) {
      pgiceberg::ThrowIcebergError(stream.error());
    }
    auto reader_ptr = pgiceberg::CheckArrowResult(
        arrow::ImportRecordBatchReader(&*stream), "import record batch reader");
    tables.push_back(pgiceberg::CheckArrowResult(
        arrow::Table::FromRecordBatchReader(reader_ptr.get()), "read Arrow table"));
  }

  if (tables.empty()) {
    result.table = arrow::Table::Make(
        ArrowSchemaFor(**schema), std::vector<std::shared_ptr<arrow::ChunkedArray>>{});
  } else if (tables.size() == 1) {
    result.table = tables.front();
  } else {
    result.table = pgiceberg::CheckArrowResult(arrow::ConcatenateTables(tables),
                                               "concatenate Arrow tables");
  }
  return result;
}

Row RowFromArrowTable(const arrow::Table& table, std::int64_t row_index, TupleDesc desc) {
  Row row(desc->natts);
  for (int i = 0; i < desc->natts; i++) {
    Form_pg_attribute attr = TupleDescAttr(desc, i);
    if (attr->attisdropped) {
      continue;
    }
    const int column_index = table.schema()->GetFieldIndex(NameStr(attr->attname));
    if (column_index < 0) {
      throw std::runtime_error("column does not exist in Iceberg table");
    }

    auto column = table.column(column_index);
    std::int64_t offset = row_index;
    int chunk_index = 0;
    while (chunk_index < column->num_chunks() &&
           offset >= column->chunk(chunk_index)->length()) {
      offset -= column->chunk(chunk_index)->length();
      chunk_index++;
    }
    if (chunk_index >= column->num_chunks()) {
      throw std::runtime_error("invalid Arrow chunk index");
    }

    bool is_null = true;
    Datum datum = pgiceberg::ConvertValue(*column->chunk(chunk_index), offset,
                                          attr->atttypid, is_null);
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

ModifyState* BeginModify(ModifyTableState* mtstate, ResultRelInfo* rinfo,
                         const Options& options) {
  auto state = std::make_unique<ModifyState>();
  state->operation = mtstate->operation;
  Relation relation = rinfo->ri_RelationDesc;
  state->tuple_desc = RelationGetDescr(relation);
  state->table = pgiceberg::LoadIcebergTable(ToCatalogOptions(options),
                                             RelationGetRelationName(relation));

  auto schema = state->table->schema();
  if (!schema) {
    pgiceberg::ThrowIcebergError(schema.error());
  }
  state->iceberg_schema = *schema;
  state->arrow_schema = ArrowSchemaFor(**schema);

  auto spec = state->table->spec();
  if (!spec) {
    pgiceberg::ThrowIcebergError(spec.error());
  }
  state->spec = *spec;
  if (!state->spec->fields().empty()) {
    throw std::runtime_error(
        "pgiceberg DML currently supports only unpartitioned Iceberg tables");
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
      throw std::runtime_error("Iceberg field \"" + field->name() +
                               "\" does not exist in foreign table");
    }
    state->attr_numbers.push_back(attr_number);
  }
  state->builders = MakeBuilders(*state->arrow_schema);

  if (state->operation == CMD_UPDATE || state->operation == CMD_DELETE) {
    auto* subplan = outerPlanState(mtstate)->plan;
    state->wholerow_attno = ExecFindJunkAttributeInTlist(subplan->targetlist, "wholerow");
    if (!AttributeNumberIsValid(state->wholerow_attno)) {
      throw std::runtime_error("could not find junk wholerow column");
    }
  }

  RegisterMemoryContextCleanup(state.get());
  return state.release();
}

TupleTableSlot* ExecInsert(ModifyState* state, TupleTableSlot* slot) {
  AppendRow(state->builders, *state->arrow_schema, state->attr_numbers,
            CopyRowFromSlot(slot, state->tuple_desc));
  state->rows++;
  return slot;
}

TupleTableSlot* ExecUpdate(ModifyState* state, TupleTableSlot* slot,
                           TupleTableSlot* plan_slot) {
  bool is_null = false;
  Datum wholerow = ExecGetJunkAttribute(plan_slot, state->wholerow_attno, &is_null);
  if (is_null) {
    throw std::runtime_error("wholerow is NULL");
  }
  state->old_rows.push_back(CopyRowFromHeapTupleDatum(wholerow, state->tuple_desc));
  state->new_rows.push_back(CopyRowFromSlot(slot, state->tuple_desc));
  state->rows++;
  return slot;
}

TupleTableSlot* ExecDelete(ModifyState* state, TupleTableSlot* slot,
                           TupleTableSlot* plan_slot) {
  bool is_null = false;
  Datum wholerow = ExecGetJunkAttribute(plan_slot, state->wholerow_attno, &is_null);
  if (is_null) {
    throw std::runtime_error("wholerow is NULL");
  }
  Row old_row = CopyRowFromHeapTupleDatum(wholerow, state->tuple_desc);
  state->old_rows.push_back(old_row);
  state->rows++;
  return StoreRowInSlot(old_row, slot, state->tuple_desc);
}

void EndModify(ModifyState* state) {
  DetachMemoryContextCleanup(state);
  std::unique_ptr<ModifyState> guard(state);
  if (state == nullptr || state->rows == 0) {
    return;
  }

  if (state->operation == CMD_INSERT) {
    auto data_file =
        WriteRows(*state->table, state->iceberg_schema, state->spec, state->arrow_schema,
                  std::move(state->builders), state->rows);
    auto append = state->table->NewFastAppend();
    if (!append) {
      pgiceberg::ThrowIcebergError(append.error());
    }
    if (data_file != nullptr) {
      (*append)->AppendFile(data_file);
    }
    auto commit = (*append)->Commit();
    if (!commit) {
      pgiceberg::ThrowIcebergError(commit.error());
    }
    return;
  }

  auto current = ReadCurrentTable(*state->table);
  auto rewrite_builders = MakeBuilders(*state->arrow_schema);
  std::vector<bool> used_changes(state->old_rows.size(), false);
  std::int64_t rewrite_rows = 0;

  for (std::int64_t row_index = 0; row_index < current.table->num_rows(); row_index++) {
    Row current_row = RowFromArrowTable(*current.table, row_index, state->tuple_desc);

    std::optional<std::size_t> matched_change;
    for (std::size_t i = 0; i < state->old_rows.size(); i++) {
      if (!used_changes[i] &&
          RowEquals(current_row, state->old_rows[i], state->tuple_desc)) {
        matched_change = i;
        used_changes[i] = true;
        break;
      }
    }

    if (!matched_change.has_value()) {
      AppendRow(rewrite_builders, *state->arrow_schema, state->attr_numbers, current_row);
      rewrite_rows++;
    } else if (state->operation == CMD_UPDATE) {
      AppendRow(rewrite_builders, *state->arrow_schema, state->attr_numbers,
                state->new_rows[*matched_change]);
      rewrite_rows++;
    }
  }

  auto replacement =
      WriteRows(*state->table, state->iceberg_schema, state->spec, state->arrow_schema,
                std::move(rewrite_builders), rewrite_rows);
  auto overwrite = OverwriteFiles::Make(state->table);
  if (!overwrite) {
    pgiceberg::ThrowIcebergError(overwrite.error());
  }
  for (const auto& file : current.data_files) {
    auto remove = (*overwrite)->RemoveFile(file);
    if (!remove) {
      pgiceberg::ThrowIcebergError(remove.error());
    }
  }
  if (replacement != nullptr) {
    auto add = (*overwrite)->AddFile(replacement);
    if (!add) {
      pgiceberg::ThrowIcebergError(add.error());
    }
  }
  auto commit = (*overwrite)->Commit();
  if (!commit) {
    pgiceberg::ThrowIcebergError(commit.error());
  }
}

}  // namespace pgiceberg::fdw
