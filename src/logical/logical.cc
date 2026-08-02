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

#include "logical/logical.h"

#include <charconv>
#include <cerrno>
#include <cstdint>
#include <cstring>
#include <limits>
#include <optional>
#include <string>
#include <string_view>
#include <vector>

#include "common/pg_error.h"
#include "common/pg_relation.h"
#include "common/status.h"
#include "fdw/modify_state.h"
#include "fdw/options.h"

extern "C" {
#include "postgres.h"
#include "access/heapam.h"
#include "access/htup_details.h"
#include "access/table.h"
#include "access/xact.h"
#include "catalog/pg_type_d.h"
#include "executor/spi.h"
#include "executor/tuptable.h"
#include "fmgr.h"
#include "utils/fmgrprotos.h"
#include "miscadmin.h"
#include "postmaster/bgworker.h"
#include "replication/logical.h"
#include "replication/output_plugin.h"
#include "replication/reorderbuffer.h"
#include "storage/ipc.h"
#include "storage/latch.h"
#include "tcop/tcopprot.h"
#include "utils/builtins.h"
#include "utils/elog.h"
#include "utils/guc.h"
#include "utils/lsyscache.h"
#include "utils/memutils.h"
#include "utils/pg_lsn.h"
#include "utils/rel.h"
#include "utils/snapmgr.h"
}

namespace pgiceberg::logical {
namespace {

constexpr const char* kWorkerName = "pgiceberg logical sync";
constexpr int kDefaultPollIntervalMs = 1000;
constexpr int kDefaultBatchSize = 1024;
constexpr int kMaxBatchSize = 100000;

char* LogicalSyncDatabase = nullptr;
char* LogicalSyncUser = nullptr;
int LogicalSyncPollIntervalMs = kDefaultPollIntervalMs;
int LogicalSyncBatchSize = kDefaultBatchSize;
volatile sig_atomic_t GotSigterm = false;
volatile sig_atomic_t GotSighup = false;

void HandleSigterm(SIGNAL_ARGS) {
  int save_errno = errno;
  GotSigterm = true;
  if (MyLatch != nullptr) {
    SetLatch(MyLatch);
  }
  errno = save_errno;
}

void HandleSighup(SIGNAL_ARGS) {
  int save_errno = errno;
  GotSighup = true;
  if (MyLatch != nullptr) {
    SetLatch(MyLatch);
  }
  errno = save_errno;
}

Status EnsureSpiOk(int result, int expected, const char* message) {
  if (result == expected) {
    return Ok();
  }
  return std::unexpected(MakeError(ERRCODE_INTERNAL_ERROR, message));
}

std::string TextDatumToString(Datum value) {
  return std::string(TextDatumGetCString(value));
}

Result<std::string> SpiTextColumn(HeapTuple tuple, TupleDesc desc, int column) {
  bool is_null = false;
  Datum value = SPI_getbinval(tuple, desc, column, &is_null);
  if (is_null) {
    return std::unexpected(MakeError(ERRCODE_NULL_VALUE_NOT_ALLOWED,
                                     "pgiceberg logical mirror row contains NULL"));
  }
  return TextDatumToString(value);
}

Result<Oid> SpiOidColumn(HeapTuple tuple, TupleDesc desc, int column) {
  bool is_null = false;
  Datum value = SPI_getbinval(tuple, desc, column, &is_null);
  if (is_null) {
    return std::unexpected(MakeError(ERRCODE_NULL_VALUE_NOT_ALLOWED,
                                     "pgiceberg logical mirror row contains NULL"));
  }
  return DatumGetObjectId(value);
}

Result<int> SpiIntColumn(HeapTuple tuple, TupleDesc desc, int column, int default_value) {
  bool is_null = false;
  Datum value = SPI_getbinval(tuple, desc, column, &is_null);
  if (is_null) {
    return default_value;
  }
  return DatumGetInt32(value);
}

struct Mirror {
  Oid source_relid = InvalidOid;
  std::string catalog;
  std::string name_space;
  std::string table_name;
  std::string slot_name;
  int batch_size = kDefaultBatchSize;
};

struct DecodedChange {
  char action = '\0';
  Oid relid = InvalidOid;
  std::vector<std::optional<std::string>> values_by_attnum;
};

bool ConsumeChar(std::string_view input, std::size_t& pos, char expected) {
  if (pos >= input.size() || input[pos] != expected) {
    return false;
  }
  pos++;
  return true;
}

Result<std::string_view> ReadToken(std::string_view input, std::size_t& pos,
                                   char delimiter, bool allow_eof = false) {
  if (pos > input.size()) {
    return std::unexpected(
        MakeError(ERRCODE_INTERNAL_ERROR, "invalid pgiceberg logical decoding record"));
  }
  const std::size_t start = pos;
  while (pos < input.size() && input[pos] != delimiter) {
    pos++;
  }
  if (pos >= input.size()) {
    if (!allow_eof || start >= input.size()) {
      return std::unexpected(
          MakeError(ERRCODE_INTERNAL_ERROR, "invalid pgiceberg logical decoding record"));
    }
    return input.substr(start);
  }
  std::string_view token = input.substr(start, pos - start);
  pos++;
  return token;
}

template <typename T>
Result<T> ParseInteger(std::string_view token) {
  T value{};
  auto [ptr, ec] = std::from_chars(token.data(), token.data() + token.size(), value);
  if (ec != std::errc() || ptr != token.data() + token.size()) {
    return std::unexpected(MakeError(
        ERRCODE_INTERNAL_ERROR, "invalid integer in pgiceberg logical decoding record"));
  }
  return value;
}

Result<DecodedChange> ParseDecodedChange(std::string_view input) {
  if (input.size() < 3) {
    return std::unexpected(
        MakeError(ERRCODE_INTERNAL_ERROR, "invalid pgiceberg logical decoding record"));
  }

  DecodedChange change;
  std::size_t pos = 0;
  change.action = input[pos++];
  if (!ConsumeChar(input, pos, '\t')) {
    return std::unexpected(
        MakeError(ERRCODE_INTERNAL_ERROR, "invalid pgiceberg logical decoding record"));
  }

  // Non-INSERT records are emitted as "<action>\t<relid>" with no trailing
  // delimiter, so allow EOF to terminate the relid token.
  PGICEBERG_ASSIGN_OR_RETURN(auto relid_token, ReadToken(input, pos, '\t', true));
  PGICEBERG_ASSIGN_OR_RETURN(change.relid, ParseInteger<Oid>(relid_token));
  if (change.action != 'I') {
    return change;
  }

  PGICEBERG_ASSIGN_OR_RETURN(auto ncols_token, ReadToken(input, pos, '\t'));
  PGICEBERG_ASSIGN_OR_RETURN(auto ncols, ParseInteger<int>(ncols_token));
  change.values_by_attnum.resize(static_cast<std::size_t>(ncols) + 1);

  for (int i = 0; i < ncols; i++) {
    PGICEBERG_ASSIGN_OR_RETURN(auto attnum_token, ReadToken(input, pos, '\t'));
    PGICEBERG_ASSIGN_OR_RETURN(auto attnum, ParseInteger<int>(attnum_token));
    PGICEBERG_ASSIGN_OR_RETURN(auto null_token, ReadToken(input, pos, '\t'));
    if (null_token.size() != 1 || (null_token[0] != 'n' && null_token[0] != 'v')) {
      return std::unexpected(MakeError(ERRCODE_INTERNAL_ERROR,
                                       "invalid NULL marker in logical decoding record"));
    }

    const std::size_t len_start = pos;
    while (pos < input.size() && input[pos] != ':') {
      pos++;
    }
    if (pos >= input.size()) {
      return std::unexpected(MakeError(
          ERRCODE_INTERNAL_ERROR, "invalid value length in logical decoding record"));
    }
    PGICEBERG_ASSIGN_OR_RETURN(
        auto len, ParseInteger<std::size_t>(input.substr(len_start, pos - len_start)));
    pos++;
    if (pos + len > input.size()) {
      return std::unexpected(MakeError(ERRCODE_INTERNAL_ERROR,
                                       "truncated value in logical decoding record"));
    }
    std::optional<std::string> value;
    if (null_token[0] == 'v') {
      value = std::string(input.substr(pos, len));
    }
    pos += len;
    if (i + 1 < ncols && !ConsumeChar(input, pos, '\t')) {
      return std::unexpected(
          MakeError(ERRCODE_INTERNAL_ERROR, "invalid logical decoding record separator"));
    }
    if (attnum <= 0) {
      return std::unexpected(MakeError(
          ERRCODE_INTERNAL_ERROR, "invalid attribute number in logical decoding record"));
    }
    if (static_cast<std::size_t>(attnum) >= change.values_by_attnum.size()) {
      change.values_by_attnum.resize(static_cast<std::size_t>(attnum) + 1);
    }
    change.values_by_attnum[static_cast<std::size_t>(attnum)] = std::move(value);
  }

  return change;
}

Result<std::vector<Mirror>> LoadMirrors() {
  const char* sql =
      "SELECT source_relid, catalog, namespace, table_name, slot_name, batch_size "
      "FROM pgiceberg.logical_mirrors "
      "WHERE enabled "
      "ORDER BY source_relid";
  const int result = SPI_execute(sql, true, 0);
  PGICEBERG_RETURN_NOT_OK(
      EnsureSpiOk(result, SPI_OK_SELECT, "could not read pgiceberg logical mirrors"));

  std::vector<Mirror> mirrors;
  mirrors.reserve(SPI_processed);
  TupleDesc desc = SPI_tuptable->tupdesc;
  for (uint64 i = 0; i < SPI_processed; i++) {
    HeapTuple tuple = SPI_tuptable->vals[i];
    Mirror mirror;
    PGICEBERG_ASSIGN_OR_RETURN(mirror.source_relid, SpiOidColumn(tuple, desc, 1));
    PGICEBERG_ASSIGN_OR_RETURN(mirror.catalog, SpiTextColumn(tuple, desc, 2));
    PGICEBERG_ASSIGN_OR_RETURN(mirror.name_space, SpiTextColumn(tuple, desc, 3));
    PGICEBERG_ASSIGN_OR_RETURN(mirror.table_name, SpiTextColumn(tuple, desc, 4));
    PGICEBERG_ASSIGN_OR_RETURN(mirror.slot_name, SpiTextColumn(tuple, desc, 5));
    PGICEBERG_ASSIGN_OR_RETURN(mirror.batch_size,
                               SpiIntColumn(tuple, desc, 6, LogicalSyncBatchSize));
    if (mirror.batch_size <= 0) {
      mirror.batch_size = LogicalSyncBatchSize;
    }
    mirrors.push_back(std::move(mirror));
  }
  return mirrors;
}

Result<std::vector<std::pair<std::string, std::string>>> PeekSlotChanges(
    const Mirror& mirror) {
  const char* sql =
      "SELECT lsn::text, data "
      "FROM pg_logical_slot_peek_changes($1::text::name, NULL, $2::integer)";
  Oid argtypes[] = {TEXTOID, INT4OID};
  Datum values[] = {CStringGetTextDatum(mirror.slot_name.c_str()),
                    Int32GetDatum(mirror.batch_size)};
  const int result = SPI_execute_with_args(sql, 2, argtypes, values, nullptr, true, 0);
  PGICEBERG_RETURN_NOT_OK(
      EnsureSpiOk(result, SPI_OK_SELECT, "could not peek pgiceberg logical slot"));

  std::vector<std::pair<std::string, std::string>> rows;
  rows.reserve(SPI_processed);
  TupleDesc desc = SPI_tuptable->tupdesc;
  for (uint64 i = 0; i < SPI_processed; i++) {
    HeapTuple tuple = SPI_tuptable->vals[i];
    PGICEBERG_ASSIGN_OR_RETURN(auto lsn, SpiTextColumn(tuple, desc, 1));
    PGICEBERG_ASSIGN_OR_RETURN(auto data, SpiTextColumn(tuple, desc, 2));
    rows.emplace_back(std::move(lsn), std::move(data));
  }
  return rows;
}

Status AdvanceSlotByCount(const Mirror& mirror, int change_count) {
  if (change_count <= 0) {
    return Ok();
  }
  // Consume exactly the peeked prefix. Advancing by LSN can skip additional
  // decoded messages that share the final LSN and were not processed yet.
  const char* sql =
      "SELECT count(*) "
      "FROM pg_logical_slot_get_changes($1::text::name, NULL, $2::integer)";
  Oid argtypes[] = {TEXTOID, INT4OID};
  Datum values[] = {CStringGetTextDatum(mirror.slot_name.c_str()),
                    Int32GetDatum(change_count)};
  const int result = SPI_execute_with_args(sql, 2, argtypes, values, nullptr, false, 1);
  PGICEBERG_RETURN_NOT_OK(
      EnsureSpiOk(result, SPI_OK_SELECT, "could not advance pgiceberg logical slot"));

  bool is_null = false;
  Datum count_datum =
      SPI_getbinval(SPI_tuptable->vals[0], SPI_tuptable->tupdesc, 1, &is_null);
  if (is_null || DatumGetInt64(count_datum) != change_count) {
    return std::unexpected(MakeError(
        ERRCODE_INTERNAL_ERROR,
        "pgiceberg logical slot advanced a different number of changes than peeked"));
  }
  return Ok();
}

Status UpdateMirrorProgress(const Mirror& mirror, const std::string& lsn,
                            const char* error_message) {
  const char* sql =
      "UPDATE pgiceberg.logical_mirrors "
      "SET last_flushed_lsn = CASE WHEN $2::pg_lsn IS NULL THEN last_flushed_lsn "
      "                            ELSE $2::pg_lsn END, "
      "    last_error = $3, "
      "    updated_at = now() "
      "WHERE source_relid = $1";
  Oid argtypes[] = {OIDOID, LSNOID, TEXTOID};
  char nulls[] = {' ', lsn.empty() ? 'n' : ' ', error_message == nullptr ? 'n' : ' ',
                  '\0'};
  Datum values[] = {
      ObjectIdGetDatum(mirror.source_relid),
      lsn.empty() ? static_cast<Datum>(0)
                  : DirectFunctionCall1(pg_lsn_in, CStringGetDatum(lsn.c_str())),
      error_message == nullptr ? static_cast<Datum>(0)
                               : CStringGetTextDatum(error_message),
  };
  const int result = SPI_execute_with_args(sql, 3, argtypes, values, nulls, false, 1);
  return EnsureSpiOk(result, SPI_OK_UPDATE, "could not update pgiceberg logical mirror");
}

Status DisableMirror(const Mirror& mirror, const char* error_message) {
  const char* sql =
      "UPDATE pgiceberg.logical_mirrors "
      "SET enabled = false, last_error = $2, updated_at = now() "
      "WHERE source_relid = $1";
  Oid argtypes[] = {OIDOID, TEXTOID};
  Datum values[] = {ObjectIdGetDatum(mirror.source_relid),
                    CStringGetTextDatum(error_message)};
  const int result = SPI_execute_with_args(sql, 2, argtypes, values, nullptr, false, 1);
  return EnsureSpiOk(result, SPI_OK_UPDATE, "could not disable pgiceberg logical mirror");
}

Result<TupleTableSlot*> SlotFromDecodedRow(Relation relation,
                                           const DecodedChange& change) {
  TupleDesc desc = RelationGetDescr(relation);
  TupleTableSlot* slot = MakeSingleTupleTableSlot(desc, &TTSOpsVirtual);
  ExecClearTuple(slot);
  for (int i = 0; i < desc->natts; i++) {
    slot->tts_values[i] = static_cast<Datum>(0);
    slot->tts_isnull[i] = true;
    Form_pg_attribute attr = TupleDescAttr(desc, i);
    if (attr->attisdropped) {
      continue;
    }

    const int attnum = attr->attnum;
    if (attnum <= 0 ||
        static_cast<std::size_t>(attnum) >= change.values_by_attnum.size()) {
      return std::unexpected(
          MakeError(ERRCODE_INTERNAL_ERROR, "decoded row is missing a source column"));
    }
    const auto& value = change.values_by_attnum[static_cast<std::size_t>(attnum)];
    if (!value.has_value()) {
      continue;
    }

    Oid typinput = InvalidOid;
    Oid typioparam = InvalidOid;
    getTypeInputInfo(attr->atttypid, &typinput, &typioparam);
    slot->tts_values[i] = OidInputFunctionCall(
        typinput, const_cast<char*>(value->c_str()), typioparam, attr->atttypmod);
    slot->tts_isnull[i] = false;
  }
  ExecStoreVirtualTuple(slot);
  return slot;
}

Status AppendDecodedRows(Relation relation, const Mirror& mirror,
                         const std::vector<DecodedChange>& rows) {
  if (rows.empty()) {
    return Ok();
  }

  fdw::Options options;
  options.catalog = mirror.catalog;
  options.name_space = mirror.name_space;
  options.table = mirror.table_name;

  std::vector<TupleTableSlot*> slots;
  slots.reserve(rows.size());
  for (const auto& row : rows) {
    PGICEBERG_ASSIGN_OR_RETURN(auto* slot, SlotFromDecodedRow(relation, row));
    slots.push_back(slot);
  }

  Status status =
      fdw::AppendSlots(relation, options, slots.data(), static_cast<int>(slots.size()));
  for (auto* slot : slots) {
    ExecDropSingleTupleTableSlot(slot);
  }
  return status;
}

Status ProcessMirror(const Mirror& mirror) {
  PGICEBERG_ASSIGN_OR_RETURN(auto rows, PeekSlotChanges(mirror));
  if (rows.empty()) {
    return Ok();
  }

  std::vector<DecodedChange> inserts;
  std::string last_lsn;
  bool saw_unsupported = false;
  for (const auto& [lsn, data] : rows) {
    last_lsn = lsn;
    PGICEBERG_ASSIGN_OR_RETURN(auto change, ParseDecodedChange(data));
    if (change.relid != mirror.source_relid) {
      continue;
    }
    if (change.action == 'I') {
      inserts.push_back(std::move(change));
      continue;
    }
    if (change.action == 'U' || change.action == 'D' || change.action == 'T') {
      saw_unsupported = true;
    }
  }

  Relation relation = table_open(mirror.source_relid, AccessShareLock);
  RelationLockGuard relation_guard(relation, AccessShareLock);
  PGICEBERG_RETURN_NOT_OK(AppendDecodedRows(relation, mirror, inserts));
  // Commit Iceberg before consuming WAL so a failed/crashy Iceberg commit cannot
  // lose changes. At-least-once delivery may produce duplicates if the process
  // crashes after Iceberg commit and before slot advancement.
  PGICEBERG_RETURN_NOT_OK(fdw::FlushPendingModifyChanges());

  if (!last_lsn.empty()) {
    PGICEBERG_RETURN_NOT_OK(AdvanceSlotByCount(mirror, static_cast<int>(rows.size())));
    PGICEBERG_RETURN_NOT_OK(UpdateMirrorProgress(mirror, last_lsn, nullptr));
  }
  if (saw_unsupported) {
    const char* message =
        "pgiceberg logical mirrors currently support only INSERT changes";
    PGICEBERG_RETURN_NOT_OK(DisableMirror(mirror, message));
    ereport(WARNING, (errmsg("%s", message)));
  }
  return Ok();
}

Status ProcessMirrors() {
  PGICEBERG_ASSIGN_OR_RETURN(auto mirrors, LoadMirrors());
  for (const auto& mirror : mirrors) {
    PGICEBERG_RETURN_NOT_OK(ProcessMirror(mirror));
  }
  return Ok();
}

void RunWorkerLoop() {
#if PG_VERSION_NUM >= 170000
  BackgroundWorkerInitializeConnection(LogicalSyncDatabase, LogicalSyncUser,
                                       BGWORKER_BYPASS_ROLELOGINCHECK);
#else
  BackgroundWorkerInitializeConnection(LogicalSyncDatabase, LogicalSyncUser,
                                       BGWORKER_BYPASS_ALLOWCONN);
#endif
  while (!GotSigterm) {
    if (GotSighup) {
      GotSighup = false;
      ProcessConfigFile(PGC_SIGHUP);
    }

    PG_TRY();
    {
      StartTransactionCommand();
      PushActiveSnapshot(GetTransactionSnapshot());
      if (SPI_connect() != SPI_OK_CONNECT) {
        ereport(ERROR, (errmsg("could not connect to SPI")));
      }
      PgStatusGuard([&]() { return ProcessMirrors(); });
      SPI_finish();
      PopActiveSnapshot();
      CommitTransactionCommand();
    }
    PG_CATCH();
    {
      EmitErrorReport();
      FlushErrorState();
      AbortCurrentTransaction();
    }
    PG_END_TRY();

    const int wait_result =
        WaitLatch(MyLatch, WL_LATCH_SET | WL_TIMEOUT | WL_EXIT_ON_PM_DEATH,
                  LogicalSyncPollIntervalMs, 0);
    if (wait_result & WL_LATCH_SET) {
      ResetLatch(MyLatch);
    }
  }
  proc_exit(0);
}

void RegisterLogicalGucs() {
  DefineCustomStringVariable(
      "pgiceberg.logical_sync_database",
      "Database where the pgiceberg logical sync worker runs.",
      "The initial implementation runs one worker against one configured database.",
      &LogicalSyncDatabase, "postgres", PGC_POSTMASTER, 0, nullptr, nullptr, nullptr);
  DefineCustomStringVariable("pgiceberg.logical_sync_user",
                             "User for the pgiceberg logical sync worker.", nullptr,
                             &LogicalSyncUser, "postgres", PGC_POSTMASTER, 0, nullptr,
                             nullptr, nullptr);
  DefineCustomIntVariable("pgiceberg.logical_sync_poll_interval_ms",
                          "Polling interval for the pgiceberg logical sync worker.",
                          nullptr, &LogicalSyncPollIntervalMs, kDefaultPollIntervalMs, 10,
                          60000, PGC_SIGHUP, 0, nullptr, nullptr, nullptr);
  DefineCustomIntVariable(
      "pgiceberg.logical_sync_batch_size",
      "Default maximum number of logical decoding changes consumed per worker poll.",
      nullptr, &LogicalSyncBatchSize, kDefaultBatchSize, 1, kMaxBatchSize, PGC_SIGHUP, 0,
      nullptr, nullptr, nullptr);
}

void RegisterWorker() {
  if (!process_shared_preload_libraries_in_progress) {
    return;
  }

  BackgroundWorker worker;
  std::memset(&worker, 0, sizeof(worker));
  worker.bgw_flags = BGWORKER_SHMEM_ACCESS | BGWORKER_BACKEND_DATABASE_CONNECTION;
  worker.bgw_start_time = BgWorkerStart_ConsistentState;
  worker.bgw_restart_time = 10;
  snprintf(worker.bgw_library_name, BGW_MAXLEN, "pgiceberg");
  snprintf(worker.bgw_function_name, BGW_MAXLEN, "pgiceberg_logical_worker_main");
  snprintf(worker.bgw_name, BGW_MAXLEN, "%s", kWorkerName);
  snprintf(worker.bgw_type, BGW_MAXLEN, "%s", kWorkerName);
  worker.bgw_main_arg = static_cast<Datum>(0);
  RegisterBackgroundWorker(&worker);
}

}  // namespace

void RegisterLogicalWorker() {
  if (!process_shared_preload_libraries_in_progress) {
    return;
  }
  RegisterLogicalGucs();
  RegisterWorker();
}

Status ProcessLogicalMirrorsOnce() { return ProcessMirrors(); }

}  // namespace pgiceberg::logical

extern "C" {

PG_FUNCTION_INFO_V1(pgiceberg_process_logical_mirrors);

Datum pgiceberg_process_logical_mirrors(PG_FUNCTION_ARGS) {
  (void)fcinfo;
  if (SPI_connect() != SPI_OK_CONNECT) {
    ereport(ERROR, (errmsg("could not connect to SPI")));
  }
  pgiceberg::PgStatusGuard(
      [&]() { return pgiceberg::logical::ProcessLogicalMirrorsOnce(); });
  SPI_finish();
  PG_RETURN_VOID();
}

PGDLLEXPORT void pgiceberg_logical_worker_main(Datum) {
  pqsignal(SIGHUP, pgiceberg::logical::HandleSighup);
  pqsignal(SIGTERM, pgiceberg::logical::HandleSigterm);
  BackgroundWorkerUnblockSignals();
  pgiceberg::logical::RunWorkerLoop();
}

static void PgIcebergOutputStartup(LogicalDecodingContext*, OutputPluginOptions* options,
                                   bool) {
  options->output_type = OUTPUT_PLUGIN_TEXTUAL_OUTPUT;
  options->receive_rewrites = false;
}

static void PgIcebergOutputBegin(LogicalDecodingContext*, ReorderBufferTXN*) {}

static void PgIcebergOutputCommit(LogicalDecodingContext*, ReorderBufferTXN*,
                                  XLogRecPtr) {}

static HeapTuple PgIcebergOutputNewTuple(ReorderBufferChange* change) {
#if PG_VERSION_NUM >= 170000
  return change->data.tp.newtuple;
#else
  return change->data.tp.newtuple == nullptr ? nullptr : &change->data.tp.newtuple->tuple;
#endif
}

static void PgIcebergOutputChange(LogicalDecodingContext* ctx, ReorderBufferTXN*,
                                  Relation relation, ReorderBufferChange* change) {
  char action = '\0';
  HeapTuple tuple = nullptr;
  switch (change->action) {
    case REORDER_BUFFER_CHANGE_INSERT:
      action = 'I';
      tuple = PgIcebergOutputNewTuple(change);
      break;
    case REORDER_BUFFER_CHANGE_UPDATE:
      action = 'U';
      break;
    case REORDER_BUFFER_CHANGE_DELETE:
      action = 'D';
      break;
    default:
      return;
  }

  OutputPluginPrepareWrite(ctx, true);
  appendStringInfo(ctx->out, "%c\t%u", action, RelationGetRelid(relation));
  if (action == 'I') {
    TupleDesc desc = RelationGetDescr(relation);
    appendStringInfo(ctx->out, "\t%d", desc->natts);
    for (int i = 0; i < desc->natts; i++) {
      Form_pg_attribute attr = TupleDescAttr(desc, i);
      if (attr->attisdropped) {
        appendStringInfo(ctx->out, "\t%d\tn\t0:", attr->attnum);
        continue;
      }
      bool is_null = false;
      Datum value = heap_getattr(tuple, attr->attnum, desc, &is_null);
      if (is_null) {
        appendStringInfo(ctx->out, "\t%d\tn\t0:", attr->attnum);
        continue;
      }
      Oid typoutput = InvalidOid;
      bool typisvarlena = false;
      getTypeOutputInfo(attr->atttypid, &typoutput, &typisvarlena);
      char* text = OidOutputFunctionCall(typoutput, value);
      const auto text_len = std::strlen(text);
      if (text_len > static_cast<std::size_t>(std::numeric_limits<int>::max())) {
        ereport(ERROR, (errcode(ERRCODE_PROGRAM_LIMIT_EXCEEDED),
                        errmsg("logical decoding attribute output is too large")));
      }
      appendStringInfo(ctx->out, "\t%d\tv\t%zu:", attr->attnum, text_len);
      appendBinaryStringInfo(ctx->out, text, static_cast<int>(text_len));
      pfree(text);
    }
  }
  OutputPluginWrite(ctx, true);
}

static void PgIcebergOutputTruncate(LogicalDecodingContext* ctx, ReorderBufferTXN*,
                                    int nrelations, Relation relations[],
                                    ReorderBufferChange*) {
  for (int i = 0; i < nrelations; i++) {
    OutputPluginPrepareWrite(ctx, true);
    appendStringInfo(ctx->out, "T\t%u", RelationGetRelid(relations[i]));
    OutputPluginWrite(ctx, true);
  }
}

PGDLLEXPORT void _PG_output_plugin_init(OutputPluginCallbacks* cb) {
  cb->startup_cb = PgIcebergOutputStartup;
  cb->begin_cb = PgIcebergOutputBegin;
  cb->change_cb = PgIcebergOutputChange;
  cb->truncate_cb = PgIcebergOutputTruncate;
  cb->commit_cb = PgIcebergOutputCommit;
}

}  // extern "C"
