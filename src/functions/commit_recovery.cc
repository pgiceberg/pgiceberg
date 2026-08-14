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

#include "common/constants.h"
#include "common/fcinfo.h"
#include "common/pg_error.h"
#include "common/status.h"
#include "engine/commit_recovery.h"
#include "engine/modify_state.h"
#include "engine/options.h"

#include <string>
#include <utility>
#include <vector>

extern "C" {
#include "postgres.h"
#include "access/htup_details.h"
#include "catalog/pg_type.h"
#include "fmgr.h"
#include "funcapi.h"
#include "utils/builtins.h"
#include "utils/jsonb.h"
#include "utils/timestamp.h"
}

namespace {

Datum JsonbDatum(const std::string& json) {
  return DirectFunctionCall1(jsonb_in, CStringGetDatum(json.c_str()));
}

TimestampTz UnixMicrosToTimestampTz(std::int64_t unix_micros) {
  return static_cast<TimestampTz>(unix_micros -
                                  pgiceberg::kPostgresUnixEpochOffsetMicros);
}

void PutRecoveryRow(ReturnSetInfo* rsinfo,
                    const pgiceberg::engine::CommitRecoveryRecord& record) {
  Datum values[6];
  bool nulls[6] = {false, false, false, false, false, false};
  values[0] = CStringGetTextDatum(record.commit_id.c_str());
  values[1] = CStringGetTextDatum(record.state.c_str());
  values[2] = CStringGetTextDatum(record.postgres_xid.c_str());
  values[3] = TimestampTzGetDatum(UnixMicrosToTimestampTz(record.created_at_unix_micros));
  values[4] = Int32GetDatum(static_cast<int32>(record.tables.size()));
  values[5] = JsonbDatum(pgiceberg::engine::CommitRecoveryRecordJson(record));
  tuplestore_putvalues(rsinfo->setResult, rsinfo->setDesc, values, nulls);
}

void PutReconcileRow(ReturnSetInfo* rsinfo,
                     const pgiceberg::engine::ReconcileResult& result) {
  Datum values[4];
  bool nulls[4] = {false, false, false, false};
  values[0] = CStringGetTextDatum(result.record.commit_id.c_str());
  values[1] = CStringGetTextDatum(result.record.state.c_str());
  values[2] = CStringGetTextDatum(result.verdict.c_str());
  values[3] = JsonbDatum(result.detail_json);
  tuplestore_putvalues(rsinfo->setResult, rsinfo->setDesc, values, nulls);
}

}  // namespace

extern "C" {
PG_FUNCTION_INFO_V1(pgiceberg_commit_recovery_log);
PG_FUNCTION_INFO_V1(pgiceberg_reconcile_commits);
PG_FUNCTION_INFO_V1(pgiceberg_repair_commit);
PG_FUNCTION_INFO_V1(pgiceberg_rollback_iceberg_snapshot);
PG_FUNCTION_INFO_V1(pgiceberg_current_xact_commit_id);

Datum pgiceberg_commit_recovery_log(PG_FUNCTION_ARGS) {
  return pgiceberg::PgResultGuard([&]() -> pgiceberg::Result<Datum> {
    ReturnSetInfo* rsinfo = reinterpret_cast<ReturnSetInfo*>(fcinfo->resultinfo);
    InitMaterializedSRF(fcinfo, 0);
    PGICEBERG_ASSIGN_OR_RETURN(auto records, pgiceberg::engine::ListCommitRecoveryLogs());
    for (const auto& record : records) {
      PutRecoveryRow(rsinfo, record);
    }
    return static_cast<Datum>(0);
  });
}

Datum pgiceberg_reconcile_commits(PG_FUNCTION_ARGS) {
  return pgiceberg::PgResultGuard([&]() -> pgiceberg::Result<Datum> {
    ReturnSetInfo* rsinfo = reinterpret_cast<ReturnSetInfo*>(fcinfo->resultinfo);
    InitMaterializedSRF(fcinfo, 0);
    PGICEBERG_ASSIGN_OR_RETURN(auto results,
                               pgiceberg::engine::ReconcileCommitRecoveryLogs());
    for (const auto& result : results) {
      PutReconcileRow(rsinfo, result);
    }
    return static_cast<Datum>(0);
  });
}

Datum pgiceberg_repair_commit(PG_FUNCTION_ARGS) {
  return pgiceberg::PgResultGuard([&]() -> pgiceberg::Result<Datum> {
    const std::string commit_id = pgiceberg::TextArg(fcinfo, 0);
    const std::string action = pgiceberg::TextArg(fcinfo, 1);
    PGICEBERG_ASSIGN_OR_RETURN(auto message,
                               pgiceberg::engine::RepairCommit(commit_id, action));
    return CStringGetTextDatum(message.c_str());
  });
}

Datum pgiceberg_rollback_iceberg_snapshot(PG_FUNCTION_ARGS) {
  return pgiceberg::PgResultGuard([&]() -> pgiceberg::Result<Datum> {
    pgiceberg::engine::Options options;
    options.catalog = pgiceberg::TextArg(fcinfo, 0);
    options.name_space = pgiceberg::TextArg(fcinfo, 1);
    options.table = pgiceberg::TextArg(fcinfo, 2);
    const int64 snapshot_id = PG_GETARG_INT64(3);
    PGICEBERG_RETURN_NOT_OK(
        pgiceberg::engine::RollbackIcebergSnapshot(options, snapshot_id));
    return static_cast<Datum>(0);
  });
}

Datum pgiceberg_current_xact_commit_id(PG_FUNCTION_ARGS) {
  auto commit_id = pgiceberg::engine::CurrentXactCommitId();
  if (!commit_id.has_value()) {
    PG_RETURN_NULL();
  }
  PG_RETURN_TEXT_P(cstring_to_text(commit_id->c_str()));
}

}  // extern "C"
