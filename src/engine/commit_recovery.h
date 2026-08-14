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

#include <cstdint>
#include <optional>
#include <string>
#include <string_view>
#include <vector>

#include "common/status.h"
#include "engine/options.h"

namespace pgiceberg::engine {

// Snapshot summary (and optional table-property) keys that identify the
// PostgreSQL transaction which published an Iceberg snapshot.  The identifier
// is the recovery key when Iceberg commits succeed and PostgreSQL later aborts.
inline constexpr std::string_view kXactCommitIdProperty = "pgiceberg.xact.commit-id";
inline constexpr std::string_view kXactPostgresXidProperty =
    "pgiceberg.xact.postgres-xid";

inline constexpr std::string_view kRecoveryStatePreparing = "preparing";
inline constexpr std::string_view kRecoveryStateIcebergPartial = "iceberg_partial";
inline constexpr std::string_view kRecoveryStateIcebergComplete = "iceberg_complete";
inline constexpr std::string_view kRecoveryStateNeedsRepair = "needs_repair";

inline constexpr std::string_view kTableIcebergPending = "pending";
inline constexpr std::string_view kTableIcebergCommitted = "committed";
inline constexpr std::string_view kTableIcebergUnknown = "unknown";

inline constexpr std::string_view kRepairActionRollback = "rollback";
inline constexpr std::string_view kRepairActionAcknowledge = "acknowledge";

struct CommitRecoveryTable {
  Options options;
  std::optional<int64_t> base_snapshot_id;
  std::optional<int64_t> committed_snapshot_id;
  std::string iceberg_state{kTableIcebergPending};
};

struct CommitRecoveryRecord {
  std::string commit_id;
  std::string postgres_xid;
  std::string state{kRecoveryStatePreparing};
  std::int64_t created_at_unix_micros = 0;
  std::vector<CommitRecoveryTable> tables;
};

struct ReconcileResult {
  CommitRecoveryRecord record;
  std::string verdict;
  std::string detail_json;
};

std::string CommitRecoveryRecordJson(const CommitRecoveryRecord& record);

Status WriteCommitRecoveryLog(const CommitRecoveryRecord& record);
Status RemoveCommitRecoveryLog(std::string_view commit_id);
Result<std::optional<CommitRecoveryRecord>> ReadCommitRecoveryLog(
    std::string_view commit_id);
Result<std::vector<CommitRecoveryRecord>> ListCommitRecoveryLogs();
Result<std::vector<ReconcileResult>> ReconcileCommitRecoveryLogs();
Result<std::string> RepairCommit(std::string_view commit_id, std::string_view action);
Status RollbackIcebergSnapshot(const Options& options, int64_t snapshot_id);
Status BestEffortRollbackCommittedTables(const CommitRecoveryRecord& record);

}  // namespace pgiceberg::engine
