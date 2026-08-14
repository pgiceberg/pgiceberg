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

#include "engine/commit_recovery.h"

#include <cctype>
#include <cerrno>
#include <cstring>
#include <fcntl.h>
#include <unistd.h>

#include <filesystem>
#include <fstream>
#include <optional>
#include <sstream>
#include <string>
#include <string_view>
#include <system_error>
#include <utility>
#include <vector>

#include <iceberg/snapshot.h>
#include <iceberg/table.h>
#include <iceberg/update/snapshot_manager.h>

#include "common/catalog.h"
#include "common/status.h"

extern "C" {
#include "postgres.h"
#include "miscadmin.h"
#include "utils/elog.h"
#include "utils/errcodes.h"
}

namespace pgiceberg::engine {
namespace {

constexpr std::string_view kLogMagic = "pgiceberg-commit-recovery";
constexpr int kLogVersion = 1;
constexpr const char* kRecoveryDirName = "pg_iceberg";
constexpr const char* kRecoveryXactDirName = "xact";

bool IsSafeCommitId(std::string_view commit_id) {
  if (commit_id.empty() || commit_id.size() > 128) {
    return false;
  }
  for (char c : commit_id) {
    if (!std::isxdigit(static_cast<unsigned char>(c))) {
      return false;
    }
  }
  return true;
}

std::string IoError(std::string_view action, const std::filesystem::path& path) {
  return std::string(action) + " " + path.string() + ": " + strerror(errno);
}

Result<std::filesystem::path> RecoveryDir() {
  if (DataDir == nullptr || DataDir[0] == '\0') {
    return std::unexpected(
        MakeError(ERRCODE_INTERNAL_ERROR, "PostgreSQL data directory is not available"));
  }
  return std::filesystem::path(DataDir) / kRecoveryDirName / kRecoveryXactDirName;
}

Result<std::filesystem::path> RecoveryLogPath(std::string_view commit_id) {
  if (!IsSafeCommitId(commit_id)) {
    return std::unexpected(MakeError(ERRCODE_INVALID_PARAMETER_VALUE,
                                     "invalid pgiceberg commit recovery identifier"));
  }
  PGICEBERG_ASSIGN_OR_RETURN(auto dir, RecoveryDir());
  return dir / (std::string(commit_id) + ".log");
}

Status FsyncFd(int fd, std::string_view action, const std::filesystem::path& path) {
  if (fsync(fd) != 0) {
    return std::unexpected(MakeError(ERRCODE_IO_ERROR, IoError(action, path)));
  }
  return Ok();
}

Status FsyncPath(const std::filesystem::path& path, bool directory) {
  const int flags = directory ? (O_RDONLY | O_DIRECTORY) : O_RDONLY;
  const int fd = open(path.c_str(), flags);
  if (fd < 0) {
    return std::unexpected(MakeError(ERRCODE_IO_ERROR, IoError("open for fsync", path)));
  }
  auto status = FsyncFd(fd, "fsync", path);
  close(fd);
  return status;
}

Status DurableWriteFile(const std::filesystem::path& path, std::string_view contents) {
  std::error_code ec;
  std::filesystem::create_directories(path.parent_path(), ec);
  if (ec) {
    return std::unexpected(MakeError(
        ERRCODE_IO_ERROR, "could not create commit recovery directory: " + ec.message()));
  }

  auto tmp = path;
  tmp += ".tmp";
  const int fd = open(tmp.c_str(), O_WRONLY | O_CREAT | O_TRUNC, 0600);
  if (fd < 0) {
    return std::unexpected(
        MakeError(ERRCODE_IO_ERROR, IoError("create commit recovery log", tmp)));
  }

  std::size_t written = 0;
  while (written < contents.size()) {
    const ssize_t n = write(fd, contents.data() + written, contents.size() - written);
    if (n < 0) {
      const std::string message = IoError("write commit recovery log", tmp);
      close(fd);
      unlink(tmp.c_str());
      return std::unexpected(MakeError(ERRCODE_IO_ERROR, message));
    }
    written += static_cast<std::size_t>(n);
  }

  auto status = FsyncFd(fd, "fsync commit recovery log", tmp);
  close(fd);
  if (!status) {
    unlink(tmp.c_str());
    return status;
  }

  if (rename(tmp.c_str(), path.c_str()) != 0) {
    const std::string message = IoError("rename commit recovery log", path);
    unlink(tmp.c_str());
    return std::unexpected(MakeError(ERRCODE_IO_ERROR, message));
  }
  return FsyncPath(path.parent_path(), true);
}

std::string JsonEscape(std::string_view value) {
  std::string out;
  out.reserve(value.size());
  for (char c : value) {
    switch (c) {
      case '\\':
        out += "\\\\";
        break;
      case '"':
        out += "\\\"";
        break;
      case '\n':
        out += "\\n";
        break;
      case '\r':
        out += "\\r";
        break;
      case '\t':
        out += "\\t";
        break;
      default:
        out += c;
        break;
    }
  }
  return out;
}

void AppendJsonString(std::ostringstream& out, std::string_view key,
                      std::string_view value) {
  out << '"' << key << "\":\"" << JsonEscape(value) << '"';
}

void AppendJsonOptionalInt(std::ostringstream& out, std::string_view key,
                           const std::optional<int64_t>& value) {
  out << '"' << key << "\":";
  if (value.has_value()) {
    out << *value;
  } else {
    out << "null";
  }
}

std::string EncodeField(std::string_view value) {
  std::ostringstream out;
  out << value.size() << ' ' << value;
  return out.str();
}

Status DecodeField(std::istream& in, std::string& value) {
  std::size_t size = 0;
  if (!(in >> size)) {
    return std::unexpected(
        MakeError(ERRCODE_DATA_EXCEPTION, "truncated pgiceberg commit recovery log"));
  }
  if (in.get() != ' ') {
    return std::unexpected(
        MakeError(ERRCODE_DATA_EXCEPTION, "invalid pgiceberg commit recovery log field"));
  }
  value.assign(size, '\0');
  if (size > 0 && !in.read(value.data(), static_cast<std::streamsize>(size))) {
    return std::unexpected(MakeError(ERRCODE_DATA_EXCEPTION,
                                     "truncated pgiceberg commit recovery log field"));
  }
  return Ok();
}

Status ExpectToken(std::istream& in, std::string_view expected) {
  std::string token;
  if (!(in >> token) || token != expected) {
    return std::unexpected(MakeError(
        ERRCODE_DATA_EXCEPTION, "unexpected token in pgiceberg commit recovery log"));
  }
  return Ok();
}

std::string FormatRecord(const CommitRecoveryRecord& record) {
  std::ostringstream out;
  out << kLogMagic << ' ' << kLogVersion << '\n';
  out << "commit_id " << EncodeField(record.commit_id) << '\n';
  out << "postgres_xid " << EncodeField(record.postgres_xid) << '\n';
  out << "state " << EncodeField(record.state) << '\n';
  out << "created_at " << record.created_at_unix_micros << '\n';
  out << "table_count " << record.tables.size() << '\n';
  for (const auto& table : record.tables) {
    out << "table\n";
    out << "catalog " << EncodeField(table.options.catalog) << '\n';
    out << "catalog_type " << EncodeField(table.options.catalog_type) << '\n';
    out << "catalog_uri " << EncodeField(table.options.catalog_uri) << '\n';
    out << "warehouse " << EncodeField(table.options.warehouse) << '\n';
    out << "catalog_name " << EncodeField(table.options.catalog_name) << '\n';
    out << "namespace " << EncodeField(table.options.name_space) << '\n';
    out << "table_name " << EncodeField(table.options.table) << '\n';
    out << "base_snapshot_id ";
    if (table.base_snapshot_id.has_value()) {
      out << *table.base_snapshot_id;
    } else {
      out << "none";
    }
    out << '\n';
    out << "committed_snapshot_id ";
    if (table.committed_snapshot_id.has_value()) {
      out << *table.committed_snapshot_id;
    } else {
      out << "none";
    }
    out << '\n';
    out << "iceberg_state " << EncodeField(table.iceberg_state) << '\n';
  }
  return out.str();
}

Result<std::optional<int64_t>> ReadOptionalSnapshotId(std::istream& in) {
  std::string token;
  if (!(in >> token)) {
    return std::unexpected(MakeError(ERRCODE_DATA_EXCEPTION,
                                     "truncated snapshot id in commit recovery log"));
  }
  if (token == "none") {
    return std::optional<int64_t>{};
  }
  try {
    return std::stoll(token);
  } catch (...) {
    return std::unexpected(
        MakeError(ERRCODE_DATA_EXCEPTION, "invalid snapshot id in commit recovery log"));
  }
}

Result<CommitRecoveryRecord> ParseRecord(std::istream& in) {
  std::string magic;
  int version = 0;
  if (!(in >> magic >> version) || magic != kLogMagic || version != kLogVersion) {
    return std::unexpected(
        MakeError(ERRCODE_DATA_EXCEPTION, "unsupported pgiceberg commit recovery log"));
  }

  CommitRecoveryRecord record;
  PGICEBERG_RETURN_NOT_OK(ExpectToken(in, "commit_id"));
  PGICEBERG_RETURN_NOT_OK(DecodeField(in, record.commit_id));
  PGICEBERG_RETURN_NOT_OK(ExpectToken(in, "postgres_xid"));
  PGICEBERG_RETURN_NOT_OK(DecodeField(in, record.postgres_xid));
  PGICEBERG_RETURN_NOT_OK(ExpectToken(in, "state"));
  PGICEBERG_RETURN_NOT_OK(DecodeField(in, record.state));
  PGICEBERG_RETURN_NOT_OK(ExpectToken(in, "created_at"));
  if (!(in >> record.created_at_unix_micros)) {
    return std::unexpected(
        MakeError(ERRCODE_DATA_EXCEPTION, "invalid created_at in commit recovery log"));
  }

  PGICEBERG_RETURN_NOT_OK(ExpectToken(in, "table_count"));
  std::size_t table_count = 0;
  if (!(in >> table_count)) {
    return std::unexpected(
        MakeError(ERRCODE_DATA_EXCEPTION, "invalid table_count in commit recovery log"));
  }
  record.tables.reserve(table_count);
  for (std::size_t i = 0; i < table_count; ++i) {
    PGICEBERG_RETURN_NOT_OK(ExpectToken(in, "table"));
    CommitRecoveryTable table;
    PGICEBERG_RETURN_NOT_OK(ExpectToken(in, "catalog"));
    PGICEBERG_RETURN_NOT_OK(DecodeField(in, table.options.catalog));
    PGICEBERG_RETURN_NOT_OK(ExpectToken(in, "catalog_type"));
    PGICEBERG_RETURN_NOT_OK(DecodeField(in, table.options.catalog_type));
    PGICEBERG_RETURN_NOT_OK(ExpectToken(in, "catalog_uri"));
    PGICEBERG_RETURN_NOT_OK(DecodeField(in, table.options.catalog_uri));
    PGICEBERG_RETURN_NOT_OK(ExpectToken(in, "warehouse"));
    PGICEBERG_RETURN_NOT_OK(DecodeField(in, table.options.warehouse));
    PGICEBERG_RETURN_NOT_OK(ExpectToken(in, "catalog_name"));
    PGICEBERG_RETURN_NOT_OK(DecodeField(in, table.options.catalog_name));
    PGICEBERG_RETURN_NOT_OK(ExpectToken(in, "namespace"));
    PGICEBERG_RETURN_NOT_OK(DecodeField(in, table.options.name_space));
    PGICEBERG_RETURN_NOT_OK(ExpectToken(in, "table_name"));
    PGICEBERG_RETURN_NOT_OK(DecodeField(in, table.options.table));
    PGICEBERG_RETURN_NOT_OK(ExpectToken(in, "base_snapshot_id"));
    PGICEBERG_ASSIGN_OR_RETURN(table.base_snapshot_id, ReadOptionalSnapshotId(in));
    PGICEBERG_RETURN_NOT_OK(ExpectToken(in, "committed_snapshot_id"));
    PGICEBERG_ASSIGN_OR_RETURN(table.committed_snapshot_id, ReadOptionalSnapshotId(in));
    PGICEBERG_RETURN_NOT_OK(ExpectToken(in, "iceberg_state"));
    PGICEBERG_RETURN_NOT_OK(DecodeField(in, table.iceberg_state));
    record.tables.push_back(std::move(table));
  }
  return record;
}

Result<std::optional<int64_t>> LoadCurrentSnapshotId(const iceberg::Table& table) {
  auto snapshot = table.current_snapshot();
  if (!snapshot) {
    if (snapshot.error().kind == iceberg::ErrorKind::kNotFound) {
      return std::optional<int64_t>{};
    }
    return std::unexpected(
        MakePgError(snapshot.error(), "load current Iceberg snapshot"));
  }
  return snapshot.value()->snapshot_id;
}

bool SnapshotHasCommitId(const iceberg::Snapshot& snapshot, std::string_view commit_id) {
  auto it = snapshot.summary.find(std::string(kXactCommitIdProperty));
  return it != snapshot.summary.end() && it->second == commit_id;
}

bool IsAncestorOf(const iceberg::Table& table, int64_t ancestor_id,
                  int64_t descendant_id) {
  auto current = table.SnapshotById(descendant_id);
  while (current) {
    if (current.value()->snapshot_id == ancestor_id) {
      return true;
    }
    if (!current.value()->parent_snapshot_id.has_value()) {
      return false;
    }
    current = table.SnapshotById(*current.value()->parent_snapshot_id);
  }
  return false;
}

std::string TableVerdict(const CommitRecoveryTable& table,
                         const iceberg::Table& iceberg_table,
                         std::string_view commit_id) {
  auto current = iceberg_table.current_snapshot();
  if (!current) {
    if (table.iceberg_state == kTableIcebergPending) {
      return "not_published";
    }
    return "missing_snapshot";
  }
  const auto& snapshot = *current.value();
  if (table.committed_snapshot_id.has_value() &&
      snapshot.snapshot_id == *table.committed_snapshot_id) {
    return "iceberg_published";
  }
  if (SnapshotHasCommitId(snapshot, commit_id)) {
    return "iceberg_published";
  }
  if (table.base_snapshot_id.has_value() &&
      snapshot.snapshot_id == *table.base_snapshot_id) {
    return "already_rolled_back";
  }
  if (table.committed_snapshot_id.has_value() &&
      IsAncestorOf(iceberg_table, *table.committed_snapshot_id, snapshot.snapshot_id)) {
    return "superseded";
  }
  return "diverged";
}

std::string OverallVerdict(const std::vector<std::string>& table_verdicts) {
  bool any_published = false;
  bool any_pending = false;
  bool any_rolled_back = false;
  bool any_unresolved = false;
  for (const auto& verdict : table_verdicts) {
    if (verdict == "iceberg_published") {
      any_published = true;
    } else if (verdict == "not_published") {
      any_pending = true;
    } else if (verdict == "already_rolled_back") {
      any_rolled_back = true;
    } else {
      any_unresolved = true;
    }
  }
  if (any_unresolved) {
    return "needs_operator";
  }
  if (any_published && any_pending) {
    return "iceberg_partial";
  }
  if (any_published) {
    return "iceberg_orphan";
  }
  if (any_rolled_back || any_pending) {
    return "stale_intent";
  }
  return "needs_operator";
}

Status RollbackTableToBase(const CommitRecoveryTable& table) {
  if (!table.base_snapshot_id.has_value()) {
    return std::unexpected(MakeError(
        ERRCODE_FEATURE_NOT_SUPPORTED,
        "cannot roll back Iceberg table \"" + table.options.name_space + "." +
            table.options.table + "\" because it had no parent snapshot",
        "Use action 'acknowledge' to keep the published Iceberg snapshot, or replace "
        "the table."));
  }
  return RollbackIcebergSnapshot(table.options, *table.base_snapshot_id);
}

}  // namespace

std::string CommitRecoveryRecordJson(const CommitRecoveryRecord& record) {
  std::ostringstream out;
  out << '{';
  AppendJsonString(out, "commit_id", record.commit_id);
  out << ',';
  AppendJsonString(out, "postgres_xid", record.postgres_xid);
  out << ',';
  AppendJsonString(out, "state", record.state);
  out << ",\"created_at_unix_micros\":" << record.created_at_unix_micros;
  out << ",\"tables\":[";
  for (std::size_t i = 0; i < record.tables.size(); ++i) {
    const auto& table = record.tables[i];
    if (i > 0) {
      out << ',';
    }
    out << '{';
    AppendJsonString(out, "catalog", table.options.catalog);
    out << ',';
    AppendJsonString(out, "namespace", table.options.name_space);
    out << ',';
    AppendJsonString(out, "table", table.options.table);
    out << ',';
    AppendJsonOptionalInt(out, "base_snapshot_id", table.base_snapshot_id);
    out << ',';
    AppendJsonOptionalInt(out, "committed_snapshot_id", table.committed_snapshot_id);
    out << ',';
    AppendJsonString(out, "iceberg_state", table.iceberg_state);
    out << '}';
  }
  out << "]}";
  return out.str();
}

Status WriteCommitRecoveryLog(const CommitRecoveryRecord& record) {
  PGICEBERG_ASSIGN_OR_RETURN(auto path, RecoveryLogPath(record.commit_id));
  return DurableWriteFile(path, FormatRecord(record));
}

Status RemoveCommitRecoveryLog(std::string_view commit_id) {
  PGICEBERG_ASSIGN_OR_RETURN(auto path, RecoveryLogPath(commit_id));
  std::error_code ec;
  std::filesystem::remove(path, ec);
  if (ec) {
    return std::unexpected(MakeError(
        ERRCODE_IO_ERROR, "could not remove commit recovery log: " + ec.message()));
  }
  auto dir = path.parent_path();
  if (std::filesystem::exists(dir)) {
    (void)FsyncPath(dir, true);
  }
  return Ok();
}

Result<std::optional<CommitRecoveryRecord>> ReadCommitRecoveryLog(
    std::string_view commit_id) {
  PGICEBERG_ASSIGN_OR_RETURN(auto path, RecoveryLogPath(commit_id));
  if (!std::filesystem::exists(path)) {
    return std::optional<CommitRecoveryRecord>{};
  }
  std::ifstream in(path);
  if (!in) {
    return std::unexpected(MakeError(
        ERRCODE_IO_ERROR, "could not read commit recovery log " + path.string()));
  }
  PGICEBERG_ASSIGN_OR_RETURN(auto record, ParseRecord(in));
  return record;
}

Result<std::vector<CommitRecoveryRecord>> ListCommitRecoveryLogs() {
  PGICEBERG_ASSIGN_OR_RETURN(auto dir, RecoveryDir());
  std::vector<CommitRecoveryRecord> records;
  std::error_code ec;
  if (!std::filesystem::exists(dir, ec) || ec) {
    return records;
  }
  std::error_code iter_ec;
  for (std::filesystem::directory_iterator it(dir, iter_ec), end; !iter_ec && it != end;
       it.increment(iter_ec)) {
    const auto& entry = *it;
    if (!entry.is_regular_file() || entry.path().extension() != ".log") {
      continue;
    }
    std::ifstream in(entry.path());
    if (!in) {
      elog(WARNING, "could not read pgiceberg commit recovery log %s",
           entry.path().c_str());
      continue;
    }
    auto parsed = ParseRecord(in);
    if (!parsed) {
      elog(WARNING, "%s", parsed.error().message().c_str());
      continue;
    }
    records.push_back(std::move(parsed).value());
  }
  return records;
}

Result<std::vector<ReconcileResult>> ReconcileCommitRecoveryLogs() {
  PGICEBERG_ASSIGN_OR_RETURN(auto records, ListCommitRecoveryLogs());
  std::vector<ReconcileResult> results;
  results.reserve(records.size());
  for (auto& record : records) {
    std::vector<std::string> table_verdicts;
    table_verdicts.reserve(record.tables.size());
    std::ostringstream detail;
    detail << "{\"tables\":[";
    for (std::size_t i = 0; i < record.tables.size(); ++i) {
      auto& table = record.tables[i];
      std::string verdict;
      std::string error;
      auto catalog_options = ToCatalogOptions(table.options);
      if (!catalog_options) {
        verdict = "catalog_error";
        error = catalog_options.error().message();
      } else {
        auto loaded = LoadIcebergTable(*catalog_options, table.options.table.c_str());
        if (!loaded) {
          verdict = "load_error";
          error = loaded.error().message();
        } else {
          verdict = TableVerdict(table, **loaded, record.commit_id);
        }
      }
      table_verdicts.push_back(verdict);
      if (i > 0) {
        detail << ',';
      }
      detail << '{';
      AppendJsonString(detail, "catalog", table.options.catalog);
      detail << ',';
      AppendJsonString(detail, "namespace", table.options.name_space);
      detail << ',';
      AppendJsonString(detail, "table", table.options.table);
      detail << ',';
      AppendJsonString(detail, "verdict", verdict);
      if (!error.empty()) {
        detail << ',';
        AppendJsonString(detail, "error", error);
      }
      detail << '}';
    }
    detail << "]}";
    results.push_back(ReconcileResult{.record = std::move(record),
                                      .verdict = OverallVerdict(table_verdicts),
                                      .detail_json = detail.str()});
  }
  return results;
}

CatalogOptions CatalogOptionsFromStored(const Options& options) {
  CatalogOptions catalog_options;
  catalog_options.catalog_type = options.catalog_type;
  catalog_options.catalog_uri = options.catalog_uri;
  catalog_options.warehouse = options.warehouse;
  catalog_options.catalog_name = options.catalog_name;
  catalog_options.name_space = options.name_space;
  catalog_options.table = options.table;
  return catalog_options;
}

Result<CatalogOptions> CatalogOptionsForRepair(const Options& options) {
  if (!options.catalog_uri.empty()) {
    return CatalogOptionsFromStored(options);
  }
  return ToCatalogOptions(options);
}

Status RollbackIcebergSnapshot(const Options& options, int64_t snapshot_id) {
  PGICEBERG_ASSIGN_OR_RETURN(auto catalog_options, CatalogOptionsForRepair(options));
  PGICEBERG_ASSIGN_OR_RETURN(auto table,
                             LoadIcebergTable(catalog_options, options.table.c_str()));
  PGICEBERG_ASSIGN_OR_RETURN(auto current_id, LoadCurrentSnapshotId(*table));
  if (current_id.has_value() && *current_id == snapshot_id) {
    return Ok();
  }
  PGICEBERG_ASSIGN_OR_RETURN(auto manager,
                             FromIcebergResult(iceberg::SnapshotManager::Make(table),
                                               "create Iceberg snapshot manager"));
  manager->RollbackTo(snapshot_id);
  return FromIcebergStatus(manager->Commit(), "rollback Iceberg snapshot");
}

Status BestEffortRollbackCommittedTables(const CommitRecoveryRecord& record) {
  Status first_error = Ok();
  for (auto it = record.tables.rbegin(); it != record.tables.rend(); ++it) {
    if (it->iceberg_state != kTableIcebergCommitted &&
        it->iceberg_state != kTableIcebergUnknown) {
      continue;
    }
    auto status = RollbackTableToBase(*it);
    if (!status) {
      elog(WARNING,
           "pgiceberg could not roll back Iceberg table %s.%s after "
           "PostgreSQL abort: %s",
           it->options.name_space.c_str(), it->options.table.c_str(),
           status.error().message().c_str());
      if (first_error) {
        first_error = std::unexpected(status.error());
      }
    }
  }
  return first_error;
}

Result<std::string> RepairCommit(std::string_view commit_id, std::string_view action) {
  PGICEBERG_ASSIGN_OR_RETURN(auto loaded, ReadCommitRecoveryLog(commit_id));
  if (!loaded.has_value()) {
    return std::unexpected(MakeError(ERRCODE_UNDEFINED_OBJECT,
                                     "pgiceberg commit recovery log \"" +
                                         std::string(commit_id) + "\" does not exist"));
  }
  auto record = std::move(*loaded);

  if (action == kRepairActionAcknowledge) {
    PGICEBERG_RETURN_NOT_OK(RemoveCommitRecoveryLog(commit_id));
    return "acknowledged Iceberg snapshots for commit " + record.commit_id;
  }
  if (action != kRepairActionRollback) {
    return std::unexpected(MakeError(
        ERRCODE_INVALID_PARAMETER_VALUE,
        "pgiceberg.repair_commit action must be \"rollback\" or \"acknowledge\""));
  }

  for (auto it = record.tables.rbegin(); it != record.tables.rend(); ++it) {
    if (it->iceberg_state == kTableIcebergPending) {
      continue;
    }
    PGICEBERG_ASSIGN_OR_RETURN(auto catalog_options,
                               CatalogOptionsForRepair(it->options));
    auto table = LoadIcebergTable(catalog_options, it->options.table.c_str());
    if (!table) {
      return std::unexpected(table.error());
    }
    PGICEBERG_ASSIGN_OR_RETURN(auto current_id, LoadCurrentSnapshotId(**table));
    bool current_is_ours = false;
    if (current_id.has_value()) {
      if (it->committed_snapshot_id.has_value() &&
          *current_id == *it->committed_snapshot_id) {
        current_is_ours = true;
      } else {
        auto current_snapshot = (*table)->current_snapshot();
        current_is_ours =
            current_snapshot &&
            SnapshotHasCommitId(*current_snapshot.value(), record.commit_id);
      }
    }
    if (it->iceberg_state != kTableIcebergPending && current_id.has_value() &&
        !current_is_ours) {
      if (it->base_snapshot_id.has_value() && *current_id == *it->base_snapshot_id) {
        continue;
      }
      return std::unexpected(MakeError(
          ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE,
          "Iceberg table \"" + it->options.name_space + "." + it->options.table +
              "\" current snapshot is no longer the in-doubt pgiceberg commit",
          "Use action \"acknowledge\" if a later writer already superseded this "
          "snapshot."));
    }
    PGICEBERG_RETURN_NOT_OK(RollbackTableToBase(*it));
  }
  PGICEBERG_RETURN_NOT_OK(RemoveCommitRecoveryLog(commit_id));
  return "rolled back Iceberg snapshots for commit " + record.commit_id;
}

}  // namespace pgiceberg::engine
