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

#include "common/pg_logger.h"

#include <array>
#include <atomic>
#include <cstdio>
#include <cstdint>
#include <format>
#include <limits>
#include <mutex>
#include <string>
#include <string_view>
#include <thread>
#include <utility>
#include <vector>

#include <iceberg/logging/log_level.h>
#include <iceberg/logging/logger.h>

extern "C" {
#include "postgres.h"
#include "postmaster/syslogger.h"
#include "utils/guc.h"
#include "utils/elog.h"
}

namespace pgiceberg {
namespace {

std::thread::id g_backend_main_thread_id;
bool g_stderr_redirected_to_collector = false;
char* IcebergLogLevelGuc = nullptr;
constexpr std::size_t kMaxPendingWorkerLogs = 1024;
constexpr std::size_t kWorkerFatalBufferSize = 1024;

class PgElogLogger;
std::weak_ptr<PgElogLogger> g_pg_logger;

bool IsBackendMainThread() noexcept {
  return std::this_thread::get_id() == g_backend_main_thread_id;
}

int IcebergLevelToPgElevel(iceberg::LogLevel level) noexcept {
  switch (level) {
    case iceberg::LogLevel::kTrace:
      return DEBUG2;
    case iceberg::LogLevel::kDebug:
      return DEBUG1;
    case iceberg::LogLevel::kInfo:
      return LOG;
    case iceberg::LogLevel::kWarn:
    case iceberg::LogLevel::kError:
    case iceberg::LogLevel::kCritical:
    case iceberg::LogLevel::kFatal:
      return WARNING;
    case iceberg::LogLevel::kOff:
      return LOG;
  }
  std::unreachable();
}

void AppendJsonString(std::string& output, std::string_view value) {
  constexpr char kHexDigits[] = "0123456789abcdef";

  output += '"';
  for (const unsigned char ch : value) {
    switch (ch) {
      case '"':
        output += "\\\"";
        break;
      case '\\':
        output += "\\\\";
        break;
      case '\b':
        output += "\\b";
        break;
      case '\f':
        output += "\\f";
        break;
      case '\n':
        output += "\\n";
        break;
      case '\r':
        output += "\\r";
        break;
      case '\t':
        output += "\\t";
        break;
      default:
        if (ch < 0x20) {
          output += "\\u00";
          output += kHexDigits[ch >> 4];
          output += kHexDigits[ch & 0x0f];
        } else {
          output += static_cast<char>(ch);
        }
        break;
    }
  }
  output += '"';
}

std::string FormatAttributes(const std::vector<iceberg::LogAttribute>& attributes) {
  if (attributes.empty()) {
    return {};
  }

  std::string formatted = "attributes={";
  for (std::size_t i = 0; i < attributes.size(); ++i) {
    if (i > 0) {
      formatted += ',';
    }
    AppendJsonString(formatted, attributes[i].key);
    formatted += ':';
    AppendJsonString(formatted, attributes[i].value);
  }
  formatted += '}';
  return formatted;
}

std::string FormatDetail(const iceberg::LogMessage& message) {
  auto detail = std::format("iceberg_level={}", iceberg::ToString(message.level));
  auto attributes = FormatAttributes(message.attributes);
  if (!attributes.empty()) {
    detail += ' ';
    detail += attributes;
  }
  return detail;
}

void EmitWorkerFatal(const iceberg::LogMessage& message) noexcept {
  // A fatal log is immediately followed by abort(), so it cannot wait for the
  // backend main thread to drain the regular worker queue. Keep this fallback
  // allocation-free and bounded; all non-fatal worker records still use elog on
  // the main thread.
  constexpr std::string_view kPrefix = "WARNING:  pgiceberg: ";
  constexpr std::string_view kDetail =
      "\nDETAIL:  iceberg_level=fatal worker_thread=true\n";
  static_assert(kPrefix.size() + kDetail.size() <= kWorkerFatalBufferSize);

  std::array<char, kWorkerFatalBufferSize> output{};
  std::size_t length = 0;
  const auto append = [&output, &length](std::string_view text) noexcept {
    for (const char ch : text) {
      if (length >= output.size()) {
        return;
      }
      output[length++] = ch;
    }
  };

  append(kPrefix);
  const std::size_t available = output.size() - length - kDetail.size();
  const bool truncated = message.message.size() > available;
  const std::size_t message_limit =
      truncated && available >= 3 ? available - 3 : available;
  std::size_t copied = 0;
  for (const unsigned char ch : message.message) {
    if (copied >= message_limit) {
      break;
    }
    output[length++] = ch < 0x20 || ch == 0x7f ? ' ' : static_cast<char>(ch);
    ++copied;
  }
  if (truncated && available >= 3) {
    append("...");
  }
  append(kDetail);

  if (g_stderr_redirected_to_collector) {
    write_pipe_chunks(output.data(), static_cast<int>(length), LOG_DESTINATION_STDERR);
    return;
  }
  const auto written = std::fwrite(output.data(), 1, length, stderr);
  (void)written;
  std::fflush(stderr);
}

void EmitToPostgres(const iceberg::LogMessage& message) {
  const int elevel = IcebergLevelToPgElevel(message.level);
  if (!message_level_is_interesting(elevel)) {
    return;
  }

  const std::string detail = FormatDetail(message);
  if (errstart(elevel, TEXTDOMAIN)) {
    errmsg_internal("pgiceberg: %s", message.message.c_str());
    errdetail_log("%s", detail.c_str());
    errfinish(message.location.file_name(), static_cast<int>(message.location.line()),
              message.location.function_name());
  }
}

void EmitFormattingFailureToPostgres() noexcept {
  if (errstart(WARNING, TEXTDOMAIN)) {
    errmsg_internal("pgiceberg: log record could not be formatted");
    errfinish(__FILE__, __LINE__, __func__);
  }
}

class PgElogLogger final : public iceberg::Logger {
 public:
  explicit PgElogLogger(iceberg::LogLevel level = iceberg::LogLevel::kWarn)
      : level_(level) {
    pending_.reserve(kMaxPendingWorkerLogs);
    draining_.reserve(kMaxPendingWorkerLogs);
  }

  bool ShouldLog(iceberg::LogLevel level) const noexcept override {
    return level >= level_.load(std::memory_order_relaxed);
  }

  void Log(iceberg::LogMessage&& message) noexcept override {
    if (!IsBackendMainThread()) {
      if (message.level == iceberg::LogLevel::kFatal) {
        EmitWorkerFatal(message);
        return;
      }
      Enqueue(std::move(message));
      return;
    }

    Drain();
    Emit(std::move(message));
  }

  void SetLevel(iceberg::LogLevel level) noexcept override {
    level_.store(level, std::memory_order_relaxed);
  }

  iceberg::LogLevel level() const noexcept override {
    return level_.load(std::memory_order_relaxed);
  }

  void Flush() noexcept override { Drain(); }

  void Drain() noexcept {
    if (!IsBackendMainThread()) {
      return;
    }

    std::uint64_t dropped = 0;
    try {
      std::lock_guard lock(mutex_);
      pending_.swap(draining_);
      dropped = std::exchange(dropped_, 0);
    } catch (...) {
      return;
    }

    for (auto& message : draining_) {
      Emit(std::move(message));
    }
    draining_.clear();
    if (dropped > 0) {
      try {
        iceberg::LogMessage warning{
            .level = iceberg::LogLevel::kWarn,
            .message = std::format("dropped {} worker log records because the pending "
                                   "queue reached its {}-record limit",
                                   dropped, kMaxPendingWorkerLogs),
        };
        EmitToPostgres(warning);
      } catch (...) {
        EmitFormattingFailureToPostgres();
      }
    }
  }

 private:
  void Enqueue(iceberg::LogMessage&& message) noexcept {
    try {
      std::lock_guard lock(mutex_);
      if (pending_.size() >= kMaxPendingWorkerLogs) {
        if (dropped_ < std::numeric_limits<std::uint64_t>::max()) {
          ++dropped_;
        }
        return;
      }
      try {
        pending_.push_back(std::move(message));
      } catch (...) {
        if (dropped_ < std::numeric_limits<std::uint64_t>::max()) {
          ++dropped_;
        }
      }
    } catch (...) {  // NOLINT(bugprone-empty-catch)
    }
  }

  static void Emit(iceberg::LogMessage&& message) noexcept {
    try {
      EmitToPostgres(message);
    } catch (...) {
      EmitFormattingFailureToPostgres();
    }
  }

  std::atomic<iceberg::LogLevel> level_;
  std::mutex mutex_;
  std::vector<iceberg::LogMessage> pending_;
  std::vector<iceberg::LogMessage> draining_;
  std::uint64_t dropped_ = 0;
};

class ContextLogger final : public iceberg::Logger {
 public:
  ContextLogger(std::shared_ptr<iceberg::Logger> inner,
                std::vector<iceberg::LogAttribute> attributes)
      : inner_(std::move(inner)), attributes_(std::move(attributes)) {}

  bool ShouldLog(iceberg::LogLevel level) const noexcept override {
    return inner_->ShouldLog(level);
  }

  void Log(iceberg::LogMessage&& message) noexcept override {
    try {
      message.attributes.insert(message.attributes.end(), attributes_.begin(),
                                attributes_.end());
    } catch (...) {
      // Logging is best effort. Preserve the record without context rather than
      // terminating the backend if decorating it cannot allocate.
      message.attributes.clear();
    }
    inner_->Log(std::move(message));
  }

  void SetLevel(iceberg::LogLevel level) noexcept override { inner_->SetLevel(level); }

  iceberg::LogLevel level() const noexcept override { return inner_->level(); }

 private:
  std::shared_ptr<iceberg::Logger> inner_;
  std::vector<iceberg::LogAttribute> attributes_;
};

void ApplyConfiguredIcebergLogLevel() {
  if (IcebergLogLevelGuc == nullptr) {
    return;
  }
  const auto parsed = iceberg::LogLevelFromString(IcebergLogLevelGuc);
  if (parsed) {
    iceberg::SetDefaultLevel(*parsed);
  }
}

}  // namespace

static bool CheckIcebergLogLevel(char** newval, void** /*extra*/, GucSource /*source*/) {
  if (newval == nullptr || *newval == nullptr || **newval == '\0') {
    return false;
  }
  return static_cast<bool>(iceberg::LogLevelFromString(*newval));
}

static void OnIcebergLogLevelAssign(const char* newval, void* /*extra*/) {
  const auto parsed = iceberg::LogLevelFromString(newval);
  if (!parsed) {
    return;
  }
  iceberg::SetDefaultLevel(*parsed);
}

void InstallDefaultIcebergLogger() {
  g_backend_main_thread_id = std::this_thread::get_id();
  g_stderr_redirected_to_collector = Logging_collector;
  auto logger = std::make_shared<PgElogLogger>(iceberg::LogLevel::kWarn);
  g_pg_logger = logger;
  iceberg::SetDefaultLogger(std::move(logger));
  ApplyConfiguredIcebergLogLevel();
}

void RegisterIcebergLoggingGucs() {
  DefineCustomStringVariable(
      "pgiceberg.iceberg_log_level",
      "Minimum iceberg-cpp log level emitted by pgiceberg for this backend.",
      "Valid values: off, trace, debug, info, warn, error, critical, fatal.",
      &IcebergLogLevelGuc, "warn", PGC_USERSET, 0, CheckIcebergLogLevel,
      OnIcebergLogLevelAssign, nullptr);
}

std::shared_ptr<iceberg::Logger> MakeOperationLogger(std::string_view operation,
                                                     std::string_view relation) {
  return std::make_shared<ContextLogger>(
      iceberg::GetCurrentLogger(),
      std::vector<iceberg::LogAttribute>{
          {.key = "operation", .value = std::string(operation)},
          {.key = "relation", .value = std::string(relation)},
      });
}

OperationLoggerScope::OperationLoggerScope(std::string_view operation,
                                           std::string_view relation)
    : OperationLoggerScope(MakeOperationLogger(operation, relation)) {}

OperationLoggerScope::OperationLoggerScope(
    std::shared_ptr<iceberg::Logger> logger) noexcept
    : scope_(std::move(logger)) {}

OperationLoggerScope::~OperationLoggerScope() {
  if (auto logger = g_pg_logger.lock()) {
    logger->Drain();
  }
}

}  // namespace pgiceberg
