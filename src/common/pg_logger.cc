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

#include <atomic>
#include <chrono>
#include <cstdio>
#include <format>
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
#include "utils/guc.h"
#include "utils/elog.h"
}

namespace pgiceberg {
namespace {

std::thread::id g_backend_main_thread_id;
char* IcebergLogLevelGuc = nullptr;
std::shared_ptr<iceberg::Logger> g_pg_logger;

std::string_view Basename(std::string_view path) noexcept {
  const auto pos = path.find_last_of("/\\");
  return pos == std::string_view::npos ? path : path.substr(pos + 1);
}

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

std::string FormatAttributes(const std::vector<iceberg::LogAttribute>& attributes) {
  if (attributes.empty()) {
    return {};
  }
  std::string formatted = " {";
  for (std::size_t i = 0; i < attributes.size(); ++i) {
    if (i > 0) {
      formatted += ' ';
    }
    formatted += attributes[i].key;
    formatted += '=';
    formatted += attributes[i].value;
  }
  formatted += '}';
  return formatted;
}

std::string FormatLine(const iceberg::LogMessage& message) {
  const auto now =
      std::chrono::floor<std::chrono::milliseconds>(std::chrono::system_clock::now());
  return std::format(
      "{:%Y-%m-%dT%H:%M:%S}Z {} [{}:{}] {}{}",
      now, iceberg::ToString(message.level),
      Basename(message.location.file_name()), message.location.line(), message.message,
      FormatAttributes(message.attributes));
}

void EmitToStderr(std::string_view line) noexcept {
  std::fwrite(line.data(), 1, line.size(), stderr);
  std::fputc('\n', stderr);
  std::fflush(stderr);
}

void EmitToPostgres(int elevel, const std::string& line) noexcept {
  if (!message_level_is_interesting(elevel)) {
    return;
  }
  if (errstart(elevel, TEXTDOMAIN)) {
    errmsg_internal("pgiceberg: %s", line.c_str());
    errfinish(__FILE__, __LINE__, __func__);
  }
}

class PgElogLogger final : public iceberg::Logger {
 public:
  explicit PgElogLogger(iceberg::LogLevel level = iceberg::LogLevel::kWarn)
      : level_(level) {}

  bool ShouldLog(iceberg::LogLevel level) const noexcept override {
    return level >= level_.load(std::memory_order_relaxed);
  }

  void Log(iceberg::LogMessage&& message) noexcept override {
    try {
      const std::string line = FormatLine(message);
      const int elevel = IcebergLevelToPgElevel(message.level);
      std::lock_guard lock(mutex_);
      if (IsBackendMainThread()) {
        EmitToPostgres(elevel, line);
      } else {
        EmitToStderr(line);
      }
    } catch (...) {
      try {
        std::lock_guard lock(mutex_);
        EmitToStderr("pgiceberg: <log formatting error>");
      } catch (...) {
      }
    }
  }

  void SetLevel(iceberg::LogLevel level) noexcept override {
    level_.store(level, std::memory_order_relaxed);
  }

  iceberg::LogLevel level() const noexcept override {
    return level_.load(std::memory_order_relaxed);
  }

 private:
  std::atomic<iceberg::LogLevel> level_;
  std::mutex mutex_;
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
    message.attributes.insert(message.attributes.end(), attributes_.begin(),
                              attributes_.end());
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

void InstallFatalHandler() {
  iceberg::SetFatalHandler([](const std::source_location& location,
                              std::string_view message) {
    if (g_pg_logger == nullptr) {
      return;
    }
    g_pg_logger->Log(iceberg::LogMessage{
        .level = iceberg::LogLevel::kFatal,
        .message = std::string(message),
        .location = location,
    });
  });
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
  g_pg_logger = std::make_shared<PgElogLogger>(iceberg::LogLevel::kWarn);
  iceberg::SetDefaultLogger(g_pg_logger);
  ApplyConfiguredIcebergLogLevel();
  InstallFatalHandler();
}

void RegisterIcebergLoggingGucs() {
  DefineCustomStringVariable(
      "pgiceberg.iceberg_log_level",
      "Minimum iceberg-cpp log level emitted by pgiceberg for this backend.",
      "Valid values: off, trace, debug, info, warn, error, critical, fatal.",
      &IcebergLogLevelGuc, "warn", PGC_USERSET, 0, CheckIcebergLogLevel,
      OnIcebergLogLevelAssign, nullptr);
}

struct OperationLoggerScope::Impl {
  std::shared_ptr<iceberg::Logger> logger;
  iceberg::ScopedLogger scope;

  Impl(std::string_view operation, std::string_view relation)
      : logger(std::make_shared<ContextLogger>(
            iceberg::GetDefaultLogger(),
            std::vector<iceberg::LogAttribute>{
                {.key = "operation", .value = std::string(operation)},
                {.key = "relation", .value = std::string(relation)},
            })),
        scope(logger) {}
};

OperationLoggerScope::OperationLoggerScope(std::string_view operation,
                                           std::string_view relation)
    : impl_(std::make_unique<Impl>(operation, relation)) {}

OperationLoggerScope::~OperationLoggerScope() = default;

}  // namespace pgiceberg
