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

#include <memory>
#include <string_view>

#include <iceberg/logging/logger.h>

namespace pgiceberg {

// Install the shared iceberg-cpp logger that forwards to PostgreSQL elog.
void InstallDefaultIcebergLogger();

// Register pgiceberg.iceberg_log_level (PGC_USERSET).
void RegisterIcebergLoggingGucs();

// Create a reusable logger decorated with one engine operation's context.
std::shared_ptr<iceberg::Logger> MakeOperationLogger(std::string_view operation,
                                                     std::string_view relation);

// Bind an operation logger on the current thread for the lifetime of this scope.
class OperationLoggerScope {
 public:
  OperationLoggerScope(std::string_view operation, std::string_view relation);
  explicit OperationLoggerScope(std::shared_ptr<iceberg::Logger> logger) noexcept;
  ~OperationLoggerScope();

  OperationLoggerScope(const OperationLoggerScope&) = delete;
  OperationLoggerScope& operator=(const OperationLoggerScope&) = delete;

 private:
  iceberg::ScopedLogger scope_;
};

}  // namespace pgiceberg
