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

#include <cstdlib>
#include <cstring>
#include <utility>

#include "common/status.h"

extern "C" {
#include "postgres.h"
#include "fmgr.h"
#include "utils/elog.h"
#include "utils/errcodes.h"
}

namespace pgiceberg {
namespace detail {

constexpr const char* kErrorCopyFailed =
    "pgiceberg error: out of memory while copying error";

struct PgErrorReportData {
  int sqlerrcode = ERRCODE_FDW_ERROR;
  char* message = nullptr;
  char* hint = nullptr;
};

inline char* CopyCStringForReport(const char* value) noexcept {
  if (value == nullptr) {
    return nullptr;
  }
  const std::size_t size = std::strlen(value) + 1;
  auto* copy = static_cast<char*>(std::malloc(size));
  if (copy == nullptr) {
    return const_cast<char*>(kErrorCopyFailed);
  }
  std::memcpy(copy, value, size);
  return copy;
}

inline char* CopyPrefixedCStringForReport(const char* prefix,
                                          const char* value) noexcept {
  if (value == nullptr) {
    return CopyCStringForReport(prefix);
  }
  const std::size_t prefix_size = std::strlen(prefix);
  const std::size_t value_size = std::strlen(value);
  auto* copy = static_cast<char*>(std::malloc(prefix_size + value_size + 1));
  if (copy == nullptr) {
    return CopyCStringForReport(prefix);
  }
  std::memcpy(copy, prefix, prefix_size + 1);
  std::memcpy(copy + prefix_size, value, value_size + 1);
  return copy;
}

inline PgErrorReportData CopyErrorForReport(const PgError& error) noexcept {
  return PgErrorReportData{
      .sqlerrcode = error.sqlerrcode(),
      .message = CopyCStringForReport(error.what()),
      .hint = error.hint().empty() ? nullptr : CopyCStringForReport(error.hint().c_str()),
  };
}

[[noreturn]] inline void ReportErrorData(const PgErrorReportData& error) {
  const char* message = error.message == nullptr ? "pgiceberg error" : error.message;
  if (error.hint == nullptr) {
    ereport(ERROR, (errcode(error.sqlerrcode), errmsg("%s", message)));
  } else {
    ereport(ERROR, (errcode(error.sqlerrcode), errmsg("%s", message),
                    errhint("%s", error.hint)));
  }

  std::unreachable();
}

}  // namespace detail

[[noreturn]] inline void ReportError(const PgError& error) {
  const detail::PgErrorReportData report = detail::CopyErrorForReport(error);
  detail::ReportErrorData(report);
}

[[noreturn]] inline void ReportCurrentException() {
  detail::PgErrorReportData report;
  try {
    throw;
  } catch (const PgError& ex) {
    report = detail::CopyErrorForReport(ex);
  } catch (const std::exception& ex) {
    report.message = detail::CopyPrefixedCStringForReport("pgiceberg error: ", ex.what());
  } catch (...) {
    report.message =
        detail::CopyCStringForReport("pgiceberg error: unknown C++ exception");
  }

  detail::ReportErrorData(report);
}

template <typename Fn>
Datum PgGuard(Fn&& fn) {
  try {
    return fn();
  } catch (...) {
    ReportCurrentException();
  }

  return static_cast<Datum>(0);
}

template <typename Fn>
void PgStatusGuard(Fn&& fn) {
  detail::PgErrorReportData report;
  try {
    {
      Status status = fn();
      if (status) {
        return;
      }
      report = detail::CopyErrorForReport(status.error());
    }
  } catch (...) {
    ReportCurrentException();
  }

  detail::ReportErrorData(report);
}

template <typename Fn>
auto PgResultGuard(Fn&& fn) -> typename decltype(fn())::value_type {
  detail::PgErrorReportData report;
  try {
    {
      auto result = fn();
      if (result) {
        return std::move(result).value();
      }
      report = detail::CopyErrorForReport(result.error());
    }
  } catch (...) {
    ReportCurrentException();
  }

  detail::ReportErrorData(report);
}

}  // namespace pgiceberg
