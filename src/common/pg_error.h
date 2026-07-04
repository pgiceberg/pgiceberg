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

#include <utility>

#include "common/status.h"

extern "C" {
#include "postgres.h"
#include "fmgr.h"
#include "utils/elog.h"
}

namespace pgiceberg {

[[noreturn]] inline void ReportError(const PgError& error) {
  if (error.hint().empty()) {
    ereport(ERROR, (errcode(error.sqlerrcode()), errmsg("%s", error.what())));
  } else {
    ereport(ERROR, (errcode(error.sqlerrcode()), errmsg("%s", error.what()),
                    errhint("%s", error.hint().c_str())));
  }

  std::unreachable();
}

[[noreturn]] inline void ReportCurrentException() {
  try {
    throw;
  } catch (const PgError& ex) {
    ReportError(ex);
  } catch (const std::exception& ex) {
    ereport(ERROR, (errmsg("pgiceberg error: %s", ex.what())));
  } catch (...) {
    ereport(ERROR, (errmsg("pgiceberg error: unknown C++ exception")));
  }

  std::unreachable();
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
  try {
    Status status = fn();
    if (!status) {
      ReportError(status.error());
    }
  } catch (...) {
    ReportCurrentException();
  }
}

template <typename Fn>
auto PgResultGuard(Fn&& fn) -> typename decltype(fn())::value_type {
  try {
    auto result = fn();
    if (!result) {
      ReportError(result.error());
    }
    return std::move(result).value();
  } catch (...) {
    ReportCurrentException();
  }

  std::unreachable();
}

}  // namespace pgiceberg
