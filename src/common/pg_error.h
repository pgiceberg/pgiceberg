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
