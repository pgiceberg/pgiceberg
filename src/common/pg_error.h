#pragma once

#include <exception>
#include <string>
#include <utility>

extern "C" {
#include "postgres.h"
#include "fmgr.h"
#include "utils/elog.h"
}

namespace pgiceberg {

class PgError : public std::exception {
 public:
  PgError(int sqlerrcode, std::string message, std::string hint = {})
      : sqlerrcode_(sqlerrcode), message_(std::move(message)), hint_(std::move(hint)) {}

  const char* what() const noexcept override { return message_.c_str(); }

  int sqlerrcode() const { return sqlerrcode_; }
  const std::string& hint() const { return hint_; }

 private:
  int sqlerrcode_;
  std::string message_;
  std::string hint_;
};

[[noreturn]] inline void ReportCurrentException() {
  try {
    throw;
  } catch (const PgError& ex) {
    if (ex.hint().empty()) {
      ereport(ERROR, (errcode(ex.sqlerrcode()), errmsg("%s", ex.what())));
    } else {
      ereport(ERROR, (errcode(ex.sqlerrcode()), errmsg("%s", ex.what()),
                      errhint("%s", ex.hint().c_str())));
    }
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

}  // namespace pgiceberg
