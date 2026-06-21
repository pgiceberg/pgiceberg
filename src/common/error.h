#pragma once

#include <arrow/result.h>
#include <arrow/status.h>
#include <iceberg/result.h>

#include <stdexcept>
#include <string>
#include <string_view>
#include <utility>

namespace pgiceberg {

[[noreturn]] inline void ThrowIcebergError(const iceberg::Error& error,
                                           std::string_view context = {}) {
  if (context.empty()) {
    throw std::runtime_error(error.message);
  }
  throw std::runtime_error(std::string(context) + ": " + error.message);
}

inline void CheckIcebergStatus(const iceberg::Status& status,
                               std::string_view context = {}) {
  if (!status) {
    ThrowIcebergError(status.error(), context);
  }
}

template <typename T>
T CheckIcebergResult(iceberg::Result<T> result, std::string_view context = {}) {
  if (!result) {
    ThrowIcebergError(result.error(), context);
  }
  return std::move(*result);
}

inline void CheckArrowStatus(const arrow::Status& status, std::string_view context = {}) {
  if (status.ok()) {
    return;
  }
  if (context.empty()) {
    throw std::runtime_error(status.ToString());
  }
  throw std::runtime_error(std::string(context) + ": " + status.ToString());
}

template <typename T>
T CheckArrowResult(arrow::Result<T> result, std::string_view context = {}) {
  if (!result.ok()) {
    CheckArrowStatus(result.status(), context);
  }
  return std::move(*result);
}

}  // namespace pgiceberg
