#pragma once

#include <arrow/result.h>
#include <arrow/status.h>
#include <iceberg/result.h>

#include <expected>
#include <exception>
#include <string>
#include <string_view>
#include <utility>

extern "C" {
#include "postgres.h"
#include "utils/elog.h"
#include "utils/errcodes.h"
}

namespace pgiceberg {

class PgError : public std::exception {
 public:
  PgError(int sqlerrcode, std::string message, std::string hint = {})
      : sqlerrcode_(sqlerrcode), message_(std::move(message)), hint_(std::move(hint)) {}

  const char* what() const noexcept override { return message_.c_str(); }

  int sqlerrcode() const { return sqlerrcode_; }
  const std::string& message() const { return message_; }
  const std::string& hint() const { return hint_; }

 private:
  int sqlerrcode_;
  std::string message_;
  std::string hint_;
};

template <typename T>
using Result = std::expected<T, PgError>;

using Status = Result<void>;

inline Status Ok() { return {}; }

inline PgError MakeError(int sqlerrcode, std::string message, std::string hint = {}) {
  return PgError(sqlerrcode, std::move(message), std::move(hint));
}

inline std::string WithContext(std::string_view context, std::string message) {
  if (context.empty()) {
    return message;
  }
  return std::string(context) + ": " + message;
}

inline int SqlStateForIcebergError(iceberg::ErrorKind kind) {
  switch (kind) {
    case iceberg::ErrorKind::kAlreadyExists:
      return ERRCODE_DUPLICATE_OBJECT;
    case iceberg::ErrorKind::kInvalid:
    case iceberg::ErrorKind::kInvalidArgument:
    case iceberg::ErrorKind::kInvalidArrowData:
    case iceberg::ErrorKind::kInvalidExpression:
    case iceberg::ErrorKind::kInvalidManifest:
    case iceberg::ErrorKind::kInvalidManifestList:
    case iceberg::ErrorKind::kInvalidSchema:
    case iceberg::ErrorKind::kValidationFailed:
      return ERRCODE_INVALID_PARAMETER_VALUE;
    case iceberg::ErrorKind::kNoSuchNamespace:
    case iceberg::ErrorKind::kNoSuchTable:
    case iceberg::ErrorKind::kNoSuchView:
    case iceberg::ErrorKind::kNoSuchWarehouse:
    case iceberg::ErrorKind::kNotFound:
      return ERRCODE_UNDEFINED_OBJECT;
    case iceberg::ErrorKind::kNotImplemented:
    case iceberg::ErrorKind::kNotSupported:
      return ERRCODE_FEATURE_NOT_SUPPORTED;
    case iceberg::ErrorKind::kIOError:
      return ERRCODE_IO_ERROR;
    default:
      return ERRCODE_FDW_ERROR;
  }
}

inline PgError FromIcebergError(const iceberg::Error& error,
                                std::string_view context = {}) {
  return MakeError(SqlStateForIcebergError(error.kind),
                   WithContext(context, error.message));
}

inline int SqlStateForArrowStatus(const arrow::Status& status) {
  switch (status.code()) {
    case arrow::StatusCode::Invalid:
      return ERRCODE_INVALID_PARAMETER_VALUE;
    case arrow::StatusCode::IOError:
      return ERRCODE_IO_ERROR;
    case arrow::StatusCode::NotImplemented:
      return ERRCODE_FEATURE_NOT_SUPPORTED;
    default:
      return ERRCODE_FDW_ERROR;
  }
}

inline PgError FromArrowStatus(const arrow::Status& status,
                               std::string_view context = {}) {
  return MakeError(SqlStateForArrowStatus(status),
                   WithContext(context, status.ToString()));
}

inline Status ToPgStatus(const iceberg::Status& status, std::string_view context = {}) {
  if (status) {
    return Ok();
  }
  return std::unexpected(FromIcebergError(status.error(), context));
}

template <typename T>
Result<T> ToPgResult(iceberg::Result<T> result, std::string_view context = {}) {
  if (!result) {
    return std::unexpected(FromIcebergError(result.error(), context));
  }
  return std::move(*result);
}

inline Status ToPgStatus(const arrow::Status& status, std::string_view context = {}) {
  if (status.ok()) {
    return Ok();
  }
  return std::unexpected(FromArrowStatus(status, context));
}

template <typename T>
Result<T> ToPgResult(arrow::Result<T> result, std::string_view context = {}) {
  if (!result.ok()) {
    return std::unexpected(FromArrowStatus(result.status(), context));
  }
  return std::move(result).ValueOrDie();
}

}  // namespace pgiceberg

#define PGICEBERG_RETURN_NOT_OK(expr)                    \
  do {                                                   \
    auto&& _pgiceberg_status = (expr);                   \
    if (!_pgiceberg_status) [[unlikely]] {               \
      return std::unexpected(_pgiceberg_status.error()); \
    }                                                    \
  } while (0)

#define PGICEBERG_ASSIGN_OR_RETURN_IMPL(result_name, lhs, rexpr) \
  auto&& result_name = (rexpr);                                  \
  if (!result_name) [[unlikely]] {                               \
    return std::unexpected(result_name.error());                 \
  }                                                              \
  lhs = std::move(result_name).value();

#define PGICEBERG_CONCAT(x, y) x##y
#define PGICEBERG_ASSIGN_OR_RETURN_NAME(x, y) PGICEBERG_CONCAT(x, y)

#define PGICEBERG_ASSIGN_OR_RETURN(lhs, rexpr) \
  PGICEBERG_ASSIGN_OR_RETURN_IMPL(             \
      PGICEBERG_ASSIGN_OR_RETURN_NAME(_pgiceberg_result_, __COUNTER__), lhs, rexpr)
