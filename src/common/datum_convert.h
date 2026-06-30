#pragma once

#include <cstdint>

#include "common/status.h"

namespace arrow {
class Array;
}

namespace pgiceberg {

Result<std::uintptr_t> ConvertValue(const arrow::Array& array, std::int64_t offset,
                                    unsigned int pg_type, bool& is_null);

}  // namespace pgiceberg
