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

#include <cstdint>

namespace pgiceberg {

// Offsets between PostgreSQL's 2000-01-01 epoch and the Unix 1970-01-01 epoch.
inline constexpr std::int64_t kPostgresUnixEpochOffsetDays = 10957;
inline constexpr std::int64_t kPostgresUnixEpochOffsetMicros = 946684800000000LL;

}  // namespace pgiceberg
