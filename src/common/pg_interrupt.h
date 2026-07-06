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

#include <string>
#include <utility>

#include "common/status.h"

extern "C" {
#include "miscadmin.h"
#include "utils/elog.h"
}

namespace pgiceberg {

inline Status CheckForInterrupts() {
  ErrorData* error = nullptr;
  PG_TRY();
  {
    CHECK_FOR_INTERRUPTS();
  }
  PG_CATCH();
  {
    error = CopyErrorData();
    FlushErrorState();
  }
  PG_END_TRY();

  if (error == nullptr) {
    return Ok();
  }

  std::string message =
      error->message == nullptr ? "PostgreSQL interrupt" : error->message;
  std::string hint = error->hint == nullptr ? "" : error->hint;
  const int sqlerrcode = error->sqlerrcode;
  FreeErrorData(error);
  return std::unexpected(PgError(sqlerrcode, std::move(message), std::move(hint)));
}

}  // namespace pgiceberg
