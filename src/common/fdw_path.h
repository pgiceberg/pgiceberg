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

#include <algorithm>

extern "C" {
#include "postgres.h"
#include "foreign/fdwapi.h"
#include "nodes/pg_list.h"
#include "optimizer/pathnode.h"
}

namespace pgiceberg {

// PostgreSQL changed create_foreignscan_path() arguments across supported
// releases. Keep the version split here so FDW path construction stays shared.
inline ForeignPath* CreateSimpleForeignScanPath(PlannerInfo* root, RelOptInfo* baserel) {
  const auto rows = baserel->rows;
  const auto total_cost = std::max(1.0, rows);
#if PG_VERSION_NUM >= 180000
  return create_foreignscan_path(root, baserel, nullptr, rows, 0, 0, total_cost, NIL,
                                 nullptr, nullptr, NIL, NIL);
#elif PG_VERSION_NUM >= 170000
  return create_foreignscan_path(root, baserel, nullptr, rows, 0, total_cost, NIL,
                                 nullptr, nullptr, NIL, NIL);
#else
  return create_foreignscan_path(root, baserel, nullptr, rows, 0, total_cost, NIL,
                                 nullptr, nullptr, NIL);
#endif
}

}  // namespace pgiceberg
