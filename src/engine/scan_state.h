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

#include <cstddef>
#include <functional>
#include <memory>
#include <vector>

#include <iceberg/type_fwd.h>

#include "common/status.h"
#include "engine/options.h"

struct RelationData;
using Relation = RelationData*;
struct TupleTableSlot;

namespace pgiceberg::engine {

struct ScanState;

// Best-effort builder for an Iceberg scan filter, invoked once the table (and
// therefore its schema) is loaded.  Returning nullptr scans without a filter.
using ScanFilterBuilder =
    std::function<std::shared_ptr<iceberg::Expression>(const iceberg::Schema& schema)>;

Result<ScanState*> BeginScan(Relation relation, const Options& options,
                             const std::vector<int>& projected_attnums,
                             const ScanFilterBuilder& filter_builder = {});
Result<TupleTableSlot*> IterateScan(ScanState* state, TupleTableSlot* slot);
void ReScan(ScanState* state);
void EndScan(ScanState* state);
std::size_t ScanTaskCount(const ScanState* state);

}  // namespace pgiceberg::engine
