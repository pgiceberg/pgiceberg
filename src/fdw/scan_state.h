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

#include "common/status.h"
#include "fdw/options.h"

struct RelationData;
using Relation = RelationData*;
struct TupleTableSlot;

namespace pgiceberg::fdw {

struct ScanState;

Result<ScanState*> BeginScan(Relation relation, const Options& options);
Result<TupleTableSlot*> IterateScan(ScanState* state, TupleTableSlot* slot);
void ReScan(ScanState* state);
void EndScan(ScanState* state);

}  // namespace pgiceberg::fdw
