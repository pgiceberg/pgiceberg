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
