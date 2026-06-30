#pragma once

#include "common/status.h"
#include "fdw/options.h"

struct EState;
struct ModifyTableState;
struct ResultRelInfo;
struct TupleTableSlot;

namespace pgiceberg::fdw {

struct ModifyState;

Result<ModifyState*> BeginModify(ModifyTableState* mtstate, ResultRelInfo* rinfo,
                                 const Options& options);
Result<TupleTableSlot*> ExecInsert(ModifyState* state, TupleTableSlot* slot);
Result<TupleTableSlot*> ExecUpdate(ModifyState* state, TupleTableSlot* slot,
                                   TupleTableSlot* plan_slot);
Result<TupleTableSlot*> ExecDelete(ModifyState* state, TupleTableSlot* slot,
                                   TupleTableSlot* plan_slot);
Status EndModify(ModifyState* state);

}  // namespace pgiceberg::fdw
