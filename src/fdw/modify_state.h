#pragma once

#include "fdw/options.h"

struct EState;
struct ModifyTableState;
struct ResultRelInfo;
struct TupleTableSlot;

namespace pgiceberg::fdw {

struct ModifyState;

ModifyState* BeginModify(ModifyTableState* mtstate, ResultRelInfo* rinfo,
                         const Options& options);
TupleTableSlot* ExecInsert(ModifyState* state, TupleTableSlot* slot);
TupleTableSlot* ExecUpdate(ModifyState* state, TupleTableSlot* slot,
                           TupleTableSlot* plan_slot);
TupleTableSlot* ExecDelete(ModifyState* state, TupleTableSlot* slot,
                           TupleTableSlot* plan_slot);
void EndModify(ModifyState* state);

}  // namespace pgiceberg::fdw
