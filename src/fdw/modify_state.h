#pragma once

#include <memory>

#include <iceberg/type_fwd.h>

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
Result<std::shared_ptr<iceberg::Table>> ReadTableForCurrentTransaction(
    const Options& options, std::shared_ptr<iceberg::Table> table);
Result<TupleTableSlot*> ExecInsert(ModifyState* state, TupleTableSlot* slot);
Result<TupleTableSlot*> ExecUpdate(ModifyState* state, TupleTableSlot* slot,
                                   TupleTableSlot* plan_slot);
Result<TupleTableSlot*> ExecDelete(ModifyState* state, TupleTableSlot* slot,
                                   TupleTableSlot* plan_slot);
Status EndModify(ModifyState* state);
void RegisterTransactionCallbacks();

}  // namespace pgiceberg::fdw
