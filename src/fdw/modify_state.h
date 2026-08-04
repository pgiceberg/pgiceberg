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

#include <memory>
#include <optional>
#include <string>
#include <string_view>

#include <iceberg/type_fwd.h>

#include "common/status.h"
#include "fdw/options.h"

struct EState;
struct ModifyTableState;
struct ResultRelInfo;
struct TupleTableSlot;
struct RelationData;
using Relation = RelationData*;

namespace pgiceberg::fdw {

struct ModifyState;

struct LogicalCommitMetadata {
  std::string batch_id;
  std::string source_lsn;
};

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
Status AppendSlots(Relation relation, const Options& options, TupleTableSlot** slots,
                   int nslots,
                   std::optional<LogicalCommitMetadata> logical_metadata = std::nullopt);
Result<bool> IsLogicalBatchCommitted(const Options& options, const char* relation_name,
                                     std::string_view batch_id);
// Durably commit any pending Iceberg DML for the current PostgreSQL transaction.
// Safe to call before PRE_COMMIT; later PRE_COMMIT becomes a no-op for flushed work.
Status FlushPendingModifyChanges();
void RegisterTransactionCallbacks();

}  // namespace pgiceberg::fdw
