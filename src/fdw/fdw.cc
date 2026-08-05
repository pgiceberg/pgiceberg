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

#include <cstddef>
#include <cstring>
#include <cstdlib>
#include <string>

#include <iceberg/arrow/arrow_register.h>
#include <iceberg/avro/avro_register.h>
#include <iceberg/data/puffin_dv_register.h>
#include <iceberg/parquet/parquet_register.h>
#include <iceberg/schema.h>
#include <iceberg/snapshot.h>
#include <iceberg/table.h>
#include <iceberg/type.h>

#include "common/catalog.h"
#include "common/fdw_path.h"
#include "common/type_mapping.h"
#include "engine/modify_state.h"
#include "engine/options.h"
#include "engine/scan_state.h"
#include "fdw/qual_pushdown.h"
#include "fdw/scan_projection.h"

extern "C" {
#include "postgres.h"
#include "access/reloptions.h"
#include "catalog/pg_foreign_server_d.h"
#include "catalog/pg_foreign_table_d.h"
#include "commands/defrem.h"
#include "commands/explain_format.h"
#include "commands/explain_state.h"
#include "executor/executor.h"
#include "fmgr.h"
#include "foreign/fdwapi.h"
#include "foreign/foreign.h"
#include "lib/stringinfo.h"
#include "nodes/makefuncs.h"
#include "nodes/parsenodes.h"
#include "nodes/pathnodes.h"
#include "nodes/value.h"
#include "optimizer/appendinfo.h"
#include "optimizer/pathnode.h"
#include "optimizer/planmain.h"
#include "optimizer/restrictinfo.h"
#include "utils/builtins.h"
#include "utils/errcodes.h"
#include "utils/lsyscache.h"
#include "utils/rel.h"
}

#include "common/pg_error.h"

namespace {

void EnsureIcebergRegistrations() {
  // iceberg-cpp keeps file-format and Arrow integrations in process-global
  // registries.  FDW callbacks can be entered many times in one backend, so
  // register once and let later scans reuse the same registry state.
  static const bool registered = [] {
    iceberg::arrow::RegisterAll();
    iceberg::parquet::RegisterAll();
    iceberg::avro::RegisterAll();
    iceberg::RegisterPuffinDVIO();
    return true;
  }();
  (void)registered;
}

// Translate the relation's restriction clauses and remember the readable
// filter text so EXPLAIN can show what the scan will push down.  Best-effort:
// planning continues without a filter annotation on any failure.
void StashPushdownFilterText(RelOptInfo* baserel, Oid relation_oid,
                             const std::shared_ptr<iceberg::Table>& table) {
  auto schema_result =
      pgiceberg::FromIcebergResult(table->schema(), "load schema for pushdown");
  if (!schema_result.has_value() || *schema_result == nullptr) {
    return;
  }
  List* clauses = extract_actual_clauses(baserel->baserestrictinfo, false);
  auto filter = pgiceberg::fdw::TranslateQualsForPushdown(clauses, baserel->relid,
                                                          relation_oid, **schema_result);
  if (filter == nullptr) {
    return;
  }
  try {
    baserel->fdw_private = pstrdup(filter->ToString().c_str());
  } catch (...) {
    baserel->fdw_private = nullptr;
  }
}

pgiceberg::Status PgIcebergGetForeignRelSizeImpl(RelOptInfo* baserel, Oid relation_oid) {
  baserel->rows = 1000;
  PGICEBERG_ASSIGN_OR_RETURN(auto options, pgiceberg::engine::OptionsForForeignTable(
                                               relation_oid, get_rel_name(relation_oid)));
  if (options.catalog.empty()) {
    return pgiceberg::Ok();
  }
  PGICEBERG_ASSIGN_OR_RETURN(auto catalog_options,
                             pgiceberg::engine::ToCatalogOptions(options));
  PGICEBERG_ASSIGN_OR_RETURN(
      auto table, pgiceberg::LoadIcebergTable(catalog_options, options.table.c_str()));
  if (!options.snapshot_id.has_value()) {
    PGICEBERG_ASSIGN_OR_RETURN(
        table, pgiceberg::engine::ReadTableForCurrentTransaction(options, table));
    // Pushdown is limited to current-snapshot scans; see engine::BeginScan.
    StashPushdownFilterText(baserel, relation_oid, table);
  }

  std::shared_ptr<iceberg::Snapshot> snapshot;
  if (options.snapshot_id.has_value()) {
    PGICEBERG_ASSIGN_OR_RETURN(
        snapshot, pgiceberg::FromIcebergResult(table->SnapshotById(*options.snapshot_id),
                                               "load snapshot by id"));
  } else {
    auto snapshot_result =
        pgiceberg::FromIcebergResult(table->current_snapshot(), "load current snapshot");
    if (!snapshot_result.has_value()) {
      return pgiceberg::Ok();
    }
    snapshot = *snapshot_result;
  }
  if (snapshot == nullptr) {
    return pgiceberg::Ok();
  }
  auto total_records =
      snapshot->summary.find(iceberg::SnapshotSummaryFields::kTotalRecords);
  if (total_records == snapshot->summary.end()) {
    return pgiceberg::Ok();
  }
  char* end = nullptr;
  const double rows = std::strtod(total_records->second.c_str(), &end);
  if (end != total_records->second.c_str() && rows >= 0) {
    baserel->rows = rows;
  }
  return pgiceberg::Ok();
}

void PgIcebergGetForeignRelSize(PlannerInfo*, RelOptInfo* baserel, Oid relation_oid) {
  pgiceberg::PgStatusGuard(
      [&]() { return PgIcebergGetForeignRelSizeImpl(baserel, relation_oid); });
}

void PgIcebergGetForeignPaths(PlannerInfo* root, RelOptInfo* baserel, Oid) {
  add_path(baserel, reinterpret_cast<Path*>(
                        pgiceberg::CreateSimpleForeignScanPath(root, baserel)));
}

// fdw_private layout for pgiceberg foreign scans.
enum FdwPrivateIndex {
  kFdwPrivateProjection = 0,
  kFdwPrivateFilterText = 1,
};

ForeignScan* PgIcebergGetForeignPlan(PlannerInfo*, RelOptInfo* baserel, Oid, ForeignPath*,
                                     List* tlist, List* scan_clauses, Plan* outer_plan) {
  scan_clauses = extract_actual_clauses(scan_clauses, false);
  List* projection =
      pgiceberg::fdw::BuildFdwScanProjectionPrivate(baserel, tlist, scan_clauses);
  const char* filter_text = baserel->fdw_private == nullptr
                                ? ""
                                : static_cast<const char*>(baserel->fdw_private);
  List* fdw_private = list_make2(projection, makeString(pstrdup(filter_text)));
  return make_foreignscan(tlist, scan_clauses, baserel->relid, NIL, fdw_private, NIL, NIL,
                          outer_plan);
}

pgiceberg::Status PgIcebergBeginForeignScanImpl(ForeignScanState* node, int eflags) {
  if (eflags & EXEC_FLAG_EXPLAIN_ONLY) {
    return pgiceberg::Ok();
  }

  Relation relation = node->ss.ss_currentRelation;
  PGICEBERG_ASSIGN_OR_RETURN(
      auto options, pgiceberg::engine::OptionsForForeignTable(
                        RelationGetRelid(relation), RelationGetRelationName(relation)));
  if (options.catalog.empty()) {
    PGICEBERG_RETURN_NOT_OK(
        pgiceberg::engine::ValidateCatalogType(options.catalog_type.c_str()));
  }
  auto* plan = castNode(ForeignScan, node->ss.ps.plan);
  auto projected_attnums = pgiceberg::fdw::FdwScanProjectionFromList(
      list_nth_node(List, plan->fdw_private, kFdwPrivateProjection));

  // Re-translate the scan clauses once the engine has loaded the table, so
  // literals convert against the authoritative Iceberg schema.
  List* quals = plan->scan.plan.qual;
  const Index scanrelid = plan->scan.scanrelid;
  const Oid relation_oid = RelationGetRelid(relation);
  auto filter_builder = [quals, scanrelid, relation_oid](const iceberg::Schema& schema) {
    return pgiceberg::fdw::TranslateQualsForPushdown(quals, scanrelid, relation_oid,
                                                     schema);
  };

  PGICEBERG_ASSIGN_OR_RETURN(
      auto scan,
      pgiceberg::engine::BeginScan(relation, options, projected_attnums, filter_builder));
  node->fdw_state = scan;
  return pgiceberg::Ok();
}

void PgIcebergBeginForeignScan(ForeignScanState* node, int eflags) {
  pgiceberg::PgStatusGuard([&]() { return PgIcebergBeginForeignScanImpl(node, eflags); });
}

pgiceberg::Result<TupleTableSlot*> PgIcebergIterateForeignScanImpl(
    ForeignScanState* node) {
  auto* state = static_cast<pgiceberg::engine::ScanState*>(node->fdw_state);
  return pgiceberg::engine::IterateScan(state, node->ss.ss_ScanTupleSlot);
}

TupleTableSlot* PgIcebergIterateForeignScan(ForeignScanState* node) {
  return pgiceberg::PgResultGuard(
      [&]() { return PgIcebergIterateForeignScanImpl(node); });
}

void PgIcebergReScanForeignScan(ForeignScanState* node) {
  auto* state = static_cast<pgiceberg::engine::ScanState*>(node->fdw_state);
  pgiceberg::engine::ReScan(state);
}

void PgIcebergEndForeignScan(ForeignScanState* node) {
  auto* state = static_cast<pgiceberg::engine::ScanState*>(node->fdw_state);
  pgiceberg::engine::EndScan(state);
  node->fdw_state = nullptr;
}

void PgIcebergExplainForeignScan(ForeignScanState* node, ExplainState* es) {
  auto* plan = castNode(ForeignScan, node->ss.ps.plan);
  if (list_length(plan->fdw_private) > kFdwPrivateFilterText) {
    auto* filter_node =
        static_cast<Node*>(list_nth(plan->fdw_private, kFdwPrivateFilterText));
    if (filter_node != nullptr && IsA(filter_node, String)) {
      const char* filter_text = strVal(filter_node);
      if (filter_text[0] != '\0') {
        ExplainPropertyText("Iceberg Filter", filter_text, es);
      }
    }
  }
  if (es->analyze && node->fdw_state != nullptr) {
    auto* state = static_cast<pgiceberg::engine::ScanState*>(node->fdw_state);
    ExplainPropertyUInteger("Iceberg Scan Tasks", nullptr,
                            static_cast<uint64>(pgiceberg::engine::ScanTaskCount(state)),
                            es);
  }
}

List* PgIcebergPlanForeignModify(PlannerInfo*, ModifyTable*, Index, int) { return NIL; }

void PgIcebergAddForeignUpdateTargets(PlannerInfo* root, Index rtindex, RangeTblEntry*,
                                      Relation) {
  // Iceberg rows do not have a PostgreSQL TID.  Carry the whole old row through
  // the plan so UPDATE/DELETE can later match it against the Iceberg file being
  // rewritten.
  auto* var =
      makeVar(static_cast<int>(rtindex), InvalidAttrNumber, RECORDOID, -1, InvalidOid, 0);
  add_row_identity_var(root, var, rtindex, "wholerow");
}

pgiceberg::Status PgIcebergBeginForeignModifyImpl(ModifyTableState* mtstate,
                                                  ResultRelInfo* rinfo) {
  Relation relation = rinfo->ri_RelationDesc;
  PGICEBERG_ASSIGN_OR_RETURN(
      auto options, pgiceberg::engine::OptionsForForeignTable(
                        RelationGetRelid(relation), RelationGetRelationName(relation)));
  if (options.catalog.empty()) {
    PGICEBERG_RETURN_NOT_OK(
        pgiceberg::engine::ValidateCatalogType(options.catalog_type.c_str()));
  }
  PGICEBERG_RETURN_NOT_OK(pgiceberg::engine::EnsureWritableOptions(options));
  PGICEBERG_ASSIGN_OR_RETURN(auto state,
                             pgiceberg::engine::BeginModify(mtstate, rinfo, options));
  rinfo->ri_FdwState = state;
  return pgiceberg::Ok();
}

void PgIcebergBeginForeignModify(ModifyTableState* mtstate, ResultRelInfo* rinfo, List*,
                                 int, int) {
  pgiceberg::PgStatusGuard(
      [&]() { return PgIcebergBeginForeignModifyImpl(mtstate, rinfo); });
}

pgiceberg::Result<TupleTableSlot*> PgIcebergExecForeignInsertImpl(ResultRelInfo* rinfo,
                                                                  TupleTableSlot* slot) {
  auto* state = static_cast<pgiceberg::engine::ModifyState*>(rinfo->ri_FdwState);
  return pgiceberg::engine::ExecInsert(state, slot);
}

TupleTableSlot* PgIcebergExecForeignInsert(EState*, ResultRelInfo* rinfo,
                                           TupleTableSlot* slot, TupleTableSlot*) {
  return pgiceberg::PgResultGuard(
      [&]() { return PgIcebergExecForeignInsertImpl(rinfo, slot); });
}

pgiceberg::Result<TupleTableSlot*> PgIcebergExecForeignUpdateImpl(
    ResultRelInfo* rinfo, TupleTableSlot* slot, TupleTableSlot* plan_slot) {
  auto* state = static_cast<pgiceberg::engine::ModifyState*>(rinfo->ri_FdwState);
  return pgiceberg::engine::ExecUpdate(state, slot, plan_slot);
}

TupleTableSlot* PgIcebergExecForeignUpdate(EState*, ResultRelInfo* rinfo,
                                           TupleTableSlot* slot,
                                           TupleTableSlot* plan_slot) {
  return pgiceberg::PgResultGuard(
      [&]() { return PgIcebergExecForeignUpdateImpl(rinfo, slot, plan_slot); });
}

pgiceberg::Result<TupleTableSlot*> PgIcebergExecForeignDeleteImpl(
    ResultRelInfo* rinfo, TupleTableSlot* slot, TupleTableSlot* plan_slot) {
  auto* state = static_cast<pgiceberg::engine::ModifyState*>(rinfo->ri_FdwState);
  return pgiceberg::engine::ExecDelete(state, slot, plan_slot);
}

TupleTableSlot* PgIcebergExecForeignDelete(EState*, ResultRelInfo* rinfo,
                                           TupleTableSlot* slot,
                                           TupleTableSlot* plan_slot) {
  return pgiceberg::PgResultGuard(
      [&]() { return PgIcebergExecForeignDeleteImpl(rinfo, slot, plan_slot); });
}

pgiceberg::Status PgIcebergEndForeignModifyImpl(ResultRelInfo* rinfo) {
  auto* state = static_cast<pgiceberg::engine::ModifyState*>(rinfo->ri_FdwState);
  PGICEBERG_RETURN_NOT_OK(pgiceberg::engine::EndModify(state));
  rinfo->ri_FdwState = nullptr;
  return pgiceberg::Ok();
}

void PgIcebergEndForeignModify(EState*, ResultRelInfo* rinfo) {
  pgiceberg::PgStatusGuard([&]() { return PgIcebergEndForeignModifyImpl(rinfo); });
}

int PgIcebergIsForeignRelUpdatable(Relation) {
  return (1 << CMD_INSERT) | (1 << CMD_UPDATE) | (1 << CMD_DELETE);
}

pgiceberg::Result<std::string> TableNameFromImport(
    const pgiceberg::engine::Options& options, ImportForeignSchemaStmt* stmt) {
  if (!options.table.empty()) {
    return options.table;
  }
  if (stmt->list_type == FDW_IMPORT_SCHEMA_LIMIT_TO &&
      list_length(stmt->table_list) == 1) {
    auto* range = static_cast<RangeVar*>(linitial(stmt->table_list));
    return range->relname;
  }
  return std::unexpected(pgiceberg::MakeError(
      ERRCODE_FDW_INVALID_OPTION_NAME,
      "pgiceberg option \"table\" is required for IMPORT FOREIGN SCHEMA",
      "Use OPTIONS (table 'iceberg_table_name') or LIMIT TO with a single table."));
}

pgiceberg::Result<List*> PgIcebergImportForeignSchemaImpl(ImportForeignSchemaStmt* stmt,
                                                          Oid server_oid) {
  pgiceberg::engine::Options options;
  ForeignServer* server = GetForeignServer(server_oid);
  PGICEBERG_RETURN_NOT_OK(pgiceberg::engine::ApplyOptions(options, server->options));
  PGICEBERG_RETURN_NOT_OK(pgiceberg::engine::ApplyOptions(options, stmt->options));
  if (options.name_space == "default" && stmt->remote_schema != nullptr &&
      std::strlen(stmt->remote_schema) > 0) {
    options.name_space = stmt->remote_schema;
  }
  PGICEBERG_ASSIGN_OR_RETURN(auto table_name, TableNameFromImport(options, stmt));
  options.table = std::move(table_name);
  if (options.catalog.empty()) {
    PGICEBERG_RETURN_NOT_OK(
        pgiceberg::engine::ValidateCatalogType(options.catalog_type.c_str()));
  }

  PGICEBERG_ASSIGN_OR_RETURN(auto catalog_options,
                             pgiceberg::engine::ToCatalogOptions(options));
  PGICEBERG_ASSIGN_OR_RETURN(
      auto table, pgiceberg::LoadIcebergTable(catalog_options, options.table.c_str()));
  PGICEBERG_ASSIGN_OR_RETURN(
      auto schema, pgiceberg::FromIcebergResult(table->schema(), "load schema"));

  StringInfo sql = makeStringInfo();
  appendStringInfo(sql, "CREATE FOREIGN TABLE %s.%s (",
                   quote_identifier(stmt->local_schema),
                   quote_identifier(options.table.c_str()));
  const auto fields = schema->fields();
  for (std::size_t i = 0; i < fields.size(); i++) {
    if (i > 0) {
      appendStringInfoString(sql, ", ");
    }
    const auto& field = fields[i];
    const std::string field_name(field.name());
    const auto sql_type = pgiceberg::IcebergTypeToSql(*field.type());
    appendStringInfo(sql, "%s %s", quote_identifier(field_name.c_str()),
                     sql_type.c_str());
  }
  appendStringInfo(sql, ") SERVER %s OPTIONS (namespace %s, table %s)",
                   quote_identifier(stmt->server_name),
                   quote_literal_cstr(options.name_space.c_str()),
                   quote_literal_cstr(options.table.c_str()));

  return list_make1(sql->data);
}

List* PgIcebergImportForeignSchema(ImportForeignSchemaStmt* stmt, Oid server_oid) {
  return pgiceberg::PgResultGuard(
      [&]() { return PgIcebergImportForeignSchemaImpl(stmt, server_oid); });
}

pgiceberg::Result<Datum> PgIcebergFdwHandlerImpl() {
  EnsureIcebergRegistrations();
  FdwRoutine* routine = makeNode(FdwRoutine);
  routine->GetForeignRelSize = PgIcebergGetForeignRelSize;
  routine->GetForeignPaths = PgIcebergGetForeignPaths;
  routine->GetForeignPlan = PgIcebergGetForeignPlan;
  routine->BeginForeignScan = PgIcebergBeginForeignScan;
  routine->IterateForeignScan = PgIcebergIterateForeignScan;
  routine->ReScanForeignScan = PgIcebergReScanForeignScan;
  routine->EndForeignScan = PgIcebergEndForeignScan;
  routine->ExplainForeignScan = PgIcebergExplainForeignScan;
  routine->AddForeignUpdateTargets = PgIcebergAddForeignUpdateTargets;
  routine->PlanForeignModify = PgIcebergPlanForeignModify;
  routine->BeginForeignModify = PgIcebergBeginForeignModify;
  routine->ExecForeignInsert = PgIcebergExecForeignInsert;
  routine->ExecForeignUpdate = PgIcebergExecForeignUpdate;
  routine->ExecForeignDelete = PgIcebergExecForeignDelete;
  routine->EndForeignModify = PgIcebergEndForeignModify;
  routine->IsForeignRelUpdatable = PgIcebergIsForeignRelUpdatable;
  routine->ImportForeignSchema = PgIcebergImportForeignSchema;
  return PointerGetDatum(routine);
}

pgiceberg::Status PgIcebergFdwValidatorImpl(Datum raw_options) {
  List* options = untransformRelOptions(raw_options);
  ListCell* cell = nullptr;
  pgiceberg::engine::Options parsed_options;

  foreach (cell, options) {
    DefElem* def = static_cast<DefElem*>(lfirst(cell));
    if (!pgiceberg::engine::IsValidOption(def->defname)) {
      const std::string valid_options = pgiceberg::engine::ValidOptionsText();
      return std::unexpected(pgiceberg::MakeError(
          ERRCODE_FDW_INVALID_OPTION_NAME,
          std::string("invalid pgiceberg option \"") + def->defname + "\"",
          std::string("Valid options are: ") + valid_options + "."));
    }

    if (std::strcmp(def->defname, "catalog_type") == 0) {
      PGICEBERG_RETURN_NOT_OK(pgiceberg::engine::ValidateCatalogType(defGetString(def)));
    }
    if (std::strcmp(def->defname, "snapshot_id") == 0) {
      PGICEBERG_RETURN_NOT_OK(
          pgiceberg::engine::ValidateSnapshotIdOption(defGetString(def)));
    }
    PGICEBERG_RETURN_NOT_OK(pgiceberg::engine::ApplyOption(parsed_options, def));
  }

  return pgiceberg::Ok();
}

}  // namespace

extern "C" {
PG_FUNCTION_INFO_V1(pgiceberg_fdw_handler);
PG_FUNCTION_INFO_V1(pgiceberg_fdw_validator);

Datum pgiceberg_fdw_handler(PG_FUNCTION_ARGS) {
  return pgiceberg::PgResultGuard([]() { return PgIcebergFdwHandlerImpl(); });
}

Datum pgiceberg_fdw_validator(PG_FUNCTION_ARGS) {
  Datum raw_options = PG_GETARG_DATUM(0);
  pgiceberg::PgStatusGuard([=]() { return PgIcebergFdwValidatorImpl(raw_options); });
  PG_RETURN_VOID();
}

}  // extern "C"
