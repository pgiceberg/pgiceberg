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

#include <algorithm>
#include <cstddef>
#include <cstring>
#include <string>

#include <iceberg/arrow/arrow_register.h>
#include <iceberg/avro/avro_register.h>
#include <iceberg/parquet/parquet_register.h>
#include <iceberg/schema.h>
#include <iceberg/table.h>
#include <iceberg/type.h>

#include "common/catalog.h"
#include "common/type_mapping.h"
#include "fdw/modify_state.h"
#include "fdw/options.h"
#include "fdw/scan_state.h"

extern "C" {
#include "postgres.h"
#include "access/reloptions.h"
#include "catalog/pg_foreign_server_d.h"
#include "catalog/pg_foreign_table_d.h"
#include "commands/defrem.h"
#include "executor/executor.h"
#include "fmgr.h"
#include "foreign/fdwapi.h"
#include "foreign/foreign.h"
#include "lib/stringinfo.h"
#include "nodes/makefuncs.h"
#include "nodes/parsenodes.h"
#include "optimizer/appendinfo.h"
#include "optimizer/pathnode.h"
#include "optimizer/planmain.h"
#include "optimizer/restrictinfo.h"
#include "utils/builtins.h"
#include "utils/errcodes.h"
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
    return true;
  }();
  (void)registered;
}

void PgIcebergGetForeignRelSize(PlannerInfo*, RelOptInfo* baserel, Oid) {
  baserel->rows = 1000;
}

ForeignPath* CreateIcebergForeignScanPath(PlannerInfo* root, RelOptInfo* baserel) {
  const auto rows = baserel->rows;
  const auto total_cost = std::max(1.0, rows);

  // PostgreSQL changed create_foreignscan_path() arguments across supported
  // releases.  Keep the version split at the call site so the rest of the FDW
  // path construction does not need PG-version-specific wrappers.
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

void PgIcebergGetForeignPaths(PlannerInfo* root, RelOptInfo* baserel, Oid) {
  add_path(baserel, reinterpret_cast<Path*>(CreateIcebergForeignScanPath(root, baserel)));
}

ForeignScan* PgIcebergGetForeignPlan(PlannerInfo*, RelOptInfo* baserel, Oid, ForeignPath*,
                                     List* tlist, List* scan_clauses, Plan* outer_plan) {
  scan_clauses = extract_actual_clauses(scan_clauses, false);
  return make_foreignscan(tlist, scan_clauses, baserel->relid, NIL, NIL, NIL, NIL,
                          outer_plan);
}

pgiceberg::Status PgIcebergBeginForeignScanImpl(ForeignScanState* node, int eflags) {
  if (eflags & EXEC_FLAG_EXPLAIN_ONLY) {
    return pgiceberg::Ok();
  }

  Relation relation = node->ss.ss_currentRelation;
  auto options = pgiceberg::fdw::OptionsForForeignTable(
      RelationGetRelid(relation), RelationGetRelationName(relation));
  PGICEBERG_RETURN_NOT_OK(
      pgiceberg::fdw::ValidateCatalogType(options.catalog_type.c_str()));
  PGICEBERG_ASSIGN_OR_RETURN(auto scan, pgiceberg::fdw::BeginScan(relation, options));
  node->fdw_state = scan;
  return pgiceberg::Ok();
}

void PgIcebergBeginForeignScan(ForeignScanState* node, int eflags) {
  pgiceberg::PgStatusGuard([&]() { return PgIcebergBeginForeignScanImpl(node, eflags); });
}

pgiceberg::Result<TupleTableSlot*> PgIcebergIterateForeignScanImpl(
    ForeignScanState* node) {
  auto* state = static_cast<pgiceberg::fdw::ScanState*>(node->fdw_state);
  return pgiceberg::fdw::IterateScan(state, node->ss.ss_ScanTupleSlot);
}

TupleTableSlot* PgIcebergIterateForeignScan(ForeignScanState* node) {
  return pgiceberg::PgResultGuard(
      [&]() { return PgIcebergIterateForeignScanImpl(node); });
}

void PgIcebergReScanForeignScan(ForeignScanState* node) {
  auto* state = static_cast<pgiceberg::fdw::ScanState*>(node->fdw_state);
  pgiceberg::fdw::ReScan(state);
}

void PgIcebergEndForeignScan(ForeignScanState* node) {
  auto* state = static_cast<pgiceberg::fdw::ScanState*>(node->fdw_state);
  pgiceberg::fdw::EndScan(state);
  node->fdw_state = nullptr;
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
  auto options = pgiceberg::fdw::OptionsForForeignTable(
      RelationGetRelid(relation), RelationGetRelationName(relation));
  PGICEBERG_RETURN_NOT_OK(
      pgiceberg::fdw::ValidateCatalogType(options.catalog_type.c_str()));
  PGICEBERG_ASSIGN_OR_RETURN(auto state,
                             pgiceberg::fdw::BeginModify(mtstate, rinfo, options));
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
  auto* state = static_cast<pgiceberg::fdw::ModifyState*>(rinfo->ri_FdwState);
  return pgiceberg::fdw::ExecInsert(state, slot);
}

TupleTableSlot* PgIcebergExecForeignInsert(EState*, ResultRelInfo* rinfo,
                                           TupleTableSlot* slot, TupleTableSlot*) {
  return pgiceberg::PgResultGuard(
      [&]() { return PgIcebergExecForeignInsertImpl(rinfo, slot); });
}

pgiceberg::Result<TupleTableSlot*> PgIcebergExecForeignUpdateImpl(
    ResultRelInfo* rinfo, TupleTableSlot* slot, TupleTableSlot* plan_slot) {
  auto* state = static_cast<pgiceberg::fdw::ModifyState*>(rinfo->ri_FdwState);
  return pgiceberg::fdw::ExecUpdate(state, slot, plan_slot);
}

TupleTableSlot* PgIcebergExecForeignUpdate(EState*, ResultRelInfo* rinfo,
                                           TupleTableSlot* slot,
                                           TupleTableSlot* plan_slot) {
  return pgiceberg::PgResultGuard(
      [&]() { return PgIcebergExecForeignUpdateImpl(rinfo, slot, plan_slot); });
}

pgiceberg::Result<TupleTableSlot*> PgIcebergExecForeignDeleteImpl(
    ResultRelInfo* rinfo, TupleTableSlot* slot, TupleTableSlot* plan_slot) {
  auto* state = static_cast<pgiceberg::fdw::ModifyState*>(rinfo->ri_FdwState);
  return pgiceberg::fdw::ExecDelete(state, slot, plan_slot);
}

TupleTableSlot* PgIcebergExecForeignDelete(EState*, ResultRelInfo* rinfo,
                                           TupleTableSlot* slot,
                                           TupleTableSlot* plan_slot) {
  return pgiceberg::PgResultGuard(
      [&]() { return PgIcebergExecForeignDeleteImpl(rinfo, slot, plan_slot); });
}

pgiceberg::Status PgIcebergEndForeignModifyImpl(ResultRelInfo* rinfo) {
  auto* state = static_cast<pgiceberg::fdw::ModifyState*>(rinfo->ri_FdwState);
  PGICEBERG_RETURN_NOT_OK(pgiceberg::fdw::EndModify(state));
  rinfo->ri_FdwState = nullptr;
  return pgiceberg::Ok();
}

void PgIcebergEndForeignModify(EState*, ResultRelInfo* rinfo) {
  pgiceberg::PgStatusGuard([&]() { return PgIcebergEndForeignModifyImpl(rinfo); });
}

int PgIcebergIsForeignRelUpdatable(Relation) {
  return (1 << CMD_INSERT) | (1 << CMD_UPDATE) | (1 << CMD_DELETE);
}

pgiceberg::Result<std::string> TableNameFromImport(const pgiceberg::fdw::Options& options,
                                                   ImportForeignSchemaStmt* stmt) {
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
  pgiceberg::fdw::Options options;
  ForeignServer* server = GetForeignServer(server_oid);
  pgiceberg::fdw::ApplyOptions(options, server->options);
  pgiceberg::fdw::ApplyOptions(options, stmt->options);
  if (options.name_space == "default" && stmt->remote_schema != nullptr &&
      std::strlen(stmt->remote_schema) > 0) {
    options.name_space = stmt->remote_schema;
  }
  PGICEBERG_ASSIGN_OR_RETURN(auto table_name, TableNameFromImport(options, stmt));
  options.table = std::move(table_name);
  PGICEBERG_RETURN_NOT_OK(
      pgiceberg::fdw::ValidateCatalogType(options.catalog_type.c_str()));

  PGICEBERG_ASSIGN_OR_RETURN(
      auto table, pgiceberg::LoadIcebergTable(pgiceberg::fdw::ToCatalogOptions(options),
                                              options.table.c_str()));
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

}  // namespace

extern "C" {
PG_FUNCTION_INFO_V1(pgiceberg_fdw_handler);
PG_FUNCTION_INFO_V1(pgiceberg_fdw_validator);

Datum pgiceberg_fdw_handler(PG_FUNCTION_ARGS) {
  return pgiceberg::PgGuard([]() -> Datum {
    EnsureIcebergRegistrations();
    FdwRoutine* routine = makeNode(FdwRoutine);
    routine->GetForeignRelSize = PgIcebergGetForeignRelSize;
    routine->GetForeignPaths = PgIcebergGetForeignPaths;
    routine->GetForeignPlan = PgIcebergGetForeignPlan;
    routine->BeginForeignScan = PgIcebergBeginForeignScan;
    routine->IterateForeignScan = PgIcebergIterateForeignScan;
    routine->ReScanForeignScan = PgIcebergReScanForeignScan;
    routine->EndForeignScan = PgIcebergEndForeignScan;
    routine->AddForeignUpdateTargets = PgIcebergAddForeignUpdateTargets;
    routine->PlanForeignModify = PgIcebergPlanForeignModify;
    routine->BeginForeignModify = PgIcebergBeginForeignModify;
    routine->ExecForeignInsert = PgIcebergExecForeignInsert;
    routine->ExecForeignUpdate = PgIcebergExecForeignUpdate;
    routine->ExecForeignDelete = PgIcebergExecForeignDelete;
    routine->EndForeignModify = PgIcebergEndForeignModify;
    routine->IsForeignRelUpdatable = PgIcebergIsForeignRelUpdatable;
    routine->ImportForeignSchema = PgIcebergImportForeignSchema;
    PG_RETURN_POINTER(routine);
  });
}

Datum pgiceberg_fdw_validator(PG_FUNCTION_ARGS) {
  pgiceberg::PgStatusGuard([&]() -> pgiceberg::Status {
    List* options = untransformRelOptions(PG_GETARG_DATUM(0));
    ListCell* cell = nullptr;
    pgiceberg::fdw::Options parsed_options;

    foreach (cell, options) {
      DefElem* def = static_cast<DefElem*>(lfirst(cell));
      if (!pgiceberg::fdw::IsValidOption(def->defname)) {
        const std::string valid_options = pgiceberg::fdw::ValidOptionsText();
        return std::unexpected(pgiceberg::MakeError(
            ERRCODE_FDW_INVALID_OPTION_NAME,
            std::string("invalid pgiceberg option \"") + def->defname + "\"",
            std::string("Valid options are: ") + valid_options + "."));
      }

      if (std::strcmp(def->defname, "catalog_type") == 0) {
        PGICEBERG_RETURN_NOT_OK(pgiceberg::fdw::ValidateCatalogType(defGetString(def)));
      }
      pgiceberg::fdw::ApplyOption(parsed_options, def);
    }

    return pgiceberg::Ok();
  });
  PG_RETURN_VOID();
}

}  // extern "C"
