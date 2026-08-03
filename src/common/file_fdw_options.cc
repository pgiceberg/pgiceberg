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

#include "common/file_fdw_options.h"

#include <cstring>

extern "C" {
#include "access/reloptions.h"
#include "catalog/pg_foreign_server_d.h"
#include "catalog/pg_foreign_table_d.h"
#include "commands/defrem.h"
#include "foreign/foreign.h"
#include "nodes/pg_list.h"
#include "utils/errcodes.h"
}

namespace pgiceberg {

bool EndsWith(std::string_view value, std::string_view suffix) {
  return value.size() >= suffix.size() &&
         value.substr(value.size() - suffix.size()) == suffix;
}

std::string BasenameWithoutExtension(std::string_view name, std::string_view extension) {
  const auto dot = name.size() - extension.size();
  return std::string(name.substr(0, dot));
}

void ApplyFileFdwOption(FileFdwOptions& options, DefElem* def) {
  if (std::strcmp(def->defname, "filename") == 0) {
    options.filename = defGetString(def);
  } else if (std::strcmp(def->defname, "dirname") == 0) {
    options.dirname = defGetString(def);
  }
}

void ApplyFileFdwOptions(FileFdwOptions& options, List* option_list) {
  ListCell* cell = nullptr;
  foreach (cell, option_list) {
    ApplyFileFdwOption(options, static_cast<DefElem*>(lfirst(cell)));
  }
}

FileFdwOptions FileFdwOptionsForForeignTable(Oid relation_oid) {
  FileFdwOptions options;
  ForeignTable* table = GetForeignTable(relation_oid);
  ForeignServer* server = GetForeignServer(table->serverid);
  ApplyFileFdwOptions(options, server->options);
  ApplyFileFdwOptions(options, table->options);
  return options;
}

FileFdwOptions FileFdwOptionsForServer(Oid server_oid) {
  FileFdwOptions options;
  ForeignServer* server = GetForeignServer(server_oid);
  ApplyFileFdwOptions(options, server->options);
  return options;
}

Status ValidateFileFdwOptions(const FileFdwOptions& options, std::string_view fdw_name) {
  if (options.filename.empty()) {
    return std::unexpected(
        MakeError(ERRCODE_FDW_DYNAMIC_PARAMETER_VALUE_NEEDED,
                  std::string(fdw_name) + " option \"filename\" is required"));
  }
  return Ok();
}

Status ValidateFileFdwUtilityOptions(Datum raw_options, Oid catalog,
                                     const char* fdw_name) {
  List* options = untransformRelOptions(raw_options);
  FileFdwOptions parsed_options;

  ListCell* cell = nullptr;
  foreach (cell, options) {
    auto* def = static_cast<DefElem*>(lfirst(cell));
    const bool valid_option =
        (catalog == ForeignTableRelationId &&
         std::strcmp(def->defname, "filename") == 0) ||
        (catalog == ForeignServerRelationId && std::strcmp(def->defname, "dirname") == 0);
    if (!valid_option) {
      return std::unexpected(MakeError(
          ERRCODE_FDW_INVALID_OPTION_NAME,
          std::string("invalid ") + fdw_name + " option \"" + def->defname + "\"",
          "Valid options are: dirname on servers, filename on foreign tables."));
    }
    ApplyFileFdwOption(parsed_options, def);
  }

  if (catalog == ForeignTableRelationId) {
    PGICEBERG_RETURN_NOT_OK(ValidateFileFdwOptions(parsed_options, fdw_name));
  }
  return Ok();
}

bool ImportFilterMatches(ImportForeignSchemaStmt* stmt, const std::string& table_name) {
  if (stmt->list_type == FDW_IMPORT_SCHEMA_ALL) {
    return true;
  }

  bool listed = false;
  ListCell* cell = nullptr;
  foreach (cell, stmt->table_list) {
    auto* range = static_cast<RangeVar*>(lfirst(cell));
    if (table_name == range->relname) {
      listed = true;
      break;
    }
  }

  if (stmt->list_type == FDW_IMPORT_SCHEMA_LIMIT_TO) {
    return listed;
  }
  if (stmt->list_type == FDW_IMPORT_SCHEMA_EXCEPT) {
    return !listed;
  }
  return true;
}

std::string DirectoryForImport(Oid server_oid, const char* remote_schema) {
  auto options = FileFdwOptionsForServer(server_oid);
  if (remote_schema == nullptr || std::strlen(remote_schema) == 0 ||
      options.dirname.empty() || remote_schema[0] == '/') {
    return remote_schema == nullptr ? std::string{} : std::string(remote_schema);
  }
  if (std::strcmp(remote_schema, ".") == 0) {
    return options.dirname;
  }
  return options.dirname + "/" + remote_schema;
}

}  // namespace pgiceberg
