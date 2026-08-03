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

#include "fdw/options.h"

#include <array>
#include <charconv>
#include <cstring>
#include <system_error>

extern "C" {
#include "postgres.h"
#include "commands/defrem.h"
#include "foreign/foreign.h"
#include "nodes/pg_list.h"
#include "utils/errcodes.h"
}

namespace pgiceberg::fdw {
namespace {

constexpr std::array<const char*, 8> kValidOptions = {
    "catalog",  "catalog_type", "catalog_uri", "warehouse",
    "namespace", "table",       "snapshot_id", "catalog_name"};

#ifdef PGICEBERG_ENABLE_REST_CATALOG
constexpr const char* kValidCatalogTypes = "sql, sqlite, rest";
#else
constexpr const char* kValidCatalogTypes = "sql, sqlite";
#endif

}  // namespace

bool IsValidOption(const char* name) {
  for (const char* candidate : kValidOptions) {
    if (std::strcmp(name, candidate) == 0) {
      return true;
    }
  }
  return false;
}

std::string ValidOptionsText() {
  std::string result;
  for (const char* candidate : kValidOptions) {
    if (!result.empty()) {
      result += ", ";
    }
    result += candidate;
  }
  return result;
}

Status ValidateCatalogType(const char* value) {
  if (std::strcmp(value, "sql") == 0 || std::strcmp(value, "sqlite") == 0) {
    return Ok();
  }

  if (std::strcmp(value, "rest") == 0) {
#ifdef PGICEBERG_ENABLE_REST_CATALOG
    return Ok();
#else
    return std::unexpected(MakeError(
        ERRCODE_FEATURE_NOT_SUPPORTED, "pgiceberg REST catalog support is not enabled",
        "Rebuild pgiceberg with -DPGICEBERG_ENABLE_REST_CATALOG=ON to use "
        "catalog_type 'rest'."));
#endif
  }

  return std::unexpected(MakeError(
      ERRCODE_FDW_INVALID_ATTRIBUTE_VALUE,
      std::string("invalid pgiceberg catalog_type \"") + value + "\"",
      std::string("Valid catalog_type values are: ") + kValidCatalogTypes + "."));
}

Result<int64_t> ParseSnapshotIdOption(const char* value) {
  if (value == nullptr || value[0] == '\0') {
    return std::unexpected(MakeError(
        ERRCODE_FDW_INVALID_ATTRIBUTE_VALUE, "pgiceberg option \"snapshot_id\" is empty",
        "Provide a signed 64-bit Iceberg snapshot id."));
  }

  int64_t snapshot_id = 0;
  const char* begin = value;
  const char* end = value + std::strlen(value);
  auto [ptr, ec] = std::from_chars(begin, end, snapshot_id);
  if (ec != std::errc{} || ptr != end) {
    return std::unexpected(MakeError(
        ERRCODE_FDW_INVALID_ATTRIBUTE_VALUE,
        std::string("invalid pgiceberg snapshot_id \"") + value + "\"",
        "snapshot_id must be a signed 64-bit integer."));
  }
  return snapshot_id;
}

Status ValidateSnapshotIdOption(const char* value) {
  PGICEBERG_ASSIGN_OR_RETURN(auto ignored, ParseSnapshotIdOption(value));
  (void)ignored;
  return Ok();
}

Status EnsureWritableOptions(const Options& options) {
  if (options.snapshot_id.has_value()) {
    return std::unexpected(MakeError(
        ERRCODE_FEATURE_NOT_SUPPORTED,
        "pgiceberg DML is not supported when foreign table option "
        "\"snapshot_id\" is set",
        "Drop snapshot_id to read the current table snapshot before INSERT, "
        "UPDATE, or DELETE."));
  }
  return Ok();
}

Status ApplyOption(Options& options, DefElem* def) {
  const char* value = defGetString(def);
  if (std::strcmp(def->defname, "catalog") == 0) {
    options.catalog = value;
  } else if (std::strcmp(def->defname, "catalog_type") == 0) {
    options.catalog_type = value;
  } else if (std::strcmp(def->defname, "catalog_uri") == 0) {
    options.catalog_uri = value;
  } else if (std::strcmp(def->defname, "warehouse") == 0) {
    options.warehouse = value;
  } else if (std::strcmp(def->defname, "catalog_name") == 0) {
    options.catalog_name = value;
  } else if (std::strcmp(def->defname, "namespace") == 0) {
    options.name_space = value;
  } else if (std::strcmp(def->defname, "table") == 0) {
    options.table = value;
  } else if (std::strcmp(def->defname, "snapshot_id") == 0) {
    PGICEBERG_ASSIGN_OR_RETURN(options.snapshot_id, ParseSnapshotIdOption(value));
  }
  return Ok();
}

Status ApplyOptions(Options& options, List* option_list) {
  ListCell* cell = nullptr;
  foreach (cell, option_list) {
    auto* def = static_cast<DefElem*>(lfirst(cell));
    PGICEBERG_RETURN_NOT_OK(ApplyOption(options, def));
  }
  return Ok();
}

Result<Options> OptionsForForeignTable(unsigned int foreigntableid,
                                        const char* relation_name) {
  Options options;
  ForeignTable* table = GetForeignTable(foreigntableid);
  ForeignServer* server = GetForeignServer(table->serverid);
  PGICEBERG_RETURN_NOT_OK(ApplyOptions(options, server->options));
  PGICEBERG_RETURN_NOT_OK(ApplyOptions(options, table->options));
  if (options.table.empty()) {
    options.table = relation_name;
  }
  return options;
}

pgiceberg::Result<pgiceberg::CatalogOptions> ToCatalogOptions(const Options& options) {
  pgiceberg::CatalogOptions catalog_options;
  if (!options.catalog.empty()) {
    PGICEBERG_ASSIGN_OR_RETURN(catalog_options,
                               pgiceberg::LoadCatalogOptions(options.catalog));
  } else {
    catalog_options.catalog_type = options.catalog_type;
    catalog_options.catalog_uri = options.catalog_uri;
    catalog_options.warehouse = options.warehouse;
    catalog_options.catalog_name = options.catalog_name;
  }
  catalog_options.name_space = options.name_space;
  catalog_options.table = options.table;
  return catalog_options;
}

}  // namespace pgiceberg::fdw
