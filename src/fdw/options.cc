#include "fdw/options.h"

#include <array>
#include <cstring>

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
    "catalog_type", "catalog_uri", "warehouse",   "namespace",
    "table",        "snapshot_id", "config_file", "catalog_name"};

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

void ValidateCatalogType(const char* value) {
  if (std::strcmp(value, "sql") == 0 || std::strcmp(value, "sqlite") == 0) {
    return;
  }

  if (std::strcmp(value, "rest") == 0) {
#ifdef PGICEBERG_ENABLE_REST_CATALOG
    return;
#else
    ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                    errmsg("pgiceberg REST catalog support is not enabled"),
                    errhint("Rebuild pgiceberg with -DPGICEBERG_ENABLE_REST_CATALOG=ON "
                            "to use catalog_type 'rest'.")));
#endif
  }

  ereport(ERROR, (errcode(ERRCODE_FDW_INVALID_ATTRIBUTE_VALUE),
                  errmsg("invalid pgiceberg catalog_type \"%s\"", value),
                  errhint("Valid catalog_type values are: %s.", kValidCatalogTypes)));
}

void ApplyOption(Options& options, DefElem* def) {
  const char* value = defGetString(def);
  if (std::strcmp(def->defname, "catalog_type") == 0) {
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
  }
}

void ApplyOptions(Options& options, List* option_list) {
  ListCell* cell = nullptr;
  foreach (cell, option_list) {
    auto* def = static_cast<DefElem*>(lfirst(cell));
    ApplyOption(options, def);
  }
}

Options OptionsForForeignTable(unsigned int foreigntableid, const char* relation_name) {
  Options options;
  ForeignTable* table = GetForeignTable(foreigntableid);
  ForeignServer* server = GetForeignServer(table->serverid);
  ApplyOptions(options, server->options);
  ApplyOptions(options, table->options);
  if (options.table.empty()) {
    options.table = relation_name;
  }
  return options;
}

pgiceberg::CatalogOptions ToCatalogOptions(const Options& options) {
  return pgiceberg::CatalogOptions{
      .catalog_type = options.catalog_type,
      .catalog_uri = options.catalog_uri,
      .warehouse = options.warehouse,
      .catalog_name = options.catalog_name,
      .name_space = options.name_space,
      .table = options.table,
  };
}

}  // namespace pgiceberg::fdw
