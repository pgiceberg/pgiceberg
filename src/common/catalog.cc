#include "common/catalog.h"

#include <memory>
#include <sstream>
#include <string>
#include <vector>

#include <iceberg/arrow/arrow_io_internal.h>
#include <iceberg/catalog/sql/sql_catalog.h>
#include <iceberg/table.h>
#include <iceberg/table_identifier.h>

#include "common/error.h"

extern "C" {
#include "postgres.h"
#include "utils/elog.h"
#include "utils/errcodes.h"
}

namespace pgiceberg {
namespace {

std::vector<std::string> SplitNamespace(const std::string& name_space) {
  std::vector<std::string> levels;
  std::stringstream input(name_space);
  std::string level;
  while (std::getline(input, level, '.')) {
    if (!level.empty()) {
      levels.push_back(level);
    }
  }
  if (levels.empty()) {
    levels.push_back("default");
  }
  return levels;
}

iceberg::TableIdentifier TableIdentifierFor(const CatalogOptions& options,
                                            const char* relation_name) {
  const std::string table_name =
      options.table.empty() ? std::string(relation_name) : options.table;
  return iceberg::TableIdentifier{
      .ns = iceberg::Namespace{.levels = SplitNamespace(options.name_space)},
      .name = table_name};
}

void ValidateSqlCatalogOptions(const CatalogOptions& options, const char* catalog_label) {
  if (options.catalog_uri.empty()) {
    ereport(ERROR, (errcode(ERRCODE_FDW_INVALID_OPTION_NAME),
                    errmsg("pgiceberg option \"catalog_uri\" is required for %s "
                           "catalog scans",
                           catalog_label)));
  }
  if (options.warehouse.empty()) {
    ereport(ERROR, (errcode(ERRCODE_FDW_INVALID_OPTION_NAME),
                    errmsg("pgiceberg option \"warehouse\" is required for %s "
                           "catalog scans",
                           catalog_label)));
  }
}

std::shared_ptr<iceberg::sql::SqlCatalog> CreateSqlCatalog(
    const CatalogOptions& options) {
  std::shared_ptr<iceberg::FileIO> file_io(
      iceberg::arrow::ArrowFileSystemFileIO::MakeLocalFileIO().release());
  iceberg::sql::SqlCatalogConfig config{
      .name = options.catalog_name,
      .uri = options.catalog_uri,
      .warehouse_location = options.warehouse,
      .max_connections = 1,
  };

  if (options.catalog_type == "sqlite") {
    ValidateSqlCatalogOptions(options, "SQLite");
    auto catalog = iceberg::sql::SqlCatalog::MakeSqliteCatalog(config, file_io);
    if (!catalog) {
      pgiceberg::ThrowIcebergError(catalog.error());
    }
    return *catalog;
  }

  if (options.catalog_type == "sql") {
    ValidateSqlCatalogOptions(options, "SQL");
    auto catalog = iceberg::sql::SqlCatalog::MakePostgreSqlCatalog(config, file_io);
    if (!catalog) {
      pgiceberg::ThrowIcebergError(catalog.error());
    }
    return *catalog;
  }

  ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                  errmsg("pgiceberg currently supports only catalog_type 'sql' or "
                         "'sqlite' for Iceberg table access")));
}

}  // namespace

std::shared_ptr<iceberg::Table> LoadIcebergTable(const CatalogOptions& options,
                                                 const char* relation_name) {
  auto catalog = CreateSqlCatalog(options);
  auto table = catalog->LoadTable(TableIdentifierFor(options, relation_name));
  if (!table) {
    pgiceberg::ThrowIcebergError(table.error());
  }
  return *table;
}

}  // namespace pgiceberg
