#include "common/catalog.h"

#include <memory>
#include <sstream>
#include <string>
#include <vector>

#include <iceberg/arrow/arrow_io_internal.h>
#include <iceberg/catalog/sql/sql_catalog.h>
#include <iceberg/table.h>
#include <iceberg/table_identifier.h>

#include "common/status.h"

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

Status ValidateSqlCatalogOptions(const CatalogOptions& options, const char* catalog_label,
                                 bool require_warehouse) {
  if (options.catalog_uri.empty()) {
    return std::unexpected(
        MakeError(ERRCODE_FDW_INVALID_OPTION_NAME,
                  std::string("pgiceberg option \"catalog_uri\" is required for ") +
                      catalog_label + " catalog access"));
  }
  if (require_warehouse && options.warehouse.empty()) {
    return std::unexpected(
        MakeError(ERRCODE_FDW_INVALID_OPTION_NAME,
                  std::string("pgiceberg option \"warehouse\" is required for ") +
                      catalog_label + " catalog access"));
  }
  return Ok();
}

Result<std::shared_ptr<iceberg::sql::SqlCatalog>> CreateSqlCatalog(
    const CatalogOptions& options, bool require_warehouse) {
  std::shared_ptr<iceberg::FileIO> file_io(
      iceberg::arrow::ArrowFileSystemFileIO::MakeLocalFileIO().release());
  iceberg::sql::SqlCatalogConfig config{
      .name = options.catalog_name,
      .uri = options.catalog_uri,
      .warehouse_location = options.warehouse,
      .max_connections = 1,
  };

  if (options.catalog_type == "sqlite") {
    PGICEBERG_RETURN_NOT_OK(
        ValidateSqlCatalogOptions(options, "SQLite", require_warehouse));
    auto catalog = iceberg::sql::SqlCatalog::MakeSqliteCatalog(config, file_io);
    return FromIcebergResult(std::move(catalog), "create SQLite catalog");
  }

  if (options.catalog_type == "sql") {
    PGICEBERG_RETURN_NOT_OK(ValidateSqlCatalogOptions(options, "SQL", require_warehouse));
    auto catalog = iceberg::sql::SqlCatalog::MakePostgreSqlCatalog(config, file_io);
    return FromIcebergResult(std::move(catalog), "create PostgreSQL catalog");
  }

  return std::unexpected(
      MakeError(ERRCODE_FEATURE_NOT_SUPPORTED,
                "pgiceberg currently supports only catalog_type 'sql' or 'sqlite' for "
                "Iceberg table access"));
}

Status EnsureNamespaceExists(std::shared_ptr<iceberg::sql::SqlCatalog>& catalog,
                             const iceberg::Namespace& ns) {
  PGICEBERG_ASSIGN_OR_RETURN(
      auto exists, FromIcebergResult(catalog->NamespaceExists(ns), "check namespace"));
  if (exists) {
    return Ok();
  }
  return FromIcebergStatus(catalog->CreateNamespace(ns, {}), "create namespace");
}

}  // namespace

Result<std::shared_ptr<iceberg::Table>> LoadIcebergTable(const CatalogOptions& options,
                                                         const char* relation_name) {
  PGICEBERG_ASSIGN_OR_RETURN(auto catalog, CreateSqlCatalog(options, true));
  return FromIcebergResult(catalog->LoadTable(TableIdentifierFor(options, relation_name)),
                           "load Iceberg table");
}

Result<std::shared_ptr<iceberg::Table>> RegisterIcebergTable(
    const CatalogOptions& options, const char* relation_name,
    const std::string& metadata_file_location, bool drop_if_exists) {
  PGICEBERG_ASSIGN_OR_RETURN(auto catalog, CreateSqlCatalog(options, false));
  const auto ident = TableIdentifierFor(options, relation_name);
  PGICEBERG_RETURN_NOT_OK(EnsureNamespaceExists(catalog, ident.ns));

  PGICEBERG_ASSIGN_OR_RETURN(
      auto exists, FromIcebergResult(catalog->TableExists(ident), "check table"));
  if (exists) {
    if (!drop_if_exists) {
      return std::unexpected(
          MakeError(ERRCODE_DUPLICATE_TABLE,
                    "Iceberg table \"" + ident.ToString() + "\" already exists"));
    }
    PGICEBERG_RETURN_NOT_OK(
        FromIcebergStatus(catalog->DropTable(ident, false), "drop table"));
  }

  return FromIcebergResult(catalog->RegisterTable(ident, metadata_file_location),
                           "register Iceberg table");
}

}  // namespace pgiceberg
