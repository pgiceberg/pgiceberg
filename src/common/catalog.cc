#include "common/catalog.h"

#include <memory>
#include <sstream>
#include <string>
#include <vector>

#include <iceberg/arrow/arrow_io_internal.h>
#include <iceberg/catalog/sql/sql_catalog.h>
#include <iceberg/manifest/manifest_entry.h>
#include <iceberg/manifest/manifest_list.h>
#include <iceberg/manifest/manifest_reader.h>
#include <iceberg/snapshot.h>
#include <iceberg/table.h>
#include <iceberg/table_identifier.h>

#include "common/status.h"

extern "C" {
#include "postgres.h"
#include "catalog/pg_type_d.h"
#include "executor/spi.h"
#include "utils/elog.h"
#include "utils/errcodes.h"
#include "utils/builtins.h"
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

Result<CatalogOptions> LoadCatalogOptions(const std::string& catalog_name) {
  if (SPI_connect() != SPI_OK_CONNECT) {
    return std::unexpected(
        MakeError(ERRCODE_INTERNAL_ERROR, "could not connect to PostgreSQL SPI"));
  }

  const char* command =
      "SELECT catalog_type, catalog_uri, warehouse, name "
      "FROM pgiceberg.catalogs "
      "WHERE name = $1";
  Oid argtypes[] = {TEXTOID};
  Datum values[] = {CStringGetTextDatum(catalog_name.c_str())};
  const int result =
      SPI_execute_with_args(command, 1, argtypes, values, nullptr, true, 1);
  if (result != SPI_OK_SELECT) {
    SPI_finish();
    return std::unexpected(
        MakeError(ERRCODE_INTERNAL_ERROR, "could not read pgiceberg catalog registry"));
  }

  if (SPI_processed == 0) {
    SPI_finish();
    return std::unexpected(
        MakeError(ERRCODE_UNDEFINED_OBJECT,
                  "pgiceberg catalog \"" + catalog_name + "\" is not registered",
                  "Register it with pgiceberg.add_catalog(...)."));
  }

  HeapTuple tuple = SPI_tuptable->vals[0];
  TupleDesc tuple_desc = SPI_tuptable->tupdesc;
  auto text_column = [&](int column_number) -> Result<std::string> {
    bool is_null = false;
    Datum value = SPI_getbinval(tuple, tuple_desc, column_number, &is_null);
    if (is_null) {
      SPI_finish();
      return std::unexpected(MakeError(ERRCODE_NULL_VALUE_NOT_ALLOWED,
                                       "pgiceberg catalog registry contains NULL"));
    }
    return std::string(TextDatumGetCString(value));
  };

  CatalogOptions options;
  PGICEBERG_ASSIGN_OR_RETURN(options.catalog_type, text_column(1));
  PGICEBERG_ASSIGN_OR_RETURN(options.catalog_uri, text_column(2));
  PGICEBERG_ASSIGN_OR_RETURN(options.warehouse, text_column(3));
  PGICEBERG_ASSIGN_OR_RETURN(options.catalog_name, text_column(4));

  SPI_finish();
  return options;
}

Result<std::shared_ptr<iceberg::Table>> LoadIcebergTable(const CatalogOptions& options,
                                                         const char* relation_name) {
  PGICEBERG_ASSIGN_OR_RETURN(auto catalog, CreateSqlCatalog(options, true));
  return FromIcebergResult(catalog->LoadTable(TableIdentifierFor(options, relation_name)),
                           "load Iceberg table");
}

Result<std::string> LoadIcebergTableMetadataFileLocation(const CatalogOptions& options,
                                                         const char* relation_name) {
  PGICEBERG_ASSIGN_OR_RETURN(auto table, LoadIcebergTable(options, relation_name));
  return std::string(table->metadata_file_location());
}

Result<TableFilesSummary> LoadIcebergTableFilesSummary(const CatalogOptions& options,
                                                       const char* relation_name) {
  PGICEBERG_ASSIGN_OR_RETURN(auto table, LoadIcebergTable(options, relation_name));
  TableFilesSummary summary{.snapshot_count =
                                static_cast<int64_t>(table->snapshots().size())};
  if (table->snapshots().empty()) {
    return summary;
  }

  PGICEBERG_ASSIGN_OR_RETURN(
      auto snapshot,
      FromIcebergResult(table->current_snapshot(), "load current Iceberg snapshot"));
  summary.has_current_snapshot = true;
  summary.current_snapshot_id = snapshot->snapshot_id;
  if (snapshot->manifest_list.empty()) {
    return summary;
  }

  PGICEBERG_ASSIGN_OR_RETURN(auto manifest_list_reader,
                             FromIcebergResult(iceberg::ManifestListReader::Make(
                                                   snapshot->manifest_list, table->io()),
                                               "open Iceberg manifest list"));
  PGICEBERG_ASSIGN_OR_RETURN(
      auto manifests,
      FromIcebergResult(manifest_list_reader->Files(), "read Iceberg manifest list"));
  PGICEBERG_ASSIGN_OR_RETURN(auto schema,
                             FromIcebergResult(table->schema(), "load table schema"));
  PGICEBERG_ASSIGN_OR_RETURN(auto specs_by_id,
                             FromIcebergResult(table->specs(), "load partition specs"));

  summary.manifest_count = static_cast<int64_t>(manifests.size());
  for (const auto& manifest : manifests) {
    if (manifest.content == iceberg::ManifestContent::kData) {
      summary.data_manifest_count++;
    } else {
      summary.delete_manifest_count++;
    }

    PGICEBERG_ASSIGN_OR_RETURN(
        auto manifest_reader,
        FromIcebergResult(iceberg::ManifestReader::Make(manifest, table->io(), schema,
                                                        specs_by_id.get()),
                          "open Iceberg manifest"));
    PGICEBERG_ASSIGN_OR_RETURN(
        auto entries,
        FromIcebergResult(manifest_reader->LiveEntries(), "read Iceberg manifest"));

    for (const auto& entry : entries) {
      if (entry.data_file == nullptr) {
        continue;
      }
      const auto& file = *entry.data_file;
      switch (file.content) {
        case iceberg::DataFile::Content::kData:
          summary.data_file_count++;
          summary.data_file_size_in_bytes += file.file_size_in_bytes;
          break;
        case iceberg::DataFile::Content::kPositionDeletes:
          summary.delete_file_count++;
          summary.position_delete_file_count++;
          summary.delete_file_size_in_bytes += file.file_size_in_bytes;
          if (file.referenced_data_file.has_value() && file.content_offset.has_value() &&
              file.content_size_in_bytes.has_value()) {
            summary.deletion_vector_file_count++;
          }
          break;
        case iceberg::DataFile::Content::kEqualityDeletes:
          summary.delete_file_count++;
          summary.equality_delete_file_count++;
          summary.delete_file_size_in_bytes += file.file_size_in_bytes;
          break;
      }
    }
  }
  return summary;
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
