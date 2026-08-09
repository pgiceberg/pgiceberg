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

#include "common/catalog.h"

#include <filesystem>
#include <memory>
#include <optional>
#include <sstream>
#include <string>
#include <utility>
#include <vector>

#include <iceberg/arrow/arrow_io_internal.h>
#include <iceberg/catalog.h>
#include <iceberg/catalog/sql/sql_catalog.h>
#include <iceberg/manifest/manifest_entry.h>
#include <iceberg/manifest/manifest_list.h>
#include <iceberg/manifest/manifest_reader.h>
#include <iceberg/partition_spec.h>
#include <iceberg/schema.h>
#include <iceberg/snapshot.h>
#include <iceberg/sort_order.h>
#include <iceberg/table.h>
#include <iceberg/table_identifier.h>
#include <iceberg/table_properties.h>
#include <iceberg/transaction.h>
#include <iceberg/update/update_properties.h>

#ifdef PGICEBERG_ENABLE_REST_CATALOG
#include <unordered_map>

#include <iceberg/catalog/rest/catalog_properties.h>
#include <iceberg/catalog/rest/rest_catalog.h>
#endif

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
                "pgiceberg currently supports only catalog_type 'sql', 'sqlite', or "
                "'rest' for Iceberg table access"));
}

#ifdef PGICEBERG_ENABLE_REST_CATALOG
Result<std::shared_ptr<iceberg::Catalog>> CreateRestCatalog(const CatalogOptions& options) {
  if (options.catalog_uri.empty()) {
    return std::unexpected(
        MakeError(ERRCODE_FDW_INVALID_OPTION_NAME,
                  "pgiceberg option \"catalog_uri\" is required for REST catalog access"));
  }
  if (options.warehouse.empty()) {
    return std::unexpected(
        MakeError(ERRCODE_FDW_INVALID_OPTION_NAME,
                  "pgiceberg option \"warehouse\" is required for REST catalog access"));
  }

  // iceberg-cpp resolves the catalog FileIO from the warehouse location when no
  // explicit io-impl is configured, so a local warehouse path selects the Arrow
  // local FileIO that pgiceberg registers at extension load.
  std::unordered_map<std::string, std::string> properties{
      {std::string(iceberg::rest::RestCatalogProperties::kUri.key()), options.catalog_uri},
      {std::string(iceberg::rest::RestCatalogProperties::kName.key()),
       options.catalog_name},
      {std::string(iceberg::rest::RestCatalogProperties::kWarehouse.key()),
       options.warehouse},
  };
  auto config = iceberg::rest::RestCatalogProperties::FromMap(std::move(properties));
  PGICEBERG_ASSIGN_OR_RETURN(
      auto session_catalog,
      FromIcebergResult(iceberg::rest::RestCatalog::Make(config), "create REST catalog"));
  return FromIcebergResult(session_catalog->AsCatalog(),
                           "open default REST catalog session");
}
#endif

Result<std::shared_ptr<iceberg::Catalog>> CreateCatalog(const CatalogOptions& options,
                                                        bool require_warehouse) {
  if (options.catalog_type == "rest") {
#ifdef PGICEBERG_ENABLE_REST_CATALOG
    return CreateRestCatalog(options);
#else
    return std::unexpected(MakeError(
        ERRCODE_FEATURE_NOT_SUPPORTED, "pgiceberg REST catalog support is not enabled",
        "Rebuild pgiceberg with -DPGICEBERG_ENABLE_REST_CATALOG=ON to use "
        "catalog_type 'rest'."));
#endif
  }
  PGICEBERG_ASSIGN_OR_RETURN(auto sql_catalog,
                             CreateSqlCatalog(options, require_warehouse));
  return std::shared_ptr<iceberg::Catalog>(std::move(sql_catalog));
}

Status EnsureNamespaceExists(std::shared_ptr<iceberg::Catalog>& catalog,
                             const iceberg::Namespace& ns) {
  PGICEBERG_ASSIGN_OR_RETURN(
      auto exists, FromIcebergResult(catalog->NamespaceExists(ns), "check namespace"));
  if (exists) {
    return Ok();
  }
  return FromIcebergStatus(catalog->CreateNamespace(ns, {}), "create namespace");
}

Status EnsureTableName(const CatalogOptions& options) {
  if (options.table.empty()) {
    return std::unexpected(
        MakeError(ERRCODE_INVALID_PARAMETER_VALUE, "pgiceberg table name is required"));
  }
  return Ok();
}

}  // namespace

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

Status ValidateFormatVersion(int format_version) {
  if (format_version == 2 || format_version == 3) {
    return Ok();
  }
  return std::unexpected(
      MakeError(ERRCODE_INVALID_PARAMETER_VALUE,
                "unsupported Iceberg format version " + std::to_string(format_version),
                "Iceberg format version 1 is not supported; valid values are 2 and 3."));
}

Result<CatalogOptions> LoadCatalogOptions(const std::string& name) {
  const int connect_rc = SPI_connect();
  const bool started_here = (connect_rc == SPI_OK_CONNECT);
  if (!started_here && connect_rc != SPI_ERROR_CONNECT) {
    return std::unexpected(
        MakeError(ERRCODE_INTERNAL_ERROR, "could not connect to PostgreSQL SPI"));
  }

  auto finish_if_started = [started_here]() {
    if (started_here) {
      SPI_finish();
    }
  };

  const char* command =
      "SELECT catalog_type, catalog_uri, warehouse, iceberg_catalog_name "
      "FROM pgiceberg.catalogs "
      "WHERE name = $1";
  Oid argtypes[] = {TEXTOID};
  Datum values[] = {CStringGetTextDatum(name.c_str())};
  const int result =
      SPI_execute_with_args(command, 1, argtypes, values, nullptr, true, 1);
  if (result != SPI_OK_SELECT) {
    finish_if_started();
    return std::unexpected(
        MakeError(ERRCODE_INTERNAL_ERROR, "could not read pgiceberg catalog registry"));
  }

  if (SPI_processed == 0) {
    finish_if_started();
    return std::unexpected(MakeError(
        ERRCODE_UNDEFINED_OBJECT, "pgiceberg catalog \"" + name + "\" is not registered",
        "Register it with pgiceberg.add_catalog(...)."));
  }

  HeapTuple tuple = SPI_tuptable->vals[0];
  TupleDesc tuple_desc = SPI_tuptable->tupdesc;
  auto text_column = [&](int column_number) -> Result<std::string> {
    bool is_null = false;
    Datum value = SPI_getbinval(tuple, tuple_desc, column_number, &is_null);
    if (is_null) {
      return std::unexpected(MakeError(ERRCODE_NULL_VALUE_NOT_ALLOWED,
                                       "pgiceberg catalog registry contains NULL"));
    }
    return std::string(TextDatumGetCString(value));
  };

  CatalogOptions options;
  auto catalog_type = text_column(1);
  if (!catalog_type) {
    finish_if_started();
    return std::unexpected(catalog_type.error());
  }
  options.catalog_type = std::move(catalog_type).value();

  auto catalog_uri = text_column(2);
  if (!catalog_uri) {
    finish_if_started();
    return std::unexpected(catalog_uri.error());
  }
  options.catalog_uri = std::move(catalog_uri).value();

  auto warehouse = text_column(3);
  if (!warehouse) {
    finish_if_started();
    return std::unexpected(warehouse.error());
  }
  options.warehouse = std::move(warehouse).value();

  auto catalog_name = text_column(4);
  if (!catalog_name) {
    finish_if_started();
    return std::unexpected(catalog_name.error());
  }
  options.catalog_name = std::move(catalog_name).value();

  finish_if_started();
  return options;
}

Result<std::shared_ptr<iceberg::Table>> LoadIcebergTable(const CatalogOptions& options,
                                                         const char* relation_name) {
  PGICEBERG_ASSIGN_OR_RETURN(auto catalog, CreateCatalog(options, true));
  return FromIcebergResult(catalog->LoadTable(TableIdentifierFor(options, relation_name)),
                           "load Iceberg table");
}

Result<std::string> LoadIcebergTableMetadataFileLocation(const CatalogOptions& options,
                                                         const char* relation_name) {
  PGICEBERG_ASSIGN_OR_RETURN(auto table, LoadIcebergTable(options, relation_name));
  return std::string(table->metadata_file_location());
}

Result<TableFilesSummary> LoadIcebergTableFilesSummary(
    const CatalogOptions& options, const char* relation_name,
    std::optional<int64_t> snapshot_id) {
  PGICEBERG_ASSIGN_OR_RETURN(auto table, LoadIcebergTable(options, relation_name));
  TableFilesSummary summary{.snapshot_count =
                                static_cast<int64_t>(table->snapshots().size())};
  if (table->snapshots().empty()) {
    return summary;
  }

  PGICEBERG_ASSIGN_OR_RETURN(
      auto current_snapshot,
      FromIcebergResult(table->current_snapshot(), "load current Iceberg snapshot"));
  summary.has_current_snapshot = true;
  summary.current_snapshot_id = current_snapshot->snapshot_id;

  auto snapshot = current_snapshot;
  if (snapshot_id.has_value()) {
    PGICEBERG_ASSIGN_OR_RETURN(snapshot,
                               FromIcebergResult(table->SnapshotById(*snapshot_id),
                                                 "load requested Iceberg snapshot"));
  }
  summary.has_snapshot = true;
  summary.snapshot_id = snapshot->snapshot_id;
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
  PGICEBERG_ASSIGN_OR_RETURN(auto catalog, CreateCatalog(options, false));
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

Result<std::shared_ptr<iceberg::Table>> CreateUnpartitionedIcebergTable(
    const CatalogOptions& options, const std::shared_ptr<iceberg::Schema>& schema,
    int format_version, bool drop_if_exists) {
  PGICEBERG_RETURN_NOT_OK(ValidateFormatVersion(format_version));
  PGICEBERG_RETURN_NOT_OK(EnsureTableName(options));
  PGICEBERG_ASSIGN_OR_RETURN(auto catalog, CreateCatalog(options, true));
  const auto ident = TableIdentifierFor(options, options.table.c_str());
  PGICEBERG_RETURN_NOT_OK(EnsureNamespaceExists(catalog, ident.ns));

  PGICEBERG_ASSIGN_OR_RETURN(
      auto exists, FromIcebergResult(catalog->TableExists(ident), "check table"));
  if (exists) {
    if (!drop_if_exists) {
      return std::unexpected(MakeError(ERRCODE_DUPLICATE_TABLE,
                                       "Iceberg table \"" + options.name_space + "." +
                                           options.table + "\" already exists"));
    }
    PGICEBERG_RETURN_NOT_OK(
        FromIcebergStatus(catalog->DropTable(ident, false), "drop table"));
  }

  const auto table_location =
      std::filesystem::path(options.warehouse) / options.name_space / options.table;
  std::filesystem::create_directories(table_location / "metadata");
  PGICEBERG_ASSIGN_OR_RETURN(
      auto staged_table,
      FromIcebergResult(catalog->StageCreateTable(
                            ident, schema, iceberg::PartitionSpec::Unpartitioned(),
                            iceberg::SortOrder::Unsorted(), table_location.string(),
                            {{"write.parquet.compression-codec", "uncompressed"}}),
                        "stage create table"));
  PGICEBERG_ASSIGN_OR_RETURN(auto properties_update,
                             FromIcebergResult(staged_table->NewUpdateProperties(),
                                               "create table properties update"));
  PGICEBERG_RETURN_NOT_OK(
      FromIcebergStatus(properties_update
                            ->Set(iceberg::TableProperties::kFormatVersion.key(),
                                  std::to_string(format_version))
                            .Commit(),
                        "set table format version"));
  return FromIcebergResult(staged_table->Commit(), "create table");
}

Status DropIcebergTable(const CatalogOptions& options) {
  PGICEBERG_RETURN_NOT_OK(EnsureTableName(options));
  PGICEBERG_ASSIGN_OR_RETURN(auto catalog, CreateCatalog(options, true));
  const auto ident = TableIdentifierFor(options, options.table.c_str());
  PGICEBERG_ASSIGN_OR_RETURN(
      auto exists, FromIcebergResult(catalog->TableExists(ident), "check table"));
  if (!exists) {
    return Ok();
  }
  return FromIcebergStatus(catalog->DropTable(ident, false), "drop table");
}

}  // namespace pgiceberg
