#pragma once

#include <memory>
#include <string>

#include <iceberg/type_fwd.h>

#include "common/status.h"

namespace pgiceberg {

struct CatalogOptions {
  std::string catalog_type = "sql";
  std::string catalog_uri;
  std::string warehouse;
  std::string catalog_name = "pgiceberg";
  std::string name_space = "default";
  std::string table;
};

struct TableFilesSummary {
  int64_t snapshot_count = 0;
  bool has_current_snapshot = false;
  int64_t current_snapshot_id = 0;
  int64_t manifest_count = 0;
  int64_t data_manifest_count = 0;
  int64_t delete_manifest_count = 0;
  int64_t data_file_count = 0;
  int64_t delete_file_count = 0;
  int64_t position_delete_file_count = 0;
  int64_t equality_delete_file_count = 0;
  int64_t deletion_vector_file_count = 0;
  int64_t data_file_size_in_bytes = 0;
  int64_t delete_file_size_in_bytes = 0;
};

Result<std::shared_ptr<iceberg::Table>> LoadIcebergTable(const CatalogOptions& options,
                                                         const char* relation_name);
Result<std::string> LoadIcebergTableMetadataFileLocation(const CatalogOptions& options,
                                                         const char* relation_name);
Result<TableFilesSummary> LoadIcebergTableFilesSummary(const CatalogOptions& options,
                                                       const char* relation_name);
Result<CatalogOptions> LoadCatalogOptions(const std::string& catalog_name);
Result<std::shared_ptr<iceberg::Table>> RegisterIcebergTable(
    const CatalogOptions& options, const char* relation_name,
    const std::string& metadata_file_location, bool drop_if_exists);

}  // namespace pgiceberg
