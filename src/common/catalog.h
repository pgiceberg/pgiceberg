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

#pragma once

#include <memory>
#include <optional>
#include <string>
#include <vector>

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
  bool has_snapshot = false;
  int64_t snapshot_id = 0;
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

std::vector<std::string> SplitNamespace(const std::string& name_space);
Status ValidateFormatVersion(int format_version);

Result<std::shared_ptr<iceberg::Table>> LoadIcebergTable(const CatalogOptions& options,
                                                         const char* relation_name);
Result<std::string> LoadIcebergTableMetadataFileLocation(const CatalogOptions& options,
                                                         const char* relation_name);
Result<TableFilesSummary> LoadIcebergTableFilesSummary(
    const CatalogOptions& options, const char* relation_name,
    std::optional<int64_t> snapshot_id);
Result<CatalogOptions> LoadCatalogOptions(const std::string& name);
Result<std::shared_ptr<iceberg::Table>> RegisterIcebergTable(
    const CatalogOptions& options, const char* relation_name,
    const std::string& metadata_file_location, bool drop_if_exists);
Result<std::shared_ptr<iceberg::Table>> CreateUnpartitionedIcebergTable(
    const CatalogOptions& options, std::shared_ptr<iceberg::Schema> schema,
    int format_version, bool drop_if_exists);
Status DropIcebergTable(const CatalogOptions& options);

}  // namespace pgiceberg
