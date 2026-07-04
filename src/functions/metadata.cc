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
#include "common/pg_error.h"

#include <fstream>
#include <optional>
#include <sstream>
#include <string>

extern "C" {
#include "fmgr.h"
#include "postgres.h"
#include "utils/builtins.h"
#include "utils/jsonb.h"
}

namespace {

std::string TextArg(FunctionCallInfo fcinfo, int argno) {
  return text_to_cstring(PG_GETARG_TEXT_PP(argno));
}

pgiceberg::Result<pgiceberg::CatalogOptions> CatalogOptionsArg(FunctionCallInfo fcinfo) {
  PGICEBERG_ASSIGN_OR_RETURN(auto options,
                             pgiceberg::LoadCatalogOptions(TextArg(fcinfo, 0)));
  options.name_space = TextArg(fcinfo, 1);
  options.table = TextArg(fcinfo, 2);
  return options;
}

Datum JsonbDatum(const std::string& json) {
  return DirectFunctionCall1(jsonb_in, CStringGetDatum(json.c_str()));
}

std::string FilesSummaryJson(const pgiceberg::TableFilesSummary& summary) {
  std::ostringstream json;
  json << "{";
  json << "\"snapshot_count\":" << summary.snapshot_count << ",";
  json << "\"snapshot_id\":";
  if (summary.has_snapshot) {
    json << summary.snapshot_id;
  } else {
    json << "null";
  }
  json << ",";
  json << "\"current_snapshot_id\":";
  if (summary.has_current_snapshot) {
    json << summary.current_snapshot_id;
  } else {
    json << "null";
  }
  json << ",";
  json << "\"manifest_count\":" << summary.manifest_count << ",";
  json << "\"data_manifest_count\":" << summary.data_manifest_count << ",";
  json << "\"delete_manifest_count\":" << summary.delete_manifest_count << ",";
  json << "\"data_file_count\":" << summary.data_file_count << ",";
  json << "\"delete_file_count\":" << summary.delete_file_count << ",";
  json << "\"position_delete_file_count\":" << summary.position_delete_file_count << ",";
  json << "\"equality_delete_file_count\":" << summary.equality_delete_file_count << ",";
  json << "\"deletion_vector_file_count\":" << summary.deletion_vector_file_count << ",";
  json << "\"data_file_size_in_bytes\":" << summary.data_file_size_in_bytes << ",";
  json << "\"delete_file_size_in_bytes\":" << summary.delete_file_size_in_bytes;
  json << "}";
  return json.str();
}

pgiceberg::Result<std::string> ReadMetadataFile(
    const std::string& metadata_file_location) {
  std::ifstream input(metadata_file_location, std::ios::binary);
  if (!input.is_open()) {
    return std::unexpected(pgiceberg::MakeError(
        ERRCODE_IO_ERROR,
        "could not open Iceberg metadata file \"" + metadata_file_location + "\""));
  }

  std::ostringstream contents;
  contents << input.rdbuf();
  if (input.bad()) {
    return std::unexpected(pgiceberg::MakeError(
        ERRCODE_IO_ERROR,
        "could not read Iceberg metadata file \"" + metadata_file_location + "\""));
  }
  return contents.str();
}

pgiceberg::Result<std::string> LoadMetadataFileLocation(FunctionCallInfo fcinfo) {
  PGICEBERG_ASSIGN_OR_RETURN(auto options, CatalogOptionsArg(fcinfo));
  return pgiceberg::LoadIcebergTableMetadataFileLocation(options, options.table.c_str());
}

}  // namespace

extern "C" {
PG_FUNCTION_INFO_V1(pgiceberg_metadata_file_json);
PG_FUNCTION_INFO_V1(pgiceberg_table_metadata_file_location);
PG_FUNCTION_INFO_V1(pgiceberg_table_metadata_json);
PG_FUNCTION_INFO_V1(pgiceberg_table_snapshot_files_summary);

Datum pgiceberg_metadata_file_json(PG_FUNCTION_ARGS) {
  return pgiceberg::PgResultGuard([&]() -> pgiceberg::Result<Datum> {
    const std::string metadata_file_location = TextArg(fcinfo, 0);
    PGICEBERG_ASSIGN_OR_RETURN(auto json, ReadMetadataFile(metadata_file_location));
    return JsonbDatum(json);
  });
}

Datum pgiceberg_table_metadata_file_location(PG_FUNCTION_ARGS) {
  return pgiceberg::PgResultGuard([&]() -> pgiceberg::Result<Datum> {
    PGICEBERG_ASSIGN_OR_RETURN(auto metadata_file_location,
                               LoadMetadataFileLocation(fcinfo));
    return CStringGetTextDatum(metadata_file_location.c_str());
  });
}

Datum pgiceberg_table_metadata_json(PG_FUNCTION_ARGS) {
  return pgiceberg::PgResultGuard([&]() -> pgiceberg::Result<Datum> {
    PGICEBERG_ASSIGN_OR_RETURN(auto metadata_file_location,
                               LoadMetadataFileLocation(fcinfo));
    PGICEBERG_ASSIGN_OR_RETURN(auto json, ReadMetadataFile(metadata_file_location));
    return JsonbDatum(json);
  });
}

Datum pgiceberg_table_snapshot_files_summary(PG_FUNCTION_ARGS) {
  return pgiceberg::PgResultGuard([&]() -> pgiceberg::Result<Datum> {
    if (PG_ARGISNULL(0) || PG_ARGISNULL(1) || PG_ARGISNULL(2)) {
      return std::unexpected(pgiceberg::MakeError(
          ERRCODE_NULL_VALUE_NOT_ALLOWED,
          "catalog name, namespace, and table name must not be NULL"));
    }
    PGICEBERG_ASSIGN_OR_RETURN(auto options, CatalogOptionsArg(fcinfo));
    std::optional<int64_t> snapshot_id;
    if (!PG_ARGISNULL(3)) {
      snapshot_id = PG_GETARG_INT64(3);
    }
    PGICEBERG_ASSIGN_OR_RETURN(auto summary,
                               pgiceberg::LoadIcebergTableFilesSummary(
                                   options, options.table.c_str(), snapshot_id));
    return JsonbDatum(FilesSummaryJson(summary));
  });
}

}  // extern "C"
