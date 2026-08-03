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

#include <string>
#include <string_view>

#include "common/status.h"

extern "C" {
#include "postgres.h"
#include "nodes/parsenodes.h"
}

struct DefElem;
struct List;

namespace pgiceberg {

struct FileFdwOptions {
  std::string dirname;
  std::string filename;
};

bool EndsWith(std::string_view value, std::string_view suffix);
std::string BasenameWithoutExtension(std::string_view name, std::string_view extension);

void ApplyFileFdwOption(FileFdwOptions& options, DefElem* def);
void ApplyFileFdwOptions(FileFdwOptions& options, List* option_list);
FileFdwOptions FileFdwOptionsForForeignTable(Oid relation_oid);
FileFdwOptions FileFdwOptionsForServer(Oid server_oid);

Status ValidateFileFdwOptions(const FileFdwOptions& options, std::string_view fdw_name);
Status ValidateFileFdwUtilityOptions(Datum raw_options, Oid catalog,
                                     const char* fdw_name);

bool ImportFilterMatches(ImportForeignSchemaStmt* stmt, const std::string& table_name);
std::string DirectoryForImport(Oid server_oid, const char* remote_schema);

}  // namespace pgiceberg
