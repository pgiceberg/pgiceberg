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

#include "common/catalog.h"
#include "common/status.h"

struct DefElem;
struct List;

namespace pgiceberg::fdw {

struct Options {
  std::string catalog;
  std::string catalog_type = "sql";
  std::string catalog_uri;
  std::string warehouse;
  std::string catalog_name = "pgiceberg";
  std::string name_space = "default";
  std::string table;
};

bool IsValidOption(const char* name);
std::string ValidOptionsText();
Status ValidateCatalogType(const char* value);
void ApplyOption(Options& options, DefElem* def);
void ApplyOptions(Options& options, List* option_list);
Options OptionsForForeignTable(unsigned int foreigntableid, const char* relation_name);
pgiceberg::Result<pgiceberg::CatalogOptions> ToCatalogOptions(const Options& options);

}  // namespace pgiceberg::fdw
