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

#include <string_view>

#include "common/catalog.h"

struct List;

namespace pgiceberg {

// Keep connection properties out of foreign table options. Only USER MAPPING
// may contain secrets, including identities that are part of a credential pair.
bool IsCatalogProperty(std::string_view name, bool secret);
Status ValidateCatalogProperty(std::string_view name, std::string_view value);
Status ApplyCatalogProperties(CatalogOptions& options, List* properties, bool secret);
Result<CatalogOptions> ResolveCatalogCredentials(const CatalogOptions& options);
Status ValidateCatalogUri(std::string_view uri);
bool IsLocalLocation(std::string_view location);
Result<std::string> FileIOImplementation(const CatalogOptions& options);
std::string JoinLocation(std::string_view base, std::string_view child);
Status CreateLocalDirectory(std::string_view location);

}  // namespace pgiceberg
