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
#include "common/fcinfo.h"
#include "common/pg_error.h"

#include <string>

extern "C" {
PG_FUNCTION_INFO_V1(pgiceberg_register_table);

Datum pgiceberg_register_table(PG_FUNCTION_ARGS) {
  return pgiceberg::PgResultGuard([&]() -> pgiceberg::Result<Datum> {
    PGICEBERG_ASSIGN_OR_RETURN(
        auto options, pgiceberg::LoadCatalogOptions(pgiceberg::TextArg(fcinfo, 0)));
    options.name_space = pgiceberg::TextArg(fcinfo, 1);
    options.table = pgiceberg::TextArg(fcinfo, 2);
    const std::string metadata_file_location = pgiceberg::TextArg(fcinfo, 3);
    const bool drop_if_exists = PG_GETARG_BOOL(4);

    PGICEBERG_ASSIGN_OR_RETURN(auto table, pgiceberg::RegisterIcebergTable(
                                               options, options.table.c_str(),
                                               metadata_file_location, drop_if_exists));
    (void)table;

    return static_cast<Datum>(0);
  });
}

}  // extern "C"
