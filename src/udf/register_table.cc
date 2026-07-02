#include "common/catalog.h"
#include "common/pg_error.h"

#include <string>

extern "C" {
#include "utils/builtins.h"
}

namespace {

std::string TextArg(FunctionCallInfo fcinfo, int argno) {
  return text_to_cstring(PG_GETARG_TEXT_PP(argno));
}

}  // namespace

extern "C" {
PG_FUNCTION_INFO_V1(pgiceberg_register_table);
}

extern "C" Datum pgiceberg_register_table(PG_FUNCTION_ARGS) {
  return pgiceberg::PgResultGuard([&]() -> pgiceberg::Result<Datum> {
    pgiceberg::CatalogOptions options{
        .catalog_type = TextArg(fcinfo, 0),
        .catalog_uri = TextArg(fcinfo, 1),
        .catalog_name = TextArg(fcinfo, 6),
        .name_space = TextArg(fcinfo, 2),
        .table = TextArg(fcinfo, 3),
    };
    const std::string metadata_file_location = TextArg(fcinfo, 4);
    const bool drop_if_exists = PG_GETARG_BOOL(5);

    PGICEBERG_ASSIGN_OR_RETURN(auto table, pgiceberg::RegisterIcebergTable(
                                               options, options.table.c_str(),
                                               metadata_file_location, drop_if_exists));
    (void)table;

    return static_cast<Datum>(0);
  });
}
