#pragma once

#include <memory>
#include <string>

#include <iceberg/type_fwd.h>

#include "common/status.h"

extern "C" {
#include "postgres.h"
}

namespace pgiceberg {

Result<std::shared_ptr<iceberg::Type>> PostgresTypeToIcebergType(Oid pg_type,
                                                                 int32 typmod = -1);

std::string IcebergTypeToSql(const iceberg::Type& type);

}  // namespace pgiceberg
