#pragma once

#include <memory>
#include <string>

extern "C" {
#include "postgres.h"
}

namespace iceberg {
class Type;
}

namespace pgiceberg {

std::shared_ptr<iceberg::Type> PostgresTypeToIcebergType(Oid pg_type, int32 typmod = -1);

std::string IcebergTypeToSql(const iceberg::Type& type);

}  // namespace pgiceberg
