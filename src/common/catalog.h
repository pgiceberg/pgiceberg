#pragma once

#include <memory>
#include <string>

namespace iceberg {
class Table;
}

namespace pgiceberg {

struct CatalogOptions {
  std::string catalog_type = "sql";
  std::string catalog_uri;
  std::string warehouse;
  std::string catalog_name = "pgiceberg";
  std::string name_space = "default";
  std::string table;
};

std::shared_ptr<iceberg::Table> LoadIcebergTable(const CatalogOptions& options,
                                                 const char* relation_name);

}  // namespace pgiceberg
