#pragma once

#include <string>

#include "common/catalog.h"

struct DefElem;
struct List;

namespace pgiceberg::fdw {

struct Options {
  std::string catalog_type = "sql";
  std::string catalog_uri;
  std::string warehouse;
  std::string catalog_name = "pgiceberg";
  std::string name_space = "default";
  std::string table;
};

bool IsValidOption(const char* name);
std::string ValidOptionsText();
void ValidateCatalogType(const char* value);
void ApplyOption(Options& options, DefElem* def);
void ApplyOptions(Options& options, List* option_list);
Options OptionsForForeignTable(unsigned int foreigntableid, const char* relation_name);
pgiceberg::CatalogOptions ToCatalogOptions(const Options& options);

}  // namespace pgiceberg::fdw
