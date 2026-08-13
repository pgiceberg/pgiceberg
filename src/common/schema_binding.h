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

#include <cstdint>
#include <memory>
#include <optional>
#include <string>
#include <string_view>
#include <vector>

#include <arrow/type_fwd.h>
#include <iceberg/type_fwd.h>

#include "common/status.h"

extern "C" {
#include "postgres.h"
}

struct List;
struct RelationData;
using Relation = RelationData*;
struct FormData_pg_attribute;
using Form_pg_attribute = FormData_pg_attribute*;

namespace iceberg {
class Literal;
class Schema;
class SchemaField;
}  // namespace iceberg

namespace arrow {
class ArrayBuilder;
class Field;
class Schema;
}  // namespace arrow

namespace pgiceberg {

inline constexpr const char* kFieldIdOption = "field_id";

struct LocalColumn {
  AttrNumber attnum = InvalidAttrNumber;
  std::string name;
  Oid pg_type = InvalidOid;
  int32 typmod = -1;
  bool not_null = false;
  std::optional<int32_t> field_id;
};

enum class SchemaChangeKind : std::uint8_t {
  kAdded,
  kDropped,
  kRenamed,
  kTypeChanged,
  kIncompatible,
  kMissingFieldId,
};

struct SchemaChange {
  SchemaChangeKind kind = SchemaChangeKind::kAdded;
  std::optional<std::string> local_column;
  std::optional<std::string> local_type;
  std::optional<int32_t> iceberg_field_id;
  std::optional<std::string> iceberg_name;
  std::optional<std::string> iceberg_type;
  std::string detail;
};

// One Iceberg field bound to an optional local attribute for reads and writes.
struct BoundField {
  int32_t field_id = -1;
  std::string iceberg_name;
  std::shared_ptr<iceberg::Type> iceberg_type;
  bool optional = true;
  AttrNumber attnum = InvalidAttrNumber;
  std::string local_name;
  Oid pg_type = InvalidOid;
  int32 typmod = -1;
  bool renamed = false;
  bool type_changed = false;
  bool had_field_id = false;
  bool readable = true;
  bool writable = true;
  std::string incompatibility;
  std::shared_ptr<const iceberg::Literal> write_default;
  std::shared_ptr<const iceberg::Literal> initial_default;
};

struct SchemaBinding {
  std::vector<BoundField> fields;
  std::vector<LocalColumn> unmatched_local;
};

bool IsValidColumnOption(const char* name);
Result<int32_t> ParseFieldIdOption(const char* value);
Status ValidateFieldIdOption(const char* value);
std::optional<int32_t> FieldIdFromOptionList(List* options);
std::optional<int32_t> ArrowFieldId(const arrow::Field& field);
int ArrowFieldIndexById(const arrow::Schema& schema, int32_t field_id);

Result<std::vector<LocalColumn>> LoadLocalColumns(Relation relation);
Result<SchemaBinding> BindSchema(const std::vector<LocalColumn>& local,
                                 const iceberg::Schema& schema);
Result<std::vector<SchemaChange>> DiffSchema(const SchemaBinding& binding);
Status CheckScanCompatible(const SchemaBinding& binding,
                           const std::vector<int>& projected_attnums);
Status CheckWriteCompatible(const SchemaBinding& binding);

Result<std::string> ProjectedIcebergName(const LocalColumn& column,
                                         const iceberg::Schema& schema);
Result<std::vector<std::string>> ProjectedIcebergNames(
    const std::vector<LocalColumn>& local, const std::vector<int>& attnums,
    const iceberg::Schema& schema);

Result<int> BatchColumnIndex(const arrow::Schema& schema, const LocalColumn& column);
Status AppendLiteral(arrow::ArrayBuilder& builder, const iceberg::Literal& literal,
                     Oid pg_type, const arrow::DataType& arrow_type);

const char* SchemaChangeKindName(SchemaChangeKind kind);
std::string JsonEscape(std::string_view value);
std::string SchemaChangesToJson(const std::vector<SchemaChange>& changes);

iceberg::TypeId CanonicalTypeId(const iceberg::Type& type);
bool IcebergTypeReadableAs(const iceberg::Type& type, Oid pg_type, int32 typmod);
bool PostgresTypeWritableAs(Oid pg_type, int32 typmod, const iceberg::Type& type);

Status InsertColumnBindings(Relation relation);
Result<std::optional<int32_t>> LoadBoundFieldId(Oid relid, AttrNumber attnum);

}  // namespace pgiceberg
