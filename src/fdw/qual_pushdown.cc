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

// Iceberg expression headers must precede "fdw/qual_pushdown.h": that header
// pulls in postgres.h, whose Min/Max macros clash with Expressions::Min/Max.
#include <iceberg/expression/expression.h>
#include <iceberg/expression/expressions.h>
#include <iceberg/expression/literal.h>
#include <iceberg/schema.h>
#include <iceberg/type.h>
#include <iceberg/util/int128.h>
#include <iceberg/util/uuid.h>

#include "fdw/qual_pushdown.h"

#include <cmath>
#include <cstdint>
#include <cstring>
#include <limits>
#include <optional>
#include <span>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

#include "common/constants.h"

extern "C" {
#include "access/htup_details.h"
#include "catalog/pg_collation_d.h"
#include "catalog/pg_namespace_d.h"
#include "catalog/pg_operator.h"
#include "catalog/pg_type_d.h"
#include "nodes/makefuncs.h"
#include "nodes/nodeFuncs.h"
#include "nodes/primnodes.h"
#include "utils/array.h"
#include "utils/builtins.h"
#include "utils/date.h"
#include "utils/lsyscache.h"
#include "utils/syscache.h"
#include "utils/timestamp.h"
#include "utils/uuid.h"
}

namespace pgiceberg::fdw {
namespace {

constexpr int kMaxIcebergDecimalPrecision = 38;

enum class PgComparison : std::uint8_t {
  kEq,
  kNotEq,
  kLt,
  kLtEq,
  kGt,
  kGtEq,
  kLike,
  kNotLike,
};

// How a literal participates in the pushed predicate.  Equality membership
// (=, IN) tolerates lossy narrowing casts because a rounded literal can only
// match extra rows, never fewer; every other operator needs an exact literal
// or pruning could skip files that contain qualifying rows.
enum class LiteralUse : std::uint8_t {
  kEqualityMember,
  kExact,
};

struct ColumnTarget {
  std::string field_name;
  std::shared_ptr<iceberg::Type> field_type;
};

std::optional<PgComparison> CommuteComparison(PgComparison comparison) {
  switch (comparison) {
    case PgComparison::kEq:
    case PgComparison::kNotEq:
      return comparison;
    case PgComparison::kLt:
      return PgComparison::kGt;
    case PgComparison::kLtEq:
      return PgComparison::kGtEq;
    case PgComparison::kGt:
      return PgComparison::kLt;
    case PgComparison::kGtEq:
      return PgComparison::kLtEq;
    case PgComparison::kLike:
    case PgComparison::kNotLike:
      return std::nullopt;
  }
  return std::nullopt;
}

// Only operators from pg_catalog carry the standard comparison semantics the
// Iceberg translation below assumes.  User-defined operators can reuse the
// same names with arbitrary behavior.
std::optional<PgComparison> ClassifyOperator(Oid operator_oid) {
  HeapTuple tuple = SearchSysCache1(OPEROID, ObjectIdGetDatum(operator_oid));
  if (!HeapTupleIsValid(tuple)) {
    return std::nullopt;
  }
  auto* form = reinterpret_cast<Form_pg_operator>(GETSTRUCT(tuple));
  std::optional<PgComparison> comparison;
  if (form->oprnamespace == PG_CATALOG_NAMESPACE) {
    const std::string_view name(NameStr(form->oprname));
    if (name == "=") {
      comparison = PgComparison::kEq;
    } else if (name == "<>") {
      comparison = PgComparison::kNotEq;
    } else if (name == "<") {
      comparison = PgComparison::kLt;
    } else if (name == "<=") {
      comparison = PgComparison::kLtEq;
    } else if (name == ">") {
      comparison = PgComparison::kGt;
    } else if (name == ">=") {
      comparison = PgComparison::kGtEq;
    } else if (name == "~~") {
      comparison = PgComparison::kLike;
    } else if (name == "!~~") {
      comparison = PgComparison::kNotLike;
    }
  }
  ReleaseSysCache(tuple);
  return comparison;
}

Node* StripRelabel(Node* node) {
  while (node != nullptr && IsA(node, RelabelType)) {
    node = reinterpret_cast<Node*>(castNode(RelabelType, node)->arg);
  }
  return node;
}

const Var* AsScanColumn(Node* node, Index varno) {
  node = StripRelabel(node);
  if (node == nullptr || !IsA(node, Var)) {
    return nullptr;
  }
  const auto* var = castNode(Var, node);
  if (var->varno != varno || var->varlevelsup != 0 || var->varattno <= 0) {
    return nullptr;
  }
  return var;
}

std::optional<ColumnTarget> ResolveColumn(const Var& var, Oid relation_oid,
                                          const iceberg::Schema& schema) {
  char* attribute_name = get_attname(relation_oid, var.varattno, /*missing_ok=*/true);
  if (attribute_name == nullptr) {
    return std::nullopt;
  }
  auto field_result = schema.FindFieldByName(attribute_name, /*case_sensitive=*/true);
  if (!field_result.has_value() || !field_result.value().has_value()) {
    return std::nullopt;
  }
  const iceberg::SchemaField& field = field_result.value()->get();
  if (field.type() == nullptr || !field.type()->is_primitive()) {
    return std::nullopt;
  }
  return ColumnTarget{.field_name = attribute_name, .field_type = field.type()};
}

// String ordering in Iceberg column metrics is byte-wise; only the C collation
// sorts the same way in PostgreSQL.  Equality-style predicates just need the
// collation to treat equal strings as byte-equal (deterministic).
bool CollationAllowsStringPushdown(Oid collation_oid, PgComparison comparison) {
  switch (comparison) {
    case PgComparison::kLt:
    case PgComparison::kLtEq:
    case PgComparison::kGt:
    case PgComparison::kGtEq:
      return collation_oid == C_COLLATION_OID;
    default:
      return OidIsValid(collation_oid) && get_collation_isdeterministic(collation_oid);
  }
}

std::optional<iceberg::Literal> DecimalLiteralFromNumeric(
    Datum value, const iceberg::DecimalType& target) {
  char* text = DatumGetCString(DirectFunctionCall1(numeric_out, value));
  const std::string_view repr(text);
  if (repr == "NaN" || repr.ends_with("Infinity")) {
    return std::nullopt;
  }

  bool negative = false;
  std::size_t position = 0;
  if (position < repr.size() && (repr[position] == '-' || repr[position] == '+')) {
    negative = repr[position] == '-';
    position++;
  }

  int128_t unscaled = 0;
  int digits = 0;
  int fractional_digits = -1;
  bool leading_zero_run = true;
  for (; position < repr.size(); position++) {
    const char c = repr[position];
    if (c == '.') {
      if (fractional_digits >= 0) {
        return std::nullopt;
      }
      fractional_digits = 0;
      continue;
    }
    if (c < '0' || c > '9') {
      return std::nullopt;
    }
    if (leading_zero_run && c == '0' && fractional_digits < 0) {
      continue;
    }
    leading_zero_run = false;
    if (digits >= kMaxIcebergDecimalPrecision) {
      return std::nullopt;
    }
    unscaled = unscaled * 10 + (c - '0');
    digits++;
    if (fractional_digits >= 0) {
      fractional_digits++;
    }
  }
  if (fractional_digits < 0) {
    fractional_digits = 0;
  }

  // Rescale exactly to the column scale; a rounded literal would change the
  // predicate.  Comparisons stay valid even if the value needs more integer
  // digits than the column precision, as long as it fits in 38 digits.
  while (fractional_digits < target.scale()) {
    if (digits >= kMaxIcebergDecimalPrecision) {
      return std::nullopt;
    }
    unscaled *= 10;
    fractional_digits++;
    if (unscaled != 0) {
      digits++;
    }
  }
  while (fractional_digits > target.scale()) {
    if (unscaled % 10 != 0) {
      return std::nullopt;
    }
    unscaled /= 10;
    fractional_digits--;
  }
  if (negative) {
    unscaled = -unscaled;
  }
  return iceberg::Literal::Decimal(unscaled, target.precision(), target.scale());
}

std::optional<iceberg::Literal> TimestampLiteral(Datum value, bool with_timezone,
                                                 bool target_is_nanos) {
  const Timestamp timestamp = DatumGetTimestamp(value);
  if (TIMESTAMP_NOT_FINITE(timestamp)) {
    return std::nullopt;
  }
  const std::int64_t micros = timestamp + kPostgresUnixEpochOffsetMicros;
  if (target_is_nanos && (micros > std::numeric_limits<std::int64_t>::max() / 1000 ||
                          micros < std::numeric_limits<std::int64_t>::min() / 1000)) {
    // Binding would fail converting the literal to nanoseconds.
    return std::nullopt;
  }
  return with_timezone ? iceberg::Literal::TimestampTz(micros)
                       : iceberg::Literal::Timestamp(micros);
}

// Convert one PostgreSQL constant to an Iceberg literal that binds cleanly to
// the target column type.  Combinations outside this whitelist are skipped so
// pushdown can never abort a query or prune incorrectly.
std::optional<iceberg::Literal> LiteralForConst(Datum value, Oid const_type,
                                                const iceberg::Type& target,
                                                LiteralUse use) {
  const iceberg::TypeId target_id = target.type_id();
  switch (const_type) {
    case BOOLOID:
      if (target_id == iceberg::TypeId::kBoolean) {
        return iceberg::Literal::Boolean(DatumGetBool(value));
      }
      return std::nullopt;
    case INT2OID:
    case INT4OID: {
      const std::int32_t number = const_type == INT2OID
                                      ? static_cast<std::int32_t>(DatumGetInt16(value))
                                      : DatumGetInt32(value);
      switch (target_id) {
        case iceberg::TypeId::kInt:
          return iceberg::Literal::Int(number);
        case iceberg::TypeId::kLong:
          return iceberg::Literal::Long(number);
        case iceberg::TypeId::kDouble:
          return iceberg::Literal::Double(number);
        default:
          return std::nullopt;
      }
    }
    case INT8OID: {
      const std::int64_t number = DatumGetInt64(value);
      switch (target_id) {
        case iceberg::TypeId::kLong:
          // Binding also accepts kInt: out-of-range literals fold to
          // always-true/false instead of failing.
        case iceberg::TypeId::kInt:
          return iceberg::Literal::Long(number);
        default:
          return std::nullopt;
      }
    }
    case FLOAT4OID: {
      const float number = DatumGetFloat4(value);
      if (std::isnan(number)) {
        return std::nullopt;
      }
      switch (target_id) {
        case iceberg::TypeId::kFloat:
          return iceberg::Literal::Float(number);
        case iceberg::TypeId::kDouble:
          return iceberg::Literal::Double(number);
        default:
          return std::nullopt;
      }
    }
    case FLOAT8OID: {
      const double number = DatumGetFloat8(value);
      if (std::isnan(number)) {
        return std::nullopt;
      }
      switch (target_id) {
        case iceberg::TypeId::kDouble:
          return iceberg::Literal::Double(number);
        case iceberg::TypeId::kFloat:
          // Narrowing rounds the literal, which is only a safe over-match for
          // equality membership.
          if (use == LiteralUse::kEqualityMember) {
            return iceberg::Literal::Double(number);
          }
          return std::nullopt;
        default:
          return std::nullopt;
      }
    }
    case NUMERICOID:
      if (target_id == iceberg::TypeId::kDecimal) {
        return DecimalLiteralFromNumeric(
            value, static_cast<const iceberg::DecimalType&>(target));
      }
      return std::nullopt;
    case TEXTOID:
    case VARCHAROID:
      if (target_id == iceberg::TypeId::kString) {
        return iceberg::Literal::String(text_to_cstring(DatumGetTextPP(value)));
      }
      return std::nullopt;
    case DATEOID: {
      if (target_id != iceberg::TypeId::kDate) {
        return std::nullopt;
      }
      const DateADT date = DatumGetDateADT(value);
      if (DATE_NOT_FINITE(date)) {
        return std::nullopt;
      }
      const std::int64_t days =
          static_cast<std::int64_t>(date) + kPostgresUnixEpochOffsetDays;
      if (days > std::numeric_limits<std::int32_t>::max() ||
          days < std::numeric_limits<std::int32_t>::min()) {
        return std::nullopt;
      }
      return iceberg::Literal::Date(static_cast<std::int32_t>(days));
    }
    case TIMEOID:
      if (target_id == iceberg::TypeId::kTime) {
        return iceberg::Literal::Time(DatumGetTimeADT(value));
      }
      return std::nullopt;
    case TIMESTAMPOID:
      if (target_id == iceberg::TypeId::kTimestamp ||
          target_id == iceberg::TypeId::kTimestampNs) {
        return TimestampLiteral(value, /*with_timezone=*/false,
                                target_id == iceberg::TypeId::kTimestampNs);
      }
      return std::nullopt;
    case TIMESTAMPTZOID:
      if (target_id == iceberg::TypeId::kTimestampTz ||
          target_id == iceberg::TypeId::kTimestampTzNs) {
        return TimestampLiteral(value, /*with_timezone=*/true,
                                target_id == iceberg::TypeId::kTimestampTzNs);
      }
      return std::nullopt;
    case UUIDOID: {
      if (target_id != iceberg::TypeId::kUuid) {
        return std::nullopt;
      }
      const pg_uuid_t* uuid = DatumGetUUIDP(value);
      auto uuid_result =
          iceberg::Uuid::FromBytes(std::span<const std::uint8_t>(uuid->data, UUID_LEN));
      if (!uuid_result.has_value()) {
        return std::nullopt;
      }
      return iceberg::Literal::UUID(*uuid_result);
    }
    case BYTEAOID: {
      if (target_id != iceberg::TypeId::kBinary) {
        return std::nullopt;
      }
      bytea* bytes = DatumGetByteaPP(value);
      const auto* data = reinterpret_cast<const std::uint8_t*>(VARDATA_ANY(bytes));
      return iceberg::Literal::Binary(
          std::vector<std::uint8_t>(data, data + VARSIZE_ANY_EXHDR(bytes)));
    }
    default:
      return std::nullopt;
  }
}

// Accept only patterns made of plain characters followed by a single trailing
// '%'.  Any other wildcard or escape would need semantics Iceberg's
// starts-with predicate cannot express.
std::optional<std::string> StartsWithPrefix(const std::string& pattern) {
  if (pattern.empty() || pattern.back() != '%') {
    return std::nullopt;
  }
  const std::string_view prefix(pattern.data(), pattern.size() - 1);
  if (prefix.find_first_of("%_\\") != std::string_view::npos) {
    return std::nullopt;
  }
  return std::string(prefix);
}

std::shared_ptr<iceberg::Expression> TranslateComparison(const ColumnTarget& column,
                                                         PgComparison comparison,
                                                         Oid collation_oid,
                                                         const Const& constant) {
  if (column.field_type->type_id() == iceberg::TypeId::kString &&
      !CollationAllowsStringPushdown(collation_oid, comparison)) {
    return nullptr;
  }

  if (comparison == PgComparison::kLike || comparison == PgComparison::kNotLike) {
    if (column.field_type->type_id() != iceberg::TypeId::kString ||
        (constant.consttype != TEXTOID && constant.consttype != VARCHAROID)) {
      return nullptr;
    }
    auto prefix = StartsWithPrefix(text_to_cstring(DatumGetTextPP(constant.constvalue)));
    if (!prefix.has_value()) {
      return nullptr;
    }
    if (comparison == PgComparison::kLike) {
      return iceberg::Expressions::StartsWith(column.field_name, std::move(*prefix));
    }
    return iceberg::Expressions::NotStartsWith(column.field_name, std::move(*prefix));
  }

  const LiteralUse use =
      comparison == PgComparison::kEq ? LiteralUse::kEqualityMember : LiteralUse::kExact;
  auto literal =
      LiteralForConst(constant.constvalue, constant.consttype, *column.field_type, use);
  if (!literal.has_value()) {
    return nullptr;
  }
  switch (comparison) {
    case PgComparison::kEq:
      return iceberg::Expressions::Equal(column.field_name, std::move(*literal));
    case PgComparison::kNotEq:
      return iceberg::Expressions::NotEqual(column.field_name, std::move(*literal));
    case PgComparison::kLt:
      return iceberg::Expressions::LessThan(column.field_name, std::move(*literal));
    case PgComparison::kLtEq:
      return iceberg::Expressions::LessThanOrEqual(column.field_name,
                                                   std::move(*literal));
    case PgComparison::kGt:
      return iceberg::Expressions::GreaterThan(column.field_name, std::move(*literal));
    case PgComparison::kGtEq:
      return iceberg::Expressions::GreaterThanOrEqual(column.field_name,
                                                      std::move(*literal));
    default:
      return nullptr;
  }
}

std::shared_ptr<iceberg::Expression> TranslateOpExpr(const OpExpr& op_expr, Index varno,
                                                     Oid relation_oid,
                                                     const iceberg::Schema& schema) {
  if (list_length(op_expr.args) != 2) {
    return nullptr;
  }
  auto comparison = ClassifyOperator(op_expr.opno);
  if (!comparison.has_value()) {
    return nullptr;
  }

  Node* left = static_cast<Node*>(linitial(op_expr.args));
  Node* right = static_cast<Node*>(lsecond(op_expr.args));
  const Var* var = AsScanColumn(left, varno);
  Node* other = right;
  if (var == nullptr) {
    var = AsScanColumn(right, varno);
    other = left;
    if (var == nullptr) {
      return nullptr;
    }
    comparison = CommuteComparison(*comparison);
    if (!comparison.has_value()) {
      return nullptr;
    }
  }

  other = StripRelabel(other);
  if (other == nullptr || !IsA(other, Const)) {
    return nullptr;
  }
  const auto* constant = castNode(Const, other);
  if (constant->constisnull) {
    return nullptr;
  }

  auto column = ResolveColumn(*var, relation_oid, schema);
  if (!column.has_value()) {
    return nullptr;
  }
  return TranslateComparison(*column, *comparison, op_expr.inputcollid, *constant);
}

std::shared_ptr<iceberg::Expression> TranslateScalarArrayOp(
    const ScalarArrayOpExpr& array_op, Index varno, Oid relation_oid,
    const iceberg::Schema& schema) {
  if (list_length(array_op.args) != 2) {
    return nullptr;
  }
  const auto comparison = ClassifyOperator(array_op.opno);
  if (!comparison.has_value()) {
    return nullptr;
  }
  const bool is_in = array_op.useOr && *comparison == PgComparison::kEq;
  const bool is_not_in = !array_op.useOr && *comparison == PgComparison::kNotEq;
  if (!is_in && !is_not_in) {
    return nullptr;
  }

  const Var* var = AsScanColumn(static_cast<Node*>(linitial(array_op.args)), varno);
  Node* array_node = StripRelabel(static_cast<Node*>(lsecond(array_op.args)));
  if (var == nullptr || array_node == nullptr || !IsA(array_node, Const)) {
    return nullptr;
  }
  const auto* array_const = castNode(Const, array_node);
  if (array_const->constisnull) {
    return nullptr;
  }

  auto column = ResolveColumn(*var, relation_oid, schema);
  if (!column.has_value()) {
    return nullptr;
  }
  if (column->field_type->type_id() == iceberg::TypeId::kString &&
      !CollationAllowsStringPushdown(array_op.inputcollid, *comparison)) {
    return nullptr;
  }

  ArrayType* array = DatumGetArrayTypeP(array_const->constvalue);
  const Oid element_type = ARR_ELEMTYPE(array);
  int16 element_length = 0;
  bool element_by_value = false;
  char element_align = 0;
  get_typlenbyvalalign(element_type, &element_length, &element_by_value, &element_align);

  Datum* elements = nullptr;
  bool* nulls = nullptr;
  int element_count = 0;
  deconstruct_array(array, element_type, element_length, element_by_value, element_align,
                    &elements, &nulls, &element_count);

  // NULL elements never satisfy = ANY, and make <> ALL never pass; dropping
  // them keeps the pushed predicate equal or weaker in both cases.
  const LiteralUse use = is_in ? LiteralUse::kEqualityMember : LiteralUse::kExact;
  std::vector<iceberg::Literal> literals;
  literals.reserve(element_count);
  for (int i = 0; i < element_count; i++) {
    if (nulls[i]) {
      continue;
    }
    auto literal = LiteralForConst(elements[i], element_type, *column->field_type, use);
    if (!literal.has_value()) {
      return nullptr;
    }
    literals.push_back(std::move(*literal));
  }
  if (literals.empty()) {
    return nullptr;
  }
  if (is_in) {
    return iceberg::Expressions::In(column->field_name, std::move(literals));
  }
  return iceberg::Expressions::NotIn(column->field_name, std::move(literals));
}

std::shared_ptr<iceberg::Expression> TranslateBooleanColumn(
    const Var* var, bool expected, Oid relation_oid, const iceberg::Schema& schema) {
  if (var == nullptr || var->vartype != BOOLOID) {
    return nullptr;
  }
  auto column = ResolveColumn(*var, relation_oid, schema);
  if (!column.has_value() || column->field_type->type_id() != iceberg::TypeId::kBoolean) {
    return nullptr;
  }
  return iceberg::Expressions::Equal(column->field_name,
                                     iceberg::Literal::Boolean(expected));
}

std::shared_ptr<iceberg::Expression> TranslateNullTest(const NullTest& null_test,
                                                       Index varno, Oid relation_oid,
                                                       const iceberg::Schema& schema) {
  if (null_test.argisrow) {
    return nullptr;
  }
  const Var* var = AsScanColumn(reinterpret_cast<Node*>(null_test.arg), varno);
  if (var == nullptr) {
    return nullptr;
  }
  auto column = ResolveColumn(*var, relation_oid, schema);
  if (!column.has_value()) {
    return nullptr;
  }
  if (null_test.nulltesttype == IS_NULL) {
    return iceberg::Expressions::IsNull(column->field_name);
  }
  return iceberg::Expressions::NotNull(column->field_name);
}

std::shared_ptr<iceberg::Expression> TranslateClause(Node* clause, Index varno,
                                                     Oid relation_oid,
                                                     const iceberg::Schema& schema) {
  if (clause == nullptr) {
    return nullptr;
  }
  switch (nodeTag(clause)) {
    case T_OpExpr:
      return TranslateOpExpr(*castNode(OpExpr, clause), varno, relation_oid, schema);
    case T_ScalarArrayOpExpr:
      return TranslateScalarArrayOp(*castNode(ScalarArrayOpExpr, clause), varno,
                                    relation_oid, schema);
    case T_NullTest:
      return TranslateNullTest(*castNode(NullTest, clause), varno, relation_oid, schema);
    case T_Var:
      return TranslateBooleanColumn(AsScanColumn(clause, varno), true, relation_oid,
                                    schema);
    case T_BoolExpr: {
      const auto* bool_expr = castNode(BoolExpr, clause);
      if (bool_expr->boolop == NOT_EXPR && list_length(bool_expr->args) == 1) {
        return TranslateBooleanColumn(
            AsScanColumn(static_cast<Node*>(linitial(bool_expr->args)), varno), false,
            relation_oid, schema);
      }
      return nullptr;
    }
    case T_BooleanTest: {
      const auto* boolean_test = castNode(BooleanTest, clause);
      // IS NOT TRUE / IS NOT FALSE / IS (NOT) UNKNOWN accept NULL rows, which
      // Iceberg equality predicates never do; pushing them could prune files
      // whose matching rows are all NULL.
      if (boolean_test->booltesttype != IS_TRUE &&
          boolean_test->booltesttype != IS_FALSE) {
        return nullptr;
      }
      return TranslateBooleanColumn(
          AsScanColumn(reinterpret_cast<Node*>(boolean_test->arg), varno),
          boolean_test->booltesttype == IS_TRUE, relation_oid, schema);
    }
    default:
      return nullptr;
  }
}

}  // namespace

std::shared_ptr<iceberg::Expression> TranslateQualsForPushdown(
    List* quals, Index varno, Oid relation_oid, const iceberg::Schema& schema) {
  std::shared_ptr<iceberg::Expression> filter;
  ListCell* cell = nullptr;
  foreach (cell, quals) {
    auto* clause = static_cast<Node*>(lfirst(cell));
    std::shared_ptr<iceberg::Expression> translated;
    try {
      translated = TranslateClause(clause, varno, relation_oid, schema);
    } catch (...) {
      // Expression construction is not expected to throw for the shapes built
      // here; treat any failure as "clause not pushable".
      translated = nullptr;
    }
    if (translated == nullptr) {
      continue;
    }
    try {
      filter = filter == nullptr
                   ? std::move(translated)
                   : iceberg::Expressions::And(std::move(filter), std::move(translated));
    } catch (...) {
      return nullptr;
    }
  }
  return filter;
}

}  // namespace pgiceberg::fdw
