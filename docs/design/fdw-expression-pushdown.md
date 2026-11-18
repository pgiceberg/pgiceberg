<!--
Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
-->

# FDW expression pushdown

Foreign scans translate supported PostgreSQL `WHERE` clauses into Iceberg
expressions for `TableScanBuilder::Filter`. Iceberg prunes manifests using
partition summaries and files using column bounds/null counts. PostgreSQL
rechecks every clause on rows from surviving files.

## Correctness contract

A pushed filter must be equivalent to or weaker than the PostgreSQL clause.
Keeping an extra file costs I/O; excluding a file with qualifying rows loses
results. Skip translations that cannot satisfy this rule.

- Translate only `pg_catalog` operators; names alone do not establish semantics.
- Use lossless literal conversions, except `float8` to `float` for `=`/`IN`,
  where rounding can over-match.
- Push text ordering only with `C` collation, equality-style text predicates
  only with deterministic collation, and no `bpchar` comparisons because they
  ignore trailing spaces.
- Translate LIKE only for `prefix%` without other `%`, `_`, or escapes.
- Skip boolean tests such as `IS NOT TRUE` that accept NULLs.
- Drop NULL list elements to keep the pushed predicate equivalent or weaker;
  skip NaN constants. Decimal rescaling must be exact.
- Apply the [timestamp precision contract](timestamp-precision.md) when
  PostgreSQL microsecond literals target nanosecond columns.
- Skip pushdown for time-travel scans (`snapshot_id`): their schema may differ
  from the current schema used for translation.

## Supported clauses

| PostgreSQL clause | Iceberg predicate |
| --- | --- |
| `col = k` / `k = col` | `Equal` |
| `col <> k` | `NotEqual` |
| `col < / <= / > / >= k` | Corresponding range predicate; reverse commuted operators. |
| `col IS NULL` / `IS NOT NULL` | `IsNull` / `NotNull` |
| `col = ANY ('{...}')` (`IN`) | `In` |
| `col <> ALL ('{...}')` (`NOT IN`) | `NotIn` |
| `col LIKE 'p%'` / `NOT LIKE 'p%'` | `StartsWith` / `NotStartsWith` |
| `boolcol`, `NOT boolcol`, `IS TRUE`, `IS FALSE` | `Equal(true/false)` |

Combine eligible clauses with AND. Skip other relations, non-`Var` expressions
such as function calls/arithmetic, parameters, and unsupported operators.

## Literal conversions

| PostgreSQL constant | Iceberg column types |
| --- | --- |
| `bool` | `boolean` |
| `int2`, `int4` | `int`, `long`, `double` |
| `int8` | `long`, `int` (out-of-range values fold during binding). |
| `float4` | `float`, `double` |
| `float8` | `double`; `float` for `=`/`IN` only. |
| `numeric` | `decimal(P,S)` with exact rescaling. |
| `text`, `varchar` | `string` |
| `date` | `date`, shifting epoch from 2000 to 1970. |
| `time` | `time` |
| `timestamp` | `timestamp`, `timestamp_ns` |
| `timestamptz` | `timestamptz`, `timestamptz_ns` |
| `uuid` | `uuid` |
| `bytea` | `binary` |

## Code path

[`fdw/qual_pushdown.{h,cc}`](../../src/fdw/qual_pushdown.h) translates a clause
list into an unbound `iceberg::Expression` using the relation's attribute names,
scan varno, and target schema. Translation is best-effort and exception-free.

[`engine/scan_state.cc`](../../src/engine/scan_state.cc) invokes a
`ScanFilterBuilder` after loading the table/schema. `IcebergScanCursor` applies
the result before `PlanFiles`.

[`fdw/fdw.cc`](../../src/fdw/fdw.cc) builds the callback in `BeginForeignScan`.
Planning translates `baserestrictinfo` for `EXPLAIN`, stores its display string
in `fdw_private`, and prints `Iceberg Filter: ...`. `EXPLAIN ANALYZE` also reports
`Iceberg Scan Tasks: N`, the file-task count after pruning.

Full row-level residual filtering in upstream `FileScanTaskReader` is not yet
implemented. Keep PostgreSQL's recheck even if upstream adds it.

## Verification

[`test/regress/sql/fdw_pushdown.sql`](../../test/regress/sql/fdw_pushdown.sql)
checks supported operators/types and rejected translations, compares filtered
results with unfiltered scans (including NULL semantics), and verifies file
pruning through `Iceberg Scan Tasks` on separately written data files.
