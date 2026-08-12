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

## Summary

`pgiceberg` foreign scans translate PostgreSQL `WHERE` clauses into Iceberg
filter expressions and hand them to iceberg-cpp's `TableScanBuilder::Filter`.
The filter drives scan planning: manifests are pruned via partition summaries
and data files are pruned via per-column metrics (lower/upper bounds, null
counts). Rows inside surviving files are still filtered by PostgreSQL, which
keeps evaluating every scan clause locally.

The supported predicate surface includes comparison operators, `IS [NOT]
NULL`, `IN`/`NOT IN` lists, prefix `LIKE`/`NOT LIKE`, and boolean columns.

## Correctness model

Because PostgreSQL re-checks all clauses, the pushed filter does not have to
be exact — but it must never be *stronger* than the original clause.  A filter
that skips a data file containing qualifying rows silently loses rows; a
filter that keeps an extra file only costs I/O.  Every translation below is
equivalent to or weaker than its PostgreSQL clause; anything that cannot meet
that bar is not pushed.

Consequences of this rule:

- Only operators from `pg_catalog` are translated; user-defined operators can
  reuse names like `=` with different semantics.
- Constants convert to Iceberg literals only via lossless conversions
  (see matrix below).  A lossy narrowing (e.g. `float8` literal against a
  `float` column) is allowed only for equality membership (`=`, `IN`), where a
  rounded literal can only over-match.
- Text ordering comparisons (`<`, `<=`, `>`, `>=`) are pushed only under the
  `C` collation, the only PostgreSQL collation that agrees with Iceberg's
  byte-wise string bounds.  Equality-style text predicates require a
  deterministic collation.  `bpchar` columns are never pushed because their
  comparisons ignore trailing spaces.
- `LIKE` patterns become `starts_with` only for `prefix%` shapes with no other
  `%`, `_`, or escape characters.
- `IS NOT TRUE`-style boolean tests accept NULLs, which Iceberg equality never
  matches, so they are not pushed.
- NULL elements in `IN`/`NOT IN` lists are dropped (never satisfiable /
  keeps the pushed predicate weaker), `NaN` constants are skipped.
- Numeric constants push to `decimal(P,S)` columns only when they rescale to
  `S` exactly; rounding would change the predicate.
- `timestamp`/`timestamptz` literals push to nanosecond Iceberg columns
  (`timestamp_ns` / `timestamptz_ns`), but PostgreSQL only holds microsecond
  resolution on both the literal and the value it reads back, so the narrowing
  policy in [`timestamp-precision.md`](timestamp-precision.md) applies. The
  read path floors nanoseconds toward negative infinity, which keeps the
  pushed range filter equal-or-weaker than the local recheck.
- Time-travel scans (`snapshot_id` option) skip pushdown: filters bind against
  the snapshot schema, which may differ from the current table schema used
  for translation.

## Clause translation

| PostgreSQL clause | Iceberg predicate |
| --- | --- |
| `col = k` / `k = col` | `Equal` |
| `col <> k` | `NotEqual` |
| `col < / <= / > / >= k` (commuted forms flip) | `LessThan` / ... / `GreaterThanOrEqual` |
| `col IS NULL` / `IS NOT NULL` | `IsNull` / `NotNull` |
| `col = ANY ('{...}')` (`IN`) | `In` |
| `col <> ALL ('{...}')` (`NOT IN`) | `NotIn` |
| `col LIKE 'p%'` / `NOT LIKE 'p%'` | `StartsWith` / `NotStartsWith` |
| `boolcol`, `NOT boolcol`, `IS TRUE`, `IS FALSE` | `Equal(true/false)` |

Multiple pushable clauses combine with `AND`.  Clauses referencing other
relations, non-`Var` expressions (function calls, arithmetic), parameters, or
unsupported operators are simply skipped.

## Literal conversion matrix

| PostgreSQL constant | Iceberg column types |
| --- | --- |
| `bool` | `boolean` |
| `int2`, `int4` | `int`, `long`, `double` |
| `int8` | `long`, `int` (out-of-range folds to always-true/false at bind) |
| `float4` | `float`, `double` |
| `float8` | `double`; `float` for `=`/`IN` only |
| `numeric` | `decimal(P,S)` with exact rescale |
| `text`, `varchar` | `string` |
| `date` | `date` (epoch shift 2000 → 1970) |
| `time` | `time` |
| `timestamp` | `timestamp`, `timestamp_ns` (see note) |
| `timestamptz` | `timestamptz`, `timestamptz_ns` (see note) |
| `uuid` | `uuid` |
| `bytea` | `binary` |

## Architecture

- `src/fdw/qual_pushdown.{h,cc}` — pure translation from a clause `List*` to
  an unbound `iceberg::Expression`, given the scan varno, the relation OID
  (for attribute names) and the Iceberg schema (for target types).  Strictly
  best-effort and exception-free.
- `src/engine/scan_state.cc` — `engine::BeginScan` accepts a
  `ScanFilterBuilder` callback and invokes it after the table (and schema) is
  loaded; the resulting expression flows into `IcebergScanCursor`, which sets
  `TableScanBuilder::Filter` before `PlanFiles`.
- `src/fdw/fdw.cc` — builds the callback from the plan's scan clauses in
  `BeginForeignScan`.  During planning, `GetForeignRelSize` runs the same
  translation against `baserestrictinfo` to stash a human-readable filter
  string, which `GetForeignPlan` carries in `fdw_private` and
  `ExplainForeignScan` prints as `Iceberg Filter: ...`.  Under `EXPLAIN
  ANALYZE` the scan also reports `Iceberg Scan Tasks: N` — the number of file
  scan tasks planned after pruning, which regression tests use to prove files
  were actually skipped.

Row-level residual filtering inside iceberg-cpp
(`FileScanTaskReader`) is not implemented upstream yet; when it lands, the
same pushed filter will also cut rows before they reach PostgreSQL, and the
local recheck keeps results correct either way.

## Verification

`test/regress/sql/fdw_pushdown.sql` covers:

- EXPLAIN visibility of the pushed filter for every supported operator and
  literal type, plus negative cases that must not be pushed (expressions,
  suffix `LIKE`, `_` wildcards, `IS NOT TRUE`, non-`pg_catalog` semantics).
- Result correctness with filters applied (compared against unfiltered
  scans), including NULL semantics for `<>`, `NOT IN`, and boolean columns.
- File pruning: rows inserted in separate transactions produce separate data
  files; `EXPLAIN ANALYZE` shows `Iceberg Scan Tasks` dropping when a
  selective filter is pushed.
