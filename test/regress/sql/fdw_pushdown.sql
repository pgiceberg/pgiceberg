-- Licensed under the Apache License, Version 2.0 (the "License");
-- you may not use this file except in compliance with the License.
-- You may obtain a copy of the License at
--
--     http://www.apache.org/licenses/LICENSE-2.0
--
-- Unless required by applicable law or agreed to in writing, software
-- distributed under the License is distributed on an "AS IS" BASIS,
-- WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
-- See the License for the specific language governing permissions and
-- limitations under the License.

CREATE EXTENSION pgiceberg;

\pset format unaligned
\set VERBOSITY terse
SET DateStyle = 'ISO, YMD';
SET TIME ZONE 'UTC';

DO $$
BEGIN
  PERFORM pgiceberg.add_catalog(
    'pushdown_regress',
    'sqlite',
    '/tmp/pgiceberg_catalog_pushdown_regress.db',
    '/tmp/pgiceberg_warehouse_pushdown_regress'
  );

  PERFORM pgiceberg.create_table(
    'pushdown_regress',
    'default',
    'type_probe',
    ARRAY['id', 'big', 'price', 'ratio', 'amount', 'label', 'flag',
          'created', 'event_time', 'event_ts', 'event_tstz', 'payload',
          'token'],
    ARRAY[
      'integer'::regtype,
      'bigint'::regtype,
      'double precision'::regtype,
      'real'::regtype,
      'numeric'::regtype,
      'text'::regtype,
      'boolean'::regtype,
      'date'::regtype,
      'time'::regtype,
      'timestamp'::regtype,
      'timestamptz'::regtype,
      'bytea'::regtype,
      'uuid'::regtype
    ],
    ARRAY[true, false, false, false, false, false, false,
          false, false, false, false, false, false],
    true
  );

  PERFORM pgiceberg.create_table(
    'pushdown_regress',
    'default',
    'pruning_probe',
    ARRAY['id', 'category', 'amount'],
    ARRAY['bigint'::regtype, 'text'::regtype, 'double precision'::regtype],
    ARRAY[true, false, false],
    true
  );
END $$;

CREATE SERVER pushdown_iceberg
FOREIGN DATA WRAPPER pgiceberg
OPTIONS (catalog 'pushdown_regress');

CREATE FOREIGN TABLE type_probe (
  id integer,
  big bigint,
  price double precision,
  ratio real,
  amount numeric,
  label text,
  flag boolean,
  created date,
  event_time time,
  event_ts timestamp,
  event_tstz timestamptz,
  payload bytea,
  token uuid
)
SERVER pushdown_iceberg
OPTIONS (namespace 'default', table 'type_probe');

CREATE FOREIGN TABLE pruning_probe (
  id bigint,
  category text,
  amount double precision
)
SERVER pushdown_iceberg
OPTIONS (namespace 'default', table 'pruning_probe');

-- ---------------------------------------------------------------------
-- Comparison operators
-- ---------------------------------------------------------------------
EXPLAIN (COSTS OFF) SELECT id FROM type_probe WHERE id = 1;
EXPLAIN (COSTS OFF) SELECT id FROM type_probe WHERE id <> 1;
EXPLAIN (COSTS OFF) SELECT id FROM type_probe WHERE id < 5;
EXPLAIN (COSTS OFF) SELECT id FROM type_probe WHERE id <= 5;
EXPLAIN (COSTS OFF) SELECT id FROM type_probe WHERE id > 5;
EXPLAIN (COSTS OFF) SELECT id FROM type_probe WHERE id >= 5;
-- Commuted constant-first comparison flips the operator.
EXPLAIN (COSTS OFF) SELECT id FROM type_probe WHERE 5 > id;
-- Multiple pushable clauses combine with AND.
EXPLAIN (COSTS OFF) SELECT id FROM type_probe WHERE id > 1 AND id < 10;

-- ---------------------------------------------------------------------
-- Literal types
-- ---------------------------------------------------------------------
EXPLAIN (COSTS OFF) SELECT id FROM type_probe WHERE big = 10000000000;
-- int constant against a bigint column (cross-type operator).
EXPLAIN (COSTS OFF) SELECT id FROM type_probe WHERE big = 7;
EXPLAIN (COSTS OFF) SELECT id FROM type_probe WHERE price > 1.5;
-- float4 column with a float4 constant is exact.
EXPLAIN (COSTS OFF) SELECT id FROM type_probe WHERE ratio = 1.5::real;
-- numeric constant with no fractional part rescales exactly to decimal(38,0).
EXPLAIN (COSTS OFF) SELECT id FROM type_probe WHERE amount = 12345;
-- fractional numeric cannot rescale to scale 0: not pushed.
EXPLAIN (COSTS OFF) SELECT id FROM type_probe WHERE amount = 123.45;
EXPLAIN (COSTS OFF) SELECT id FROM type_probe WHERE label = 'alpha';
EXPLAIN (COSTS OFF) SELECT id FROM type_probe WHERE created = DATE '2026-01-15';
EXPLAIN (COSTS OFF) SELECT id FROM type_probe WHERE created > DATE '2026-01-15';
EXPLAIN (COSTS OFF) SELECT id FROM type_probe WHERE event_time = TIME '12:34:56';
EXPLAIN (COSTS OFF)
SELECT id FROM type_probe WHERE event_ts >= TIMESTAMP '2026-01-15 08:00:00';
EXPLAIN (COSTS OFF)
SELECT id FROM type_probe
WHERE event_tstz = TIMESTAMPTZ '2026-01-15 08:00:00+00';
EXPLAIN (COSTS OFF)
SELECT id FROM type_probe
WHERE token = '11111111-2222-3333-4444-555555555555';
EXPLAIN (COSTS OFF)
SELECT id FROM type_probe WHERE payload = decode('c0ffee', 'hex');

-- ---------------------------------------------------------------------
-- NULL tests, IN lists, LIKE, booleans
-- ---------------------------------------------------------------------
EXPLAIN (COSTS OFF) SELECT id FROM type_probe WHERE label IS NULL;
EXPLAIN (COSTS OFF) SELECT id FROM type_probe WHERE label IS NOT NULL;
EXPLAIN (COSTS OFF) SELECT id FROM type_probe WHERE id IN (1, 2, 3);
EXPLAIN (COSTS OFF) SELECT id FROM type_probe WHERE id NOT IN (1, 2);
EXPLAIN (COSTS OFF) SELECT id FROM type_probe WHERE label IN ('alpha', 'beta');
-- NULL list elements are dropped from the pushed IN list.
EXPLAIN (COSTS OFF) SELECT id FROM type_probe WHERE id IN (1, NULL);
EXPLAIN (COSTS OFF) SELECT id FROM type_probe WHERE label LIKE 'al%';
EXPLAIN (COSTS OFF) SELECT id FROM type_probe WHERE label NOT LIKE 'al%';
EXPLAIN (COSTS OFF) SELECT id FROM type_probe WHERE flag;
EXPLAIN (COSTS OFF) SELECT id FROM type_probe WHERE NOT flag;
EXPLAIN (COSTS OFF) SELECT id FROM type_probe WHERE flag = true;
EXPLAIN (COSTS OFF) SELECT id FROM type_probe WHERE flag IS TRUE;

-- ---------------------------------------------------------------------
-- Collation rules for text
-- ---------------------------------------------------------------------
-- Ordering comparison under the default collation is not pushed...
EXPLAIN (COSTS OFF) SELECT id FROM type_probe WHERE label < 'beta';
-- ...but is pushed under the byte-ordered C collation.
EXPLAIN (COSTS OFF) SELECT id FROM type_probe WHERE label < 'beta' COLLATE "C";

-- ---------------------------------------------------------------------
-- Clauses that must not be pushed
-- ---------------------------------------------------------------------
-- Expressions over columns.
EXPLAIN (COSTS OFF) SELECT id FROM type_probe WHERE upper(label) = 'ALPHA';
EXPLAIN (COSTS OFF) SELECT id FROM type_probe WHERE id + 1 = 2;
-- Column-to-column comparison.
EXPLAIN (COSTS OFF) SELECT id FROM type_probe WHERE id = big;
-- LIKE patterns that are not a plain prefix.
EXPLAIN (COSTS OFF) SELECT id FROM type_probe WHERE label LIKE '%pha';
EXPLAIN (COSTS OFF) SELECT id FROM type_probe WHERE label LIKE 'a_l%';
-- IS NOT TRUE accepts NULLs; Iceberg equality does not.
EXPLAIN (COSTS OFF) SELECT id FROM type_probe WHERE flag IS NOT TRUE;
-- Volatile comparison value.
EXPLAIN (COSTS OFF) SELECT id FROM type_probe WHERE price > random();

-- ---------------------------------------------------------------------
-- Results stay correct with filters pushed down
-- ---------------------------------------------------------------------
-- Separate statements commit separate snapshots, producing one data file
-- per row so metrics-based file pruning is observable.
INSERT INTO pruning_probe VALUES (1, 'alpha', 10.0);
INSERT INTO pruning_probe VALUES (2, 'beta', 20.0);
INSERT INTO pruning_probe VALUES (3, NULL, 30.0);

SELECT id, category, amount FROM pruning_probe ORDER BY id;
SELECT id, category, amount FROM pruning_probe WHERE id = 2;
-- <> must not surface the NULL category row.
SELECT id, category, amount FROM pruning_probe WHERE category <> 'alpha';
SELECT id, category, amount FROM pruning_probe WHERE category IS NULL;
SELECT id, category, amount FROM pruning_probe WHERE id NOT IN (1, 3);
SELECT id, category, amount FROM pruning_probe WHERE category LIKE 'be%';
SELECT id, category, amount FROM pruning_probe
WHERE id >= 2 AND category IS NOT NULL;

-- ---------------------------------------------------------------------
-- File pruning is visible in EXPLAIN ANALYZE scan task counts
-- ---------------------------------------------------------------------
EXPLAIN (ANALYZE, COSTS OFF, TIMING OFF, SUMMARY OFF, BUFFERS OFF)
SELECT id FROM pruning_probe;
EXPLAIN (ANALYZE, COSTS OFF, TIMING OFF, SUMMARY OFF, BUFFERS OFF)
SELECT id FROM pruning_probe WHERE id = 1;
EXPLAIN (ANALYZE, COSTS OFF, TIMING OFF, SUMMARY OFF, BUFFERS OFF)
SELECT id FROM pruning_probe WHERE id > 2;
EXPLAIN (ANALYZE, COSTS OFF, TIMING OFF, SUMMARY OFF, BUFFERS OFF)
SELECT id FROM pruning_probe WHERE category = 'beta';
EXPLAIN (ANALYZE, COSTS OFF, TIMING OFF, SUMMARY OFF, BUFFERS OFF)
SELECT id FROM pruning_probe WHERE category IS NULL;

-- ---------------------------------------------------------------------
-- Time-travel scans skip pushdown
-- ---------------------------------------------------------------------
CREATE FOREIGN TABLE pruning_probe_snapshot (
  id bigint,
  category text,
  amount double precision
)
SERVER pushdown_iceberg
OPTIONS (namespace 'default', table 'pruning_probe');

DO $$
DECLARE
  snap bigint;
BEGIN
  SELECT (metadata -> 'snapshots' -> 0 ->> 'snapshot-id')::bigint
  INTO snap
  FROM (
    SELECT pgiceberg.table_metadata_json(
      'pushdown_regress',
      'default',
      'pruning_probe'
    ) AS metadata
  ) AS q;

  EXECUTE format(
    'ALTER FOREIGN TABLE pruning_probe_snapshot OPTIONS (ADD snapshot_id %L)',
    snap::text
  );
END $$;

EXPLAIN (COSTS OFF) SELECT id FROM pruning_probe_snapshot WHERE id = 1;
SELECT id, category, amount FROM pruning_probe_snapshot WHERE id = 1;

\set VERBOSITY default

DROP FOREIGN TABLE pruning_probe_snapshot;
DROP FOREIGN TABLE pruning_probe;
DROP FOREIGN TABLE type_probe;
DROP SERVER pushdown_iceberg;
DROP EXTENSION pgiceberg;
