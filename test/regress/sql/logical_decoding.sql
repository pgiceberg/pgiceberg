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

\! rm -rf /tmp/pgiceberg_catalog_logical_regress.db /tmp/pgiceberg_warehouse_logical_regress

SELECT pgiceberg.add_catalog(
  'logical_catalog',
  'sqlite',
  '/tmp/pgiceberg_catalog_logical_regress.db',
  '/tmp/pgiceberg_warehouse_logical_regress'
);

CREATE TABLE logical_repeatable_source (id integer);
BEGIN ISOLATION LEVEL REPEATABLE READ;
SELECT pgiceberg.create_logical_mirror(
  'logical_repeatable_source',
  'logical_catalog',
  'default',
  'logical_repeatable_target',
  'pgiceberg_logical_repeatable'
);
ROLLBACK;

SELECT count(*) AS repeatable_slots
FROM pg_replication_slots
WHERE slot_name = 'pgiceberg_logical_repeatable';

DROP TABLE logical_repeatable_source;

CREATE TABLE logical_source (
  id integer NOT NULL,
  name text,
  amount integer
);

INSERT INTO logical_source VALUES (0, 'before', 5);

SELECT pgiceberg.create_logical_mirror(
  'logical_source',
  'logical_catalog',
  'default',
  'logical_target',
  'pgiceberg_logical_regress',
  true,
  8
);

SELECT source_table::text, slot_name::text, enabled, state, batch_size,
       initial_snapshot_lsn IS NOT NULL AS has_initial_lsn,
       backfill_rows, last_flushed_lsn IS NULL AS no_flushed_lsn,
       last_applied_batch_id IS NULL AS no_stream_batch
FROM pgiceberg.logical_mirror_status();

CREATE SERVER logical_iceberg
FOREIGN DATA WRAPPER pgiceberg
OPTIONS (catalog 'logical_catalog');

CREATE FOREIGN TABLE logical_target (
  id integer,
  name text,
  amount integer
)
SERVER logical_iceberg
OPTIONS (
  namespace 'default',
  table 'logical_target'
);

-- The row committed before mirror creation is included by the initial copy.
SELECT id, name, amount
FROM logical_target
ORDER BY id;

-- Logged mirror metadata is filtered by the output plugin.
SELECT count(*) AS setup_changes
FROM pg_logical_slot_get_changes('pgiceberg_logical_regress', NULL, NULL);

INSERT INTO logical_source VALUES (1, 'alpha', 12), (2, NULL, NULL);

SELECT substring(data FROM 1 FOR 1) || '|RELID' ||
       replace(regexp_replace(data, E'^[IDUT]\\t[0-9]+', ''), E'\t', '|') AS data
FROM pg_logical_slot_peek_changes('pgiceberg_logical_regress', NULL, NULL)
ORDER BY lsn, data;

-- Exercise the mirror processor path: durable Iceberg append then exact slot advance.
SELECT pgiceberg.process_logical_mirrors();

SELECT id, name, amount
FROM logical_target
ORDER BY id;

SELECT enabled, state, backfill_rows,
       last_flushed_lsn IS NULL AS no_flushed_lsn,
       last_applied_batch_id IS NULL AS no_stream_batch,
       last_error IS NULL AS no_error
FROM pgiceberg.logical_mirror_status();

WITH mirror AS (
  SELECT last_applied_batch_id AS batch_id
  FROM pgiceberg.logical_mirror_status()
), metadata AS (
  SELECT pgiceberg.table_metadata_json(
           'logical_catalog', 'default', 'logical_target'
         ) AS document
)
SELECT document -> 'properties' ->> 'pgiceberg.logical.last-batch-id' =
         batch_id AS table_property,
       EXISTS (
         SELECT 1
         FROM jsonb_array_elements(document -> 'snapshots') AS snapshot
         WHERE snapshot ->> 'snapshot-id' = document ->> 'current-snapshot-id'
           AND snapshot -> 'summary' ->>
                 'pgiceberg.logical.last-batch-id' = batch_id
       ) AS snapshot_summary
FROM mirror, metadata;

SELECT count(*) AS remaining_slot_changes
FROM pg_logical_slot_peek_changes('pgiceberg_logical_regress', NULL, NULL);

UPDATE logical_source SET name = 'beta' WHERE id = 1;
DELETE FROM logical_source WHERE id = 2;
TRUNCATE logical_source;

SELECT substring(data FROM 1 FOR 1) || '|RELID' ||
       replace(regexp_replace(data, E'^[IDUT]\\t[0-9]+', ''), E'\t', '|') AS data
FROM pg_logical_slot_peek_changes('pgiceberg_logical_regress', NULL, NULL)
ORDER BY lsn, data;

-- Unsupported changes must parse cleanly, advance the slot, and disable the mirror.
SELECT pgiceberg.process_logical_mirrors();

SELECT enabled, state, last_error
FROM pgiceberg.logical_mirror_status();

SELECT count(*) AS remaining_slot_changes_after_unsupported
FROM pg_logical_slot_peek_changes('pgiceberg_logical_regress', NULL, NULL);

SELECT pgiceberg.drop_logical_mirror('logical_source');

SELECT count(*) AS remaining_mirrors
FROM pgiceberg.logical_mirror_status();

SELECT count(*) AS remaining_slots
FROM pg_replication_slots
WHERE slot_name = 'pgiceberg_logical_regress';

DROP FOREIGN TABLE logical_target;
DROP SERVER logical_iceberg;
DROP TABLE logical_source;
DROP EXTENSION pgiceberg;
