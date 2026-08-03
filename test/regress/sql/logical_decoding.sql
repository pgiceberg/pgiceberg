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

CREATE TABLE logical_source (
  id integer NOT NULL,
  name text,
  amount integer
);

SELECT pgiceberg.create_logical_mirror(
  'logical_source',
  'logical_catalog',
  'default',
  'logical_target',
  'pgiceberg_logical_regress',
  true,
  8
);

SELECT source_table::text, slot_name::text, enabled, batch_size,
       last_flushed_lsn IS NULL AS no_flushed_lsn
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

-- Slot should have no pending changes after mirror setup (catalog table is UNLOGGED).
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

SELECT enabled, last_flushed_lsn IS NULL AS no_flushed_lsn,
       last_error IS NULL AS no_error
FROM pgiceberg.logical_mirror_status();

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

SELECT enabled, last_error
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
