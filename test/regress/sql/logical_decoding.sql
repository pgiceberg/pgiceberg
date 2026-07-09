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

CREATE TABLE logical_source (
  id integer NOT NULL,
  name text,
  amount numeric
);

SELECT pgiceberg.create_logical_mirror(
  'logical_source',
  'logical_catalog',
  'default',
  'logical_target',
  'pgiceberg_logical_regress',
  false,
  8
);

SELECT source_table::text, slot_name::text, enabled, batch_size,
       last_flushed_lsn IS NULL AS no_flushed_lsn
FROM pgiceberg.logical_mirror_status();

SELECT count(*) AS setup_changes
FROM pg_logical_slot_get_changes('pgiceberg_logical_regress', NULL, NULL);

INSERT INTO logical_source VALUES (1, 'alpha', 12.50), (2, NULL, NULL);

SELECT substring(data FROM 1 FOR 1) || '|RELID' ||
       replace(regexp_replace(data, E'^[IDUT]\\t[0-9]+', ''), E'\t', '|') AS data
FROM pg_logical_slot_peek_changes('pgiceberg_logical_regress', NULL, NULL)
ORDER BY lsn, data;

SELECT count(*)
FROM pg_logical_slot_get_changes('pgiceberg_logical_regress', NULL, NULL);

UPDATE logical_source SET name = 'beta' WHERE id = 1;
DELETE FROM logical_source WHERE id = 2;
TRUNCATE logical_source;

SELECT substring(data FROM 1 FOR 1) || '|RELID' ||
       replace(regexp_replace(data, E'^[IDUT]\\t[0-9]+', ''), E'\t', '|') AS data
FROM pg_logical_slot_peek_changes('pgiceberg_logical_regress', NULL, NULL)
ORDER BY lsn, data;

SELECT pgiceberg.drop_logical_mirror('logical_source');

SELECT count(*) AS remaining_mirrors
FROM pgiceberg.logical_mirror_status();

DROP TABLE logical_source;
DROP EXTENSION pgiceberg;
