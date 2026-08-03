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

-- Practical FDW coverage for iceberg-cpp: multi-snapshot history, UseSnapshot
-- time travel, column projection, SAVEPOINT subtransactions, and metadata
-- summaries keyed by snapshot_id.

CREATE EXTENSION pgiceberg;

\pset format unaligned
\set VERBOSITY terse

\! rm -f /tmp/pgiceberg_catalog_snapshot_regress.db
\! rm -rf /tmp/pgiceberg_warehouse_snapshot_regress

DO $$
BEGIN
  PERFORM pgiceberg.add_catalog(
    'snapshot_regress',
    'sqlite',
    '/tmp/pgiceberg_catalog_snapshot_regress.db',
    '/tmp/pgiceberg_warehouse_snapshot_regress'
  );

  PERFORM pgiceberg.create_table(
    'snapshot_regress',
    'default',
    'snapshot_fixture',
    ARRAY['id', 'payload'],
    ARRAY['bigint'::regtype, 'text'::regtype],
    ARRAY[true, false],
    true
  );
END $$;

CREATE SERVER snapshot_iceberg
FOREIGN DATA WRAPPER pgiceberg
OPTIONS (catalog 'snapshot_regress');

CREATE FOREIGN TABLE snapshot_fixture (
  id bigint,
  payload text
)
SERVER snapshot_iceberg
OPTIONS (
  namespace 'default',
  table 'snapshot_fixture'
);

-- Reject dead / invalid option shortcuts at validation time.
CREATE FOREIGN TABLE snapshot_bad_id (
  id bigint,
  payload text
)
SERVER snapshot_iceberg
OPTIONS (
  namespace 'default',
  table 'snapshot_fixture',
  snapshot_id 'not-a-number'
);

CREATE FOREIGN TABLE snapshot_config_file (
  id bigint,
  payload text
)
SERVER snapshot_iceberg
OPTIONS (
  namespace 'default',
  table 'snapshot_fixture',
  config_file '/tmp/unused.conf'
);

INSERT INTO snapshot_fixture VALUES (1, 'one');

SELECT
  (summary ->> 'snapshot_count')::integer AS snapshot_count,
  (summary ->> 'data_file_count')::integer AS data_file_count,
  (summary ->> 'delete_file_count')::integer AS delete_file_count,
  summary ->> 'snapshot_id' = summary ->> 'current_snapshot_id' AS is_current
FROM pgiceberg.table_snapshot_files_summary(
  'snapshot_regress',
  'default',
  'snapshot_fixture'
) AS summary;

SELECT (summary ->> 'snapshot_id')::bigint AS snap_after_one
FROM pgiceberg.table_snapshot_files_summary(
  'snapshot_regress',
  'default',
  'snapshot_fixture'
) AS summary \gset

INSERT INTO snapshot_fixture VALUES (2, 'two');

SELECT (summary ->> 'snapshot_id')::bigint AS snap_after_two
FROM pgiceberg.table_snapshot_files_summary(
  'snapshot_regress',
  'default',
  'snapshot_fixture'
) AS summary \gset

-- iceberg-cpp metadata helper for a historical snapshot id.
SELECT
  (summary ->> 'snapshot_id')::bigint = :snap_after_one AS historical_summary,
  (summary ->> 'snapshot_count')::integer AS snapshot_count,
  summary ->> 'snapshot_id' = summary ->> 'current_snapshot_id' AS is_current,
  (summary ->> 'data_file_count')::integer AS data_file_count
FROM pgiceberg.table_snapshot_files_summary(
  'snapshot_regress',
  'default',
  'snapshot_fixture',
  :snap_after_one
) AS summary;

-- Current table has both rows; column projection still reads through iceberg-cpp Select.
SELECT id
FROM snapshot_fixture
ORDER BY id;

SELECT count(*) AS current_row_count
FROM snapshot_fixture;

-- Pin UseSnapshot to the first commit and verify time travel.
ALTER FOREIGN TABLE snapshot_fixture
OPTIONS (ADD snapshot_id :'snap_after_one');

SELECT id, payload
FROM snapshot_fixture
ORDER BY id;

SELECT count(*) AS historical_row_count
FROM snapshot_fixture;

-- DML against a pinned historical snapshot must fail cleanly.
INSERT INTO snapshot_fixture VALUES (3, 'three');

ALTER FOREIGN TABLE snapshot_fixture
OPTIONS (DROP snapshot_id);

-- After dropping the pin, the current snapshot is visible again.
SELECT id, payload
FROM snapshot_fixture
ORDER BY id;

-- SAVEPOINT rollback rebuilds pending Iceberg transaction state.
BEGIN;
INSERT INTO snapshot_fixture VALUES (3, 'three');
SAVEPOINT keep_three;
INSERT INTO snapshot_fixture VALUES (4, 'four');
SELECT id FROM snapshot_fixture ORDER BY id;
ROLLBACK TO SAVEPOINT keep_three;
SELECT id FROM snapshot_fixture ORDER BY id;
COMMIT;

SELECT id, payload
FROM snapshot_fixture
ORDER BY id;

SELECT (summary ->> 'snapshot_id')::bigint AS snap_before_delete
FROM pgiceberg.table_snapshot_files_summary(
  'snapshot_regress',
  'default',
  'snapshot_fixture'
) AS summary \gset

DELETE FROM snapshot_fixture WHERE id = 1;

SELECT id, payload
FROM snapshot_fixture
ORDER BY id;

-- Historical snapshot still contains the deleted row; current does not.
ALTER FOREIGN TABLE snapshot_fixture
OPTIONS (ADD snapshot_id :'snap_before_delete');

SELECT id
FROM snapshot_fixture
ORDER BY id;

ALTER FOREIGN TABLE snapshot_fixture
OPTIONS (DROP snapshot_id);

SELECT
  (summary ->> 'snapshot_count')::integer AS snapshot_count,
  (summary ->> 'data_file_count')::integer AS data_file_count,
  summary ->> 'snapshot_id' = summary ->> 'current_snapshot_id' AS is_current
FROM pgiceberg.table_snapshot_files_summary(
  'snapshot_regress',
  'default',
  'snapshot_fixture'
) AS summary;

-- Missing snapshot id surfaces through iceberg-cpp scan planning.
ALTER FOREIGN TABLE snapshot_fixture
OPTIONS (ADD snapshot_id '1');

SELECT count(*) FROM snapshot_fixture;

ALTER FOREIGN TABLE snapshot_fixture
OPTIONS (DROP snapshot_id);

\set VERBOSITY default

DROP FOREIGN TABLE snapshot_fixture;
DROP SERVER snapshot_iceberg;
DROP EXTENSION pgiceberg;
