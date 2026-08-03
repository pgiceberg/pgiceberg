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

DO $$
BEGIN
  PERFORM pgiceberg.add_catalog(
    'metadata_regress',
    'sqlite',
    '/tmp/pgiceberg_catalog_metadata_regress.db',
    '/tmp/pgiceberg_warehouse_metadata_regress'
  );

  PERFORM pgiceberg.create_table(
    'metadata_regress',
    'default',
    'metadata_fixture',
    ARRAY['id', 'payload'],
    ARRAY['bigint'::regtype, 'text'::regtype],
    ARRAY[true, false],
    true,
    3
  );
END $$;

CREATE SERVER metadata_iceberg
FOREIGN DATA WRAPPER pgiceberg
OPTIONS (catalog 'metadata_regress');

CREATE FOREIGN TABLE metadata_fixture (
  id bigint,
  payload text
)
SERVER metadata_iceberg
OPTIONS (
  namespace 'default',
  table 'metadata_fixture'
);

INSERT INTO metadata_fixture
VALUES (1, 'one');

INSERT INTO metadata_fixture
VALUES (2, 'two');

SELECT pgiceberg.table_metadata_file_location(
  'metadata_regress',
  'default',
  'metadata_fixture'
) LIKE '/tmp/pgiceberg_warehouse_metadata_regress/default/metadata_fixture/metadata/%.metadata.json' AS has_metadata_file;

SELECT pgiceberg.table_format_version(
  'metadata_regress',
  'default',
  'metadata_fixture'
) AS format_version;

SELECT (pgiceberg.table_metadata_json(
  'metadata_regress',
  'default',
  'metadata_fixture'
) ->> 'format-version')::integer AS metadata_format_version;

SELECT pgiceberg.metadata_file_json(pgiceberg.table_metadata_file_location(
  'metadata_regress',
  'default',
  'metadata_fixture'
)) ->> 'location' AS table_location;

WITH table_metadata AS (
  SELECT pgiceberg.table_metadata_json(
    'metadata_regress',
    'default',
    'metadata_fixture'
  ) AS metadata
),
file_metadata AS (
  SELECT pgiceberg.metadata_file_json(pgiceberg.table_metadata_file_location(
    'metadata_regress',
    'default',
    'metadata_fixture'
  )) AS metadata
)
SELECT table_metadata.metadata ->> 'table-uuid' = file_metadata.metadata ->> 'table-uuid'
  AS metadata_matches
FROM table_metadata, file_metadata;

SELECT
  summary ->> 'snapshot_count' AS snapshot_count,
  summary ->> 'snapshot_id' = summary ->> 'current_snapshot_id' AS current_snapshot_summary,
  summary ->> 'manifest_count' AS manifest_count,
  summary ->> 'data_manifest_count' AS data_manifest_count,
  summary ->> 'delete_manifest_count' AS delete_manifest_count,
  summary ->> 'data_file_count' AS data_file_count,
  summary ->> 'delete_file_count' AS delete_file_count,
  summary ->> 'deletion_vector_file_count' AS deletion_vector_file_count
FROM pgiceberg.table_snapshot_files_summary(
  'metadata_regress',
  'default',
  'metadata_fixture'
) AS summary;

-- Summarize the oldest snapshot from metadata JSON history without printing ids.
WITH oldest AS (
  SELECT (metadata -> 'snapshots' -> 0 ->> 'snapshot-id')::bigint AS snapshot_id
  FROM (
    SELECT pgiceberg.table_metadata_json(
      'metadata_regress',
      'default',
      'metadata_fixture'
    ) AS metadata
  ) AS q
)
SELECT
  (summary ->> 'snapshot_id')::bigint = oldest.snapshot_id AS historical_snapshot_summary,
  summary ->> 'snapshot_id' = summary ->> 'current_snapshot_id' AS is_current,
  summary ->> 'data_file_count' AS data_file_count
FROM oldest,
LATERAL pgiceberg.table_snapshot_files_summary(
  'metadata_regress',
  'default',
  'metadata_fixture',
  oldest.snapshot_id
) AS summary;

CREATE ROLE pgiceberg_no_raw;
GRANT USAGE ON SCHEMA pgiceberg TO pgiceberg_no_raw;

SET ROLE pgiceberg_no_raw;

SELECT pgiceberg.table_metadata_file_location(
  'metadata_regress',
  'default',
  'metadata_fixture'
) LIKE '/tmp/pgiceberg_warehouse_metadata_regress/default/metadata_fixture/metadata/%.metadata.json'
  AS wrapper_visible_to_non_owner;

SELECT to_regprocedure(
  'pgiceberg._table_metadata_file_location_raw(text,text,text,text,text,text)'
) IS NULL AS raw_function_not_exposed;

RESET ROLE;

DROP OWNED BY pgiceberg_no_raw;
DROP ROLE pgiceberg_no_raw;

\set VERBOSITY default

DROP FOREIGN TABLE metadata_fixture;
DROP SERVER metadata_iceberg;
DROP EXTENSION pgiceberg;
