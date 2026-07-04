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
OPTIONS (
  catalog_type 'sqlite',
  catalog_uri '/tmp/pgiceberg_catalog_metadata_regress.db',
  warehouse '/tmp/pgiceberg_warehouse_metadata_regress',
  catalog_name 'metadata_regress'
);

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
  summary ->> 'manifest_count' AS manifest_count,
  summary ->> 'data_manifest_count' AS data_manifest_count,
  summary ->> 'delete_manifest_count' AS delete_manifest_count,
  summary ->> 'data_file_count' AS data_file_count,
  summary ->> 'delete_file_count' AS delete_file_count,
  summary ->> 'deletion_vector_file_count' AS deletion_vector_file_count
FROM pgiceberg.table_files_summary(
  'metadata_regress',
  'default',
  'metadata_fixture'
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
