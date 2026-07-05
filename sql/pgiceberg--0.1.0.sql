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

\echo Use "CREATE EXTENSION pgiceberg" to load this file. \quit

CREATE SCHEMA pgiceberg;

-- Catalog registry.
CREATE TABLE pgiceberg.catalogs (
  name text PRIMARY KEY,
  catalog_type text NOT NULL,
  catalog_uri text NOT NULL,
  warehouse text NOT NULL,
  iceberg_catalog_name text NOT NULL
);

COMMENT ON TABLE pgiceberg.catalogs IS
  'Local pgiceberg catalog registry used by helper functions that accept a catalog name.';
COMMENT ON COLUMN pgiceberg.catalogs.name IS
  'Local pgiceberg catalog name passed to helper functions.';
COMMENT ON COLUMN pgiceberg.catalogs.catalog_type IS
  'Iceberg catalog backend type, such as sql, sqlite, rest, or hms.';
COMMENT ON COLUMN pgiceberg.catalogs.catalog_uri IS
  'Connection URI for the Iceberg catalog backend.';
COMMENT ON COLUMN pgiceberg.catalogs.warehouse IS
  'Warehouse location used to create and load Iceberg table files.';
COMMENT ON COLUMN pgiceberg.catalogs.iceberg_catalog_name IS
  'Logical Iceberg catalog name used to scope tables in the catalog backend.';

CREATE FUNCTION pgiceberg.add_catalog(
  name text,
  catalog_type text,
  catalog_uri text,
  warehouse text,
  iceberg_catalog_name text DEFAULT NULL
)
RETURNS void
LANGUAGE sql
AS $$
  INSERT INTO pgiceberg.catalogs (
    name,
    catalog_type,
    catalog_uri,
    warehouse,
    iceberg_catalog_name
  )
  VALUES ($1, $2, $3, $4, COALESCE($5, $1))
  ON CONFLICT (name) DO UPDATE
  SET catalog_type = EXCLUDED.catalog_type,
      catalog_uri = EXCLUDED.catalog_uri,
      warehouse = EXCLUDED.warehouse,
      iceberg_catalog_name = EXCLUDED.iceberg_catalog_name;
$$;

COMMENT ON FUNCTION pgiceberg.add_catalog(text, text, text, text, text) IS
  'Register or replace a local pgiceberg catalog name and its Iceberg catalog connection details.';

CREATE FUNCTION pgiceberg.drop_catalog(name text)
RETURNS void
LANGUAGE sql STRICT
AS $$
  DELETE FROM pgiceberg.catalogs
  WHERE catalogs.name = $1;
$$;

COMMENT ON FUNCTION pgiceberg.drop_catalog(text) IS
  'Remove a local pgiceberg catalog name.';

-- Foreign data wrapper entrypoints.
CREATE FUNCTION pgiceberg.fdw_handler()
RETURNS fdw_handler
AS 'MODULE_PATHNAME', 'pgiceberg_fdw_handler'
LANGUAGE C STRICT;

CREATE FUNCTION pgiceberg.fdw_validator(text[], oid)
RETURNS void
AS 'MODULE_PATHNAME', 'pgiceberg_fdw_validator'
LANGUAGE C STRICT;

CREATE FOREIGN DATA WRAPPER pgiceberg
HANDLER pgiceberg.fdw_handler
VALIDATOR pgiceberg.fdw_validator;

-- Table lifecycle helpers.
CREATE FUNCTION pgiceberg.create_table(
  name text,
  namespace text,
  table_name text,
  column_names text[],
  column_types regtype[],
  column_required boolean[],
  drop_if_exists boolean DEFAULT false,
  format_version integer DEFAULT 2
)
RETURNS void
AS 'MODULE_PATHNAME', 'pgiceberg_create_table'
LANGUAGE C STRICT SECURITY DEFINER
SET search_path = pg_catalog, pgiceberg
;

COMMENT ON FUNCTION pgiceberg.create_table(text, text, text, text[], regtype[], boolean[], boolean, integer) IS
  'Create an Iceberg table through a registered pgiceberg catalog name.';

CREATE FUNCTION pgiceberg.register_table(
  name text,
  namespace text,
  table_name text,
  metadata_file_location text,
  drop_if_exists boolean DEFAULT false
)
RETURNS void
AS 'MODULE_PATHNAME', 'pgiceberg_register_table'
LANGUAGE C STRICT SECURITY DEFINER
SET search_path = pg_catalog, pgiceberg
;

COMMENT ON FUNCTION pgiceberg.register_table(text, text, text, text, boolean) IS
  'Register an existing Iceberg metadata file as a table through a registered pgiceberg catalog name.';

CREATE FUNCTION pgiceberg.register_table_from_location(
  name text,
  namespace text,
  table_name text,
  table_location text,
  drop_if_exists boolean DEFAULT false
)
RETURNS void
LANGUAGE plpgsql STRICT SECURITY DEFINER
SET search_path = pg_catalog, pgiceberg
AS $$
DECLARE
  metadata_dir text := regexp_replace(table_location, '/+$', '') || '/metadata';
  metadata_file text;
BEGIN
  SELECT file_name
  INTO metadata_file
  FROM pg_ls_dir(metadata_dir) AS file_name
  WHERE file_name LIKE '%.metadata.json'
  ORDER BY file_name DESC
  LIMIT 1;

  IF metadata_file IS NULL THEN
    RAISE EXCEPTION 'no Iceberg metadata file found in %', metadata_dir
      USING ERRCODE = '58P01',
            DETAIL = 'Expected at least one file matching %.metadata.json.';
  END IF;

  PERFORM pgiceberg.register_table(
    name,
    namespace,
    table_name,
    metadata_dir || '/' || metadata_file,
    drop_if_exists
  );
END;
$$;

COMMENT ON FUNCTION pgiceberg.register_table_from_location(text, text, text, text, boolean) IS
  'Register an existing Iceberg table by finding the latest metadata JSON under a table location.';

-- Metadata inspection helpers.
CREATE FUNCTION pgiceberg.metadata_file_json(
  metadata_file_location text
)
RETURNS jsonb
AS 'MODULE_PATHNAME', 'pgiceberg_metadata_file_json'
LANGUAGE C STRICT;

COMMENT ON FUNCTION pgiceberg.metadata_file_json(text) IS
  'Read an Iceberg metadata JSON file from disk and return it as jsonb.';

CREATE FUNCTION pgiceberg.table_metadata_file_location(
  name text,
  namespace text,
  table_name text
)
RETURNS text
AS 'MODULE_PATHNAME', 'pgiceberg_table_metadata_file_location'
LANGUAGE C STRICT SECURITY DEFINER
SET search_path = pg_catalog, pgiceberg
;

COMMENT ON FUNCTION pgiceberg.table_metadata_file_location(text, text, text) IS
  'Return the current Iceberg metadata file location for a table.';

CREATE FUNCTION pgiceberg.table_metadata_json(
  name text,
  namespace text,
  table_name text
)
RETURNS jsonb
AS 'MODULE_PATHNAME', 'pgiceberg_table_metadata_json'
LANGUAGE C STRICT SECURITY DEFINER
SET search_path = pg_catalog, pgiceberg
;

COMMENT ON FUNCTION pgiceberg.table_metadata_json(text, text, text) IS
  'Return the current Iceberg table metadata as jsonb.';

CREATE FUNCTION pgiceberg.table_snapshot_files_summary(
  name text,
  namespace text,
  table_name text,
  snapshot_id bigint DEFAULT NULL
)
RETURNS jsonb
AS 'MODULE_PATHNAME', 'pgiceberg_table_snapshot_files_summary'
LANGUAGE C SECURITY DEFINER
SET search_path = pg_catalog, pgiceberg
;

COMMENT ON FUNCTION pgiceberg.table_snapshot_files_summary(text, text, text, bigint) IS
  'Return manifest, data file, delete file, and deletion vector counts for an Iceberg snapshot; use the current snapshot when snapshot_id is NULL.';

CREATE FUNCTION pgiceberg.table_format_version(
  name text,
  namespace text,
  table_name text
)
RETURNS integer
LANGUAGE plpgsql STRICT SECURITY DEFINER
SET search_path = pg_catalog, pgiceberg
AS $$
BEGIN
  RETURN (pgiceberg.table_metadata_json($1, $2, $3) ->> 'format-version')::integer;
END;
$$;

COMMENT ON FUNCTION pgiceberg.table_format_version(text, text, text) IS
  'Return the Iceberg table format version from the current table metadata.';
