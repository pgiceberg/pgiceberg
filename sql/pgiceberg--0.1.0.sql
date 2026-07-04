\echo Use "CREATE EXTENSION pgiceberg" to load this file. \quit

CREATE SCHEMA pgiceberg;

CREATE TABLE pgiceberg.catalogs (
  name text PRIMARY KEY,
  catalog_type text NOT NULL,
  catalog_uri text NOT NULL,
  warehouse text NOT NULL
);

COMMENT ON TABLE pgiceberg.catalogs IS
  'Local pgiceberg catalog registry used by helper functions that accept a catalog name.';
COMMENT ON COLUMN pgiceberg.catalogs.name IS
  'Catalog name passed to pgiceberg helper functions and to the Iceberg catalog backend.';
COMMENT ON COLUMN pgiceberg.catalogs.catalog_type IS
  'Iceberg catalog backend type, such as sql, sqlite, rest, or hms.';
COMMENT ON COLUMN pgiceberg.catalogs.catalog_uri IS
  'Connection URI for the Iceberg SQL catalog backend.';
COMMENT ON COLUMN pgiceberg.catalogs.warehouse IS
  'Warehouse location used to create and load Iceberg table files.';

CREATE FUNCTION pgiceberg.fdw_handler()
RETURNS fdw_handler
AS 'MODULE_PATHNAME', 'pgiceberg_fdw_handler'
LANGUAGE C STRICT;

CREATE FUNCTION pgiceberg.fdw_validator(text[], oid)
RETURNS void
AS 'MODULE_PATHNAME', 'pgiceberg_fdw_validator'
LANGUAGE C STRICT;

CREATE FUNCTION pgiceberg.add_catalog(
  name text,
  catalog_type text,
  catalog_uri text,
  warehouse text
)
RETURNS void
LANGUAGE sql STRICT
AS $$
  INSERT INTO pgiceberg.catalogs (
    name,
    catalog_type,
    catalog_uri,
    warehouse
  )
  VALUES ($1, $2, $3, $4)
  ON CONFLICT (name) DO UPDATE
  SET catalog_type = EXCLUDED.catalog_type,
      catalog_uri = EXCLUDED.catalog_uri,
      warehouse = EXCLUDED.warehouse;
$$;

COMMENT ON FUNCTION pgiceberg.add_catalog(text, text, text, text) IS
  'Register or replace a local pgiceberg catalog and its Iceberg catalog connection details.';

CREATE FUNCTION pgiceberg.drop_catalog(name text)
RETURNS void
LANGUAGE sql STRICT
AS $$
  DELETE FROM pgiceberg.catalogs
  WHERE catalogs.name = $1;
$$;

COMMENT ON FUNCTION pgiceberg.drop_catalog(text) IS
  'Remove a local pgiceberg catalog name.';

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

CREATE FUNCTION pgiceberg.table_files_summary(
  name text,
  namespace text,
  table_name text
)
RETURNS jsonb
AS 'MODULE_PATHNAME', 'pgiceberg_table_files_summary'
LANGUAGE C STRICT SECURITY DEFINER
SET search_path = pg_catalog, pgiceberg
;

COMMENT ON FUNCTION pgiceberg.table_files_summary(text, text, text) IS
  'Return snapshot, manifest, data file, delete file, and deletion vector counts for an Iceberg table.';

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

CREATE FOREIGN DATA WRAPPER pgiceberg
HANDLER pgiceberg.fdw_handler
VALIDATOR pgiceberg.fdw_validator;
