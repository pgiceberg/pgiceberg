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
) USING heap;

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

CREATE TABLE pgiceberg.table_bindings (
  relid oid PRIMARY KEY,
  catalog text NOT NULL,
  namespace text NOT NULL,
  table_name text NOT NULL,
  created_at timestamptz NOT NULL DEFAULT now(),
  updated_at timestamptz NOT NULL DEFAULT now()
) USING heap;

COMMENT ON TABLE pgiceberg.table_bindings IS
  'Native table access method binding from a PostgreSQL relation to an Iceberg catalog table.';
COMMENT ON COLUMN pgiceberg.table_bindings.relid IS
  'OID of the PostgreSQL relation that uses the pgiceberg iceberg table access method.';
COMMENT ON COLUMN pgiceberg.table_bindings.catalog IS
  'Local pgiceberg catalog name from pgiceberg.catalogs.';
COMMENT ON COLUMN pgiceberg.table_bindings.namespace IS
  'Iceberg namespace used to load the table.';
COMMENT ON COLUMN pgiceberg.table_bindings.table_name IS
  'Iceberg table name used to load the table.';

-- UNLOGGED so mirror progress/status DML is not WAL-logged. Otherwise every
-- successful apply would leave catalog updates in the same replication slot
-- that consumes source-table changes (and an in-function drain cannot see
-- those updates until the surrounding transaction commits).
CREATE UNLOGGED TABLE pgiceberg.logical_mirrors (
  source_relid oid PRIMARY KEY,
  catalog text NOT NULL,
  namespace text NOT NULL,
  table_name text NOT NULL,
  slot_name name NOT NULL UNIQUE,
  enabled boolean NOT NULL DEFAULT true,
  batch_size integer NOT NULL DEFAULT 1024 CHECK (batch_size > 0),
  last_flushed_lsn pg_lsn,
  last_error text,
  created_at timestamptz NOT NULL DEFAULT now(),
  updated_at timestamptz NOT NULL DEFAULT now()
) USING heap;

COMMENT ON TABLE pgiceberg.logical_mirrors IS
  'Append-only logical decoding mirrors from PostgreSQL heap tables to Iceberg tables. UNLOGGED so progress updates do not enter mirror replication slots.';
COMMENT ON COLUMN pgiceberg.logical_mirrors.source_relid IS
  'OID of the PostgreSQL source table consumed through logical decoding.';
COMMENT ON COLUMN pgiceberg.logical_mirrors.slot_name IS
  'Logical replication slot consumed by the pgiceberg background worker.';
COMMENT ON COLUMN pgiceberg.logical_mirrors.last_flushed_lsn IS
  'Latest LSN whose decoded changes were durably appended to Iceberg and then consumed from the slot (at-least-once).';

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

CREATE FUNCTION pgiceberg.table_am_handler(internal)
RETURNS table_am_handler
AS 'MODULE_PATHNAME', 'pgiceberg_table_am_handler'
LANGUAGE C STRICT;

CREATE ACCESS METHOD iceberg TYPE TABLE HANDLER pgiceberg.table_am_handler;

COMMENT ON ACCESS METHOD iceberg IS
  'Native pgiceberg table access method for Apache Iceberg tables.';

CREATE FUNCTION pgiceberg.parquet_fdw_handler()
RETURNS fdw_handler
AS 'MODULE_PATHNAME', 'pgiceberg_parquet_fdw_handler'
LANGUAGE C STRICT;

CREATE FUNCTION pgiceberg.parquet_fdw_validator(text[], oid)
RETURNS void
AS 'MODULE_PATHNAME', 'pgiceberg_parquet_fdw_validator'
LANGUAGE C STRICT;

CREATE FOREIGN DATA WRAPPER pgiceberg_parquet
HANDLER pgiceberg.parquet_fdw_handler
VALIDATOR pgiceberg.parquet_fdw_validator;

CREATE FUNCTION pgiceberg.avro_fdw_handler()
RETURNS fdw_handler
AS 'MODULE_PATHNAME', 'pgiceberg_avro_fdw_handler'
LANGUAGE C STRICT;

CREATE FUNCTION pgiceberg.avro_fdw_validator(text[], oid)
RETURNS void
AS 'MODULE_PATHNAME', 'pgiceberg_avro_fdw_validator'
LANGUAGE C STRICT;

CREATE FOREIGN DATA WRAPPER pgiceberg_avro
HANDLER pgiceberg.avro_fdw_handler
VALIDATOR pgiceberg.avro_fdw_validator;

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

REVOKE EXECUTE ON FUNCTION pgiceberg.create_table(text, text, text, text[], regtype[], boolean[], boolean, integer) FROM PUBLIC;

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

REVOKE EXECUTE ON FUNCTION pgiceberg.register_table(text, text, text, text, boolean) FROM PUBLIC;

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

REVOKE EXECUTE ON FUNCTION pgiceberg.register_table_from_location(text, text, text, text, boolean) FROM PUBLIC;

-- Append-only logical decoding mirrors.
CREATE FUNCTION pgiceberg.create_logical_mirror(
  src regclass,
  catalog text,
  namespace text,
  table_name text,
  slot_name text DEFAULT NULL,
  create_iceberg_table boolean DEFAULT true,
  batch_size integer DEFAULT 1024
)
RETURNS void
LANGUAGE plpgsql SECURITY DEFINER
SET search_path = pg_catalog, pgiceberg
AS $$
DECLARE
  source_oid oid := src::oid;
  resolved_slot name := COALESCE(
    slot_name,
    'pgiceberg_' || source_oid::text
  )::name;
  column_names text[];
  column_types regtype[];
  column_required boolean[];
  source_kind "char";
  previous_slot name;
BEGIN
  IF NOT has_table_privilege(session_user, src, 'SELECT') THEN
    RAISE EXCEPTION 'permission denied for table %', src
      USING ERRCODE = '42501';
  END IF;

  SELECT relkind
  INTO source_kind
  FROM pg_class
  WHERE oid = source_oid;

  IF source_kind IS DISTINCT FROM 'r' THEN
    RAISE EXCEPTION 'pgiceberg logical mirrors require a heap source table'
      USING ERRCODE = '0A000';
  END IF;

  IF batch_size <= 0 THEN
    RAISE EXCEPTION 'batch_size must be greater than zero'
      USING ERRCODE = '22023';
  END IF;

  -- Block concurrent writers while establishing the slot so committed inserts
  -- between setup steps are not missed by an append-only "from now" mirror.
  EXECUTE format('LOCK TABLE %s IN SHARE ROW EXCLUSIVE MODE', src);

  SELECT
    array_agg(attname::text ORDER BY attnum),
    array_agg(atttypid::regtype ORDER BY attnum),
    array_agg(attnotnull ORDER BY attnum)
  INTO column_names, column_types, column_required
  FROM pg_attribute
  WHERE attrelid = source_oid
    AND attnum > 0
    AND NOT attisdropped;

  IF column_names IS NULL THEN
    RAISE EXCEPTION 'source table % has no columns', src
      USING ERRCODE = '42P16';
  END IF;

  SELECT logical_mirrors.slot_name
  INTO previous_slot
  FROM pgiceberg.logical_mirrors
  WHERE source_relid = source_oid;

  -- Create the replication slot before Iceberg table setup so the slot's
  -- start point is established as early as possible under the table lock.
  IF NOT EXISTS (
    SELECT 1
    FROM pg_replication_slots
    WHERE pg_replication_slots.slot_name = resolved_slot::text
  ) THEN
    PERFORM pg_create_logical_replication_slot(resolved_slot, 'pgiceberg');
  END IF;

  IF create_iceberg_table THEN
    PERFORM pgiceberg.create_table(
      catalog,
      namespace,
      table_name,
      column_names,
      column_types,
      column_required,
      false,
      2
    );
  END IF;

  INSERT INTO pgiceberg.logical_mirrors (
    source_relid,
    catalog,
    namespace,
    table_name,
    slot_name,
    enabled,
    batch_size,
    last_error,
    created_at,
    updated_at
  )
  VALUES (
    source_oid,
    catalog,
    namespace,
    table_name,
    resolved_slot,
    true,
    batch_size,
    NULL,
    now(),
    now()
  )
  ON CONFLICT (source_relid) DO UPDATE
  SET catalog = EXCLUDED.catalog,
      namespace = EXCLUDED.namespace,
      table_name = EXCLUDED.table_name,
      slot_name = EXCLUDED.slot_name,
      enabled = true,
      batch_size = EXCLUDED.batch_size,
      last_error = NULL,
      updated_at = now();

  IF previous_slot IS NOT NULL
     AND previous_slot IS DISTINCT FROM resolved_slot
     AND EXISTS (
       SELECT 1
       FROM pg_replication_slots
       WHERE pg_replication_slots.slot_name = previous_slot::text
     ) THEN
    PERFORM pg_drop_replication_slot(previous_slot);
  END IF;
END;
$$;

COMMENT ON FUNCTION pgiceberg.create_logical_mirror(regclass, text, text, text, text, boolean, integer) IS
  'Create or replace an append-only logical decoding mirror from a PostgreSQL heap table to an Iceberg table. Starts from the slot creation LSN (no automatic backfill). Requires SELECT on the source table.';

REVOKE EXECUTE ON FUNCTION pgiceberg.create_logical_mirror(regclass, text, text, text, text, boolean, integer) FROM PUBLIC;

CREATE FUNCTION pgiceberg.drop_logical_mirror(
  src regclass,
  drop_slot boolean DEFAULT true
)
RETURNS void
LANGUAGE plpgsql SECURITY DEFINER
SET search_path = pg_catalog, pgiceberg
AS $$
DECLARE
  mirror_slot name;
BEGIN
  IF NOT has_table_privilege(session_user, src, 'SELECT') THEN
    RAISE EXCEPTION 'permission denied for table %', src
      USING ERRCODE = '42501';
  END IF;

  SELECT slot_name
  INTO mirror_slot
  FROM pgiceberg.logical_mirrors
  WHERE source_relid = src::oid;

  DELETE FROM pgiceberg.logical_mirrors
  WHERE source_relid = src::oid;

  IF drop_slot AND mirror_slot IS NOT NULL THEN
    PERFORM pg_drop_replication_slot(mirror_slot)
    WHERE EXISTS (
      SELECT 1
      FROM pg_replication_slots
      WHERE pg_replication_slots.slot_name = mirror_slot::text
    );
  END IF;
END;
$$;

COMMENT ON FUNCTION pgiceberg.drop_logical_mirror(regclass, boolean) IS
  'Remove a pgiceberg logical mirror and optionally drop its logical replication slot. Call before DROP EXTENSION to avoid orphaned slots.';

REVOKE EXECUTE ON FUNCTION pgiceberg.drop_logical_mirror(regclass, boolean) FROM PUBLIC;

CREATE FUNCTION pgiceberg.logical_mirror_status()
RETURNS TABLE (
  source_relid oid,
  source_table regclass,
  catalog text,
  namespace text,
  table_name text,
  slot_name name,
  enabled boolean,
  batch_size integer,
  last_flushed_lsn pg_lsn,
  restart_lsn pg_lsn,
  confirmed_flush_lsn pg_lsn,
  active boolean,
  last_error text
)
LANGUAGE sql STABLE SECURITY DEFINER
SET search_path = pg_catalog, pgiceberg
AS $$
  SELECT
    m.source_relid,
    m.source_relid::regclass,
    m.catalog,
    m.namespace,
    m.table_name,
    m.slot_name,
    m.enabled,
    m.batch_size,
    m.last_flushed_lsn,
    s.restart_lsn,
    s.confirmed_flush_lsn,
    s.active,
    m.last_error
  FROM pgiceberg.logical_mirrors AS m
  LEFT JOIN pg_replication_slots AS s
    ON s.slot_name = m.slot_name::text
  ORDER BY m.source_relid;
$$;

COMMENT ON FUNCTION pgiceberg.logical_mirror_status() IS
  'Return pgiceberg logical mirror metadata and replication slot progress.';

CREATE FUNCTION pgiceberg.process_logical_mirrors()
RETURNS void
AS 'MODULE_PATHNAME', 'pgiceberg_process_logical_mirrors'
LANGUAGE C;

COMMENT ON FUNCTION pgiceberg.process_logical_mirrors() IS
  'Process enabled logical mirrors once: append INSERT changes to Iceberg, then advance each slot. Delivery is at-least-once.';

REVOKE EXECUTE ON FUNCTION pgiceberg.process_logical_mirrors() FROM PUBLIC;
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
