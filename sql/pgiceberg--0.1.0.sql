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

CREATE TABLE pgiceberg.column_bindings (
  relid oid NOT NULL REFERENCES pgiceberg.table_bindings(relid) ON DELETE CASCADE,
  attnum smallint NOT NULL,
  field_id integer NOT NULL CHECK (field_id > 0),
  PRIMARY KEY (relid, attnum),
  UNIQUE (relid, field_id)
) USING heap;

COMMENT ON TABLE pgiceberg.column_bindings IS
  'Native table access method mapping from PostgreSQL attributes to Iceberg field ids.';
COMMENT ON COLUMN pgiceberg.column_bindings.relid IS
  'OID of the PostgreSQL iceberg table.';
COMMENT ON COLUMN pgiceberg.column_bindings.attnum IS
  'PostgreSQL attribute number.';
COMMENT ON COLUMN pgiceberg.column_bindings.field_id IS
  'Iceberg schema field id that survives rename and reordering.';

-- The logical output plugin excludes the pgiceberg schema, so this control
-- table can remain crash-safe without feeding its progress updates back into
-- mirror replication slots.
CREATE TABLE pgiceberg.logical_mirrors (
  source_relid oid PRIMARY KEY,
  catalog text NOT NULL,
  namespace text NOT NULL,
  table_name text NOT NULL,
  slot_name name NOT NULL UNIQUE,
  UNIQUE (catalog, namespace, table_name),
  enabled boolean NOT NULL DEFAULT true,
  state text NOT NULL DEFAULT 'ready'
    CHECK (state IN ('backfilling', 'ready', 'error')),
  batch_size integer NOT NULL DEFAULT 1024 CHECK (batch_size > 0),
  initial_snapshot_lsn pg_lsn,
  backfill_rows bigint NOT NULL DEFAULT 0 CHECK (backfill_rows >= 0),
  last_flushed_lsn pg_lsn,
  last_applied_batch_id text,
  last_error text,
  created_at timestamptz NOT NULL DEFAULT now(),
  updated_at timestamptz NOT NULL DEFAULT now()
) USING heap;

COMMENT ON TABLE pgiceberg.logical_mirrors IS
  'Crash-safe logical decoding mirror configuration and progress from PostgreSQL heap tables to Iceberg tables.';
COMMENT ON COLUMN pgiceberg.logical_mirrors.source_relid IS
  'OID of the PostgreSQL source table consumed through logical decoding.';
COMMENT ON COLUMN pgiceberg.logical_mirrors.slot_name IS
  'Logical replication slot consumed by the pgiceberg background worker.';
COMMENT ON COLUMN pgiceberg.logical_mirrors.state IS
  'Mirror lifecycle state: backfilling, ready for streaming, or stopped on error.';
COMMENT ON COLUMN pgiceberg.logical_mirrors.initial_snapshot_lsn IS
  'Consistent point returned when the logical replication slot was created.';
COMMENT ON COLUMN pgiceberg.logical_mirrors.backfill_rows IS
  'Number of source rows copied before the mirror entered ready state.';
COMMENT ON COLUMN pgiceberg.logical_mirrors.last_flushed_lsn IS
  'Latest LSN whose decoded changes were durably appended to Iceberg and then consumed from the slot.';
COMMENT ON COLUMN pgiceberg.logical_mirrors.last_applied_batch_id IS
  'SHA-256 identity of the latest slot prefix durably applied to Iceberg and consumed.';

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

-- Internal bounded initial-copy primitive. Mirror creation holds the source
-- write barrier before calling it; direct execution is intentionally revoked.
CREATE FUNCTION pgiceberg.backfill_logical_mirror(
  src regclass,
  catalog text,
  namespace text,
  table_name text,
  batch_size integer,
  source_lsn text,
  require_empty_target boolean
)
RETURNS bigint
AS 'MODULE_PATHNAME', 'pgiceberg_backfill_logical_mirror'
LANGUAGE C STRICT SECURITY DEFINER
SET search_path = pg_catalog, pgiceberg;

REVOKE EXECUTE ON FUNCTION pgiceberg.backfill_logical_mirror(regclass, text, text, text, integer, text, boolean) FROM PUBLIC;

-- INSERT-only logical decoding mirrors with an optional consistent backfill.
CREATE FUNCTION pgiceberg.create_logical_mirror(
  src regclass,
  catalog text,
  namespace text,
  table_name text,
  slot_name text DEFAULT NULL,
  create_iceberg_table boolean DEFAULT true,
  batch_size integer DEFAULT 1024,
  backfill boolean DEFAULT true
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
  initial_lsn pg_lsn;
  copied_rows bigint := 0;
  slot_exists boolean;
  created_slot boolean := false;
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

  IF EXISTS (
    SELECT 1
    FROM pg_class AS c
    JOIN pg_namespace AS n ON n.oid = c.relnamespace
    WHERE c.oid = source_oid
      AND n.nspname = 'pgiceberg'
  ) THEN
    RAISE EXCEPTION 'relations in schema pgiceberg cannot be logical mirror sources'
      USING ERRCODE = '0A000';
  END IF;

  IF batch_size <= 0 THEN
    RAISE EXCEPTION 'batch_size must be greater than zero'
      USING ERRCODE = '22023';
  END IF;

  IF backfill AND current_setting('transaction_isolation') <> 'read committed' THEN
    RAISE EXCEPTION 'consistent logical mirror backfill requires READ COMMITTED isolation'
      USING ERRCODE = '0A000';
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

  IF EXISTS (
    SELECT 1
    FROM pgiceberg.logical_mirrors AS m
    WHERE m.catalog = $2
      AND m.namespace = $3
      AND m.table_name = $4
      AND m.source_relid <> source_oid
  ) THEN
    RAISE EXCEPTION 'Iceberg target %.%.% already belongs to another logical mirror',
      catalog, namespace, table_name
      USING ERRCODE = '23505';
  END IF;

  SELECT EXISTS (
    SELECT 1
    FROM pg_replication_slots
    WHERE pg_replication_slots.slot_name = resolved_slot::text
  )
  INTO slot_exists;

  IF backfill AND (slot_exists OR previous_slot IS NOT NULL) THEN
    RAISE EXCEPTION 'consistent backfill requires a new logical mirror and slot'
      USING ERRCODE = '55000',
            HINT = 'Drop the existing mirror and slot first, or call with backfill => false.';
  END IF;

  -- Create the replication slot before Iceberg table setup so the slot's
  -- start point is established as early as possible under the table lock.
  IF NOT slot_exists THEN
    SELECT lsn
    INTO initial_lsn
    FROM pg_create_logical_replication_slot(resolved_slot, 'pgiceberg');
    created_slot := true;
  ELSE
    SELECT COALESCE(confirmed_flush_lsn, restart_lsn)
    INTO initial_lsn
    FROM pg_replication_slots
    WHERE pg_replication_slots.slot_name = resolved_slot::text;
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
    state,
    batch_size,
    initial_snapshot_lsn,
    backfill_rows,
    last_flushed_lsn,
    last_applied_batch_id,
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
    CASE WHEN backfill THEN 'backfilling' ELSE 'ready' END,
    batch_size,
    initial_lsn,
    0,
    NULL,
    NULL,
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
      state = EXCLUDED.state,
      batch_size = EXCLUDED.batch_size,
      initial_snapshot_lsn = EXCLUDED.initial_snapshot_lsn,
      backfill_rows = 0,
      last_flushed_lsn = NULL,
      last_applied_batch_id = NULL,
      last_error = NULL,
      updated_at = now();

  IF backfill THEN
    copied_rows := pgiceberg.backfill_logical_mirror(
      src,
      catalog,
      namespace,
      table_name,
      batch_size,
      initial_lsn::text,
      NOT create_iceberg_table
    );

    UPDATE pgiceberg.logical_mirrors
    SET state = 'ready',
        backfill_rows = copied_rows,
        updated_at = now()
    WHERE source_relid = source_oid;
  END IF;

  IF previous_slot IS NOT NULL
     AND previous_slot IS DISTINCT FROM resolved_slot
     AND EXISTS (
       SELECT 1
       FROM pg_replication_slots
       WHERE pg_replication_slots.slot_name = previous_slot::text
     ) THEN
    PERFORM pg_drop_replication_slot(previous_slot);
  END IF;
EXCEPTION WHEN OTHERS THEN
  IF created_slot
     AND EXISTS (
       SELECT 1
       FROM pg_replication_slots
       WHERE pg_replication_slots.slot_name = resolved_slot::text
         AND NOT active
     ) THEN
    PERFORM pg_drop_replication_slot(resolved_slot);
  END IF;
  RAISE;
END;
$$;

COMMENT ON FUNCTION pgiceberg.create_logical_mirror(regclass, text, text, text, text, boolean, integer, boolean) IS
  'Create an INSERT-only logical decoding mirror from a PostgreSQL heap table to Iceberg, with a consistent initial backfill by default. Requires SELECT on the source table.';

REVOKE EXECUTE ON FUNCTION pgiceberg.create_logical_mirror(regclass, text, text, text, text, boolean, integer, boolean) FROM PUBLIC;

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
  state text,
  batch_size integer,
  initial_snapshot_lsn pg_lsn,
  backfill_rows bigint,
  last_flushed_lsn pg_lsn,
  last_applied_batch_id text,
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
    m.state,
    m.batch_size,
    m.initial_snapshot_lsn,
    m.backfill_rows,
    m.last_flushed_lsn,
    m.last_applied_batch_id,
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
  'Process ready logical mirrors once: idempotently append INSERT batches to Iceberg, then advance each slot.';

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

CREATE FUNCTION pgiceberg.schema_diff_json(rel regclass)
RETURNS jsonb
AS 'MODULE_PATHNAME', 'pgiceberg_schema_diff_json'
LANGUAGE C STRICT SECURITY DEFINER
SET search_path = pg_catalog, pgiceberg
;

COMMENT ON FUNCTION pgiceberg.schema_diff_json(regclass) IS
  'Compare a foreign table or iceberg table with the current Iceberg schema and return JSON schema changes.';

CREATE FUNCTION pgiceberg.schema_diff(rel regclass)
RETURNS TABLE (
  change text,
  local_column text,
  local_type text,
  iceberg_field_id integer,
  iceberg_name text,
  iceberg_type text,
  detail text
)
LANGUAGE sql STRICT SECURITY DEFINER
SET search_path = pg_catalog, pgiceberg
AS $$
  SELECT
    x.change,
    x.local_column,
    x.local_type,
    x.iceberg_field_id,
    x.iceberg_name,
    x.iceberg_type,
    x.detail
  FROM jsonb_to_recordset(pgiceberg.schema_diff_json($1)) AS x(
    change text,
    local_column text,
    local_type text,
    iceberg_field_id integer,
    iceberg_name text,
    iceberg_type text,
    detail text
  )
  ORDER BY x.iceberg_field_id NULLS LAST, x.local_column NULLS LAST, x.change;
$$;

COMMENT ON FUNCTION pgiceberg.schema_diff(regclass) IS
  'Compare a foreign table or iceberg table with the current Iceberg schema.';

CREATE FUNCTION pgiceberg.refresh_schema(rel regclass)
RETURNS void
AS 'MODULE_PATHNAME', 'pgiceberg_refresh_schema'
LANGUAGE C STRICT SECURITY DEFINER
SET search_path = pg_catalog, pgiceberg
;

COMMENT ON FUNCTION pgiceberg.refresh_schema(regclass) IS
  'Apply ALTER FOREIGN TABLE statements so the local definition matches the current Iceberg schema, preserving field ids.';

REVOKE EXECUTE ON FUNCTION pgiceberg.refresh_schema(regclass) FROM PUBLIC;

CREATE FUNCTION pgiceberg.update_schema(
  name text,
  namespace text,
  table_name text,
  add_names text[],
  add_types regtype[],
  drop_names text[],
  rename_from text[],
  rename_to text[]
)
RETURNS void
AS 'MODULE_PATHNAME', 'pgiceberg_update_schema'
LANGUAGE C STRICT SECURITY DEFINER
SET search_path = pg_catalog, pgiceberg
;

COMMENT ON FUNCTION pgiceberg.update_schema(text, text, text, text[], regtype[], text[], text[], text[]) IS
  'Apply Iceberg schema updates: add optional columns, drop columns, and rename columns.';

REVOKE EXECUTE ON FUNCTION pgiceberg.update_schema(text, text, text, text[], regtype[], text[], text[], text[]) FROM PUBLIC;
