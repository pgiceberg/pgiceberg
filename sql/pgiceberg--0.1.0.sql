\echo Use "CREATE EXTENSION pgiceberg" to load this file. \quit

CREATE SCHEMA pgiceberg;

CREATE FUNCTION pgiceberg.fdw_handler()
RETURNS fdw_handler
AS 'MODULE_PATHNAME', 'pgiceberg_fdw_handler'
LANGUAGE C STRICT;

CREATE FUNCTION pgiceberg.fdw_validator(text[], oid)
RETURNS void
AS 'MODULE_PATHNAME', 'pgiceberg_fdw_validator'
LANGUAGE C STRICT;

CREATE FUNCTION pgiceberg.create_table(
  catalog_type text,
  catalog_uri text,
  warehouse text,
  namespace text,
  table_name text,
  column_names text[],
  column_types regtype[],
  column_required boolean[],
  drop_if_exists boolean DEFAULT false,
  catalog_name text DEFAULT 'pgiceberg',
  format_version integer DEFAULT 2
)
RETURNS void
AS 'MODULE_PATHNAME', 'pgiceberg_create_table'
LANGUAGE C STRICT;

CREATE FOREIGN DATA WRAPPER pgiceberg
HANDLER pgiceberg.fdw_handler
VALIDATOR pgiceberg.fdw_validator;
