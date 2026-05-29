CREATE EXTENSION pgiceberg;

\pset format unaligned

SELECT fdwname
FROM pg_foreign_data_wrapper
WHERE fdwname = 'pgiceberg';

\set VERBOSITY terse

CREATE SERVER invalid_option
FOREIGN DATA WRAPPER pgiceberg
OPTIONS (unknown_option 'value');

CREATE SERVER invalid_catalog_type
FOREIGN DATA WRAPPER pgiceberg
OPTIONS (catalog_type 'invalid');

CREATE SERVER rest_catalog
FOREIGN DATA WRAPPER pgiceberg
OPTIONS (catalog_type 'rest');

\set VERBOSITY default

DROP EXTENSION pgiceberg;
