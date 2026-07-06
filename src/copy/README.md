# COPY support

This directory is reserved for future pgiceberg COPY support.

`COPY ... WITH (FORMAT iceberg)` is intentionally not supported today.
PostgreSQL 18 has internal COPY format routines, but it does not
provide a stable extension API for registering an external COPY format.

Once PostgreSQL core provides a supported custom COPY format API, pgiceberg can
use this directory for the Iceberg COPY format handler implementation.

Reference: https://www.postgresql.org/message-id/flat/20231204.153548.2126325458835528809.kou%40clear-code.com
