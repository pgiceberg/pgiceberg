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

-- Commit identifiers, recovery-log SQL, and Iceberg snapshot rollback. Snapshot
-- ids are random, so they are applied through DO blocks and never printed.

CREATE EXTENSION pgiceberg;

\pset format unaligned
\set VERBOSITY terse

\! rm -f /tmp/pgiceberg_catalog_commit_recovery_regress.db
\! rm -rf /tmp/pgiceberg_warehouse_commit_recovery_regress

SELECT pgiceberg.current_xact_commit_id() IS NULL AS commit_id_idle;

SELECT count(*) AS recovery_log_rows
FROM pgiceberg.commit_recovery_log();

SELECT count(*) AS reconcile_rows
FROM pgiceberg.reconcile_commits();

SELECT pgiceberg.repair_commit('ffffffffffffffffffffffffffffffff', 'rollback');

SELECT pgiceberg.repair_commit('not_hex', 'rollback');

DO $$
BEGIN
  PERFORM pgiceberg.add_catalog(
    'commit_recovery_regress',
    'sqlite',
    '/tmp/pgiceberg_catalog_commit_recovery_regress.db',
    '/tmp/pgiceberg_warehouse_commit_recovery_regress'
  );

  PERFORM pgiceberg.create_table(
    'commit_recovery_regress',
    'default',
    'commit_a',
    ARRAY['id'],
    ARRAY['bigint'::regtype],
    ARRAY[true],
    true
  );

  PERFORM pgiceberg.create_table(
    'commit_recovery_regress',
    'default',
    'commit_b',
    ARRAY['id'],
    ARRAY['bigint'::regtype],
    ARRAY[true],
    true
  );
END $$;

CREATE SERVER commit_recovery_iceberg
FOREIGN DATA WRAPPER pgiceberg
OPTIONS (catalog 'commit_recovery_regress');

CREATE FOREIGN TABLE commit_a (
  id bigint
)
SERVER commit_recovery_iceberg
OPTIONS (
  namespace 'default',
  table 'commit_a'
);

CREATE FOREIGN TABLE commit_b (
  id bigint
)
SERVER commit_recovery_iceberg
OPTIONS (
  namespace 'default',
  table 'commit_b'
);

BEGIN;
INSERT INTO commit_a VALUES (1);
INSERT INTO commit_b VALUES (10);
SELECT pgiceberg.current_xact_commit_id() IS NOT NULL AS commit_id_pending;
SELECT count(*) AS recovery_log_before_commit
FROM pgiceberg.commit_recovery_log();
COMMIT;

SELECT count(*) AS recovery_log_after_commit
FROM pgiceberg.commit_recovery_log();

SELECT count(*) AS reconcile_after_commit
FROM pgiceberg.reconcile_commits();

SELECT pgiceberg.current_xact_commit_id() IS NULL AS commit_id_after_commit;

BEGIN;
INSERT INTO commit_a VALUES (99);
ROLLBACK;

SELECT id
FROM commit_a
ORDER BY id;

SELECT count(*) AS recovery_log_after_rollback
FROM pgiceberg.commit_recovery_log();

-- Both tables published by the same PostgreSQL transaction share one commit id.
SELECT
  (
    SELECT s -> 'summary' ->> 'pgiceberg.xact.commit-id'
    FROM jsonb_array_elements(
      pgiceberg.table_metadata_json(
        'commit_recovery_regress',
        'default',
        'commit_a'
      ) -> 'snapshots'
    ) AS s
    ORDER BY (s ->> 'snapshot-id')::bigint
    LIMIT 1
  ) =
  (
    SELECT s -> 'summary' ->> 'pgiceberg.xact.commit-id'
    FROM jsonb_array_elements(
      pgiceberg.table_metadata_json(
        'commit_recovery_regress',
        'default',
        'commit_b'
      ) -> 'snapshots'
    ) AS s
    ORDER BY (s ->> 'snapshot-id')::bigint
    LIMIT 1
  ) AS shared_commit_id,
  (
    SELECT s -> 'summary' ->> 'pgiceberg.xact.commit-id'
    FROM jsonb_array_elements(
      pgiceberg.table_metadata_json(
        'commit_recovery_regress',
        'default',
        'commit_a'
      ) -> 'snapshots'
    ) AS s
    ORDER BY (s ->> 'snapshot-id')::bigint
    LIMIT 1
  ) IS NOT NULL AS commit_id_present;

INSERT INTO commit_a VALUES (2);

SELECT count(*) AS two_row_count
FROM commit_a;

DO $$
DECLARE
  first_snapshot bigint;
BEGIN
  SELECT (metadata -> 'snapshots' -> 0 ->> 'snapshot-id')::bigint
  INTO first_snapshot
  FROM (
    SELECT pgiceberg.table_metadata_json(
      'commit_recovery_regress',
      'default',
      'commit_a'
    ) AS metadata
  ) AS q;

  PERFORM pgiceberg.rollback_iceberg_snapshot(
    'commit_recovery_regress',
    'default',
    'commit_a',
    first_snapshot
  );
END $$;

SELECT id
FROM commit_a
ORDER BY id;

DROP FOREIGN TABLE commit_b;
DROP FOREIGN TABLE commit_a;
DROP SERVER commit_recovery_iceberg;
DROP EXTENSION pgiceberg;
