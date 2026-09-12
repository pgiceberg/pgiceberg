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

-- A leftover log after PostgreSQL COMMIT must not be treated as an orphan.
SELECT current_setting('data_directory') AS leftover_pgdata \gset
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
  ) AS leftover_cid,
  (
    SELECT s -> 'summary' ->> 'pgiceberg.xact.postgres-xid'
    FROM jsonb_array_elements(
      pgiceberg.table_metadata_json(
        'commit_recovery_regress',
        'default',
        'commit_a'
      ) -> 'snapshots'
    ) AS s
    ORDER BY (s ->> 'snapshot-id')::bigint
    LIMIT 1
  ) AS leftover_xid \gset
\setenv PGICEBERG_TEST_PGDATA :leftover_pgdata
\setenv PGICEBERG_TEST_CID :leftover_cid
\setenv PGICEBERG_TEST_XID :leftover_xid
\! python3 -c "import os, pathlib; pgdata=os.environ['PGICEBERG_TEST_PGDATA']; cid=os.environ['PGICEBERG_TEST_CID']; xid=os.environ['PGICEBERG_TEST_XID']; enc=lambda s: f'{len(s)} {s}'; path=pathlib.Path(pgdata)/'pg_iceberg'/'xact'/f'{cid}.log'; path.parent.mkdir(parents=True, exist_ok=True); path.write_text('pgiceberg-commit-recovery 1\ncommit_id '+enc(cid)+'\npostgres_xid '+enc(xid)+'\nstate 17 iceberg_complete\ncreated_at 0\ntable_count 0\n')"

SELECT count(*) AS leftover_log_rows
FROM pgiceberg.commit_recovery_log();

SELECT verdict AS leftover_verdict
FROM pgiceberg.reconcile_commits();

SELECT count(*) AS leftover_log_after_reconcile
FROM pgiceberg.commit_recovery_log();

BEGIN;
INSERT INTO commit_a VALUES (99);
ROLLBACK;

SELECT id
FROM commit_a
ORDER BY id;

SELECT count(*) AS recovery_log_after_rollback
FROM pgiceberg.commit_recovery_log();

BEGIN;
SAVEPOINT s1;
INSERT INTO commit_a VALUES (50);
SELECT pgiceberg.current_xact_commit_id() IS NOT NULL AS commit_id_in_savepoint;
ROLLBACK TO SAVEPOINT s1;
SELECT pgiceberg.current_xact_commit_id() IS NULL AS commit_id_after_savepoint_rollback;
COMMIT;

BEGIN;
INSERT INTO commit_a VALUES (51);
SAVEPOINT s2;
INSERT INTO commit_a VALUES (52);
ROLLBACK TO SAVEPOINT s2;
SELECT pgiceberg.current_xact_commit_id() IS NOT NULL AS commit_id_after_partial_savepoint;
ROLLBACK;

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

-- A recovery log still marked pending must inspect Iceberg before deleting.
INSERT INTO commit_a VALUES (2);

SELECT current_setting('data_directory') AS pending_pgdata \gset
SELECT catalog_type AS pending_catalog_type,
       catalog_uri AS pending_catalog_uri,
       warehouse AS pending_warehouse,
       iceberg_catalog_name AS pending_catalog_name
FROM pgiceberg.catalogs
WHERE name = 'commit_recovery_regress' \gset
SELECT
  (
    SELECT s -> 'summary' ->> 'pgiceberg.xact.commit-id'
    FROM jsonb_array_elements(
      pgiceberg.table_metadata_json(
        'commit_recovery_regress',
        'default',
        'commit_a'
      ) -> 'snapshots'
    ) WITH ORDINALITY AS t(s, n)
    ORDER BY n DESC
    LIMIT 1
  ) AS pending_cid,
  (
    SELECT (s ->> 'snapshot-id')::bigint
    FROM jsonb_array_elements(
      pgiceberg.table_metadata_json(
        'commit_recovery_regress',
        'default',
        'commit_a'
      ) -> 'snapshots'
    ) WITH ORDINALITY AS t(s, n)
    ORDER BY n
    LIMIT 1
  ) AS pending_base_snapshot \gset
\setenv PGICEBERG_TEST_PGDATA :pending_pgdata
\setenv PGICEBERG_TEST_CID :pending_cid
\setenv PGICEBERG_TEST_BASE :pending_base_snapshot
\setenv PGICEBERG_TEST_CATALOG_TYPE :pending_catalog_type
\setenv PGICEBERG_TEST_CATALOG_URI :pending_catalog_uri
\setenv PGICEBERG_TEST_WAREHOUSE :pending_warehouse
\setenv PGICEBERG_TEST_CATALOG_NAME :pending_catalog_name
\! python3 -c "import os, pathlib; enc=lambda s: f'{len(s)} {s}'; cid=os.environ['PGICEBERG_TEST_CID']; path=pathlib.Path(os.environ['PGICEBERG_TEST_PGDATA'])/'pg_iceberg'/'xact'/f'{cid}.log'; path.parent.mkdir(parents=True, exist_ok=True); path.write_text('pgiceberg-commit-recovery 1\ncommit_id '+enc(cid)+'\npostgres_xid 1 0\nstate 9 preparing\ncreated_at 0\ntable_count 1\ntable\ncatalog '+enc('commit_recovery_regress')+'\ncatalog_type '+enc(os.environ['PGICEBERG_TEST_CATALOG_TYPE'])+'\ncatalog_uri '+enc(os.environ['PGICEBERG_TEST_CATALOG_URI'])+'\nwarehouse '+enc(os.environ['PGICEBERG_TEST_WAREHOUSE'])+'\ncatalog_name '+enc(os.environ['PGICEBERG_TEST_CATALOG_NAME'])+'\nnamespace '+enc('default')+'\ntable_name '+enc('commit_a')+'\nbase_snapshot_id '+os.environ['PGICEBERG_TEST_BASE']+'\ncommitted_snapshot_id none\niceberg_state '+enc('pending')+'\n')"

SELECT pgiceberg.repair_commit(:'pending_cid', 'rollback')
  LIKE 'rolled back Iceberg snapshots for commit %' AS pending_repair;

SELECT id
FROM commit_a
ORDER BY id;

SELECT count(*) AS pending_log_after_repair
FROM pgiceberg.commit_recovery_log();

DROP FOREIGN TABLE commit_b;
DROP FOREIGN TABLE commit_a;
DROP SERVER commit_recovery_iceberg;
DROP EXTENSION pgiceberg;
