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

-- Underprivileged role for SQLsmith. Avoid superuser so generated queries
-- cannot call administrative functions such as pg_terminate_backend.

DO $$
BEGIN
  IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'fuzz_user') THEN
    CREATE ROLE fuzz_user LOGIN PASSWORD 'fuzz';
  END IF;
END $$;

GRANT CONNECT ON DATABASE pgiceberg_fuzz TO fuzz_user;
GRANT USAGE ON SCHEMA public TO fuzz_user;
GRANT USAGE ON SCHEMA fuzz_native TO fuzz_user;
GRANT USAGE ON SCHEMA fuzz_dataset TO fuzz_user;

GRANT SELECT, INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA public TO fuzz_user;
GRANT SELECT, INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA fuzz_native TO fuzz_user;
GRANT SELECT, INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA fuzz_dataset TO fuzz_user;

ALTER DEFAULT PRIVILEGES IN SCHEMA public
  GRANT SELECT, INSERT, UPDATE, DELETE ON TABLES TO fuzz_user;
ALTER DEFAULT PRIVILEGES IN SCHEMA fuzz_native
  GRANT SELECT, INSERT, UPDATE, DELETE ON TABLES TO fuzz_user;
ALTER DEFAULT PRIVILEGES IN SCHEMA fuzz_dataset
  GRANT SELECT, INSERT, UPDATE, DELETE ON TABLES TO fuzz_user;

GRANT USAGE ON FOREIGN SERVER fuzz_iceberg TO fuzz_user;
GRANT USAGE ON FOREIGN SERVER yellow_trip_server TO fuzz_user;
GRANT USAGE ON FOREIGN SERVER parquet_server TO fuzz_user;
GRANT USAGE ON FOREIGN SERVER avro_server TO fuzz_user;
