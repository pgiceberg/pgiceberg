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

CREATE EXTENSION pgiceberg;

\pset format unaligned
\set VERBOSITY terse

CREATE SERVER avro_server FOREIGN DATA WRAPPER pgiceberg_avro;

CREATE FOREIGN TABLE avro_manifest_entries (
  status integer,
  snapshot_id bigint,
  sequence_number bigint,
  file_sequence_number bigint
)
SERVER avro_server
OPTIONS (
  filename '/tmp/pgiceberg_yellow_trip_dataset_regress/default/yellow_trip/metadata/81fc6f9d-c034-4d6b-ad00-895e23ab2189-m0.avro'
);

SELECT count(*) AS row_count,
       min(status) AS min_status,
       max(status) AS max_status,
       min(snapshot_id) AS min_snapshot_id,
       max(snapshot_id) AS max_snapshot_id
FROM avro_manifest_entries;

\set VERBOSITY default

DROP FOREIGN TABLE avro_manifest_entries;
DROP SERVER avro_server;
DROP EXTENSION pgiceberg;
