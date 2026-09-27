# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""Local REST/S3 protocol fixture; no containers, cloud accounts, or Python packages."""

import base64
import collections
import hashlib
import http.server
import json
import re
import subprocess
import sys
import tempfile
import threading
import time
import urllib.parse
import uuid
import xml.etree.ElementTree as ET


class State:
    def __init__(self):
        self.lock = threading.RLock()
        self.objects = {}
        self.uploads = {}
        self.tokens = {}
        self.storage_tokens = {}
        self.requests = collections.Counter()
        self.refreshes = collections.Counter()
        self.s3_requests = []

    def credential(self, lifetime):
        n = len(self.storage_tokens) + 1
        key, token = f"vended-{n}", f"storage-{n}"
        expiry = int(time.time() * 1000) + lifetime
        self.storage_tokens[key] = (token, expiry)
        return {"prefix": "s3://bucket/default/events", "config": {
            "s3.access-key-id": key, "s3.secret-access-key": "vended-secret",
            "s3.session-token": token, "s3.session-token-expires-at-ms": str(expiry)}}


class Handler(http.server.BaseHTTPRequestHandler):
    protocol_version = "HTTP/1.1"

    def log_message(self, *args):
        pass

    def handle(self):
        try:
            super().handle()
        except ConnectionResetError:
            # Timeout tests deliberately close keep-alive connections.
            pass

    def body(self):
        if self.headers.get("Transfer-Encoding", "").lower() == "chunked":
            chunks = []
            while True:
                length = int(self.rfile.readline().split(b";", 1)[0], 16)
                if not length:
                    while self.rfile.readline() not in (b"\r\n", b"\n", b""):
                        pass
                    break
                chunks.append(self.rfile.read(length))
                self.rfile.read(2)
            data = b"".join(chunks)
        else:
            data = self.rfile.read(int(self.headers.get("Content-Length", "0")))
        if "aws-chunked" in self.headers.get("Content-Encoding", ""):
            chunks = []
            while data:
                header, data = data.split(b"\r\n", 1)
                length = int(header.split(b";", 1)[0], 16)
                if not length:
                    break
                chunks.append(data[:length])
                data = data[length + 2:]
            data = b"".join(chunks)
        return data

    def reply(self, code, body=b"", headers=None):
        if isinstance(body, (dict, list)):
            body = json.dumps(body).encode()
            headers = dict(headers or {}, **{"Content-Type": "application/json"})
        if isinstance(body, str):
            body = body.encode()
        self.send_response(code)
        for key, value in (headers or {}).items():
            self.send_header(key, str(value))
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        if self.command != "HEAD":
            try:
                self.wfile.write(body)
            except (BrokenPipeError, ConnectionResetError):
                pass

    def error(self, code):
        self.reply(code, {"error": {"code": code, "type": "RESTException", "message": "fixture error"}})

    def rest_auth(self, mode):
        header = self.headers.get("Authorization", "")
        if mode == "basic":
            return header == "Basic " + base64.b64encode(b"reader:test-password").decode()
        token = header.removeprefix("Bearer ")
        return self.server.state.tokens.get(token, 0) > time.time()

    def handle_request(self):
        request = urllib.parse.urlsplit(self.path)
        path = urllib.parse.unquote(request.path)
        query = urllib.parse.parse_qs(request.query, keep_blank_values=True)
        body = self.body()
        state = self.server.state
        if path in ("/slow", "/bucket/slow-s3"):
            with state.lock:
                state.requests[(self.command, path)] += 1
            time.sleep(4 if path.endswith("slow-s3") else 0.5)
            self.reply(200, {})
            return
        with state.lock:
            state.requests[(self.command, path)] += 1
            if path == "/retry":
                if state.requests[(self.command, path)] < 3:
                    self.reply(503, {"error": {"code": 403, "type": "RESTException", "message": "retry"}})
                else:
                    self.reply(200, {})
                return
            if path == "/forbidden":
                self.reply(403, {"error": {"code": 503, "type": "do-not-leak-type", "message": "do-not-leak-secret"}})
                return
            if path == "/redirect":
                self.reply(307, headers={"Location": "/redirect-target"})
                return
            if path == "/redirect-target":
                self.reply(200, {})
                return
            if path == "/retry-after":
                self.reply(503, {}, {"Retry-After": "30"})
                return
            if path == "/bad-token":
                self.reply(200, {"access_token": {"secret": "do-not-leak-token"},
                                 "token_type": "Bearer"})
                return
            if path == "/commit":
                self.error(503)
                return
            if path == "/token":
                form = urllib.parse.parse_qs(body.decode())
                grant = form.get("grant_type", [""])[0]
                if grant == "client_credentials":
                    if form.get("client_id") != ["client"] or form.get("client_secret") != ["test-client-secret"]:
                        self.error(401)
                        return
                elif grant != "urn:ietf:params:oauth:grant-type:token-exchange":
                    self.error(400)
                    return
                token = f"oauth-{len(state.tokens) + 1}"
                state.tokens[token] = time.time() + 1
                self.reply(200, {"access_token": token, "token_type": "Bearer", "expires_in": 1})
                return
            if path.startswith(("/basic/", "/oauth/")):
                mode = path.split("/")[1]
                if not self.rest_auth(mode):
                    self.error(401)
                    return
                if path.endswith("/v1/config"):
                    self.reply(200, {"defaults": {}, "overrides": {}})
                elif path.endswith("/credentials"):
                    state.refreshes[mode] += 1
                    self.reply(200, {"storage-credentials": [state.credential(5000)]})
                elif path.endswith("/tables/events"):
                    metadata_key = next(key for key in reversed(state.objects) if key.startswith("/bucket/default/events/") and key.endswith(".metadata.json"))
                    self.reply(200, {"metadata-location": "s3:/" + metadata_key,
                        "metadata": json.loads(state.objects[metadata_key]),
                        "config": {"credentials.uri": f"/{mode}/credentials"},
                        "storage-credentials": [state.credential(800)]})
                elif path.endswith("/metrics"):
                    self.reply(204)
                else:
                    self.error(404)
                return
            if not path.startswith("/bucket"):
                self.error(404)
                return
            auth = self.headers.get("Authorization", "")
            match = re.search(r"Credential=([^/]+)/", auth)
            key = match[1] if match else ""
            state.s3_requests.append((path, key))
            allowed = key == "test-key"
            if key in state.storage_tokens:
                token, expiry = state.storage_tokens[key]
                allowed = (self.headers.get("x-amz-security-token") == token and
                           expiry > time.time() * 1000 and
                           path.startswith("/bucket/default/events/"))
            if not allowed:
                self.reply(403, "<Error><Code>AccessDenied</Code><Message>denied</Message></Error>")
                return
            if self.command == "HEAD":
                if path in ("/bucket", "/bucket/"):
                    self.reply(200)
                elif path in state.objects:
                    self.reply(200, state.objects[path], {"ETag": '"fixture"'})
                else:
                    self.reply(404)
            elif self.command == "GET":
                if "list-type" in query or "prefix" in query:
                    self.reply(200, "<ListBucketResult><Name>bucket</Name><KeyCount>0</KeyCount><IsTruncated>false</IsTruncated></ListBucketResult>")
                elif path not in state.objects:
                    self.reply(404, "<Error><Code>NoSuchKey</Code></Error>")
                else:
                    data = state.objects[path]
                    if "Range" in self.headers:
                        start, end = self.headers["Range"].removeprefix("bytes=").split("-", 1)
                        start, end = int(start), int(end) if end else len(data) - 1
                        self.reply(206, data[start:end + 1], {"Content-Range": f"bytes {start}-{end}/{len(data)}"})
                    else:
                        self.reply(200, data)
            elif self.command == "PUT":
                if "uploadId" in query:
                    state.uploads[query["uploadId"][0]][1][int(query["partNumber"][0])] = body
                else:
                    state.objects[path] = body
                self.reply(200, headers={"ETag": '"' + hashlib.md5(body).hexdigest() + '"'})
            elif self.command == "POST":
                if "uploads" in query:
                    upload = str(uuid.uuid4())
                    state.uploads[upload] = (path, {})
                    self.reply(200, f"<InitiateMultipartUploadResult><Bucket>bucket</Bucket><Key>{path[8:]}</Key><UploadId>{upload}</UploadId></InitiateMultipartUploadResult>")
                elif "uploadId" in query:
                    key_path, parts = state.uploads.pop(query["uploadId"][0])
                    state.objects[key_path] = b"".join(value for _, value in sorted(parts.items()))
                    self.reply(200, '<CompleteMultipartUploadResult><ETag>"fixture"</ETag></CompleteMultipartUploadResult>')
                elif "delete" in query:
                    result = "<DeleteResult>"
                    for element in ET.fromstring(body).iter():
                        if element.tag.rsplit("}", 1)[-1] == "Key":
                            state.objects.pop("/bucket/" + element.text, None)
                            result += f"<Deleted><Key>{element.text}</Key></Deleted>"
                    self.reply(200, result + "</DeleteResult>")
                else:
                    self.error(400)
            elif self.command == "DELETE":
                if "uploadId" in query:
                    state.uploads.pop(query["uploadId"][0], None)
                else:
                    state.objects.pop(path, None)
                self.reply(204)

    do_GET = handle_request
    do_HEAD = handle_request
    do_POST = handle_request
    do_PUT = handle_request
    do_DELETE = handle_request


def check_postgres(bindir, extension_path, endpoint, temp):
    """Exercise the extension factory and mappings against the same wire fixture."""
    import pathlib
    import socket

    root = pathlib.Path(temp)
    data, sock = root / "pgdata", root / "socket"
    sock.mkdir()
    password_file = root / "password"
    password_file.write_text("unused-admin-password\n")
    password_file.chmod(0o600)
    subprocess.run([bindir + "/initdb", "-D", str(data), "-U", "network_admin",
                    "--auth-local=trust", "--auth-host=scram-sha-256",
                    "--pwfile=" + str(password_file), "--no-sync"],
                   check=True, stdout=subprocess.DEVNULL, timeout=30)
    with socket.socket() as probe:
        probe.bind(("127.0.0.1", 0))
        port = probe.getsockname()[1]
    with (data / "postgresql.conf").open("a") as config:
        config.write(f"\nport={port}\nlisten_addresses='127.0.0.1'\n")
        config.write(f"unix_socket_directories='{sock}'\n")
        config.write(f"extension_control_path='{extension_path}:$system'\n")
    control = [bindir + "/pg_ctl", "-D", str(data), "-w"]
    log = root / "postgres.log"
    subprocess.run(control + ["-l", str(log), "start"], check=True,
                   stdout=subprocess.DEVNULL, timeout=30)
    psql = [bindir + "/psql", "-X", "-q", "-A", "-t", "-v", "ON_ERROR_STOP=1",
            "-h", str(sock), "-p", str(port), "-U", "network_admin", "-d", "postgres"]

    def sql(command):
        result = subprocess.run(psql, input=command, text=True, capture_output=True,
                                timeout=40)
        if result.returncode:
            raise AssertionError(result.stderr)
        return result.stdout.strip()

    def literal(value):
        return "'" + value.replace("'", "''") + "'"

    try:
        sql("CREATE EXTENSION pgiceberg;")
        db_password = "database:pass/@ with sp'ace\\slash"
        sql(f"CREATE ROLE network_catalog_user LOGIN PASSWORD {literal(db_password)};"
            "GRANT CREATE ON SCHEMA public TO network_catalog_user;")
        for kind in ("sqlite", "sql"):
            uri = str(root / "extension.sqlite") if kind == "sqlite" else f"postgresql://127.0.0.1:{port}/postgres"
            db_options = (f", catalog_user 'network_catalog_user', catalog_password {literal(db_password)}"
                          if kind == "sql" else "")
            sql(f"""
                CREATE SERVER credentials_{kind} FOREIGN DATA WRAPPER pgiceberg
                OPTIONS (file_io 's3', s3_endpoint {literal(endpoint)}, s3_region 'us-east-1',
                         s3_path_style_access 'true', s3_max_retries '0');
                CREATE USER MAPPING FOR CURRENT_USER SERVER credentials_{kind}
                OPTIONS (s3_access_key_id 'test-key', s3_secret_access_key 'test-secret'{db_options});
                SELECT pgiceberg.add_catalog('network_{kind}', '{kind}', {literal(uri)},
                    's3://bucket/extension-{kind}', credential_server => 'credentials_{kind}');
                SELECT pgiceberg.create_table('network_{kind}', 'default', 'events',
                    ARRAY['id'], ARRAY['bigint'::regtype], ARRAY[true], true, 3);
                CREATE SERVER data_{kind} FOREIGN DATA WRAPPER pgiceberg OPTIONS (catalog 'network_{kind}');
                CREATE FOREIGN TABLE events_{kind} (id bigint) SERVER data_{kind} OPTIONS (table 'events');
                INSERT INTO events_{kind} VALUES (1), (2);
                UPDATE events_{kind} SET id = 3 WHERE id = 2;
                DELETE FROM events_{kind} WHERE id = 1;
            """)
            assert sql(f"SELECT count(*), sum(id) FROM events_{kind};") == "1|3"
            assert sql(f"SELECT catalog_uri NOT LIKE '%password%' FROM pgiceberg.catalogs WHERE name = 'network_{kind}';") == "t"
        sql(f"""
            CREATE SERVER rest_credentials FOREIGN DATA WRAPPER pgiceberg
            OPTIONS (rest_auth_type 'basic', s3_endpoint {literal(endpoint)},
                     s3_region 'us-east-1', s3_path_style_access 'true');
            CREATE USER MAPPING FOR CURRENT_USER SERVER rest_credentials
            OPTIONS (rest_username 'reader', rest_password 'test-password');
            SELECT pgiceberg.add_catalog('network_rest', 'rest', {literal(endpoint + '/basic')},
                's3://bucket', credential_server => 'rest_credentials');
            CREATE ROLE network_reader;
            GRANT USAGE ON SCHEMA pgiceberg TO network_reader;
            GRANT USAGE ON FOREIGN SERVER rest_credentials TO network_reader;
            CREATE USER MAPPING FOR network_reader SERVER rest_credentials
            OPTIONS (rest_username 'reader', rest_password 'wrong-password');
        """)
        assert sql("SELECT pgiceberg.table_format_version('network_rest', 'default', 'events');") in ("2", "3")
        sql("""
            SET ROLE network_reader;
            DO $$ BEGIN
                BEGIN
                    PERFORM pgiceberg.table_format_version('network_rest', 'default', 'events');
                    RAISE EXCEPTION 'SECURITY DEFINER used the owner mapping';
                EXCEPTION WHEN SQLSTATE 'HV000' THEN NULL;
                END;
            END $$;
        """)
        sql("ALTER USER MAPPING FOR network_reader SERVER rest_credentials OPTIONS (SET rest_password 'test-password');")
        assert sql("SET ROLE network_reader; SELECT pgiceberg.table_format_version('network_rest', 'default', 'events');") in ("2", "3")
        sql(f"""
            ALTER SERVER rest_credentials OPTIONS (SET rest_auth_type 'oauth2',
                ADD rest_oauth2_server_uri {literal(endpoint + '/token')});
            ALTER USER MAPPING FOR CURRENT_USER SERVER rest_credentials
                OPTIONS (ADD rest_credential 'client:test-client-secret');
            SELECT pgiceberg.add_catalog('network_rest', 'rest', {literal(endpoint + '/oauth')},
                's3://bucket', credential_server => 'rest_credentials');
        """)
        assert sql("SELECT pgiceberg.table_format_version('network_rest', 'default', 'events');") in ("2", "3")
        # Recovery must retain an explicit mapping requirement and resolve the
        # repair caller's current mapping even through SECURITY DEFINER.
        snapshot = sql("SELECT pgiceberg.table_metadata_json('network_sql', 'default', 'events')->>'current-snapshot-id';")
        commit_id = "eeeeeeeeeeeeeeeeeeeeeeeeeeeeee01"
        fields = {
            "catalog": "network_sql", "catalog_type": "sql",
            "catalog_uri": f"postgresql://127.0.0.1:{port}/postgres",
            "warehouse": "s3://bucket/extension-sql", "catalog_name": "network_sql",
            "namespace": "default", "table_name": "events",
            "credential_server": "credentials_sql",
        }
        encode = lambda value: f"{len(value)} {value}"
        record = (f"pgiceberg-commit-recovery 2\ncommit_id {encode(commit_id)}\n"
                  "postgres_xid 1 0\nstate 12 needs_repair\ncreated_at 0\ntable_count 1\ntable\n")
        record += "".join(f"{key} {encode(value)}\n" for key, value in fields.items())
        record += (f"credential_mapping_required 1\nbase_snapshot_id {snapshot}\n"
                   "committed_snapshot_id none\niceberg_state 7 pending\n")
        recovery = data / "pg_iceberg" / "xact" / (commit_id + ".log")
        recovery.parent.mkdir(parents=True, exist_ok=True)
        recovery.write_text(record)
        sql("GRANT USAGE ON FOREIGN SERVER credentials_sql TO network_reader;"
            "GRANT EXECUTE ON FUNCTION pgiceberg.repair_commit(text, text) TO network_reader;")
        sql(f"""
            SET ROLE network_reader;
            DO $$ BEGIN
                BEGIN
                    PERFORM pgiceberg.repair_commit('{commit_id}', 'rollback');
                    RAISE EXCEPTION 'repair lost its required caller mapping';
                EXCEPTION WHEN SQLSTATE '42704' THEN NULL;
                END;
            END $$;
        """)
        assert recovery.exists(), "failed repair removed the recovery log"
        sql(f"""
            CREATE USER MAPPING FOR network_reader SERVER credentials_sql
            OPTIONS (s3_access_key_id 'test-key', s3_secret_access_key 'test-secret',
                     catalog_user 'network_catalog_user', catalog_password {literal(db_password)});
        """)
        assert "rolled back" in sql(f"SET ROLE network_reader; SELECT pgiceberg.repair_commit('{commit_id}', 'rollback');")
        assert not recovery.exists(), "successful repair retained the recovery log"
    except Exception:
        print(log.read_text(), file=sys.stderr)
        raise
    finally:
        subprocess.run(control + ["-m", "fast", "stop"], check=True,
                       stdout=subprocess.DEVNULL, timeout=30)
    server_log = log.read_text()
    assert "was terminated by signal" not in server_log, server_log
    assert "FinalizeS3 was not called" not in server_log, server_log


def main():
    with tempfile.TemporaryDirectory(prefix="pgiceberg-network-") as temp:
        server = http.server.ThreadingHTTPServer(("127.0.0.1", 0), Handler)
        server.daemon_threads = True
        server.state = State()
        thread = threading.Thread(target=server.serve_forever, daemon=True)
        thread.start()
        try:
            endpoint = f"http://127.0.0.1:{server.server_port}"
            subprocess.run([sys.argv[1], endpoint, temp + "/catalog.sqlite"], check=True, timeout=90)
            if len(sys.argv) == 4:
                check_postgres(sys.argv[2], sys.argv[3], endpoint, temp)
            state = server.state
            assert state.requests[("GET", "/retry")] == 3
            assert state.requests[("GET", "/forbidden")] == 1
            assert state.requests[("GET", "/redirect-target")] == 0
            assert state.requests[("GET", "/retry-after")] == 1
            assert state.requests[("POST", "/bad-token")] == 1
            assert state.requests[("POST", "/commit")] == 1
            assert state.requests[("GET", "/slow")] == 3
            assert state.requests[("POST", "/token")] >= 3
            assert state.refreshes["basic"] == 1 and state.refreshes["oauth"] == 1
            siblings = [key for path, key in state.s3_requests if "events-other" in path]
            assert siblings and all(key == "pgiceberg-unconfigured" for key in siblings)
            assert state.objects["/bucket/default/events/data/renewed-basic"] == b"renewed"
            assert state.objects["/bucket/default/events/data/renewed-oauth"] == b"renewed"
        finally:
            server.shutdown()
            server.server_close()
            thread.join()


if __name__ == "__main__":
    main()
