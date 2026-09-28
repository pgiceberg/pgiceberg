#!/usr/bin/env python3
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http:#www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""Exercise real metadata workers in an isolated PostgreSQL cluster (PG 16+)."""
import argparse
import concurrent.futures
import os
from pathlib import Path
import signal
import subprocess
import tempfile
import time

parser = argparse.ArgumentParser()
parser.add_argument('--bindir', required=True)
parser.add_argument('--library', required=True)
parser.add_argument('--control-path', default='')
parser.add_argument('--log', required=True)
args = parser.parse_args()


def eventually(operation, predicate=lambda value: bool(value), timeout=20):
    deadline = time.monotonic() + timeout
    last = None
    while time.monotonic() < deadline:
        try:
            last = operation()
            if predicate(last):
                return last
        except (subprocess.CalledProcessError, AssertionError) as exc:
            last = exc
        time.sleep(0.1)
    raise AssertionError(f'condition did not become true: {last}')


with tempfile.TemporaryDirectory(prefix='pgim-') as directory:
    root = Path(directory)
    data = root / 'data'
    log = root / 'server.log'
    env = {key: value for key, value in os.environ.items()
           if not key.startswith('PG')}
    env.update(PGHOST=directory, PGPORT='5432', PGUSER='postgres', PGDATABASE='postgres')
    stopped_pid = None
    started = False
    clients = []

    def run(binary, *argv, check=True):
        return subprocess.run([str(Path(args.bindir) / binary), *map(str, argv)],
                              env=env, capture_output=True, text=True,
                              timeout=30, check=check)

    def sql(command, database='postgres', check=True):
        result = run('psql', '-XAt', '-v', 'ON_ERROR_STOP=1', '-d', database,
                     '-c', command, check=check)
        return result.stdout.strip() if check else result

    def ping(database='worker_a'):
        return tuple(map(int, sql('SELECT * FROM pgiceberg.metadata_worker_ping()', database).split('|')))

    def active(pid):
        return sql(f'SELECT count(*) FROM pg_stat_activity WHERE pid = {pid}') == '1'

    def worker_count(database):
        return sql("SELECT count(*) FROM pg_stat_activity WHERE "
                   f"backend_type = 'pgiceberg metadata worker' AND datname = '{database}'")

    def configure(names):
        sql(f"ALTER SYSTEM SET pgiceberg.metadata_databases = '{names}'")
        sql('SELECT pg_reload_conf()')

    def launch(command, name):
        client_env = dict(env, PGAPPNAME=name)
        process = subprocess.Popen([str(Path(args.bindir) / 'psql'), '-XAt',
                                    '-v', 'ON_ERROR_STOP=1', '-d', 'worker_a', '-c', command],
                                   env=client_env, stdout=subprocess.PIPE,
                                   stderr=subprocess.PIPE, text=True)
        clients.append(process)
        return process

    def client_waiting(name):
        return sql("SELECT count(*) FROM pg_stat_activity WHERE "
                   f"application_name = '{name}' AND wait_event_type = 'Extension'") == '1'

    def pause_worker(pid):
        os.kill(pid, signal.SIGSTOP)

    try:
        run('initdb', '-D', data, '-A', 'trust', '-U', 'postgres', '--no-locale')
        config = (
            "listen_addresses = ''\n"
            f"unix_socket_directories = '{directory}'\n"
            f"dynamic_library_path = '{Path(args.library).parent}:$libdir'\n"
            f"shared_preload_libraries = '{args.library}'\n"
            "max_worker_processes = 12\n"
            "pgiceberg.metadata_databases = 'worker_a, worker_b'\n"
            "pgiceberg.metadata_max_connections = 4\n"
            "pgiceberg.logical_sync_poll_interval_ms = 60000\n"
        )
        if args.control_path:
            config += f"extension_control_path = '{args.control_path}:$system'\n"
        with (data / 'postgresql.conf').open('a') as output:
            output.write(config)
        run('pg_ctl', '-D', data, '-l', log, '-w', 'start')
        started = True
        for database in ('worker_a', 'worker_b', 'disabled'):
            sql(f'CREATE DATABASE {database}')
            sql('CREATE EXTENSION pgiceberg', database)
        a = eventually(ping)
        b = eventually(lambda: ping('worker_b'))
        assert a[1] != b[1] and a[0] != b[0] and a[3] == b[3] == 1
        assert worker_count('worker_a') == worker_count('worker_b') == '1'
        print('PASS: one worker per enabled database', flush=True)

        echo = "SELECT pgiceberg.metadata_worker_echo(decode(repeat('00ff7e81', 16384), 'hex')) = decode(repeat('00ff7e81', 16384), 'hex')"
        assert sql(echo, 'worker_a') == 't'
        assert sql("SELECT octet_length(pgiceberg.metadata_worker_echo(''::bytea))", 'worker_a') == '0'
        with concurrent.futures.ThreadPoolExecutor(max_workers=4) as pool:
            assert all(value == 't' for value in pool.map(lambda _: sql(echo, 'worker_a'), range(24)))
        oversized = sql("SELECT pgiceberg.metadata_worker_echo(decode(repeat('00', 65537), 'hex'))", 'worker_a', False)
        assert oversized.returncode and 'exceeds 65536 bytes' in oversized.stderr
        disabled = sql('SELECT pgiceberg.metadata_worker_ping()', 'disabled', False)
        assert disabled.returncode and 'not enabled' in disabled.stderr
        sql('CREATE ROLE rpc_guest')
        sql('GRANT USAGE ON SCHEMA pgiceberg TO rpc_guest', 'worker_a')
        denied = sql('SET ROLE rpc_guest; SELECT pgiceberg.metadata_worker_ping()', 'worker_a', False)
        assert denied.returncode and 'permission denied for function' in denied.stderr
        print('PASS: binary payload, queue wrap, concurrent clients, limits and SQL permissions', flush=True)

        stopped_pid = a[1]
        pause_worker(stopped_pid)
        # Timeout cleanup must release slots even when the worker cannot run.
        for _ in range(6):
            result = sql('SET pgiceberg.metadata_request_timeout_ms=80; SELECT pgiceberg.metadata_worker_ping()', 'worker_a', False)
            assert result.returncode and 'request timed out' in result.stderr
        result = sql('SET statement_timeout=80; SELECT pgiceberg.metadata_worker_ping()', 'worker_a', False)
        assert result.returncode and 'statement timeout' in result.stderr
        savepoint = run('psql', '-XAtq', '-v', 'ON_ERROR_STOP=0', '-d', 'worker_a',
                        '-c', 'BEGIN; SAVEPOINT before_rpc',
                        '-c', 'SET LOCAL pgiceberg.metadata_request_timeout_ms=80; SELECT pgiceberg.metadata_worker_ping()',
                        '-c', 'ROLLBACK TO SAVEPOINT before_rpc',
                        '-c', "SELECT 'survived'; COMMIT")
        assert savepoint.stdout.strip() == 'survived' and 'request timed out' in savepoint.stderr
        bounded = [launch('SELECT pgiceberg.metadata_worker_ping()', f'bounded_{i}') for i in range(4)]
        for i in range(4):
            eventually(lambda i=i: client_waiting(f'bounded_{i}'))
        result = sql('SELECT pgiceberg.metadata_worker_ping()', 'worker_a', False)
        assert result.returncode and 'request limit reached' in result.stderr
        sql("SELECT pg_terminate_backend(pid) FROM pg_stat_activity WHERE application_name LIKE 'bounded_%'")
        for client in bounded:
            client.communicate(timeout=10)
            assert client.returncode
        os.kill(stopped_pid, signal.SIGCONT)
        stopped_pid = None
        assert eventually(ping)[1:3] == a[1:3]
        print('PASS: timeout, statement cancellation, connection cap and backend exit cleanup', flush=True)

        stopped_pid = a[1]
        pause_worker(stopped_pid)
        interrupted = launch(echo, 'interrupted_rpc')
        eventually(lambda: client_waiting('interrupted_rpc'))
        sql(f'SELECT pg_terminate_backend({a[1]})')
        os.kill(stopped_pid, signal.SIGCONT)
        stopped_pid = None
        _, error = interrupted.communicate(timeout=10)
        assert interrupted.returncode and ('disconnected' in error or 'detached' in error)
        a2 = eventually(ping, lambda result: result[1] != a[1] and result[2] > a[2])
        assert ping('worker_b')[1:3] == b[1:3]
        launcher = int(sql("SELECT pid FROM pg_stat_activity WHERE backend_type = 'pgiceberg metadata launcher'"))
        sql(f'SELECT pg_terminate_backend({launcher})')
        eventually(lambda: sql("SELECT pid FROM pg_stat_activity WHERE backend_type = 'pgiceberg metadata launcher'"),
                   lambda pid: bool(pid) and int(pid) != launcher)
        assert ping()[1:3] == a2[1:3]
        assert worker_count('worker_a') == '1'
        print('PASS: worker generation rollover and launcher restart without duplicate children', flush=True)

        sql('BEGIN; DROP EXTENSION pgiceberg CASCADE; ROLLBACK', 'worker_a')
        assert ping()[1:3] == a2[1:3]
        sql('DROP EXTENSION pgiceberg CASCADE', 'worker_a')
        eventually(lambda: not active(a2[1]))
        assert ping('worker_b')[1:3] == b[1:3]
        sql('CREATE EXTENSION pgiceberg', 'worker_a')
        a3 = eventually(ping)
        assert a3[2] > a2[2]
        configure('worker_b')
        eventually(lambda: not active(a3[1]))
        assert ping('worker_b')[1:3] == b[1:3]
        configure('worker_a, worker_b')
        eventually(ping)
        print('PASS: transactional DROP/CREATE EXTENSION and per-database SIGHUP enable/disable', flush=True)

        # PostgreSQL treats abnormal shared-memory worker death as an instance
        # crash. Verify recovery with the test cluster's own worker, too.
        before_crash = ping()
        os.kill(before_crash[1], signal.SIGKILL)
        eventually(ping, lambda value: value[1] != before_crash[1])
        eventually(lambda: ping('worker_b'))
        print('PASS: PostgreSQL crash recovery after abnormal worker exit', flush=True)

        # Shared state must be reconstructed after a postmaster restart.
        run('pg_ctl', '-D', data, '-m', 'fast', '-w', 'restart', '-l', log)
        eventually(ping)
        eventually(lambda: ping('worker_b'))
        assert worker_count('worker_a') == worker_count('worker_b') == '1'
        print('PASS: cluster restart', flush=True)

        # Existing functionality still loads without shared preloading; only the
        # new diagnostic RPCs need it. The full regress suite covers old paths.
        run('pg_ctl', '-D', data, '-m', 'fast', '-w', 'stop')
        started = False
        with (data / 'postgresql.conf').open('a') as output:
            output.write("shared_preload_libraries = ''\n")
        run('pg_ctl', '-D', data, '-l', log, '-w', 'start')
        started = True
        result = sql('SELECT pgiceberg.metadata_worker_ping()', 'worker_a', False)
        assert result.returncode and 'require shared_preload_libraries' in result.stderr
        assert 'cannot read pg_class' not in log.read_text()
        print('PASS: clear error without preload', flush=True)
    except subprocess.CalledProcessError as error:
        print(error.stdout, error.stderr, flush=True)
        raise
    finally:
        if stopped_pid:
            try:
                os.kill(stopped_pid, signal.SIGCONT)
            except ProcessLookupError:
                pass
        for client in clients:
            if client.poll() is None:
                client.kill()
                client.communicate(timeout=10)
        if started:
            run('pg_ctl', '-D', data, '-m', 'immediate', '-w', 'stop', check=False)
        if log.exists():
            Path(args.log).write_text(log.read_text())
