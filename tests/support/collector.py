"""Real collector and isolated database services; no fabricated receiver metrics."""

import os
import select
import shutil
import socket
import socketserver
import ssl
import subprocess
import threading
import time
from contextlib import contextmanager
from dataclasses import dataclass, field
from pathlib import Path
from urllib.parse import urlsplit
from uuid import uuid4

from support.uvicorn_server import ROOT


@dataclass(frozen=True)
class CollectorSources:
    environment: dict[str, str] = field(repr=False)
    postgres_dsn: str = field(repr=False)
    monitor_dsn: str = field(repr=False)
    database: str
    redis: object = field(repr=False)
    redis_user: str
    key_prefix: str


def certificate(directory, name):
    """Create ephemeral test credentials, never committed or printed."""
    key, cert = directory / f"{name}.key", directory / f"{name}.pem"
    result = subprocess.run(
        [
            "openssl",
            "req",
            "-x509",
            "-newkey",
            "rsa:2048",
            "-nodes",
            "-keyout",
            str(key),
            "-out",
            str(cert),
            "-days",
            "1",
            "-subj",
            "/CN=localhost",
            "-addext",
            "subjectAltName=DNS:localhost,DNS:postgres,IP:127.0.0.1",
        ],
        capture_output=True,
        timeout=10,
        check=True,
    )
    assert result.returncode == 0
    return cert, key


@contextmanager
def redis_tls_forwarder(cert, key, upstream):
    """TLS termination forwards actual Redis protocol to the isolated service."""
    context = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
    context.load_cert_chain(cert, key)
    stopped = threading.Event()
    active = set()
    guard = threading.Lock()

    class Handler(socketserver.BaseRequestHandler):
        def handle(self):
            sockets = [self.request]
            with guard:
                active.add(self.request)
            try:
                self.request.settimeout(2)
                client = context.wrap_socket(self.request, server_side=True)
                sockets.append(client)
                backend = socket.create_connection(upstream, timeout=2)
                sockets.append(backend)
                with guard:
                    active.update(sockets)
                while not stopped.is_set():
                    readable, _, _ = select.select([client, backend], [], [], 0.1)
                    if client.pending() and client not in readable:
                        readable.append(client)
                    for source in readable:
                        data = source.recv(16384)
                        if not data:
                            return
                        (backend if source is client else client).sendall(data)
            except (OSError, ValueError):
                # Includes expected untrusted-certificate handshakes and teardown.
                return
            finally:
                with guard:
                    for connection in sockets:
                        active.discard(connection)
                        connection.close()

    class Server(socketserver.ThreadingTCPServer):
        allow_reuse_address = True
        daemon_threads = True

    server = Server(("127.0.0.1", 0), Handler)
    thread = threading.Thread(
        target=server.serve_forever, kwargs={"poll_interval": 0.05}
    )
    thread.start()
    try:
        yield server.server_address[1]
    finally:
        stopped.set()
        with guard:
            for connection in tuple(active):
                try:
                    connection.shutdown(socket.SHUT_RDWR)
                except OSError:
                    pass
        server.shutdown()
        server.server_close()
        thread.join(timeout=3)
        assert not thread.is_alive()


@contextmanager
def collector_sources(directory):
    """A real read-only PG role, DB allow-list and Redis INFO-only ACL per test."""
    import psycopg
    import redis
    from psycopg import sql
    from psycopg.conninfo import conninfo_to_dict, make_conninfo

    directory.mkdir(parents=True, exist_ok=True)
    if os.geteuid() == 0:
        # The collector runs as nobody; only the public CA needs readable parents.
        parent = directory
        while parent != Path("/tmp") and parent.is_relative_to("/tmp"):
            parent.chmod(0o755)
            parent = parent.parent
    base_dsn = os.environ["ALTIMETER_TEST_POSTGRES_DSN"]
    redis_url = os.environ["ALTIMETER_TEST_REDIS_URL"]
    identity = uuid4().hex
    database, username = f"collector_{identity}", f"monitor_{identity}"
    password = uuid4().hex
    prefix = f"collector:{identity}:"
    cert, key = certificate(directory, "server")
    wrong_ca, _ = certificate(directory, "untrusted")
    pg = conninfo_to_dict(base_dsn)
    redis_address = urlsplit(redis_url)
    client = redis.Redis.from_url(redis_url, socket_timeout=3)
    with psycopg.connect(base_dsn, autocommit=True) as admin:
        # Configure real PostgreSQL TLS; the server owns the files (mode 0600).
        for source, suffix in ((cert, "crt"), (key, "key")):
            path = f"/tmp/collector-{identity}.{suffix}"
            admin.execute(
                sql.SQL(
                    "COPY (SELECT unnest(string_to_array({}, E'\\n'))) TO PROGRAM {}"
                ).format(
                    sql.Literal(source.read_text().strip()),
                    sql.Literal(f"umask 077; cat > {path}"),
                )
            )
            setting = "ssl_cert_file" if suffix == "crt" else "ssl_key_file"
            admin.execute(
                sql.SQL("ALTER SYSTEM SET {} = {}").format(
                    sql.Identifier(setting), sql.Literal(path)
                )
            )
        admin.execute("ALTER SYSTEM SET ssl = on")
        admin.execute("SELECT pg_reload_conf()")
        admin.execute(sql.SQL("CREATE DATABASE {}").format(sql.Identifier(database)))
        admin.execute(
            sql.SQL(
                "CREATE ROLE {} LOGIN PASSWORD {} NOSUPERUSER "
                "NOCREATEDB NOCREATEROLE NOREPLICATION"
            ).format(sql.Identifier(username), sql.Literal(password))
        )
        admin.execute(
            sql.SQL("GRANT pg_read_all_stats TO {}").format(sql.Identifier(username))
        )
        admin.execute(
            sql.SQL("ALTER ROLE {} SET default_transaction_read_only = on").format(
                sql.Identifier(username)
            )
        )
        client.execute_command(
            "ACL",
            "SETUSER",
            username,
            "reset",
            "on",
            f">{password}",
            "+info",
            "+ping",
            "+hello",
            "+client|setname",
            "+client|setinfo",
        )
        monitor = make_conninfo(
            base_dsn,
            dbname=database,
            user=username,
            password=password,
            sslmode="verify-full",
            sslrootcert=str(cert),
            connect_timeout=2,
        )
        until = time.monotonic() + 5
        while True:
            try:
                with psycopg.connect(monitor) as connection:
                    assert connection.execute("SELECT 1").fetchone() == (1,)
                break
            except psycopg.OperationalError:
                if time.monotonic() >= until:
                    raise
                time.sleep(0.05)
        try:
            with redis_tls_forwarder(
                cert, key, (redis_address.hostname, redis_address.port or 6379)
            ) as port:
                yield CollectorSources(
                    {
                        "ALTIMETER_PG_HOST": pg.get("host", "postgres"),
                        "ALTIMETER_PG_PORT": pg.get("port", "5432"),
                        "ALTIMETER_PG_DATABASE": database,
                        "ALTIMETER_PG_USERNAME": username,
                        "ALTIMETER_PG_PASSWORD": password,
                        "ALTIMETER_PG_CA_FILE": str(cert),
                        "ALTIMETER_REDIS_ENDPOINT": f"localhost:{port}",
                        "ALTIMETER_REDIS_USERNAME": username,
                        "ALTIMETER_REDIS_PASSWORD": password,
                        "ALTIMETER_REDIS_CA_FILE": str(cert),
                        "ALTIMETER_TEST_WRONG_CA": str(wrong_ca),
                    },
                    make_conninfo(base_dsn, dbname=database),
                    monitor,
                    database,
                    client,
                    username,
                    prefix,
                )
        finally:
            client.execute_command("ACL", "DELUSER", username)
            for key_name in client.scan_iter(match=f"{prefix}*", count=100):
                client.delete(key_name)
            client.close()
            admin.execute(
                sql.SQL("DROP DATABASE {} WITH (FORCE)").format(
                    sql.Identifier(database)
                )
            )
            admin.execute(sql.SQL("DROP ROLE {}").format(sql.Identifier(username)))


@contextmanager
def running_collector(directory, receiver, environment, *, extra=()):
    directory.mkdir(parents=True, exist_ok=True)
    binary = shutil.which("otelcol-contrib")
    assert binary, "Run the pinned collector in a reference verification image"
    config = os.environ.get(
        "ALTIMETER_TEST_COLLECTOR_CONFIG", str(ROOT / "deploy/collector.yaml")
    )
    env = {
        "PATH": os.environ.get("PATH", ""),
        "GOMEMLIMIT": "192MiB",
        "ALTIMETER_COLLECTOR_RECEIVERS": "[postgresql, redis, sqlquery/lock_wait]",
        "ALTIMETER_METRICS_ENDPOINT": receiver.url("metrics"),
        "ALTIMETER_METRICS_AUTHORIZATION": "Bearer test-only",
        "ALTIMETER_COLLECTOR_INSTANCE_ID": str(uuid4()),
        "ALTIMETER_COLLECTION_INTERVAL": "500ms",
        **environment,
    }
    with (directory / "collector.log").open("w+") as log:
        process = subprocess.Popen(
            [binary, "--config", config, *extra],
            env=env,
            stdout=log,
            stderr=subprocess.STDOUT,
            **(
                {"user": 65534, "group": 65534, "extra_groups": ()}
                if os.geteuid() == 0
                else {}
            ),
        )
        try:
            yield process, log, env
        finally:
            if process.poll() is None:
                process.terminate()
            try:
                process.wait(timeout=8)
            except subprocess.TimeoutExpired:
                process.kill()
                process.wait(timeout=3)
            log.seek(0)
            print(log.read(), end="")


def metric_points(receiver):
    return [
        (resource.resource, scope.scope.name, metric, point)
        for request in receiver.requests
        if request.path == "/v1/metrics"
        for resource in request.message.resource_metrics
        for scope in resource.scope_metrics
        for metric in scope.metrics
        for point in getattr(metric, metric.WhichOneof("data")).data_points
    ]


def wait_for_metrics(receiver, names, process, timeout=15):
    until = time.monotonic() + timeout
    while time.monotonic() < until:
        assert process.poll() is None, "The real collector must stay running"
        points = metric_points(receiver)
        if names <= {metric.name for _, _, metric, _ in points}:
            return points
        time.sleep(0.05)
    raise AssertionError(
        f"Missing metrics: {names - {m.name for _, _, m, _ in points}}"
    )


@contextmanager
def ecs_metadata():
    """Only the ECS metadata HTTP boundary is a stand-in; collection is native."""
    import json
    from http.server import BaseHTTPRequestHandler, HTTPServer

    task = uuid4().hex
    container = uuid4().hex
    requests = []
    canary = "private_metadata_canary"
    metadata = {
        "TaskARN": f"arn:aws:ecs:us-east-1:000000000000:task/reference/{task}",
        "Cluster": "reference",
        "Family": canary,
        "Revision": "1",
        "AvailabilityZone": "us-east-1a",
        "KnownStatus": "RUNNING",
        "Limits": {"CPU": 1, "Memory": 128},
        "Containers": [
            {
                "DockerId": container,
                "Name": canary,
                "DockerName": canary,
                "Image": canary,
                "Labels": {"private": canary},
                "KnownStatus": "RUNNING",
            }
        ],
    }
    stats = {
        container: {
            "id": container,
            "name": canary,
            "read": "2026-10-31T00:00:01Z",
            "preread": "2026-10-31T00:00:00Z",
            "memory_stats": {
                "usage": 80 * 1024 * 1024,
                "limit": 128 * 1024 * 1024,
                "stats": {"cache": 16 * 1024 * 1024},
            },
            "cpu_stats": {
                "cpu_usage": {"total_usage": 2500000000, "percpu_usage": [2500000000]}
            },
            "precpu_stats": {"cpu_usage": {"total_usage": 2000000000}},
        }
    }

    class Handler(BaseHTTPRequestHandler):
        def do_GET(self):
            requests.append(self.path)
            payload = {"/v4/task": metadata, "/v4/task/stats": stats}.get(self.path)
            body = json.dumps(payload).encode()
            self.send_response(200 if payload is not None else 404)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)

        def log_message(self, format, *args):
            pass

    server = HTTPServer(("127.0.0.1", 0), Handler)
    thread = threading.Thread(
        target=server.serve_forever, kwargs={"poll_interval": 0.05}
    )
    thread.start()
    try:
        yield (
            {
                "ECS_CONTAINER_METADATA_URI_V4": f"http://127.0.0.1:{server.server_port}/v4",
            },
            requests,
            task,
        )
    finally:
        server.shutdown()
        server.server_close()
        thread.join(timeout=3)
        assert not thread.is_alive()
