"""Pinned native collector, real PostgreSQL/Redis, decoded OTLP (not live PostHog)."""

import time

import psycopg
import pytest
import redis
from support.collector import collector_sources, running_collector, wait_for_metrics

pytestmark = pytest.mark.integration

FLEET_METRICS = {
    "postgresql.backends",
    "postgresql.connection.max",
    "postgresql.deadlocks",
    "altimeter.postgresql.locks",
    "altimeter.postgresql.lock_waits",
    "redis.clients.connected",
    "redis.clients.blocked",
    "redis.memory.used",
    "redis.memory.rss",
    "redis.keys.evicted",
}
CANARY = "private_sql_table_canary"


@pytest.fixture(scope="module")
def sources(tmp_path_factory):
    with collector_sources(tmp_path_factory.mktemp("collector-sources")) as configured:
        yield configured


def test_kamal_real_sources_are_readonly_and_export_only_allowed_metrics(
    tmp_path, sources, otlp_receiver
):
    with psycopg.connect(sources.postgres_dsn, autocommit=True) as connection:
        connection.execute(f"CREATE TABLE {CANARY} (id integer)")
        connection.execute(f"INSERT INTO {CANARY} VALUES (1)")
    with psycopg.connect(sources.monitor_dsn) as monitor:
        assert monitor.execute("SHOW default_transaction_read_only").fetchone() == (
            "on",
        )
        with pytest.raises(psycopg.errors.ReadOnlySqlTransaction):
            monitor.execute("CREATE TABLE forbidden_write (id integer)")
    denied_client = redis.Redis(
        host=sources.redis.connection_pool.connection_kwargs["host"],
        port=sources.redis.connection_pool.connection_kwargs["port"],
        username=sources.redis_user,
        password=sources.environment["ALTIMETER_REDIS_PASSWORD"],
        socket_timeout=2,
    )
    try:
        assert denied_client.info("memory")["used_memory"] > 0
        with pytest.raises(redis.exceptions.NoPermissionError):
            denied_client.set(sources.key_prefix + "forbidden", "private-value")
    finally:
        denied_client.close()
    with running_collector(tmp_path, otlp_receiver, sources.environment) as (
        process,
        _,
        _,
    ):
        points = wait_for_metrics(
            otlp_receiver, {"postgresql.backends", "redis.clients.connected"}, process
        )
        assert all(
            point.as_int > 0
            for _, _, metric, point in points
            if metric.name == "redis.clients.connected"
        )
        observed = {metric.name for _, _, metric, _ in points}
        assert observed <= FLEET_METRICS, observed - FLEET_METRICS
        points = wait_for_metrics(otlp_receiver, FLEET_METRICS, process)
        time.sleep(0.6)
    wire = b"".join(
        request.message.SerializeToString() for request in otlp_receiver.requests
    )
    assert CANARY.encode() not in wire
    assert sources.environment["ALTIMETER_PG_PASSWORD"].encode() not in wire
    for resource, _, metric, point in points:
        assert metric.name in FLEET_METRICS
        for item in (*resource.attributes, *point.attributes):
            if item.key in {"db.namespace", "postgresql.database.name"}:
                assert item.value.string_value == sources.database
        assert {item.key for item in point.attributes} <= {"db.namespace"}
        assert {item.key for item in resource.attributes} <= {
            "service.name",
            "service.instance.id",
            "deployment.environment.name",
            "server.address",
            "server.port",
            "postgresql.database.name",
        }


def test_profiles_validate_with_native_binary_and_disabled_sources_do_not_start(
    tmp_path, sources, otlp_receiver
):
    import json
    import shutil
    import subprocess

    import psutil
    from support.collector import ROOT, ecs_metadata

    binary = shutil.which("otelcol-contrib")
    version = subprocess.check_output([binary, "--version"], text=True, timeout=10)
    assert version.strip() == "otelcol-contrib version 0.164.0"
    with ecs_metadata() as (metadata_env, metadata_requests, _):
        with running_collector(
            tmp_path, otlp_receiver, sources.environment | metadata_env
        ) as (process, _, env):
            wait_for_metrics(otlp_receiver, FLEET_METRICS, process)
            assert psutil.Process(process.pid).uids().effective != 0
            assert metadata_requests == [], (
                "Kamal must never instantiate the ECS receiver"
            )
            rendered = subprocess.check_output(
                [
                    binary,
                    "print-config",
                    "--format=json",
                    "--config",
                    str(ROOT / "deploy/collector.yaml"),
                ],
                env=env,
                text=True,
                timeout=10,
            )
            config = json.loads(rendered)
            assert config["service"]["pipelines"]["metrics"]["receivers"] == [
                "postgresql",
                "redis",
                "sqlquery/lock_wait",
            ]
            assert config["service"]["pipelines"]["metrics"]["processors"] == [
                "memory_limiter",
                "filter/allowed",
                "transform/attributes",
                "batch",
            ]
            assert "otlp" not in config["receivers"]
            limiter = config["processors"]["memory_limiter"]
            assert 0 < limiter["limit_mib"] <= 256
            assert 0 < limiter["spike_limit_mib"] < limiter["limit_mib"]
            batch = config["processors"]["batch"]
            assert 0 < batch["send_batch_size"] <= batch["send_batch_max_size"] <= 128
            exporter = config["exporters"]["otlp_http"]
            # Native configoptional represents enabled components as an object.
            assert exporter["sending_queue"] is not None
            disabled = subprocess.check_output(
                [
                    binary,
                    "print-config",
                    "--format=json",
                    "--config",
                    str(ROOT / "deploy/collector.yaml"),
                    "--set=exporters.otlp_http.sending_queue.enabled=false",
                ],
                env=env,
                text=True,
                timeout=10,
            )
            assert (
                json.loads(disabled)["exporters"]["otlp_http"]["sending_queue"] is None
            )
            assert 0 < exporter["sending_queue"]["queue_size"] <= 8
            assert exporter["sending_queue"].get("storage") is None
            assert 0 < exporter["sending_queue"]["num_consumers"] <= 1
            assert config["receivers"]["postgresql"]["databases"] == [sources.database]
            for receiver in ("postgresql", "redis"):
                tls = config["receivers"][receiver]["tls"]
                # Native print-config omits false zero-value TLS fields.
                assert tls.get("insecure", False) is False
                assert tls.get("insecure_skip_verify", False) is False
            assert all(
                not event.get("enabled", False)
                for event in config["receivers"]["postgresql"]["events"].values()
            )
            assert (
                config["receivers"]["sqlquery/lock_wait"]["additional_params"][
                    "statement_timeout"
                ]
                == "1000"
            )


def test_ecs_task_only_metadata_units_and_private_fields(tmp_path, otlp_receiver):
    from support.collector import ecs_metadata

    expected = {
        "ecs.task.cpu.usage.total": ("Nanoseconds", 2500000000),
        "ecs.task.memory.usage": ("Bytes", 80 * 1024 * 1024),
        "ecs.task.memory.utilized": ("MiB", 64),
        "ecs.task.memory.reserved": ("MiB", 128),
    }
    with ecs_metadata() as (metadata_env, metadata_requests, task):
        # Missing CA paths and nonexistent databases must never be opened in ECS.
        environment = metadata_env | {
            "ALTIMETER_COLLECTOR_RECEIVERS": "[awsecscontainermetrics]",
            "ALTIMETER_PG_CA_FILE": "/absent/postgres.pem",
            "ALTIMETER_REDIS_CA_FILE": "/absent/redis.pem",
        }
        with running_collector(tmp_path, otlp_receiver, environment) as (process, _, _):
            points = wait_for_metrics(otlp_receiver, set(expected), process)
        assert set(metadata_requests) == {"/v4/task", "/v4/task/stats"}
    assert {metric.name for _, _, metric, _ in points} == set(expected)
    for resource, _, metric, point in points:
        assert (metric.unit, point.as_int) == expected[metric.name]
        attrs = {item.key: item.value.string_value for item in resource.attributes}
        assert attrs["aws.ecs.task.id"] == task
        assert set(attrs) <= {
            "service.name",
            "service.instance.id",
            "deployment.environment.name",
            "aws.ecs.task.id",
            "aws.ecs.task.arn",
            "cloud.region",
            "cloud.availability_zone",
        }
        assert not point.attributes
    wire = b"".join(
        request.message.SerializeToString() for request in otlp_receiver.requests
    )
    assert b"private_metadata_canary" not in wire
    assert all(metric.name not in FLEET_METRICS for _, _, metric, _ in points)


@pytest.fixture(scope="module")
def application_wheel(tmp_path_factory):
    from support import gunicorn_server, uvicorn_server
    from support.process_web import web_profile

    installer = gunicorn_server if web_profile() == "wsgi" else uvicorn_server
    return installer.install_wheel(
        tmp_path_factory.mktemp("collector-app-wheel"), extras=("process", "psycopg")
    )


_LOCK_APPLICATION = r"""
import importlib.metadata
import json
import os
from pathlib import Path
import sys
from dataclasses import replace
import psycopg
import django_altimeter
from django_altimeter import span
from django_altimeter.conf import Config, SignalConfig
from django_altimeter.privacy import DEFAULT_POLICY
from django_altimeter.runtime import setup, shutdown, get_process_runtime

assert Path(django_altimeter.__file__).resolve().is_relative_to(Path(sys.prefix))
distribution = importlib.metadata.distribution('django-altimeter')
direct = json.loads(distribution.read_text('direct_url.json'))
assert direct['archive_info']['hashes']['sha256'] == os.environ['WHEEL_SHA256']
endpoint = os.environ['RECEIVER_ENDPOINT']
result = setup(Config(
    enabled=True, role='web', service_name='collector-application',
    deployment_environment='test', release='v1',
    integrations=frozenset({'process', 'psycopg'}),
    traces=SignalConfig(True, endpoint + '/v1/traces', destination='collector'),
    metrics=SignalConfig(True, endpoint + '/v1/metrics', destination='collector'),
    metric_export_interval_millis=60000, batch_schedule_delay_millis=60000,
    export_timeout=1,
    policy=replace(DEFAULT_POLICY,
                   span_names=DEFAULT_POLICY.span_names | {'collector.wait'}),
))
assert result.ok, result.codes
runtime = get_process_runtime()
assert runtime.meter_provider.force_flush(timeout_millis=2000)
with span('collector.wait'):
    with psycopg.connect(os.environ['APPLICATION_DSN'], autocommit=True) as connection:
        row = connection.execute('SELECT id FROM collector_lock WHERE id = 1 '
                                 'FOR UPDATE /* private_query_canary */').fetchone()
assert runtime.meter_provider.force_flush(timeout_millis=2000)
outcome = shutdown()
print(json.dumps({'row': list(row), 'writer': runtime.instance_id,
                  'completed': outcome.completed}), flush=True)
"""


def test_controlled_lock_wait_beside_real_driver_trace_and_process_writer(
    tmp_path, sources, otlp_receiver, application_wheel
):
    import json
    import os
    import subprocess

    from support.collector import metric_points

    with psycopg.connect(sources.postgres_dsn, autocommit=True) as admin:
        admin.execute("CREATE TABLE collector_lock (id integer PRIMARY KEY)")
        admin.execute("INSERT INTO collector_lock VALUES (1)")
    with psycopg.connect(sources.postgres_dsn) as holder:
        holder.execute("SELECT id FROM collector_lock WHERE id = 1 FOR UPDATE")
        with running_collector(tmp_path, otlp_receiver, sources.environment) as (
            process,
            _,
            env,
        ):
            wait_for_metrics(otlp_receiver, FLEET_METRICS, process)
            child = subprocess.Popen(
                [str(application_wheel.python), "-I", "-c", _LOCK_APPLICATION],
                env={
                    "PATH": os.environ.get("PATH", ""),
                    "APPLICATION_DSN": sources.postgres_dsn,
                    "RECEIVER_ENDPOINT": otlp_receiver.endpoint,
                    "WHEEL_SHA256": application_wheel.sha256,
                },
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                text=True,
            )
            try:
                deadline = time.monotonic() + 15
                blocked = []
                while time.monotonic() < deadline:
                    assert child.poll() is None, "The real query must be blocked"
                    blocked = [
                        point
                        for _, _, metric, point in metric_points(otlp_receiver)
                        if metric.name == "altimeter.postgresql.lock_waits"
                        and point.as_int == 1
                    ]
                    if blocked:
                        break
                    time.sleep(0.05)
                assert blocked, "The static aggregate must expose the real lock wait"
                holder.commit()
                stdout, stderr = child.communicate(timeout=10)
                assert child.returncode == 0, stderr
                report = json.loads(stdout)
                assert report["row"] == [1] and report["completed"] is True
                assert report["writer"] != env["ALTIMETER_COLLECTOR_INSTANCE_ID"]
                released_at = time.time_ns()
                deadline = time.monotonic() + 5
                while time.monotonic() < deadline:
                    if any(
                        metric.name == "altimeter.postgresql.lock_waits"
                        and point.time_unix_nano >= released_at
                        and point.as_int == 0
                        for _, _, metric, point in metric_points(otlp_receiver)
                    ):
                        break
                    time.sleep(0.05)
                else:
                    pytest.fail("The wait count did not return to zero")
            finally:
                if child.poll() is None:
                    child.kill()
                    child.communicate(timeout=3)
    spans = [
        span
        for request in otlp_receiver.requests
        if request.path == "/v1/traces"
        for resource in request.message.resource_spans
        for scope in resource.scope_spans
        for span in scope.spans
    ]
    parents = [span for span in spans if span.name == "collector.wait"]
    queries = [span for span in spans if span.name == "SELECT" and span.kind == 3]
    assert len(parents) == len(queries) == 1
    assert queries[0].parent_span_id == parents[0].span_id
    assert (
        queries[0].start_time_unix_nano
        < blocked[0].time_unix_nano
        < queries[0].end_time_unix_nano
    )
    process_points = [
        (resource, point)
        for resource, _, metric, point in metric_points(otlp_receiver)
        if metric.name == "process.memory.usage"
    ]
    assert process_points and all(point.as_int > 0 for _, point in process_points)
    assert all(
        any(
            item.key == "service.instance.id"
            and item.value.string_value == report["writer"]
            for item in resource.attributes
        )
        for resource, _ in process_points
    )
    wire = b"".join(
        request.message.SerializeToString() for request in otlp_receiver.requests
    )
    assert b"private_query_canary" not in wire


@pytest.mark.parametrize(
    "fault",
    ["healthy", "postgres_denied", "redis_denied", "tls", "stopped", "ecs_absent"],
)
def test_optional_collector_gap_preserves_real_web_and_direct_process_export(
    tmp_path, sources, otlp_receiver, application_wheel, fault
):
    from support.collector import metric_points
    from support.otlp_receiver import OTLPReceiver
    from support.process_web import running_process_web, server_spans

    environment = dict(sources.environment)
    if fault == "postgres_denied":
        environment["ALTIMETER_PG_USERNAME"] = "nonexistent_collector_monitor"
    elif fault == "redis_denied":
        environment["ALTIMETER_REDIS_PASSWORD"] = "rejected_credential_canary"
    elif fault == "tls":
        environment["ALTIMETER_PG_CA_FILE"] = environment["ALTIMETER_TEST_WRONG_CA"]
        environment["ALTIMETER_REDIS_CA_FILE"] = environment["ALTIMETER_TEST_WRONG_CA"]
    elif fault == "ecs_absent":
        environment["ALTIMETER_COLLECTOR_RECEIVERS"] = "[awsecscontainermetrics]"
        environment["ECS_CONTAINER_METADATA_URI_V4"] = "http://127.0.0.1:1/v4"
    with OTLPReceiver() as app_receiver:
        with running_collector(tmp_path / "collector", otlp_receiver, environment) as (
            process,
            _,
            _,
        ):
            if fault == "stopped":
                process.terminate()
                process.wait(timeout=5)
            with running_process_web(
                tmp_path / "app",
                wheel=application_wheel,
                receiver=app_receiver,
                process_enabled=True,
            ) as (server, health):
                for _ in range(5):
                    status, _, body = server.request("/api/benchmark")
                    assert status == 200 and body == b"x" * 1024
                # Let both real sources attempt collection, including TLS/ACL failure.
                time.sleep(3)
            names = {metric.name for _, _, metric, _ in metric_points(otlp_receiver)}
        if fault == "healthy":
            assert names == FLEET_METRICS
        elif fault == "postgres_denied":
            assert names == {
                name for name in FLEET_METRICS if name.startswith("redis.")
            }
        elif fault == "redis_denied":
            assert names == {
                name for name in FLEET_METRICS if not name.startswith("redis.")
            }
        else:
            assert names == set()
        assert len(server_spans(app_receiver)) == 5
        points = [
            (resource, point)
            for resource, _, metric, point in metric_points(app_receiver)
            if metric.name == "process.memory.usage"
        ]
        assert points and all(point.as_int > 0 for _, point in points)
        assert all(
            any(
                item.key == "service.instance.id"
                and item.value.string_value == health["instance_id"]
                for item in resource.attributes
            )
            for resource, _ in points
        )
        assert all(
            metric.name not in FLEET_METRICS
            for _, _, metric, _ in metric_points(app_receiver)
        )
    wire = b"".join(
        request.message.SerializeToString() for request in otlp_receiver.requests
    )
    assert b"rejected_credential_canary" not in wire


def test_stalled_export_has_bounded_memory_batch_queue_and_shutdown(
    tmp_path, sources, otlp_receiver
):
    """Predeclared 12s failure probe: <=512MiB RSS; tail growth <=32MiB."""
    import psutil

    for _ in range(50):
        otlp_receiver.queue_response(hang=True)
    samples = []
    with running_collector(tmp_path, otlp_receiver, sources.environment) as (
        process,
        _,
        _,
    ):
        observer = psutil.Process(process.pid)
        started = time.monotonic()
        while time.monotonic() - started < 12:
            assert process.poll() is None
            samples.append((time.monotonic() - started, observer.memory_info().rss))
            time.sleep(0.25)
        assert len(samples) >= 40
        assert max(value for _, value in samples) <= 512 * 1024 * 1024
        warm = [value for elapsed, value in samples if 4 <= elapsed < 6]
        tail = [value for elapsed, value in samples if elapsed >= 10]
        assert warm and tail and max(tail) <= max(warm) + 32 * 1024 * 1024
        print(
            {
                "samples": len(samples),
                "peak_rss_bytes": max(v for _, v in samples),
                "tail_growth_bytes": max(tail) - max(warm),
            }
        )
        requests = otlp_receiver.requests
        assert len(requests) >= 2, (
            "The real exporter must attempt and retry stalled sends"
        )
        for request in requests:
            points = [
                point
                for resource in request.message.resource_metrics
                for scope in resource.scope_metrics
                for metric in scope.metrics
                for point in getattr(metric, metric.WhichOneof("data")).data_points
            ]
            assert 0 < len(points) <= 128
        stopping = time.monotonic()
    assert time.monotonic() - stopping < 8
    assert process.returncode == 0, (
        "Native bounded shutdown must finish without killing it"
    )
