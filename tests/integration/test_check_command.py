"""Drive the installed-wheel Django CLI and real local telemetry receivers."""

import hashlib
import importlib.metadata
import json
import os
import subprocess
import sys
from concurrent.futures import ThreadPoolExecutor
from datetime import UTC, datetime, timedelta
from pathlib import Path
from time import monotonic
from uuid import UUID, uuid4

import pytest
from opentelemetry.proto.collector.trace.v1.trace_service_pb2 import (
    ExportTraceServiceResponse,
)
from support.posthog_query import PostHogQueryServer

pytestmark = pytest.mark.integration
ROOT = Path(__file__).resolve().parents[2]
_SIGNALS = ("traces", "logs", "metrics")
_CONFIG_CANARY = "configuration-secret-canary"
_HEADER_CANARY = "transport-secret-canary"
_INGEST_CANARY = "phc_ingestion-secret-canary"
_DSN_CANARY = "dsn-secret-canary"
_VENDOR_CANARY = "vendor-secret-canary"
_OPERATOR_CANARY = "phx_operator-secret-canary"
_CANARIES = (
    _CONFIG_CANARY,
    _HEADER_CANARY,
    _INGEST_CANARY,
    _DSN_CANARY,
    _VENDOR_CANARY,
    _OPERATOR_CANARY,
)
_CLI = r"""
import _thread
import importlib.metadata
import json
import os
from pathlib import Path
import sys
import threading
from time import monotonic

sys.path.insert(0, os.environ["REFERENCE_PATH"])

foreign = None
if os.environ.get("FOREIGN_PROVIDER") == "true":
    from opentelemetry import trace
    from opentelemetry.metrics import NoOpMeterProvider
    from opentelemetry.sdk.resources import Resource
    from opentelemetry.sdk.trace import TracerProvider
    foreign = TracerProvider(resource=Resource({}), shutdown_on_exit=False,
                             meter_provider=NoOpMeterProvider())
    trace.set_tracer_provider(foreign)

threads = []
sockets = []
exporters = []
thread_functions = tuple(
    getattr(_thread, name)
    for name in ("start_new_thread", "start_new", "start_joinable_thread")
    if hasattr(_thread, name)
)
def observe(frame, event, arg):
    if event == "c_call" and arg in thread_functions:
        threads.append(arg.__name__)
    if event == "call":
        if frame.f_code is threading.Thread.start.__code__:
            threads.append("Thread.start")
        module = frame.f_globals.get("__name__", "")
        if (module.startswith("opentelemetry.exporter.")
            and frame.f_code.co_name == "__init__"):
            exporters.append(module)
def audit(event, args):
    if event.startswith("socket."):
        sockets.append(event)
sys.addaudithook(audit)
sys.setprofile(observe)
started = monotonic()
try:
    import django_altimeter
    origin = Path(django_altimeter.__file__).resolve()
    assert origin.is_relative_to(Path(sys.prefix).resolve()), origin
    installed = importlib.metadata.distribution("django-altimeter")
    direct_url = json.loads(installed.read_text("direct_url.json"))
    assert direct_url["archive_info"]["hashes"]["sha256"] == os.environ["WHEEL_SHA256"]
    assert not direct_url.get("dir_info", {}).get("editable", False)
    if os.environ.get("OBSERVATION_CONTROL") == "true":
        from socket import socket
        from opentelemetry.exporter.otlp.proto.http.trace_exporter import (
            OTLPSpanExporter,
        )
        with socket():
            pass
        thread = threading.Thread(target=lambda: None)
        thread.start()
        thread.join()
        exporter = OTLPSpanExporter(
            endpoint="http://127.0.0.1:9/v1/traces", timeout=0.1
        )
        exporter.shutdown()
    from django.core.management import execute_from_command_line
    execute_from_command_line(["django", *sys.argv[1:]])
finally:
    sys.setprofile(None)
    if foreign is not None:
        assert trace.get_tracer_provider() is foreign
        foreign.shutdown()
    Path(os.environ["OBSERVATION_PATH"]).write_text(json.dumps({
        "threads": threads, "sockets": sockets, "exporters": exporters,
        "command_seconds": monotonic() - started,
    }))
"""


@pytest.fixture(scope="module")
def wheel_python(tmp_path_factory: pytest.TempPathFactory) -> tuple[Path, str]:
    wheels = list((ROOT / "dist").glob("django_altimeter-*.whl"))
    assert len(wheels) == 1, "Run uv build before installed-wheel CLI tests"
    workspace = tmp_path_factory.mktemp("check-command-wheel")
    wheel = workspace / wheels[0].name
    wheel.write_bytes(wheels[0].read_bytes())
    python = workspace / "venv" / "bin" / "python"
    commands = [
        [sys.executable, "-m", "venv", str(python.parents[1])],
        [
            str(python),
            "-m",
            "pip",
            "install",
            "--disable-pip-version-check",
            str(wheel),
            *[
                f"{name}=={importlib.metadata.version(name)}"
                for name in (
                    "Django",
                    "requests",
                    "urllib3",
                    "opentelemetry-api",
                    "opentelemetry-sdk",
                    "opentelemetry-exporter-otlp-proto-http",
                )
            ],
        ],
    ]
    for command in commands:
        result = subprocess.run(
            command,
            cwd=workspace,
            capture_output=True,
            text=True,
            timeout=180,
            check=False,
        )
        assert result.returncode == 0, result.stdout + result.stderr
    return python, hashlib.sha256(wheel.read_bytes()).hexdigest()


def _invoke(
    wheel_python: tuple[Path, str],
    tmp_path: Path,
    *arguments: str,
    environment: dict[str, str] | None = None,
    json_output: bool = True,
) -> tuple[subprocess.CompletedProcess[str], dict, float]:
    python, wheel_hash = wheel_python
    observation = tmp_path / f"cli-observation-{uuid4()}.json"
    started = monotonic()
    result = subprocess.run(
        [
            str(python),
            "-I",
            "-c",
            _CLI,
            "altimeter_check",
            *(["--json"] if json_output else []),
            *arguments,
        ],
        cwd=tmp_path,
        env={
            "PATH": os.environ.get("PATH", ""),
            "DJANGO_SETTINGS_MODULE": "altimeter_reference.settings",
            "REFERENCE_PATH": str(ROOT / "reference"),
            "OBSERVATION_PATH": str(observation),
            "WHEEL_SHA256": wheel_hash,
            "ALTIMETER_ENABLED": "false",
            **(environment or {}),
        },
        capture_output=True,
        text=True,
        timeout=15,
        check=False,
    )
    elapsed = monotonic() - started
    assert observation.is_file(), result.stdout + result.stderr
    for canary in _CANARIES:
        assert canary not in result.stdout + result.stderr
    return result, json.loads(observation.read_text()), elapsed


def _status(result: subprocess.CompletedProcess[str]) -> dict:
    status = json.loads(result.stdout)
    assert type(status) is dict
    assert set(status) == {
        "mode",
        "ok",
        "configured",
        "codes",
        "service",
        "versions",
        "integrations",
        "signals",
    }
    assert [row["signal"] for row in status["signals"]] == list(_SIGNALS)
    return status


def _rows(status: dict) -> dict[str, dict]:
    return {row["signal"]: row for row in status["signals"]}


def _assert_inert(observed: dict) -> None:
    assert observed["threads"] == []
    assert observed["sockets"] == []
    assert observed["exporters"] == []


def _enabled_environment(receiver=None, *, signals=_SIGNALS) -> dict[str, str]:
    return {
        "ALTIMETER_ENABLED": "true",
        "ALTIMETER_SERVICE_NAME": "command-test",
        "ALTIMETER_DEPLOYMENT_ENVIRONMENT": "test",
        "ALTIMETER_RELEASE": "test-release",
        "ALTIMETER_ROLE": "web",
        "ALTIMETER_POSTHOG_TOKEN": _INGEST_CANARY,
        **{
            key: value
            for signal in _SIGNALS
            for key, value in {
                f"ALTIMETER_{signal.upper()}_ENABLED": str(signal in signals).lower(),
                f"ALTIMETER_{signal.upper()}_COLLECTOR_ENDPOINT": (
                    receiver.url(signal)
                    if receiver
                    else f"http://127.0.0.1:9/v1/{signal}"
                ),
                f"ALTIMETER_{signal.upper()}_COLLECTOR_HEADERS": (
                    f"x-api-key={_HEADER_CANARY}"
                ),
            }.items()
        },
    }


def _probe_records(captures) -> dict[str, list[tuple[dict, object]]]:
    records = {signal: [] for signal in _SIGNALS}
    for capture in captures:
        signal = capture.path.rsplit("/", 1)[-1]
        assert signal in records
        assert capture.headers["x-api-key"] == _HEADER_CANARY
        assert capture.headers.get("Authorization") is None
        for canary in _CANARIES:
            assert canary.encode() not in capture.message.SerializeToString()
        groups = getattr(
            capture.message, f"resource_{'spans' if signal == 'traces' else signal}"
        )
        for group in groups:
            attributes = {
                item.key: getattr(item.value, item.value.WhichOneof("value"))
                for item in group.resource.attributes
            }
            assert attributes["altimeter.role"] == "probe"
            assert attributes["altimeter.probe"] is True
            assert UUID(attributes["service.instance.id"]).version == 4
            assert attributes["service.name"] == "command-test"
            assert attributes["deployment.environment.name"] == "test"
            assert attributes["service.version"] == "test-release"
            scopes = getattr(
                group, f"scope_{'spans' if signal == 'traces' else signal}"
            )
            for scope in scopes:
                assert scope.scope.name == "django_altimeter"
                if signal == "traces":
                    for span in scope.spans:
                        assert span.name == "altimeter.probe"
                        assert len(span.trace_id) == 16 and len(span.span_id) == 8
                        assert span.end_time_unix_nano >= span.start_time_unix_nano > 0
                        records[signal].append((attributes, span))
                elif signal == "logs":
                    for log in scope.log_records:
                        assert log.body.string_value == "altimeter.probe"
                        assert log.time_unix_nano > 0
                        records[signal].append((attributes, log))
                else:
                    for metric in scope.metrics:
                        assert metric.name == "altimeter.probe"
                        data = getattr(metric, metric.WhichOneof("data"))
                        for point in data.data_points:
                            assert point.time_unix_nano > 0
                            assert getattr(point, point.WhichOneof("value")) == 1
                            assert not point.attributes
                            records[signal].append((attributes, point))
    return records


@pytest.mark.parametrize("json_output", [True, False])
def test_disabled_local_succeeds_without_starting_telemetry(
    wheel_python, tmp_path, json_output
) -> None:
    result, observed, _ = _invoke(wheel_python, tmp_path, json_output=json_output)
    assert result.returncode == 0, result.stdout + result.stderr
    _assert_inert(observed)
    if json_output:
        status = _status(result)
        assert status["mode"] == "local" and status["ok"] is True
        assert status["configured"] is True
        assert all(row["configured"] is False for row in status["signals"])
        assert all(
            row["accepted"] is None and row["observed"] is None
            for row in status["signals"]
        )
    else:
        assert "config.disabled" in result.stdout


def test_enabled_local_reports_safe_effective_configuration_without_export(
    wheel_python, tmp_path
) -> None:
    result, observed, _ = _invoke(
        wheel_python, tmp_path, environment=_enabled_environment()
    )
    assert result.returncode == 0, result.stdout + result.stderr
    _assert_inert(observed)
    status = _status(result)
    assert status["ok"] is True and status["configured"] is True
    assert status["service"] == {
        "name": "command-test",
        "environment": "test",
        "release": "test-release",
        "role": "web",
    }
    assert status["versions"]["Django"] == importlib.metadata.version("Django")
    assert status["versions"]["opentelemetry-sdk"] == importlib.metadata.version(
        "opentelemetry-sdk"
    )
    assert all(row["configured"] is True for row in status["signals"])
    assert all(
        row["accepted"] is None and row["observed"] is None for row in status["signals"]
    )
    assert not status["integrations"]["missing"]


@pytest.mark.parametrize("invalid", ["enabled", "dsn", "credential_reuse"])
def test_invalid_requested_configuration_fails_safely(
    wheel_python, tmp_path, invalid
) -> None:
    environment = _enabled_environment()
    if invalid == "enabled":
        environment["ALTIMETER_ENABLED"] = _CONFIG_CANARY
    elif invalid == "credential_reuse":
        environment["ALTIMETER_POSTHOG_TOKEN"] = _HEADER_CANARY
    else:
        environment["ALTIMETER_TRACES_COLLECTOR_ENDPOINT"] = (
            f"http://user:{_DSN_CANARY}@127.0.0.1:9/v1/traces"
        )
    result, observed, _ = _invoke(wheel_python, tmp_path, environment=environment)
    assert result.returncode == 1, result.stdout + result.stderr
    assert _CONFIG_CANARY not in result.stdout + result.stderr
    _assert_inert(observed)
    status = _status(result)
    assert status["ok"] is False and status["configured"] is False
    assert status["codes"]


@pytest.mark.parametrize("field", ["SERVICE_NAME", "RELEASE"])
def test_configured_metadata_never_displays_credentials(
    wheel_python, tmp_path, field
) -> None:
    environment = _enabled_environment()
    environment[f"ALTIMETER_{field}"] = f"https://user:{_DSN_CANARY}@example.invalid"
    result, observed, _ = _invoke(wheel_python, tmp_path, environment=environment)
    assert result.returncode == 0, result.stdout + result.stderr
    assert _status(result)["configured"] is True
    _assert_inert(observed)


@pytest.mark.parametrize("mode", ["--local", "--probe"])
def test_foreign_provider_conflict_fails_without_replacing_it(
    wheel_python, tmp_path, mode
) -> None:
    result, observed, _ = _invoke(
        wheel_python,
        tmp_path,
        mode,
        "--signal",
        "traces",
        environment={**_enabled_environment(), "FOREIGN_PROVIDER": "true"},
    )
    assert result.returncode == 1, result.stdout + result.stderr
    status = _status(result)
    assert status["ok"] is False
    assert "setup.traces_foreign" in status["codes"] + _rows(status)["traces"]["codes"]
    _assert_inert(observed)


@pytest.mark.parametrize("integration", ["celery", "rq"])
def test_requested_missing_queue_dependency_fails_safely(
    wheel_python, tmp_path, integration
) -> None:
    result, observed, _ = _invoke(
        wheel_python,
        tmp_path,
        environment={**_enabled_environment(), "ALTIMETER_INTEGRATIONS": integration},
    )
    assert result.returncode == 1, result.stdout + result.stderr
    status = _status(result)
    assert f"check.{integration}_missing" in status["codes"]
    assert integration in status["integrations"]["missing"]
    _assert_inert(observed)


@pytest.mark.parametrize(
    "arguments",
    [
        ("--signal", _CONFIG_CANARY),
        ("--timeout", _CONFIG_CANARY),
        ("--timeout", "nan"),
        (f"--{_CONFIG_CANARY}",),
        ("--probe", "--read-back"),
    ],
)
def test_invalid_cli_arguments_fail_without_echoing_values(
    wheel_python, tmp_path, arguments
) -> None:
    result, observed, _ = _invoke(wheel_python, tmp_path, *arguments)
    assert result.returncode == 2, result.stdout + result.stderr
    assert "check.invalid_arguments" in result.stdout + result.stderr
    _assert_inert(observed)


@pytest.mark.parametrize("enabled", [False, True])
def test_probe_with_no_enabled_signal_fails(wheel_python, tmp_path, enabled) -> None:
    environment = _enabled_environment(signals=()) if enabled else {}
    result, observed, _ = _invoke(
        wheel_python, tmp_path, "--probe", environment=environment
    )
    assert result.returncode == 1, result.stdout + result.stderr
    status = _status(result)
    assert status["ok"] is False
    assert "nothing_to_probe" in status["codes"]
    assert not any(row["accepted"] is True for row in status["signals"])
    _assert_inert(observed)


def test_explicit_disabled_signal_is_valid_locally_but_cannot_be_probed(
    wheel_python, tmp_path
) -> None:
    environment = _enabled_environment(signals=("traces",))
    local, observed, _ = _invoke(
        wheel_python, tmp_path, "--signal", "logs", environment=environment
    )
    assert local.returncode == 0, local.stdout + local.stderr
    _assert_inert(observed)
    result, observed, _ = _invoke(
        wheel_python, tmp_path, "--probe", "--signal", "logs", environment=environment
    )
    assert result.returncode == 1, result.stdout + result.stderr
    assert "probe.signal_disabled" in _status(result)["codes"]
    _assert_inert(observed)


def test_probe_sends_one_safe_record_per_signal_with_fresh_writer(
    wheel_python, tmp_path, otlp_receiver
) -> None:
    identities = []
    for _ in range(2):
        before = len(otlp_receiver.requests)
        result, observed, _ = _invoke(
            wheel_python,
            tmp_path,
            "--probe",
            environment=_enabled_environment(otlp_receiver),
        )
        assert result.returncode == 0, result.stdout + result.stderr
        status = _status(result)
        assert status["mode"] == "probe" and status["ok"] is True
        assert all(
            row["requested"] is True
            and row["accepted"] is True
            and row["observed"] is None
            for row in status["signals"]
        )
        captures = otlp_receiver.requests[before:]
        assert len(captures) == 3
        records = _probe_records(captures)
        assert all(len(items) == 1 for items in records.values())
        writers = {items[0][0]["service.instance.id"] for items in records.values()}
        assert len(writers) == 1
        identities.extend(writers)
        _assert_inert(observed)
    assert len(set(identities)) == 2


@pytest.mark.parametrize("signal", _SIGNALS)
def test_probe_only_sends_explicit_selected_signal(
    wheel_python, tmp_path, otlp_receiver, signal
) -> None:
    result, _, _ = _invoke(
        wheel_python,
        tmp_path,
        "--probe",
        "--signal",
        signal,
        "--signal",
        signal,
        environment=_enabled_environment(otlp_receiver),
    )
    assert result.returncode == 0, result.stdout + result.stderr
    rows = _rows(_status(result))
    assert rows[signal]["accepted"] is True
    assert all(
        row["accepted"] is None and row["requested"] is False
        for name, row in rows.items()
        if name != signal
    )
    assert len(otlp_receiver.requests) == 1
    records = _probe_records(otlp_receiver.requests)
    assert len(records[signal]) == 1
    assert all(not items for name, items in records.items() if name != signal)


@pytest.mark.parametrize(
    "response,code,accepted",
    [
        ("partial", "export.partial", False),
        ("invalid", "export.invalid", False),
        ("warning", "export.warning", True),
        ("auth", "export.http_error", False),
    ],
)
@pytest.mark.parametrize("json_output", [True, False])
def test_probe_uses_transport_acceptance_and_hides_vendor_text(
    wheel_python, tmp_path, otlp_receiver, response, code, accepted, json_output
) -> None:
    if response in {"partial", "warning"}:
        message = ExportTraceServiceResponse()
        message.partial_success.rejected_spans = 1 if response == "partial" else 0
        message.partial_success.error_message = _VENDOR_CANARY
        otlp_receiver.queue_response(body=message.SerializeToString())
    elif response == "invalid":
        otlp_receiver.queue_response(body=b"\xff" + _VENDOR_CANARY.encode())
    else:
        otlp_receiver.queue_response(
            status=401, body=_VENDOR_CANARY.encode(), reason=_VENDOR_CANARY
        )
    result, _, _ = _invoke(
        wheel_python,
        tmp_path,
        "--probe",
        "--signal",
        "traces",
        environment=_enabled_environment(otlp_receiver),
        json_output=json_output,
    )
    assert result.returncode == (0 if accepted else 1), result.stdout + result.stderr
    if json_output:
        status = _status(result)
        assert status["ok"] is accepted
        trace = _rows(status)["traces"]
        assert trace["accepted"] is accepted and trace["observed"] is None
        assert code in trace["codes"]
    else:
        assert f"mode=probe ok={str(accepted).lower()}" in result.stdout
        trace_row = next(
            line for line in result.stdout.splitlines() if line.startswith("traces ")
        )
        assert f"accepted={str(accepted).lower()}" in trace_row
        assert code in trace_row
    assert len(_probe_records(otlp_receiver.requests)["traces"]) == 1


def test_probe_hanging_receiver_fails_with_bounded_wait(
    wheel_python, tmp_path, otlp_receiver
) -> None:
    otlp_receiver.queue_response(hang=True)
    result, observed, elapsed = _invoke(
        wheel_python,
        tmp_path,
        "--probe",
        "--signal",
        "traces",
        environment={
            **_enabled_environment(otlp_receiver),
            "ALTIMETER_EXPORT_TIMEOUT": "2",
        },
    )
    assert result.returncode == 1, result.stdout + result.stderr
    status = _status(result)
    assert status["ok"] is False and _rows(status)["traces"]["accepted"] is False
    assert observed["command_seconds"] < 4.0
    assert elapsed < 5.0
    assert len(_probe_records(otlp_receiver.requests)["traces"]) == 1


def _readback_environment(server) -> dict[str, str]:
    return {
        "ALTIMETER_READBACK_HOST": server.endpoint,
        "ALTIMETER_READBACK_PROJECT_ID": "123",
        "ALTIMETER_READBACK_API_KEY": _OPERATOR_CANARY,
    }


def _seed_received_probe(receiver, server, evidence: str) -> str:
    captures = receiver.wait_for_requests(count=3, timeout=5)
    records = _probe_records(captures)
    writers = {items[0][0]["service.instance.id"] for items in records.values()}
    assert len(writers) == 1
    identity = writers.pop()
    for signal, items in records.items():
        if evidence == "trace_only" and signal != "traces":
            continue
        record = items[0][1]
        nanos = (
            record.start_time_unix_nano if signal == "traces" else record.time_unix_nano
        )
        timestamp = datetime.fromtimestamp(nanos / 1_000_000_000, UTC)
        server.add_record(
            signal,
            instance_id=str(uuid4()) if evidence == "wrong_writer" else identity,
            timestamp=timestamp - timedelta(minutes=10)
            if evidence == "stale"
            else timestamp,
            probe=evidence != "untagged",
            name="unrelated" if evidence == "wrong_name" else "altimeter.probe",
            scope="unrelated" if evidence == "wrong_scope" else "django_altimeter",
        )
    return identity


@pytest.mark.parametrize(
    "evidence",
    [
        "all",
        "trace_only",
        "wrong_writer",
        "stale",
        "untagged",
        "wrong_name",
        "wrong_scope",
    ],
)
def test_readback_requires_each_fresh_tagged_signal(
    wheel_python, tmp_path, otlp_receiver, evidence
) -> None:
    with PostHogQueryServer() as server, ThreadPoolExecutor(max_workers=1) as executor:
        seeded = executor.submit(_seed_received_probe, otlp_receiver, server, evidence)
        result, _, _ = _invoke(
            wheel_python,
            tmp_path,
            "--read-back",
            "--timeout",
            "0.5",
            environment={
                **_enabled_environment(otlp_receiver),
                **_readback_environment(server),
            },
        )
        identity = seeded.result(timeout=5)
        assert result.returncode == (0 if evidence == "all" else 1), (
            result.stdout + result.stderr
        )
        status = _status(result)
        assert status["mode"] == "read-back" and status["configured"] is True
        assert status["ok"] is (evidence == "all")
        rows = _rows(status)
        assert all(row["accepted"] is True for row in rows.values())
        for signal, row in rows.items():
            assert row["observed"] is (
                evidence == "all"
                or (evidence == "trace_only" and signal == "traces")
                or (evidence == "wrong_scope" and signal != "metrics")
            )
        tables = set()
        assert 3 <= len(server.requests) <= 18
        for request in server.requests:
            assert request.path == "/api/projects/123/query/"
            assert request.headers["Authorization"] == f"Bearer {_OPERATOR_CANARY}"
            assert request.body["query"]["values"]["instance_id"] == identity
            sql = request.body["query"]["query"]
            assert "altimeter.probe" in sql and "service.instance.id" in sql
            for signal, table in {
                "traces": "trace_spans",
                "logs": "logs",
                "metrics": "metrics",
            }.items():
                if f"FROM posthog.{table}\n" in sql:
                    tables.add(signal)
        assert tables == set(_SIGNALS)


def test_unsupported_readback_fails_without_disclosing_vendor_body(
    wheel_python, tmp_path, otlp_receiver
) -> None:
    with PostHogQueryServer() as server:
        server.queue_response(status=404, body=_VENDOR_CANARY.encode())
        result, _, _ = _invoke(
            wheel_python,
            tmp_path,
            "--read-back",
            "--signal",
            "traces",
            "--timeout",
            "0.5",
            environment={
                **_enabled_environment(otlp_receiver),
                **_readback_environment(server),
            },
        )
        assert result.returncode == 1, result.stdout + result.stderr
        status = _status(result)
        row = _rows(status)["traces"]
        assert row["accepted"] is True and row["observed"] is False
        assert "readback.unsupported" in row["codes"]
        assert len(server.requests) == 1
        assert len(_probe_records(otlp_receiver.requests)["traces"]) == 1


def test_requested_readback_without_credentials_never_passes(
    wheel_python, tmp_path, otlp_receiver
) -> None:
    result, _, _ = _invoke(
        wheel_python,
        tmp_path,
        "--read-back",
        "--signal",
        "traces",
        "--timeout",
        "0.5",
        environment=_enabled_environment(otlp_receiver),
    )
    assert result.returncode == 1, result.stdout + result.stderr
    status = _status(result)
    assert status["ok"] is False
    row = _rows(status)["traces"]
    assert row["accepted"] is True and row["observed"] is False
    assert "readback.credentials_missing" in status["codes"] + row["codes"]
    assert len(_probe_records(otlp_receiver.requests)["traces"]) == 1


def test_cli_observer_detects_real_thread_socket_and_exporter(
    wheel_python, tmp_path
) -> None:
    result, observed, _ = _invoke(
        wheel_python, tmp_path, environment={"OBSERVATION_CONTROL": "true"}
    )
    assert result.returncode == 0, result.stdout + result.stderr
    assert observed["threads"] and observed["exporters"]
    assert "socket.__new__" in observed["sockets"]
