"""Exercise Django startup in isolated processes using the installed wheel."""

import hashlib
import importlib.metadata
import json
import os
import socket
import subprocess
import sys
from pathlib import Path

import pytest

pytestmark = pytest.mark.integration
ROOT = Path(__file__).resolve().parents[2]
_ENABLED_ENV = {
    "ALTIMETER_SERVICE_NAME": "boundary-app",
    "ALTIMETER_DEPLOYMENT_ENVIRONMENT": "test",
    "ALTIMETER_RELEASE": "test-release",
    **{
        f"ALTIMETER_{signal.upper()}_COLLECTOR_ENDPOINT": (
            f"http://127.0.0.1:9/v1/{signal}"
        )
        for signal in ("traces", "logs", "metrics")
    },
}

_PROBE = r"""
import _thread
import hashlib
import importlib.metadata
import json
import os
from pathlib import Path
import sys
import threading

sys.path.insert(0, os.environ["REFERENCE_PATH"])
threads = []
network = []
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
        if (
            module.startswith("opentelemetry.exporter.")
            and frame.f_code.co_name == "__init__"
        ):
            exporters.append(module)

def audit(event, args):
    if event.startswith("socket."):
        observed = {"event": event}
        if event == "socket.__new__":
            observed.update(family=int(args[1]), kind=int(args[2]), protocol=args[3])
        elif event == "socket.bind":
            observed["address"] = args[1]
        network.append(observed)

sys.addaudithook(audit)
sys.setprofile(observe)
import django_altimeter

origin = Path(django_altimeter.__file__).resolve()
assert origin.is_relative_to(Path(sys.prefix).resolve()), origin
installed = importlib.metadata.distribution("django-altimeter")
direct_url = json.loads(installed.read_text("direct_url.json"))
assert direct_url["archive_info"]["hashes"]["sha256"] == os.environ["WHEEL_SHA256"]
assert not direct_url.get("dir_info", {}).get("editable", False)

mode = os.environ["BOUNDARY_MODE"]
messages = []
working = None
setup_result = None
setup_network = None
shutdown_result = None
correlation = None
if mode == "observe":
    from socket import socket
    from opentelemetry.exporter.otlp.proto.http.trace_exporter import OTLPSpanExporter
    with socket():
        pass
    thread = threading.Thread(target=lambda: None)
    thread.start()
    thread.join()
    exporter = OTLPSpanExporter(endpoint="http://127.0.0.1:9/v1/traces", timeout=0.1)
    exporter.shutdown()
elif mode != "import":
    import django
    from django.apps import apps
    from django.core import checks
    from django.core.management import execute_from_command_line
    from django.http import HttpRequest

    if mode == "migrate":
        execute_from_command_line(
            ["django", "migrate", "--noinput", "--verbosity", "0"]
        )
    elif mode == "shell":
        execute_from_command_line(["django", "shell", "--command", "assert 2 + 2 == 4"])
    else:
        django.setup()
        if mode == "ready":
            apps.get_app_config("django_altimeter").ready()
    foreign = ()
    if mode == "setup_unowned":
        from opentelemetry import metrics, trace
        from opentelemetry._logs import get_logger_provider, set_logger_provider
        from opentelemetry.metrics import NoOpMeterProvider
        from opentelemetry.sdk._logs import LoggerProvider
        from opentelemetry.sdk.metrics import MeterProvider
        from opentelemetry.sdk.resources import Resource
        from opentelemetry.sdk.trace import TracerProvider

        foreign = (
            TracerProvider(
                resource=Resource({}), shutdown_on_exit=False,
                meter_provider=NoOpMeterProvider(),
            ),
            LoggerProvider(
                resource=Resource({}), shutdown_on_exit=False,
                meter_provider=NoOpMeterProvider(),
            ),
            MeterProvider(resource=Resource({}), shutdown_on_exit=False),
        )
        trace.set_tracer_provider(foreign[0])
        set_logger_provider(foreign[1])
        metrics.set_meter_provider(foreign[2])
        assert "urllib3" not in sys.modules
        # Only the already-installed foreign providers precede this boundary.
        threads.clear()
        network.clear()
        exporters.clear()
    if mode.startswith("setup"):
        result = django_altimeter.setup(role="web")
        setup_result = {
            "enabled_signals": result.enabled_signals,
            "codes": result.codes,
            "ok": result.ok,
        }
        setup_network = list(network)
        if mode in {"setup_inert", "setup_unowned"}:
            assert django_altimeter.get_correlation() is None
            with django_altimeter.span("reference.operation") as span:
                assert not span.is_recording()
                assert django_altimeter.get_correlation() is None
                django_altimeter.enrich(object())
            expected = LookupError("application exception")
            try:
                with django_altimeter.span("reference.operation"):
                    raise expected
            except LookupError as error:
                assert error is expected
            else:
                raise AssertionError("Application exception was suppressed")
        elif mode == "setup_emit":
            with django_altimeter.span(
                "GET",
                attributes={
                    "http.response.status_code": 200,
                    "password": "public-api-secret-canary",
                },
            ):
                current = django_altimeter.get_correlation()
                assert current is not None
                correlation = {
                    "trace_id": current.trace_id,
                    "span_id": current.span_id,
                    "trace_flags": current.trace_flags,
                }
        stopped = django_altimeter.shutdown(timeout=2.0)
        shutdown_result = {"completed": stopped.completed, "codes": stopped.codes}
        if foreign:
            assert trace.get_tracer_provider() is foreign[0]
            assert get_logger_provider() is foreign[1]
            assert metrics.get_meter_provider() is foreign[2]
            for provider in foreign:
                provider.shutdown()
    from altimeter_reference.urls import health
    response = health(HttpRequest())
    working = (
        response.status_code == 200 and json.loads(response.content) == {"ok": True}
    )
    messages = [
        {"id": message.id, "message": message.msg, "level": message.level}
        for message in checks.run_checks(tags=["django_altimeter"])
    ]
sys.setprofile(None)
print("BOUNDARY " + json.dumps({
    "threads": threads,
    "network": network,
    "exporters": exporters,
    "messages": messages,
    "working": working,
    "origin": str(origin),
    "setup": setup_result,
    "setup_network": setup_network,
    "shutdown": shutdown_result,
    "correlation": correlation,
}))
"""


def _run(command: list[str], **kwargs) -> subprocess.CompletedProcess[str]:
    result = subprocess.run(
        command, capture_output=True, text=True, timeout=180, check=False, **kwargs
    )
    assert result.returncode == 0, result.stdout + result.stderr
    return result


@pytest.fixture(scope="module")
def wheel_python(tmp_path_factory: pytest.TempPathFactory) -> tuple[Path, str]:
    wheels = list((ROOT / "dist").glob("django_altimeter-*.whl"))
    assert len(wheels) == 1, "Run uv build before the installed-wheel boundary tests"
    source_wheel = wheels[0]
    workspace = tmp_path_factory.mktemp("django-boundary-wheel")
    wheel = workspace / source_wheel.name
    wheel.write_bytes(source_wheel.read_bytes())
    environment = workspace / "venv"
    _run([sys.executable, "-m", "venv", str(environment)], cwd=workspace)
    python = environment / "bin" / "python"
    versions = [
        f"{name}=={importlib.metadata.version(name)}"
        for name in ("Django", "requests", "urllib3")
    ]
    _run(
        [
            str(python),
            "-m",
            "pip",
            "install",
            "--disable-pip-version-check",
            str(wheel),
            *versions,
        ],
        cwd=workspace,
    )
    return python, hashlib.sha256(wheel.read_bytes()).hexdigest()


def _probe(
    wheel_python: tuple[Path, str],
    tmp_path: Path,
    mode: str,
    *,
    enabled: str = "false",
    overrides: dict[str, str] | None = None,
) -> dict:
    python, wheel_hash = wheel_python
    environment = {
        "PATH": os.environ.get("PATH", ""),
        "DJANGO_SETTINGS_MODULE": "altimeter_reference.settings",
        "REFERENCE_PATH": str(ROOT / "reference"),
        "WHEEL_SHA256": wheel_hash,
        "BOUNDARY_MODE": mode,
        "ALTIMETER_ENABLED": enabled,
        **(overrides or {}),
    }
    result = _run([str(python), "-I", "-c", _PROBE], cwd=tmp_path, env=environment)
    snapshots = [
        line.removeprefix("BOUNDARY ")
        for line in result.stdout.splitlines()
        if line.startswith("BOUNDARY ")
    ]
    assert len(snapshots) == 1, result.stdout + result.stderr
    return json.loads(snapshots[0])


def _assert_inert(snapshot: dict) -> None:
    assert snapshot["threads"] == []
    assert snapshot["network"] == []
    assert snapshot["exporters"] == []


@pytest.mark.parametrize("mode", ["import", "ready", "migrate", "shell"])
@pytest.mark.parametrize("enabled", ["false", "true"])
def test_management_and_import_are_inert(wheel_python, tmp_path, mode, enabled) -> None:
    snapshot = _probe(
        wheel_python, tmp_path, mode, enabled=enabled, overrides=_ENABLED_ENV
    )
    _assert_inert(snapshot)
    assert snapshot["messages"] == []
    if mode != "import":
        assert snapshot["working"] is True


@pytest.mark.parametrize("mode", ["ready", "migrate", "shell"])
def test_bad_configuration_warns_without_breaking_the_callable(
    wheel_python, tmp_path, mode
) -> None:
    snapshot = _probe(wheel_python, tmp_path, mode, enabled="invalid-canary-value")
    _assert_inert(snapshot)
    assert snapshot["working"] is True
    assert snapshot["messages"], (
        "Invalid telemetry settings must produce a check warning"
    )
    assert all(
        message["id"] == "django_altimeter.W001" for message in snapshot["messages"]
    )
    assert all(message["level"] == 30 for message in snapshot["messages"])
    assert "invalid-canary-value" not in json.dumps(snapshot)


def test_side_effect_observer_detects_real_operations(wheel_python, tmp_path) -> None:
    snapshot = _probe(wheel_python, tmp_path, "observe")
    assert snapshot["threads"]
    assert "socket.__new__" in {event["event"] for event in snapshot["network"]}
    assert snapshot["exporters"]


@pytest.mark.parametrize("enabled", ["false", "invalid-canary-value"])
def test_public_disabled_or_invalid_setup_leaves_application_working(
    wheel_python, tmp_path, enabled
) -> None:
    snapshot = _probe(wheel_python, tmp_path, "setup_inert", enabled=enabled)
    _assert_inert(snapshot)
    assert snapshot["working"] is True
    assert snapshot["setup"]["enabled_signals"] == []
    assert snapshot["setup"]["ok"] is False
    assert snapshot["setup"]["codes"]
    assert snapshot["shutdown"]["completed"] is True
    assert "invalid-canary-value" not in json.dumps(snapshot)


def test_explicit_setup_installs_native_signals_without_network(
    wheel_python, tmp_path
) -> None:
    snapshot = _probe(
        wheel_python,
        tmp_path,
        "setup_enabled",
        enabled="true",
        overrides=_ENABLED_ENV,
    )
    assert snapshot["working"] is True
    assert snapshot["setup"]["ok"] is True
    assert set(snapshot["setup"]["enabled_signals"]) == {"traces", "logs", "metrics"}
    assert snapshot["setup"]["codes"] == []
    _assert_enabled_setup_network(snapshot)
    assert snapshot["threads"]
    assert snapshot["exporters"]


def test_public_setup_exports_a_safe_span(
    wheel_python, tmp_path, otlp_receiver
) -> None:
    snapshot = _probe(
        wheel_python,
        tmp_path,
        "setup_emit",
        enabled="true",
        overrides={
            **_ENABLED_ENV,
            "ALTIMETER_TRACES_COLLECTOR_ENDPOINT": otlp_receiver.url("traces"),
        },
    )
    assert snapshot["setup"]["ok"] is True
    assert snapshot["setup"]["codes"] == []
    _assert_enabled_setup_network(snapshot)
    assert snapshot["working"] is True
    assert snapshot["shutdown"]["completed"] is True
    (captured,) = otlp_receiver.wait_for_requests(count=1, timeout=2)
    assert captured.path == "/v1/traces"
    (resource,) = captured.message.resource_spans
    (scope,) = resource.scope_spans
    (span,) = scope.spans
    assert scope.scope.name == "django_altimeter"
    assert span.name == "GET"
    assert {item.key: item.value.int_value for item in span.attributes} == {
        "http.response.status_code": 200,
    }
    assert span.trace_id.hex() == snapshot["correlation"]["trace_id"]
    assert span.span_id.hex() == snapshot["correlation"]["span_id"]
    attributes = {
        item.key: item.value.string_value for item in resource.resource.attributes
    }
    assert attributes["service.name"] == "boundary-app"
    assert attributes["deployment.environment.name"] == "test"
    assert attributes["service.version"] == "test-release"
    assert attributes["altimeter.role"] == "web"
    assert attributes["service.instance.id"]
    assert b"public-api-secret-canary" not in captured.message.SerializeToString()


def _assert_enabled_setup_network(snapshot: dict) -> None:
    # The pinned urllib3 import can test IPv6 support locally. No other socket
    # event is permitted: connect, sendto and DNS resolution all fail this check.
    local_probe = [
        {
            "event": "socket.__new__",
            "family": socket.AF_INET6,
            "kind": socket.SOCK_STREAM,
            "protocol": 0,
        },
        {"event": "socket.bind", "address": ["::1", 0]},
    ]
    assert snapshot["setup_network"] in ([], local_probe)


def test_unowned_setup_stays_inert_with_real_foreign_providers(
    wheel_python, tmp_path
) -> None:
    snapshot = _probe(
        wheel_python, tmp_path, "setup_unowned", enabled="true", overrides=_ENABLED_ENV
    )
    _assert_inert(snapshot)
    assert snapshot["working"] is True
    assert snapshot["setup"]["enabled_signals"] == []
    assert set(snapshot["setup"]["codes"]) == {
        "setup.traces_foreign",
        "setup.logs_foreign",
        "setup.metrics_foreign",
    }
    assert snapshot["shutdown"]["completed"] is True
