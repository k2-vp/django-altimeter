"""Launch real uvicorn from a copied wheel, outside the source checkout."""

import hashlib
import importlib.metadata
import json
import os
import signal
import socket
import subprocess
import sys
import time
from contextlib import contextmanager
from dataclasses import dataclass
from http.client import HTTPConnection
from pathlib import Path

from support.otlp_receiver import OTLPReceiver

ROOT = Path(__file__).resolve().parents[2]


@dataclass(frozen=True)
class WheelEnvironment:
    python: Path
    sha256: str


def install_wheel(
    workspace: Path,
    *,
    extras: tuple[str, ...] = (),
    server_dependencies: tuple[str, ...] = (
        "django-ninja==1.6.2",
        "uvicorn==0.46.0",
    ),
) -> WheelEnvironment:
    workspace.mkdir(parents=True, exist_ok=True)
    wheels = list((ROOT / "dist").glob("django_altimeter-*.whl"))
    assert len(wheels) == 1, "Run uv build before the real-server tests"
    wheel = workspace / wheels[0].name
    wheel.write_bytes(wheels[0].read_bytes())
    subprocess.run([sys.executable, "-m", "venv", str(workspace / "venv")], check=True)
    python = workspace / "venv/bin/python"
    subprocess.run(
        [
            str(python),
            "-m",
            "pip",
            "install",
            "--disable-pip-version-check",
            f"{wheel}[{','.join(extras)}]" if extras else str(wheel),
            *server_dependencies,
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
        check=True,
        timeout=180,
    )
    return WheelEnvironment(python, hashlib.sha256(wheel.read_bytes()).hexdigest())


_DRIVER = r"""
import importlib.metadata
import json
import os
from pathlib import Path
import sys
import threading
from urllib.parse import urlsplit

sys.path.insert(0, os.environ["REFERENCE_PATH"])
import django_altimeter
origin = Path(django_altimeter.__file__).resolve()
assert origin.is_relative_to(Path(sys.prefix).resolve()), origin
distribution = importlib.metadata.distribution("django-altimeter")
direct = json.loads(distribution.read_text("direct_url.json"))
assert direct["archive_info"]["hashes"]["sha256"] == os.environ["WHEEL_SHA256"]
assert not direct.get("dir_info", {}).get("editable", False)

workers = int(os.environ["SERVER_WORKERS"])
observed = {"setup": [], "exporters": [], "collector_connect": [], "profile_events": 0}
collector_port = urlsplit(os.environ["RECEIVER_ENDPOINT"]).port

def profile(frame, event, arg):
    observed["profile_events"] += 1
    if event != "call":
        return
    module = frame.f_globals.get("__name__", "")
    name = frame.f_code.co_name
    if module == "django_altimeter.runtime" and name == "setup":
        observed["setup"].append(os.getpid())
    if module.startswith("opentelemetry.exporter.") and name == "__init__":
        observed["exporters"].append(module)

def audit(event, args):
    if event == "socket.connect" and isinstance(args[1], tuple):
        if len(args[1]) >= 2 and args[1][1] == collector_port:
            observed["collector_connect"].append(os.getpid())

# Only the non-serving supervisor is profiled. Single-worker benchmarks have
# no tracing/profile callbacks in their request execution path.
if workers > 1:
    sys.setprofile(profile)
    threading.setprofile(profile)
    sys.addaudithook(audit)
import uvicorn
try:
    uvicorn.run(
        os.environ.get(
            "REFERENCE_FACTORY", "altimeter_reference.asgi:create_application",
        ),
        factory=True, host="127.0.0.1", port=int(os.environ["SERVER_PORT"]),
        workers=workers, loop="asyncio", http="h11", lifespan="on",
        access_log=False, log_level="warning", timeout_graceful_shutdown=2,
    )
finally:
    sys.setprofile(None)
    threading.setprofile(None)
    Path(os.environ["SUPERVISOR_REPORT"]).write_text(json.dumps({
        **observed, "pid": os.getpid(), "origin": str(origin),
    }))
"""


@dataclass(frozen=True)
class Server:
    port: int
    process: subprocess.Popen
    workspace: Path
    name: str = "uvicorn"

    def request(self, path: str, *, method="GET", headers=None, timeout=5):
        connection = HTTPConnection("127.0.0.1", self.port, timeout=timeout)
        try:
            connection.request(method, path, headers=headers or {})
            response = connection.getresponse()
            return response.status, dict(response.getheaders()), response.read()
        finally:
            connection.close()

    @property
    def lifecycle(self):
        path = self.workspace / "lifecycle.jsonl"
        return (
            [json.loads(line) for line in path.read_text().splitlines()]
            if path.exists()
            else []
        )

    @property
    def supervisor(self):
        return json.loads((self.workspace / "supervisor.json").read_text())

    @property
    def output(self):
        return (self.workspace / "server.log").read_text()

    def stop(self):
        if self.process.poll() is None:
            self.process.send_signal(signal.SIGTERM)
            try:
                self.process.wait(timeout=7)
            except subprocess.TimeoutExpired:
                os.killpg(self.process.pid, signal.SIGKILL)
                self.process.wait(timeout=2)
                raise AssertionError(
                    f"{self.name} exceeded shutdown budget\n{self.output}"
                ) from None
        assert self.process.returncode in (0, -signal.SIGTERM), self.output


@contextmanager
def running_server(
    workspace: Path,
    *,
    wheel: WheelEnvironment,
    receiver: OTLPReceiver,
    enabled=True,
    workers=1,
    extra_env=None,
):
    workspace.mkdir(parents=True, exist_ok=True)
    with socket.socket() as candidate:
        candidate.bind(("127.0.0.1", 0))
        port = candidate.getsockname()[1]
    environment = {
        "PATH": os.environ.get("PATH", ""),
        "REFERENCE_PATH": str(ROOT / "reference"),
        "DJANGO_SETTINGS_MODULE": "altimeter_reference.web_settings",
        "WHEEL_SHA256": wheel.sha256,
        "SERVER_PORT": str(port),
        "SERVER_WORKERS": str(workers),
        "SUPERVISOR_REPORT": str(workspace / "supervisor.json"),
        "REFERENCE_LIFECYCLE_FILE": str(workspace / "lifecycle.jsonl"),
        "RECEIVER_ENDPOINT": receiver.endpoint,
        "ALTIMETER_ENABLED": "true" if enabled else "false",
        "ALTIMETER_SERVICE_NAME": "reference-web",
        "ALTIMETER_DEPLOYMENT_ENVIRONMENT": "test",
        "ALTIMETER_RELEASE": wheel.sha256[:16],
        "ALTIMETER_INTEGRATIONS": "asgi,django",
        "ALTIMETER_LOGS_ENABLED": "false",
        "OTEL_SEMCONV_STABILITY_OPT_IN": "http",
        "ALTIMETER_TRACES_COLLECTOR_ENDPOINT": receiver.url("traces"),
        "ALTIMETER_METRICS_COLLECTOR_ENDPOINT": receiver.url("metrics"),
        "ALTIMETER_BATCH_SCHEDULE_DELAY_MILLIS": "250",
        "ALTIMETER_METRIC_EXPORT_INTERVAL_MILLIS": "250",
        "ALTIMETER_EXPORT_TIMEOUT": "0.2",
        **(extra_env or {}),
    }
    with (workspace / "server.log").open("w") as output:
        process = subprocess.Popen(
            [str(wheel.python), "-I", "-c", _DRIVER],
            cwd=workspace,
            env=environment,
            stdout=output,
            stderr=subprocess.STDOUT,
            start_new_session=True,
        )
        server = Server(port, process, workspace)
        try:
            deadline = time.monotonic() + 15
            while time.monotonic() < deadline:
                assert process.poll() is None, server.output
                try:
                    status, _, body = server.request("/health/")
                    if status == 200 and json.loads(body)["ok"]:
                        break
                except (OSError, ConnectionError):
                    time.sleep(0.025)
            else:
                raise AssertionError(f"uvicorn did not become ready\n{server.output}")
            yield server
        finally:
            server.stop()
            # Preserve diagnostics in pytest output; never hide cited stderr.
            print(server.output, end="")
