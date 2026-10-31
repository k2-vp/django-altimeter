"""Real no-preload gunicorn gthread, using the exact copied wheel."""

import json
import os
import socket
import subprocess
import time
from contextlib import contextmanager
from pathlib import Path

from support.otlp_receiver import OTLPReceiver
from support.uvicorn_server import ROOT, Server, WheelEnvironment
from support.uvicorn_server import install_wheel as _install_wheel


def install_wheel(workspace: Path, *, extras: tuple[str, ...] = ()) -> WheelEnvironment:
    return _install_wheel(
        workspace,
        extras=extras,
        server_dependencies=("gunicorn==23.0.0", "djangorestframework==3.16.1"),
    )


_DRIVER = r"""
import importlib.metadata
import json
import os
from pathlib import Path
import sys
import threading
from urllib.parse import urlsplit

sys.path.insert(0, os.environ["REFERENCE_PATH"])
if os.environ.get("APPLICATION_PATH"):
    sys.path.insert(0, os.environ["APPLICATION_PATH"])
import django_altimeter
origin = Path(django_altimeter.__file__).resolve()
assert origin.is_relative_to(Path(sys.prefix).resolve()), origin
distribution = importlib.metadata.distribution("django-altimeter")
direct = json.loads(distribution.read_text("direct_url.json"))
assert direct["archive_info"]["hashes"]["sha256"] == os.environ["WHEEL_SHA256"]
assert not direct.get("dir_info", {}).get("editable", False)

master_pid = os.getpid()
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
    if os.getpid() == master_pid and event == "socket.connect":
        if (isinstance(args[1], tuple) and len(args[1]) >= 2
                and args[1][1] == collector_port):
            observed["collector_connect"].append(os.getpid())

def child():
    sys.setprofile(None)
    threading.setprofile(None)

if os.environ.get("OBSERVE_SUPERVISOR") == "1":
    sys.setprofile(profile)
    threading.setprofile(profile)
    sys.addaudithook(audit)
    os.register_at_fork(after_in_child=child)

sys.argv = ["gunicorn", os.environ["WSGI_APPLICATION"],
    "--bind", "127.0.0.1:" + os.environ["SERVER_PORT"],
    "--worker-class", "gthread", "--workers", os.environ["SERVER_WORKERS"],
    "--threads", os.environ["SERVER_THREADS"], "--timeout", "90",
    "--graceful-timeout", "5", "--keep-alive", "5",
    "--max-requests", os.environ["MAX_REQUESTS"], "--max-requests-jitter", "0",
    "--config", os.environ["GUNICORN_CONFIG"], "--log-level", "warning"]
if os.environ["PRELOAD"] == "1":
    sys.argv.append("--preload")
from gunicorn.app.wsgiapp import run
try:
    run()
finally:
    child()
    if os.getpid() == master_pid:
        Path(os.environ["SUPERVISOR_REPORT"]).write_text(json.dumps({
            **observed, "pid": master_pid, "origin": str(origin),
        }))
"""


@contextmanager
def running_server(
    workspace: Path,
    *,
    wheel: WheelEnvironment,
    receiver: OTLPReceiver,
    enabled=True,
    workers=1,
    threads=4,
    max_requests=0,
    preload=False,
    extra_env=None,
    application="altimeter_reference.wsgi:application",
    module_path=None,
    observe_supervisor=True,
):
    workspace.mkdir(parents=True, exist_ok=True)
    with socket.socket() as candidate:
        candidate.bind(("127.0.0.1", 0))
        port = candidate.getsockname()[1]
    environment = {
        "PATH": os.environ.get("PATH", ""),
        "REFERENCE_PATH": str(ROOT / "reference"),
        "APPLICATION_PATH": str(module_path) if module_path else "",
        "WSGI_APPLICATION": application,
        "GUNICORN_CONFIG": str(ROOT / "reference/altimeter_reference/gunicorn_conf.py"),
        "DJANGO_SETTINGS_MODULE": "altimeter_reference.wsgi_settings",
        "WHEEL_SHA256": wheel.sha256,
        "SERVER_PORT": str(port),
        "SERVER_WORKERS": str(workers),
        "SERVER_THREADS": str(threads),
        "MAX_REQUESTS": str(max_requests),
        "PRELOAD": "1" if preload else "0",
        "OBSERVE_SUPERVISOR": "1" if observe_supervisor else "0",
        "SUPERVISOR_REPORT": str(workspace / "supervisor.json"),
        "REFERENCE_LIFECYCLE_FILE": str(workspace / "lifecycle.jsonl"),
        "REFERENCE_CLOSE_FILE": str(workspace / "close.jsonl"),
        "RECEIVER_ENDPOINT": receiver.endpoint,
        "ALTIMETER_ENABLED": "true" if enabled else "false",
        "ALTIMETER_SERVICE_NAME": "reference-web",
        "ALTIMETER_DEPLOYMENT_ENVIRONMENT": "test",
        "ALTIMETER_RELEASE": wheel.sha256[:16],
        "ALTIMETER_INTEGRATIONS": "wsgi,django",
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
        server = Server(port, process, workspace, name="gunicorn")
        try:
            deadline = time.monotonic() + 20
            while time.monotonic() < deadline:
                assert process.poll() is None, server.output
                try:
                    status, _, body = server.request("/health/")
                    if status == 200 and json.loads(body)["ok"]:
                        break
                except (OSError, ConnectionError):
                    time.sleep(0.025)
            else:
                raise AssertionError(f"gunicorn did not become ready\n{server.output}")
            yield server
        finally:
            server.stop()
            print(server.output, end="")
