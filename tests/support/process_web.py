"""Run the existing web profiles with optional process observations."""

import json
import os
import shutil
import subprocess
from contextlib import contextmanager

from support import gunicorn_server, uvicorn_server

PROCESS_NAMES = frozenset(
    {
        "process.cpu.time",
        "process.memory.usage",
        "process.thread.count",
        "process.unix.file_descriptor.count",
        "altimeter.process.gc.collections",
        "altimeter.process.gc.collected",
        "altimeter.process.gc.uncollectable",
    }
)
PSUTIL_NAMES = frozenset(name for name in PROCESS_NAMES if name.startswith("process."))
SAFE_LOG = "process.web.completed"
CANARY = "process-web-private-canary"


def web_profile():
    return "wsgi" if os.environ.get("ALTIMETER_REFERENCE_WSGI") == "1" else "asgi"


def install_process_wheel(workspace):
    installer = (
        gunicorn_server.install_wheel
        if web_profile() == "wsgi"
        else uvicorn_server.install_wheel
    )
    return installer(workspace, extras=("process",))


def metric_points(receiver):
    return [
        (resource.resource, metric, point)
        for request in receiver.requests
        if request.path == "/v1/metrics"
        for resource in request.message.resource_metrics
        for scope in resource.scope_metrics
        for metric in scope.metrics
        for point in getattr(metric, metric.WhichOneof("data")).data_points
    ]


def server_spans(receiver):
    return [
        span
        for request in receiver.requests
        if request.path == "/v1/traces"
        for resource in request.message.resource_spans
        for scope in resource.scope_spans
        for span in scope.spans
        if span.kind == 2
    ]


def http_counts(receiver):
    totals = {"altimeter.http.server.requests": 0, "http.server.request.duration": 0}
    for _, metric, point in metric_points(receiver):
        if metric.name == "altimeter.http.server.requests":
            totals[metric.name] += point.as_int
        elif metric.name == "http.server.request.duration":
            totals[metric.name] += point.count
    return totals


def break_psutil_installation(wheel, mode):
    """Remove the dependency or damage its real extension in this private venv."""
    if mode == "missing":
        subprocess.run(
            [str(wheel.python), "-m", "pip", "uninstall", "-y", "psutil"],
            check=True,
            timeout=60,
        )
    elif mode == "broken":
        script = """
import importlib.metadata
from pathlib import Path
import sys

distribution = importlib.metadata.distribution("psutil")
extensions = [distribution.locate_file(path) for path in distribution.files
              if str(path).endswith(".so") and "_psutil" in str(path)]
assert extensions
for path in extensions:
    assert Path(path).resolve().is_relative_to(Path(sys.prefix).resolve())
    Path(path).write_bytes(b"process-web-private-canary")
"""
        subprocess.run([str(wheel.python), "-I", "-c", script], check=True, timeout=30)
    else:
        raise ValueError(mode)
    failed_import = subprocess.run(
        [str(wheel.python), "-I", "-c", "import psutil"],
        check=False,
        timeout=30,
    )
    assert failed_import.returncode != 0, "The dependency fault must be real"


def _logging_reference(workspace):
    reference = workspace / "reference"
    shutil.copytree(
        uvicorn_server.ROOT / "reference/altimeter_reference",
        reference / "altimeter_reference",
    )
    settings = "wsgi_settings" if web_profile() == "wsgi" else "web_settings"
    routes = "wsgi_api" if web_profile() == "wsgi" else "api"
    (reference / "altimeter_reference/process_settings.py").write_text(
        f"from altimeter_reference import {settings} as base\n"
        "globals().update({key: value for key, value in vars(base).items()\n"
        "                  if key.isupper()})\n"
        f"from altimeter_reference.{settings} import ALTIMETER as original\n"
        "ALTIMETER = {**original, 'LOG_BODIES': ['process.web.completed'],\n"
        " 'ATTRIBUTE_VALUES': [*original['ATTRIBUTE_VALUES'],\n"
        "                      ('http.route', 'process-log')]}\n"
        "ROOT_URLCONF = 'altimeter_reference.process_urls'\n"
        "LOGGING = {'version': 1, 'disable_existing_loggers': False,\n"
        " 'handlers': {'console': {'class': 'logging.StreamHandler'}},\n"
        " 'loggers': {'reference.process': {'handlers': ['console'],\n"
        "                                  'level': 'WARNING', 'propagate': False}}}\n"
    )
    (reference / "altimeter_reference/process_urls.py").write_text(
        f"from altimeter_reference.{routes} import urlpatterns as original\n"
        "import logging\n"
        "from django.http import JsonResponse\n"
        "from django.urls import path\n"
        "from django_altimeter import get_correlation\n"
        "from django_altimeter.logging import install_logging\n"
        "def log(request):\n"
        "    install_logging(routes=('reference.process',))\n"
        "    logger = logging.getLogger('reference.process')\n"
        "    logger.warning('process.web.completed')\n"
        "    logger.warning('process-web-private-canary')\n"
        "    correlation = get_correlation()\n"
        "    return JsonResponse({'ok': True,\n"
        "        'trace_id': correlation.trace_id if correlation else None,\n"
        "        'span_id': correlation.span_id if correlation else None})\n"
        "urlpatterns = [*original, path('process-log', log)]\n"
    )
    return {
        "REFERENCE_PATH": str(reference),
        "DJANGO_SETTINGS_MODULE": "altimeter_reference.process_settings",
    }


@contextmanager
def running_process_web(workspace, *, wheel, receiver, process_enabled, logs=False):
    profile = web_profile()
    environment = {
        "ALTIMETER_INTEGRATIONS": f"{profile},django"
        + (",process" if process_enabled else ""),
        "ALTIMETER_LOGS_ENABLED": "true" if logs else "false",
        "ALTIMETER_LOGS_COLLECTOR_ENDPOINT": receiver.url("logs"),
        "ALTIMETER_BATCH_SCHEDULE_DELAY_MILLIS": "250",
        "ALTIMETER_METRIC_EXPORT_INTERVAL_MILLIS": "250",
        "ALTIMETER_EXPORT_TIMEOUT": "0.2",
    }
    if logs:
        environment.update(_logging_reference(workspace))
    launcher = (
        gunicorn_server.running_server
        if profile == "wsgi"
        else uvicorn_server.running_server
    )
    options = {"observe_supervisor": False} if profile == "wsgi" else {}
    with launcher(
        workspace, wheel=wheel, receiver=receiver, extra_env=environment, **options
    ) as server:
        status, _, body = server.request("/health/")
        assert status == 200
        health = json.loads(body)
        assert health["ok"] is True
        if profile == "wsgi":
            startup = [
                event
                for event in server.lifecycle
                if event["event"] == "startup" and event["pid"] == health["pid"]
            ]
            assert len(startup) == 1
            assert health["pid"] != server.process.pid
            health["instance_id"] = startup[0]["instance_id"]
        assert health["instance_id"] is not None, (
            "Existing web telemetry must remain owned"
        )
        yield server, health
