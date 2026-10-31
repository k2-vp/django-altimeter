"""Real Celery bootstrap: register hooks in the parent, initialize in its children.

Optional JSONL observations go to a local test file, never to telemetry. Native
signals surround the integration's receivers without replacing any receiver.
"""

import json
import logging
import os
import sys
import threading
from dataclasses import asdict
from time import monotonic
from urllib.parse import urlsplit

import django
from celery import Celery, signals
from celery.loaders.app import AppLoader
from kombu import Queue

from django_altimeter import get_correlation
from django_altimeter.celery import install
from django_altimeter.runtime import get_process_runtime

os.environ.setdefault("DJANGO_SETTINGS_MODULE", "altimeter_reference.celery_settings")
django.setup()

queue = os.environ.get("REFERENCE_CELERY_QUEUE", "reference")
prefix = os.environ.get("REFERENCE_CELERY_PREFIX", "reference:")


class ReferenceLoader(AppLoader):
    """Exercise Celery's real outside-task error path, once per worker child."""

    _internal_failed = False

    def on_task_init(self, task_id, task):
        super().on_task_init(task_id, task)
        if task.name == "reference.internal" and not self._internal_failed:
            self._internal_failed = True
            observe("loader_error")
            logging.getLogger("reference.tasks").error("reference.task.failed")
            raise ValueError("loader-error-canary")


app = Celery(
    "altimeter_reference",
    broker=os.environ.get("ALTIMETER_TEST_CELERY_BROKER", "redis://redis:6379/13"),
    backend=os.environ.get("ALTIMETER_TEST_CELERY_BACKEND", "redis://redis:6379/14"),
    include=["altimeter_reference.celery_tasks"],
    loader="altimeter_reference.celery_app:ReferenceLoader",
)
app.conf.update(
    task_default_queue=queue,
    task_routes={"reference.override": {"queue": queue}},
    task_queues=(Queue(queue), Queue(queue + ".override")),
    broker_transport_options={"global_keyprefix": prefix, "visibility_timeout": 30},
    result_backend_transport_options={"global_keyprefix": prefix},
    task_serializer="json",
    result_serializer="json",
    accept_content=["json"],
    result_expires=120,
    worker_prefetch_multiplier=1,
    worker_send_task_events=True,
    task_send_sent_event=True,
    worker_hijack_root_logger=False,
    worker_lost_wait=0.5,
    broker_connection_retry_on_startup=False,
    broker_connection_timeout=2,
    task_publish_retry=False,
    beat_max_loop_interval=0.1,
)
if os.environ.get("REFERENCE_CELERY_BEAT") == "1":
    app.conf.beat_schedule = {
        "reference-tick": {
            "task": "reference.beat",
            "schedule": 0.5,
            "options": {"queue": queue},
        }
    }

_phase = None
_phase_start = 0.0
_phase_network = []
_phase_runtime = None
_parent = os.getpid()
_parent_calls = []
_parent_connections = []
_collector_port = urlsplit(os.environ.get("RECEIVER_ENDPOINT", "")).port


def observe(event, **values):
    destination = os.environ.get("REFERENCE_CELERY_EVENTS")
    if not destination:
        return
    runtime = get_process_runtime()
    correlation = get_correlation()
    record = {
        "event": event,
        "pid": os.getpid(),
        "time": monotonic(),
        "instance_id": runtime.instance_id if runtime else None,
        "correlation": asdict(correlation) if correlation else None,
        **values,
    }
    # One append write keeps concurrent child observations on separate lines.
    descriptor = os.open(destination, os.O_WRONLY | os.O_CREAT | os.O_APPEND, 0o600)
    try:
        os.write(descriptor, (json.dumps(record) + "\n").encode())
    finally:
        os.close(descriptor)


def _audit(event, args):
    if _phase == "setup" and event in (
        "socket.connect",
        "socket.getaddrinfo",
        "socket.sendto",
    ):
        _phase_network.append(event)
    if event == "socket.connect" and os.getpid() == _parent:
        address = args[1]
        if isinstance(address, tuple) and len(address) > 1:
            if address[1] == _collector_port:
                _parent_connections.append(event)


if os.environ.get("REFERENCE_CELERY_EVENTS"):
    sys.addaudithook(_audit)


def _profile(frame, event, result):
    if event != "call":
        return
    module = frame.f_globals.get("__name__", "")
    function = frame.f_code.co_name
    if (module == "django_altimeter.runtime" and function == "setup") or (
        module.startswith("opentelemetry.exporter.") and function == "__init__"
    ):
        _parent_calls.append(module + "." + function)


def _begin(kind):
    global _phase, _phase_start, _phase_network, _phase_runtime
    _phase = kind
    _phase_network = []
    _phase_runtime = get_process_runtime()
    _phase_start = monotonic()


def _end(kind):
    global _phase
    elapsed = monotonic() - _phase_start
    _phase = None
    result = getattr(_phase_runtime, "_shutdown_result", None)
    runtime = get_process_runtime()
    observe(
        kind,
        elapsed=elapsed,
        network=_phase_network,
        codes=list(result.codes) if kind == "drain" and result else [],
        completed=result.completed if kind == "drain" and result else None,
        enabled_signals=list(runtime.result.enabled_signals) if runtime else [],
        export_codes={
            session.signal: session.outcome.code for session in _phase_runtime._sessions
        }
        if kind == "drain" and _phase_runtime
        else {},
    )


def _child_begin(**kwargs):
    sys.setprofile(None)
    _begin("setup")


def _drain_begin(**kwargs):
    _begin("drain")


def _beat_begin(**kwargs):
    _begin("setup")


signals.worker_process_init.connect(_child_begin, weak=False)
signals.worker_process_shutdown.connect(_drain_begin, weak=False)
signals.worker_shutdown.connect(_drain_begin, weak=False)
signals.beat_init.connect(_beat_begin, weak=False)

# Complete the finite task registry before integration installation.
app.loader.import_default_modules()
install(app)


def _child_ready(**kwargs):
    _end("setup")


def _worker_init_end(**kwargs):
    if (
        os.environ.get("REFERENCE_CELERY_EVENTS")
        and os.environ.get("REFERENCE_CELERY_POOL") != "solo"
        and os.environ.get("REFERENCE_CELERY_MEASURE") != "1"
    ):
        sys.setprofile(_profile)


def _drain_end(**kwargs):
    _end("drain")


def _ready(**kwargs):
    observe("ready", parent_calls=_parent_calls, parent_connections=_parent_connections)


def _beat_ready(**kwargs):
    _end("setup")
    observe("beat_ready")


def _failure(exception=None, **kwargs):
    observe(
        "failure",
        exception_type=type(exception).__name__,
        parent_calls=_parent_calls if os.getpid() == _parent else [],
        parent_connections=_parent_connections if os.getpid() == _parent else [],
        threads=[thread.name for thread in threading.enumerate()],
    )
    logging.getLogger("reference.tasks").error("reference.task.failed")


def _postrun(task_id=None, state=None, **kwargs):
    observe("postrun", task_id=task_id, state=state)


def _internal(exception=None, **kwargs):
    observe("internal", exception_type=type(exception).__name__)


signals.worker_process_init.connect(_child_ready, weak=False)
signals.worker_init.connect(_worker_init_end, weak=False)
signals.worker_process_shutdown.connect(_drain_end, weak=False)
signals.worker_shutdown.connect(_drain_end, weak=False)
signals.worker_ready.connect(_ready, weak=False)
signals.beat_init.connect(_beat_ready, weak=False)
signals.task_failure.connect(_failure, weak=False)
signals.task_postrun.connect(_postrun, weak=False)
signals.task_internal_error.connect(_internal, weak=False)
