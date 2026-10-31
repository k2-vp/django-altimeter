"""Native Celery signal/trace-path checks; real-worker proof lives in integration."""

import json
import os
import subprocess
import sys

import pytest
from support.otlp_receiver import OTLPReceiver

_BOOTSTRAP = r"""
import json
import logging
import os
import threading
from dataclasses import asdict
from uuid import uuid4

import django
from django.conf import settings
settings.configure(INSTALLED_APPS=[], ALTIMETER={
    "SPAN_NAMES": ["application.work"],
    "ATTRIBUTE_VALUES": [("task.name", "native." + name) for name in
                         ("safe", "failure", "retry", "cancel", "internal")],
    "LOG_BODIES": ["native.failed"],
    "QUEUE_NAMES": ["native-queue"],
})
django.setup()
from celery import Celery, Task, signals
from celery.app.trace import build_tracer
from celery.exceptions import Retry
from celery.loaders.base import BaseLoader
from celery.worker.worker import WorkController
from django_altimeter import get_correlation, shutdown
from django_altimeter.runtime import get_process_runtime

from django_altimeter._celery_worker import (
    install_worker_signals, current_attempt_context,
)

class NativeLoader(BaseLoader):
    def on_task_init(self, task_id, task):
        super().on_task_init(task_id, task)
        if task.name == "native.internal":
            raise ValueError("internal-canary")

app = Celery("native-worker-test", broker="redis://127.0.0.1:6379/13",
             loader=NativeLoader, fixups=[])
app.conf.update(task_ignore_result=True, task_store_errors_even_if_ignored=False,
                worker_hijack_root_logger=False)
logging.basicConfig(level=logging.WARNING)
logging.getLogger("celery.task").propagate = False
logging.getLogger("celery.task").addHandler(logging.NullHandler())

order = []
def observe(label):
    def receiver(**kwargs):
        order.append((label, get_correlation() is not None))
        return label
    return receiver
for signal, name in ((signals.task_prerun, "prerun"),
                     (signals.task_failure, "failure"),
                     (signals.task_postrun, "postrun")):
    signal.connect(observe(name + ".before"), weak=False)
assert install_worker_signals(app) == ()
assert install_worker_signals(app) == ()
for signal, name in ((signals.task_prerun, "prerun"),
                     (signals.task_failure, "failure"),
                     (signals.task_postrun, "postrun")):
    signal.connect(observe(name + ".after"), weak=False)

snapshots = []
class NativeTask(Task):
    def on_failure(self, exc, task_id, args, kwargs, einfo):
        super().on_failure(exc, task_id, args, kwargs, einfo)
        snapshots.append(("error_handler", get_correlation() is not None))

def native_body(mode):
    active = current_attempt_context()
    snapshots.append((mode, asdict(active) if active else None))
    if mode == "failure":
        raise ValueError("exception-canary")
    if mode == "retry":
        raise Retry("retry-canary", when=1)
    if mode == "cancel":
        raise KeyboardInterrupt("cancel-canary")
    return "unchanged-result-canary"

tasks = {name: app.task(base=NativeTask, name="native." + name)(native_body)
         for name in ("safe", "failure", "retry", "cancel", "internal")}

def invoke(name):
    task = tasks[name]
    task_id = str(uuid4())
    request = {"id": task_id, "retries": 0,
               "delivery_info": {"routing_key": "native-queue"}}
    tracer = build_tracer(task.name, task, app=app, eager=False)
    try:
        result = tracer(task_id, (name,), {}, request=request)
        return result.retval if result.info is None else result.info.state
    except KeyboardInterrupt:
        return "KeyboardInterrupt"

before_threads = {thread.ident for thread in threading.enumerate()}
assert get_process_runtime() is None
"""


def _run(source, receiver, *, mode="enabled"):
    environment = {
        **os.environ,
        "ALTIMETER_ENABLED": "false" if mode == "disabled" else "true",
        "ALTIMETER_SERVICE_NAME": "" if mode == "invalid" else "native-worker",
        "ALTIMETER_DEPLOYMENT_ENVIRONMENT": "test",
        "ALTIMETER_RELEASE": "test",
        "ALTIMETER_INTEGRATIONS": "celery",
        "ALTIMETER_TRACES_COLLECTOR_ENDPOINT": receiver.url("traces"),
        "ALTIMETER_LOGS_COLLECTOR_ENDPOINT": receiver.url("logs"),
        "ALTIMETER_METRICS_COLLECTOR_ENDPOINT": receiver.url("metrics"),
        "ALTIMETER_LOGS_ENABLED": "true",
        "ALTIMETER_BATCH_SCHEDULE_DELAY_MILLIS": "60000",
        "ALTIMETER_METRIC_EXPORT_INTERVAL_MILLIS": "60000",
        "OTEL_SEMCONV_STABILITY_OPT_IN": "http",
    }
    result = subprocess.run(
        [sys.executable, "-c", _BOOTSTRAP + source],
        env=environment,
        text=True,
        capture_output=True,
        timeout=15,
    )
    print(result.stderr, end="")
    assert result.returncode == 0, result.stdout + result.stderr
    return json.loads(result.stdout.splitlines()[-1])


def _spans(receiver):
    return [
        span
        for request in receiver.requests
        if request.path == "/v1/traces"
        for resource in request.message.resource_spans
        for scope in resource.scope_spans
        for span in scope.spans
    ]


@pytest.mark.parametrize("mode", ["enabled", "disabled", "invalid"])
def test_native_solo_attempts_preserve_results_receivers_and_cleanup(mode):
    with OTLPReceiver() as receiver:
        output = _run(
            r"""
worker = WorkController(app=app, pool="solo", concurrency=1,
                        without_gossip=True, without_mingle=True,
                        without_heartbeat=True)
results = [invoke(name) for name in
           ("failure", "safe", "retry", "cancel", "internal", "safe")]
assert get_correlation() is None
assert current_attempt_context() is None
shutdown()
print(json.dumps({"results": results, "order": order, "snapshots": snapshots}))
""",
            receiver,
            mode=mode,
        )
        assert output["results"] == [
            "FAILURE",
            "unchanged-result-canary",
            "RETRY",
            "KeyboardInterrupt",
            "FAILURE",
            "unchanged-result-canary",
        ]
        first = output["order"][:6]
        assert [row[0] for row in first] == [
            "prerun.before",
            "prerun.after",
            "failure.before",
            "failure.after",
            "postrun.before",
            "postrun.after",
        ]
        enabled = mode == "enabled"
        assert [row[1] for row in first] == [
            False,
            enabled,
            enabled,
            enabled,
            enabled,
            False,
        ]
        assert all(
            not value for label, value in output["order"] if label == "postrun.after"
        )
        assert (output["snapshots"][0][1] is not None) == enabled
        assert [
            value for label, value in output["snapshots"] if label == "error_handler"
        ] == [enabled, False]
        observed = _spans(receiver)
        assert len(observed) == (6 if enabled else 0)
        if enabled:
            assert all(
                span.name == "celery.process" and span.kind == 5 for span in observed
            )
            assert len({span.trace_id for span in observed}) == 6
            outcomes = [
                next(
                    attribute.value.string_value
                    for attribute in span.attributes
                    if attribute.key == "altimeter.outcome"
                )
                for span in observed
            ]
            assert outcomes == [
                "failure",
                "success",
                "retry",
                "unknown",
                "unknown",
                "success",
            ]
        assert "canary" not in str(receiver.requests)


def test_prefork_parent_failure_is_providerless_and_preserves_receivers():
    with OTLPReceiver() as receiver:
        output = _run(
            r"""
worker = WorkController(app=app, pool="prefork", concurrency=1,
                        without_gossip=True, without_mingle=True,
                        without_heartbeat=True)
responses = signals.task_failure.send(sender=tasks["failure"], task_id="parent",
                                      exception=ValueError("canary"))
assert all(not isinstance(value, BaseException) for _, value in responses)
assert get_process_runtime() is None
assert current_attempt_context() is None
assert {thread.ident for thread in threading.enumerate()} == before_threads
values = [value for _, value in responses if value is not None]
print(json.dumps({"order": order, "results": values}))
""",
            receiver,
        )
        assert output["order"] == [["failure.before", False], ["failure.after", False]]
        assert output["results"] == ["failure.before", "failure.after"]
        assert receiver.requests == ()


def test_unowned_tracing_preserves_foreign_parent_and_baggage():
    with OTLPReceiver() as receiver:
        output = _run(
            r"""
from opentelemetry import baggage, context, trace
from opentelemetry.sdk.trace import TracerProvider
foreign = TracerProvider(shutdown_on_exit=False)
trace.set_tracer_provider(foreign)
worker = WorkController(app=app, pool="solo", concurrency=1,
                        without_gossip=True, without_mingle=True,
                        without_heartbeat=True)
parent = foreign.get_tracer("foreign").start_span("foreign-parent-canary")
token = context.attach(trace.set_span_in_context(parent,
    baggage.set_baggage("identity-canary", "baggage-canary")))
@app.task(name="unregistered-task-canary")
def check():
    assert trace.get_current_span() is parent
    assert baggage.get_baggage("identity-canary") == "baggage-canary"
    assert get_correlation() is None
    assert current_attempt_context() is None
    return "native-result"
try:
    result = build_tracer(check.name, check, app=app, eager=False)(
        str(uuid4()), (), {}, {"delivery_info": {"routing_key": "queue-canary"}})
    assert trace.get_current_span() is parent
    assert baggage.get_baggage("identity-canary") == "baggage-canary"
finally:
    context.detach(token)
    parent.end()
    shutdown()
    foreign.shutdown()
print(json.dumps(result.retval))
""",
            receiver,
        )
        assert output == "native-result"
        assert _spans(receiver) == []
        points = [
            point
            for request in receiver.requests
            if request.path == "/v1/metrics"
            for resource in request.message.resource_metrics
            for scope in resource.scope_metrics
            for metric in scope.metrics
            if metric.name == "altimeter.worker.attempts"
            for point in metric.sum.data_points
        ]
        assert sum(point.as_int for point in points) == 1
        assert "canary" not in str(receiver.requests)


def test_unsupported_thread_pool_does_not_start_telemetry():
    with OTLPReceiver() as receiver:
        output = _run(
            r"""
worker = WorkController(app=app, pool="threads", concurrency=1,
                        without_gossip=True, without_mingle=True,
                        without_heartbeat=True)
assert get_process_runtime() is None
assert invoke("safe") == "unchanged-result-canary"
assert current_attempt_context() is None
assert {thread.ident for thread in threading.enumerate()} == before_threads
print(json.dumps(order))
""",
            receiver,
        )
        assert output == [
            ["prerun.before", False],
            ["prerun.after", False],
            ["postrun.before", False],
            ["postrun.after", False],
        ]
        assert receiver.requests == ()


def test_bounded_attempt_view_cannot_escape_cleanup_or_break_large_ids():
    with OTLPReceiver() as receiver:
        output = _run(
            r"""
from contextvars import copy_context
worker = WorkController(app=app, pool="solo", concurrency=1,
                        without_gossip=True, without_mingle=True,
                        without_heartbeat=True)
copies = []
views = []
@app.task(name="native.context")
def capture():
    copies.append(copy_context())
    views.append(current_attempt_context() is not None)
    return "native-result"
for task_id in (str(uuid4()), "oversized-canary" * 2048):
    result = build_tracer(capture.name, capture, app=app, eager=False)(
        task_id, (), {}, {"id": task_id})
    assert result.retval == "native-result"
    assert get_correlation() is None
    assert current_attempt_context() is None
    assert all(saved.run(current_attempt_context) is None for saved in copies)
shutdown()
print(json.dumps(views))
""",
            receiver,
        )
        assert output == [True, False]
        assert len(_spans(receiver)) == 2
        assert "canary" not in str(receiver.requests)


def test_nested_native_execution_does_not_finish_outer_attempt():
    with OTLPReceiver() as receiver:
        output = _run(
            r"""
worker = WorkController(app=app, pool="solo", concurrency=1,
                        without_gossip=True, without_mingle=True,
                        without_heartbeat=True)
@app.task(name="native.nested")
def nested():
    before = get_correlation()
    active = current_attempt_context()
    assert before is not None and active is not None
    assert invoke("safe") == "unchanged-result-canary"
    assert get_correlation() == before
    assert current_attempt_context() == active
    return "native-result"
task_id = str(uuid4())
result = build_tracer(nested.name, nested, app=app, eager=False)(
    task_id, (), {}, {"id": task_id})
assert get_correlation() is None
assert current_attempt_context() is None
shutdown()
print(json.dumps(result.retval))
""",
            receiver,
        )
        assert output == "native-result"
        assert len(_spans(receiver)) == 1
        assert "canary" not in str(receiver.requests)


def test_native_span_callback_failure_keeps_task_result_and_metric_lifetime():
    with OTLPReceiver() as receiver:
        output = _run(
            r"""
from time import sleep
from opentelemetry.sdk.trace import SpanProcessor
worker = WorkController(app=app, pool="solo", concurrency=1,
                        without_gossip=True, without_mingle=True,
                        without_heartbeat=True)
class ApplicationProcessor(SpanProcessor):
    def on_start(self, span, parent_context=None):
        super().on_start(span, parent_context=parent_context)
        raise ValueError("processor-canary")
get_process_runtime().tracer_provider.add_span_processor(ApplicationProcessor())
@app.task(name="native.delayed")
def delayed_attempt():
    sleep(0.03)
    return "native-result"
task_id = str(uuid4())
result = build_tracer(delayed_attempt.name, delayed_attempt, app=app, eager=False)(
    task_id, (), {}, {"id": task_id})
assert get_correlation() is None
assert current_attempt_context() is None
shutdown()
print(json.dumps(result.retval))
""",
            receiver,
        )
        assert output == "native-result"
        duration_points = [
            point
            for request in receiver.requests
            if request.path == "/v1/metrics"
            for resource in request.message.resource_metrics
            for scope in resource.scope_metrics
            for metric in scope.metrics
            if metric.name == "altimeter.worker.duration"
            for point in metric.histogram.data_points
        ]
        assert sum(point.count for point in duration_points) == 1
        assert sum(point.sum for point in duration_points) >= 0.03
        assert all(
            any(
                item.key == "altimeter.outcome" and item.value.string_value == "success"
                for item in point.attributes
            )
            for point in duration_points
        )
        assert "canary" not in str(receiver.requests)
