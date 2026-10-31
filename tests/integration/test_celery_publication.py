"""Real Redis/Celery publication from an installed wheel; no live-service claim."""

import pytest
from support.celery_worker import attributes, running_worker, spans
from support.uvicorn_server import install_wheel

pytestmark = pytest.mark.integration
CANARY = "CELERY_ARGUMENT_CANARY"


@pytest.fixture(scope="module")
def wheel(tmp_path_factory):
    return install_wheel(
        tmp_path_factory.mktemp("celery-publication-wheel"), extras=("celery", "redis")
    )


def test_real_publication_has_one_safe_producer(wheel, tmp_path, otlp_receiver):
    with running_worker(tmp_path, wheel=wheel, receiver=otlp_receiver) as worker:
        result = worker.submit(args=[CANARY])
        assert result["state"] == "SUCCESS"
        assert result["result"]["payload"] == CANARY
        assert result["result"]["pid"] != worker.process.pid
    producers = [span for _, span in spans(otlp_receiver) if span.kind == 4]
    assert len(producers) == 1
    producer = producers[0]
    assert producer.name == "celery.publish"
    assert attributes(producer.attributes) == {
        "messaging.system": "celery",
        "task.name": "reference.echo",
        "queue.name": worker.environment["REFERENCE_CELERY_QUEUE"],
    }
    assert producer.start_time_unix_nano < producer.end_time_unix_nano
    assert CANARY not in str(otlp_receiver.requests)


_PUBLICATION_CASES = r"""
import json
from copy import deepcopy
from datetime import datetime, timedelta, timezone
from celery import Celery, chain, group, signals
from django_altimeter import get_correlation, setup, shutdown
from django_altimeter.celery import install

setup(role="web")
order, captures, nested = [], [], []
def before_first(headers=None, **kwargs):
    order.append("before.first")
    if headers.get("nested"):
        child = app.send_task("reference.echo", args=["nested-result-canary"])
        nested.append(child.get(timeout=10, interval=.02))
def before_second(headers=None, exchange=None, routing_key=None, **kwargs):
    order.append("before.second")
    captures.append({"headers": deepcopy(headers), "exchange": exchange,
                     "routing_key": routing_key})
def after_first(**kwargs):
    order.append("after.first")
def after_second(**kwargs):
    order.append("after.second")
for signal, receiver in ((signals.before_task_publish, before_first),
                         (signals.before_task_publish, before_second),
                         (signals.after_task_publish, after_first),
                         (signals.after_task_publish, after_second)):
    signal.connect(receiver, weak=False)

class ExistingApp(Celery):
    def send_task(self, *args, **kwargs):
        self.calls.append((args, kwargs.copy()))
        result = super().send_task(*args, **kwargs)
        self.returned = result
        return result
publisher = ExistingApp("existing-override", fixups=[], set_as_current=False)
publisher.conf.update(app.conf)
publisher.calls = []
assert install(publisher) == ()
assert install(publisher) == ()
headers = {"business": "business-header-canary", "nested": True}
original_headers = deepcopy(headers)
eta = datetime.now(timezone.utc) + timedelta(seconds=.3)
result = publisher.send_task("reference.echo", ["normal-result-canary"],
                            {"delay": .01}, headers=headers, eta=eta, priority=3)
assert result is publisher.returned
assert len(publisher.calls) == 1
assert publisher.calls[0][0] == (
    "reference.echo", ["normal-result-canary"], {"delay": .01})
assert publisher.calls[0][1]["headers"] is headers
assert publisher.calls[0][1]["eta"] is eta
assert publisher.calls[0][1]["priority"] == 3
assert headers == original_headers
normal = result.get(timeout=10, interval=.02)
override = app.tasks["reference.override"].apply_async(args=["override-result-canary"])
override_result = override.get(timeout=10, interval=.02)
unknown = app.send_task("UNREGISTERED_TASK_CANARY")
unknown.get(timeout=10, interval=.02, propagate=False)
assert unknown.state == "FAILURE"
group_result = group(app.signature("reference.echo", args=["group-result-canary"])
                     for _ in range(2)).apply_async().get(timeout=10, interval=.02)
chain_result = chain(
    app.signature("reference.echo", args=["chain-first-canary"], immutable=True),
    app.signature("reference.echo", args=["chain-last-canary"], immutable=True),
).apply_async().get(timeout=10, interval=.02)
assert get_correlation() is None
shutdown(timeout=2)
print(json.dumps({"normal": normal, "nested": nested, "override": override_result,
                  "group": group_result, "chain": chain_result,
                  "captures": captures, "order": order, "eta": eta.timestamp()}))
"""


@pytest.mark.parametrize("mode", ["enabled", "disabled", "invalid"])
def test_native_override_canvas_nested_and_resolved_routing(
    wheel, tmp_path, otlp_receiver, mode
):
    with running_worker(
        tmp_path, wheel=wheel, receiver=otlp_receiver, mode=mode
    ) as worker:
        result = worker.execute(_PUBLICATION_CASES)
    assert result["normal"]["payload"] == "normal-result-canary"
    assert result["nested"][0]["payload"] == "nested-result-canary"
    assert result["override"]["payload"] == "override-result-canary"
    assert [row["payload"] for row in result["group"]] == ["group-result-canary"] * 2
    assert result["chain"]["payload"] == "chain-last-canary"
    assert (
        result["order"]
        == [
            "before.first",
            "before.first",
            "before.second",
            "after.first",
            "after.second",
            "before.second",
            "after.first",
            "after.second",
        ]
        + ["before.first", "before.second", "after.first", "after.second"] * 5
    )
    captures = result["captures"]
    assert len(captures) == 7  # Chain continuation is published in the worker.
    resolved = next(
        row for row in captures if row["headers"]["task"] == "reference.override"
    )
    assert (
        resolved["routing_key"]
        == worker.environment["REFERENCE_CELERY_QUEUE"] + ".override"
    )
    assert (
        next(
            row["headers"]["business"]
            for row in captures
            if row["headers"].get("nested")
        )
        == "business-header-canary"
    )
    producers = [span for _, span in spans(otlp_receiver) if span.kind == 4]
    if mode != "enabled":
        assert otlp_receiver.requests == ()
        assert all("django_altimeter" not in row["headers"] for row in captures)
        return
    assert len(producers) == 8
    assert len({span.span_id for span in producers}) == 8
    override_span = next(
        span
        for span in producers
        if attributes(span.attributes)["task.name"] == "reference.override"
    )
    assert attributes(override_span.attributes)["queue.name"] == resolved["routing_key"]
    assert (
        len(
            [
                span
                for span in producers
                if attributes(span.attributes)["task.name"] == "_OTHER"
            ]
        )
        == 1
    )
    attempt_ids = [
        row["headers"]["django_altimeter"]["attempt"]["traceparent"] for row in captures
    ]
    assert len(set(attempt_ids)) == len(attempt_ids)
    epochs = [
        row["headers"]["django_altimeter"]["attempt"]["published_at"]
        for row in captures
    ]
    assert len(set(epochs)) == len(epochs)
    parent = next(
        row["headers"]["django_altimeter"]
        for row in captures
        if row["headers"].get("nested")
    )
    assert parent["attempt"]["eligible_at"] == result["eta"]
    assert all(
        "canary" not in request.message.SerializeToString().decode("latin1").lower()
        for request in otlp_receiver.requests
    )


_BROKER_FAILURE = r"""
import json
import socket
from celery import Celery
from kombu.exceptions import OperationalError
from django_altimeter import get_correlation, setup, shutdown, span
from django_altimeter.celery import install
setup(role="web")
class ExistingApp(Celery):
    def send_task(self, *args, **kwargs):
        self.calls += 1
        return super().send_task(*args, **kwargs)
failed = ExistingApp("unreachable", fixups=[], set_as_current=False)
failed.conf.update(app.conf)
failed.calls = 0
with socket.socket() as unavailable:
    unavailable.bind(("127.0.0.1", 0))  # Real reserved port with no listener.
    failed.conf.broker_url = "redis://127.0.0.1:%d/13" % unavailable.getsockname()[1]
    failed.conf.broker_connection_timeout = .2
    assert install(failed) == ()
    with span("reference.task.work"):
        original = get_correlation()
        try:
            failed.send_task(
                "reference.echo", args=["failed-argument-canary"], retry=False)
        except OperationalError as exc:
            error = type(exc).__name__
            assert exc.__traceback__ is not None
        else:
            raise AssertionError("closed broker unexpectedly accepted publication")
        assert get_correlation() == original
assert get_correlation() is None
assert failed.calls == 1
result = app.send_task("reference.echo", args=["recovery-result-canary"])
recovered = result.get(timeout=10, interval=.02)
assert get_correlation() is None
shutdown(timeout=2)
print(json.dumps({"error": error, "recovered": recovered, "calls": failed.calls}))
"""


@pytest.mark.parametrize("mode", ["enabled", "disabled", "invalid"])
def test_broker_failure_preserves_exception_calls_and_context(
    wheel, tmp_path, otlp_receiver, mode
):
    with running_worker(
        tmp_path, wheel=wheel, receiver=otlp_receiver, mode=mode
    ) as worker:
        result = worker.execute(_BROKER_FAILURE)
    assert result["error"] == "OperationalError"
    assert result["calls"] == 1
    assert result["recovered"]["payload"] == "recovery-result-canary"
    producers = [span for _, span in spans(otlp_receiver) if span.kind == 4]
    if mode == "enabled":
        assert len(producers) == 2
        failed = next(span for span in producers if span.status.code == 2)
        succeeded = next(span for span in producers if span.status.code == 0)
        assert failed.parent_span_id
        assert not succeeded.parent_span_id
        assert succeeded.trace_id != failed.trace_id
    else:
        assert otlp_receiver.requests == ()
    assert "canary" not in str(otlp_receiver.requests).lower()


def test_retry_refreshes_attempt_and_eta_but_keeps_origin(
    wheel, tmp_path, otlp_receiver
):
    origin = "00-" + "1" * 32 + "-" + "2" * 16 + "-01"
    old = {
        "v": 1,
        "origin": {"traceparent": origin},
        "attempt": {
            "traceparent": origin,
            "published_at": 1,
            "eligible_at": 2,
            "previous_traceparent": origin,
        },
    }
    with running_worker(tmp_path, wheel=wheel, receiver=otlp_receiver) as worker:
        result = worker.submit(
            "reference.retry",
            args=[CANARY],
            options={
                "headers": {
                    "django_altimeter": old,
                    "business": "header-canary",
                    "baggage": "identity-canary",
                }
            },
        )
        assert result["state"] == "SUCCESS"
        assert result["result"]["payload"] == CANARY
        attempts = worker.wait_for("attempt", count=2)
    first, second = [row["headers"]["django_altimeter"] for row in attempts]
    assert first["origin"] == second["origin"] == {"traceparent": origin}
    assert first["attempt"]["published_at"] > 2
    assert second["attempt"]["published_at"] > first["attempt"]["published_at"]
    assert "eligible_at" not in first["attempt"]
    assert "previous_traceparent" not in first["attempt"]
    assert second["attempt"]["eligible_at"] > second["attempt"]["published_at"]
    assert first["attempt"]["traceparent"] != second["attempt"]["traceparent"]
    producers = [span for _, span in spans(otlp_receiver) if span.kind == 4]
    consumers = [span for _, span in spans(otlp_receiver) if span.kind == 5]
    assert len(producers) == len(consumers) == 2
    first_consumer = next(
        span
        for span in consumers
        if span.parent_span_id.hex() == first["attempt"]["traceparent"].split("-")[2]
    )
    last_consumer = next(
        span
        for span in consumers
        if span.parent_span_id.hex() == second["attempt"]["traceparent"].split("-")[2]
    )
    assert (
        second["attempt"]["previous_traceparent"].split("-")[2]
        == first_consumer.span_id.hex()
    )
    assert len(last_consumer.links) == 1
    assert last_consumer.links[0].span_id == first_consumer.span_id
    assert last_consumer.links[0].trace_id == first_consumer.trace_id
    assert last_consumer.trace_id == first_consumer.trace_id
    assert all(row["headers"]["business"] == "header-canary" for row in attempts)
    assert CANARY not in str(otlp_receiver.requests)
    assert "canary" not in str(otlp_receiver.requests).lower()


@pytest.mark.parametrize(
    "metadata",
    [
        None,
        {"v": 0, "traceparent": "legacy-canary"},
        {"v": 1, "origin": "oversized-canary" * 1000, "attempt": {}},
    ],
)
def test_malformed_carrier_is_replaced_without_affecting_delivery(
    wheel, tmp_path, otlp_receiver, metadata
):
    with running_worker(tmp_path, wheel=wheel, receiver=otlp_receiver) as worker:
        result = worker.execute(
            r"""
import json
from celery import signals
from django_altimeter import setup, shutdown
from django_altimeter.celery import read_publication
setup(role="web")
metadata = json.load(sys.stdin)
observed = []
def capture(headers=None, **kwargs):
    observed.append(headers.copy())
signals.before_task_publish.connect(capture, weak=False)
headers = {"django_altimeter": metadata, "business": "business-canary"}
result = app.send_task("reference.echo", args=["result-canary"], headers=headers)
assert headers == {"django_altimeter": metadata, "business": "business-canary"}
value = result.get(timeout=10, interval=.02)
assert read_publication(observed[0]) is not None
assert len(json.dumps(observed[0]["django_altimeter"]).encode()) <= 1024
shutdown(timeout=2)
print(json.dumps({"value": value, "headers": observed[0]}))
""",
            input=metadata,
        )
    assert result["value"]["payload"] == "result-canary"
    assert result["headers"]["business"] == "business-canary"
    assert set(result["headers"]["django_altimeter"]) == {"v", "origin", "attempt"}
    assert len([span for _, span in spans(otlp_receiver) if span.kind == 4]) == 1
    assert "canary" not in str(otlp_receiver.requests).lower()


def test_foreign_provider_does_not_inject_owned_metadata(
    wheel, tmp_path, otlp_receiver
):
    with running_worker(tmp_path, wheel=wheel, receiver=otlp_receiver) as worker:
        result = worker.execute(r"""
import json
from celery import signals
from opentelemetry import trace
from opentelemetry.sdk.trace import TracerProvider
from django_altimeter import get_correlation, setup, shutdown
foreign = TracerProvider()
trace.set_tracer_provider(foreign)
setup(role="web")
observed = []
def capture(headers=None, **kwargs):
    observed.append(headers.copy())
signals.before_task_publish.connect(capture, weak=False)
with foreign.get_tracer("application").start_as_current_span("foreign"):
    assert get_correlation() is None
    result = app.send_task("reference.echo", args=["unowned-result-canary"])
    value = result.get(timeout=10, interval=.02)
assert "django_altimeter" not in observed[0]
shutdown(timeout=2)
foreign.shutdown()
print(json.dumps({"value": value, "headers": observed[0]}))
""")
    assert result["value"]["payload"] == "unowned-result-canary"
    assert "django_altimeter" not in result["headers"]
    assert not [span for _, span in spans(otlp_receiver) if span.kind == 4]
    consumers = [span for _, span in spans(otlp_receiver) if span.kind == 5]
    assert len(consumers) == 1
    assert not consumers[0].parent_span_id
    assert "canary" not in str(otlp_receiver.requests).lower()


def test_existing_override_child_span_does_not_replace_publication_parent(
    wheel, tmp_path, otlp_receiver
):
    with running_worker(tmp_path, wheel=wheel, receiver=otlp_receiver) as worker:
        result = worker.execute(r"""
import json
from celery import Celery
from django_altimeter import setup, shutdown, span
from django_altimeter.celery import install
setup(role="web")
class ExistingApp(Celery):
    def send_task(self, *args, **kwargs):
        with span("reference.task.work"):
            return super().send_task(*args, **kwargs)
publisher = ExistingApp("timed-override", fixups=[], set_as_current=False)
publisher.conf.update(app.conf)
assert install(publisher) == ()
result = publisher.send_task("reference.echo", args=["override-child-canary"])
value = result.get(timeout=10, interval=.02)
shutdown(timeout=2)
print(json.dumps(value))
""")
    assert result["payload"] == "override-child-canary"
    producers = [span for _, span in spans(otlp_receiver) if span.kind == 4]
    consumers = [span for _, span in spans(otlp_receiver) if span.kind == 5]
    assert len(producers) == len(consumers) == 1
    assert consumers[0].parent_span_id == producers[0].span_id
    assert consumers[0].trace_id == producers[0].trace_id
    assert "canary" not in str(otlp_receiver.requests).lower()


def test_base_wheel_imports_without_celery_or_rq(tmp_path):
    import subprocess

    environment = install_wheel(tmp_path / "base-wheel", server_dependencies=())
    result = subprocess.run(
        [
            str(environment.python),
            "-I",
            "-c",
            r"""
import importlib.util
import sys
import threading
from pathlib import Path
before = {thread.ident for thread in threading.enumerate()}
assert importlib.util.find_spec("celery") is None
assert importlib.util.find_spec("rq") is None
import django_altimeter.celery as integration
assert Path(integration.__file__).resolve().is_relative_to(Path(sys.prefix).resolve())
assert integration.install(object()) == ("celery.unavailable",)
assert "celery" not in sys.modules and "rq" not in sys.modules
assert {thread.ident for thread in threading.enumerate()} == before
""",
        ],
        text=True,
        capture_output=True,
        timeout=15,
    )
    print(result.stderr, end="")
    assert result.returncode == 0, result.stdout


def test_concurrent_publishers_and_copied_context_do_not_reuse_dispatch(
    wheel, tmp_path, otlp_receiver
):
    with running_worker(tmp_path, wheel=wheel, receiver=otlp_receiver) as worker:
        output = worker.execute(r"""
import json
from concurrent.futures import ThreadPoolExecutor
from contextvars import copy_context
from dataclasses import asdict
from threading import Barrier, Lock
from celery import signals
from django_altimeter import get_correlation, setup, shutdown, span
setup(role="web")
captured, contexts = [], []
lock = Lock()
def capture(headers=None, **kwargs):
    with lock:
        captured.append(headers.copy())
        contexts.append(copy_context())
signals.before_task_publish.connect(capture, weak=False)
barrier = Barrier(2)
def publish(index):
    assert get_correlation() is None
    with span("reference.task.work"):
        parent = get_correlation()
        barrier.wait(timeout=5)
        result = app.send_task("reference.echo", args=["thread-canary"])
        assert get_correlation() == parent
    assert get_correlation() is None
    return {"id": result.id, "parent": asdict(parent)}
with ThreadPoolExecutor(max_workers=2) as executor:
    sent = list(executor.map(publish, range(2)))
for row in sent:
    value = app.AsyncResult(row["id"]).get(timeout=10, interval=.02)
    assert value["payload"] == "thread-canary"
late = contexts[0]
headers = {"id": "outside-native-dispatch", "task": "reference.echo"}
late.run(signals.before_task_publish.send, sender="reference.echo", headers=headers)
assert "django_altimeter" not in headers
result = late.run(app.send_task, "reference.echo", args=["copied-context-canary"])
assert result.get(timeout=10, interval=.02)["payload"] == "copied-context-canary"
assert get_correlation() is None
shutdown(timeout=2)
print(json.dumps({"sent": sent, "captured": captured}))
""")
    producers = [span for _, span in spans(otlp_receiver) if span.kind == 4]
    assert len(producers) == 3
    assert len({span.span_id for span in producers}) == 3
    for row in output["sent"]:
        parent = row["parent"]
        assert (
            len(
                [
                    span
                    for span in producers
                    if span.parent_span_id.hex() == parent["span_id"]
                    and span.trace_id.hex() == parent["trace_id"]
                ]
            )
            == 1
        )
    carriers = [
        row["django_altimeter"]
        for row in output["captured"]
        if "django_altimeter" in row
    ]
    assert len(carriers) == 3
    assert len({row["attempt"]["traceparent"] for row in carriers}) == 3
    assert "canary" not in str(otlp_receiver.requests).lower()


def test_native_sdk_callback_failure_does_not_break_publication(
    wheel, tmp_path, otlp_receiver
):
    with running_worker(tmp_path, wheel=wheel, receiver=otlp_receiver) as worker:
        result = worker.execute(r"""
import json
from celery import signals
from opentelemetry.sdk.trace import SpanProcessor
from django_altimeter import get_correlation, setup, shutdown
from django_altimeter.runtime import get_process_runtime
setup(role="web")
class BrokenProcessor(SpanProcessor):
    def on_start(self, span, parent_context=None):
        raise RuntimeError("processor-error-canary")
get_process_runtime().tracer_provider.add_span_processor(BrokenProcessor())
observed = []
def capture(headers=None, **kwargs):
    observed.append(headers.copy())
signals.before_task_publish.connect(capture, weak=False)
result = app.send_task("reference.echo", args=["unchanged-result-canary"])
value = result.get(timeout=10, interval=.02)
assert get_correlation() is None
assert "django_altimeter" not in observed[0]
shutdown(timeout=2)
print(json.dumps(value))
""")
    assert result["payload"] == "unchanged-result-canary"
    assert len([span for _, span in spans(otlp_receiver) if span.kind == 5]) == 1
    assert not [span for _, span in spans(otlp_receiver) if span.kind == 4]
    assert "canary" not in str(otlp_receiver.requests).lower()


@pytest.mark.parametrize("matching", [False, True])
def test_declared_queue_does_not_override_actual_named_exchange_route(
    wheel, tmp_path, otlp_receiver, matching
):
    with running_worker(tmp_path, wheel=wheel, receiver=otlp_receiver) as worker:
        result = worker.execute(
            r"""
import json
import os
from kombu import Exchange, Queue
from django_altimeter import setup, shutdown
setup(role="web")
matching = json.load(sys.stdin)
queue = os.environ["REFERENCE_CELERY_QUEUE"]
exchange = Exchange(queue + ".exchange", type="direct")
with app.connection_for_write() as connection:
    with connection.channel() as channel:
        Queue(queue + ".override", exchange, routing_key="actual")(channel).declare()
result = app.send_task(
    "reference.echo", args=["named-route-canary"],
    exchange=exchange.name, routing_key="actual",
    declare=[Queue(queue + ".override" if matching else queue, exchange,
                   routing_key="actual" if matching else "unrelated")],
)
value = result.get(timeout=10, interval=.02)
shutdown(timeout=2)
print(json.dumps(value))
""",
            input=matching,
        )
    assert result["payload"] == "named-route-canary"
    producers = [span for _, span in spans(otlp_receiver) if span.kind == 4]
    assert len(producers) == 1
    # A declaration alone is not proof that this routing key reaches that queue.
    expected = (
        worker.environment["REFERENCE_CELERY_QUEUE"] + ".override"
        if matching
        else "_OTHER"
    )
    assert attributes(producers[0].attributes)["queue.name"] == expected
    assert "canary" not in str(otlp_receiver.requests).lower()
