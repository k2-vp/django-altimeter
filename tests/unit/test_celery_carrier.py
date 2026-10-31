"""Bounded carrier validation and provider-free registration; not worker proof."""

import json
import os
import subprocess
import sys
from copy import deepcopy
from dataclasses import FrozenInstanceError

import pytest

from django_altimeter.celery import Publication, read_publication

ORIGIN = "00-" + "1" * 32 + "-" + "2" * 16 + "-01"
ATTEMPT = "00-" + "3" * 32 + "-" + "4" * 16 + "-00"
PREVIOUS = "00-" + "5" * 32 + "-" + "6" * 16 + "-01"


def carrier():
    return {
        "django_altimeter": {
            "v": 1,
            "origin": {"traceparent": ORIGIN},
            "attempt": {
                "traceparent": ATTEMPT,
                "published_at": 1730000000.25,
                "eligible_at": 1730000001.5,
                "previous_traceparent": PREVIOUS,
            },
        },
        "business": "unchanged",
    }


def test_reader_preserves_bounded_facts_without_mutation():
    headers = carrier()
    before = deepcopy(headers)
    result = read_publication(headers)
    assert result == Publication(ORIGIN, ATTEMPT, 1730000000.25, 1730000001.5, PREVIOUS)
    assert headers == before
    assert len(json.dumps(headers["django_altimeter"]).encode()) <= 1024
    with pytest.raises(FrozenInstanceError):
        result.published_at = 0


def test_optional_fields_and_zero_epoch_are_valid():
    headers = carrier()
    headers["django_altimeter"]["attempt"] = {"traceparent": ATTEMPT, "published_at": 0}
    assert read_publication(headers) == Publication(ORIGIN, ATTEMPT, 0)


class Hostile:
    def __float__(self):
        raise AssertionError("user conversion called")

    def __len__(self):
        raise AssertionError("user len called")

    def __iter__(self):
        raise AssertionError("user iteration called")


class HostileDict(dict):
    def get(self, key, default=None):
        raise AssertionError("user get called")

    def __len__(self):
        raise AssertionError("user len called")


@pytest.mark.parametrize(
    "value", [None, [], Hostile(), HostileDict(), "CANARY" * 10000]
)
def test_non_native_headers_are_absent(value):
    assert read_publication(value) is None


@pytest.mark.parametrize(
    "value",
    [
        None,
        [],
        Hostile(),
        HostileDict(),
        {"v": 0},
        {"v": True},
        {str(i): Hostile() for i in range(1000)},
    ],
)
def test_invalid_namespace_is_absent_without_traversal(value):
    assert read_publication({"django_altimeter": value}) is None


@pytest.mark.parametrize("field", ["published_at", "eligible_at"])
@pytest.mark.parametrize(
    "value", [None, True, "1", float("nan"), float("inf"), -1, 2**10000, [], Hostile()]
)
def test_invalid_epochs_are_absent(field, value):
    headers = carrier()
    headers["django_altimeter"]["attempt"][field] = value
    assert read_publication(headers) is None


@pytest.mark.parametrize(
    "location,field",
    [
        ("origin", "traceparent"),
        ("attempt", "traceparent"),
        ("attempt", "previous_traceparent"),
    ],
)
@pytest.mark.parametrize(
    "value",
    [
        None,
        Hostile(),
        "x" * 10000,
        "00-" + "0" * 32 + "-" + "2" * 16 + "-01",
        "ff-" + "1" * 32 + "-" + "2" * 16 + "-01",
        "é" * 55,
    ],
)
def test_invalid_traceparents_are_absent(location, field, value):
    headers = carrier()
    headers["django_altimeter"][location][field] = value
    assert read_publication(headers) is None


@pytest.mark.parametrize("location", [None, "origin", "attempt"])
def test_unknown_fields_reject_before_their_values_are_read(location):
    headers = carrier()
    target = headers["django_altimeter"]
    if location:
        target = target[location]
    target["baggage"] = Hostile()
    assert read_publication(headers) is None


def test_import_and_install_create_no_provider_or_thread():
    source = r"""
import inspect
import json
import sys
import threading
from opentelemetry import trace
before_threads = {thread.ident for thread in threading.enumerate()}
before_provider = trace.get_tracer_provider()
from django_altimeter.celery import install
assert "celery" not in sys.modules
from celery import Celery, signals
class ExistingApp(Celery):
    def send_task(self, *args, **kwargs):
        return super().send_task(*args, **kwargs)
app = ExistingApp("registration", fixups=[])
original = app.send_task
before_receivers = tuple(signals.before_task_publish.receivers)
assert install(app) == ()
wrapped = app.send_task
assert install(app) == ()
assert app.send_task is wrapped
assert wrapped.__self__ is app
assert wrapped.__wrapped__ is original.__func__
assert inspect.signature(wrapped) == inspect.signature(original)
receivers = tuple(signals.before_task_publish.receivers)
assert receivers[:len(before_receivers)] == before_receivers
assert len(signals.before_task_publish.receivers) == len(before_receivers) + 1
assert trace.get_tracer_provider() is before_provider
assert {thread.ident for thread in threading.enumerate()} == before_threads
print(json.dumps({"installed": True}))
"""
    result = subprocess.run(
        [sys.executable, "-c", source],
        env=os.environ.copy(),
        text=True,
        capture_output=True,
        timeout=15,
    )
    print(result.stderr, end="")
    assert result.returncode == 0, result.stdout
    assert json.loads(result.stdout) == {"installed": True}


def test_outer_header_keys_cannot_invoke_user_equality():
    calls = []

    class Collision:
        def __hash__(self):
            return hash("django_altimeter")

        def __eq__(self, other):
            calls.append(other)
            raise RuntimeError("user equality called")

    headers = {Collision(): "unchanged"}
    assert read_publication(headers) is None
    assert calls == []
    assert len(headers) == 1


def test_oversized_outer_headers_are_rejected_without_reading_values():
    headers = {str(index): Hostile() for index in range(129)}
    headers.update(carrier())
    assert read_publication(headers) is None
    assert len(headers) == 131


def test_install_preserves_an_existing_callable_override():
    source = r"""
import json
from celery import Celery
from django_altimeter.celery import install
app = Celery("callable-override", fixups=[])
calls = []
expected = object()
def existing(name, *args, **kwargs):
    calls.append((name, args, kwargs))
    return expected
app.send_task = existing
assert install(app) == ()
assert install(app) == ()
assert app.send_task("native.name", (1,), headers={"business": "safe"}) is expected
assert calls == [("native.name", ((1,),), {"headers": {"business": "safe"}})]
print(json.dumps({"calls": len(calls)}))
"""
    result = subprocess.run(
        [sys.executable, "-c", source], text=True, capture_output=True, timeout=15
    )
    print(result.stderr, end="")
    assert result.returncode == 0, result.stdout
    assert json.loads(result.stdout) == {"calls": 1}
