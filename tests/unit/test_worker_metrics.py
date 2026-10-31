"""Worker metric admission/Views and decoded native OTLP evidence."""

import json
import os
import subprocess
import sys

import pytest
from support.otlp_receiver import OTLPReceiver

_BUCKETS = (0.005, 0.01, 0.05, 0.1, 0.5, 1, 5, 15, 30, 60, 120, 300, 900, 3600, 86400)
_INSTRUMENTS = (
    "altimeter.worker.attempts",
    "altimeter.worker.duration",
    "altimeter.worker.enqueue_to_start",
    "altimeter.worker.intentional_delay",
)


def test_native_worker_views_preserve_schema_and_long_finite_buckets():
    # Exercise the existing runtime/export boundary before adding the recorder.
    source = """
import json, os
from dataclasses import replace
from django_altimeter.conf import Config, SignalConfig
from django_altimeter.privacy import DEFAULT_POLICY
from django_altimeter.runtime import setup, shutdown, get_process_runtime
names = ('altimeter.worker.attempts', 'altimeter.worker.duration',
         'altimeter.worker.enqueue_to_start', 'altimeter.worker.intentional_delay')
attributes = {'messaging.system': 'celery', 'task.name': 'reference.work',
              'queue.name': 'priority', 'altimeter.outcome': 'success'}
config = Config(enabled=True, role='celery-prefork',
    integrations=frozenset({'celery'}),
    service_name='worker-metric-test', deployment_environment='test', release='v1',
    metrics=SignalConfig(True, os.environ['TEST_ENDPOINT'] + '/v1/metrics',
                         destination='collector'),
    queue_names=frozenset({'priority'}), metric_export_interval_millis=60000,
    policy=replace(DEFAULT_POLICY,
        attribute_values=frozenset(attributes.items()),
        metric_instruments=frozenset(('django_altimeter', name)
                                    for name in (*names, 'control.calls'))))
result = setup(config)
assert result.enabled_signals == ('metrics',), result.codes
runtime = get_process_runtime()
meter = runtime.meter_provider.get_meter('django_altimeter')
meter.create_counter('control.calls').add(7, {'private': 'WORKER_CANARY'})
counter = meter.create_counter(names[0], unit='{attempt}')
histograms = [meter.create_histogram(name, unit='s') for name in names[1:]]
for seconds in (20, 36, 60):
    counter.add(1, {**attributes, 'private': 'WORKER_CANARY'})
    for histogram in histograms:
        histogram.record(seconds, {**attributes, 'private': 'WORKER_CANARY'})
assert shutdown().completed
print(json.dumps(True))
"""
    with OTLPReceiver() as receiver:
        process = subprocess.run(
            [sys.executable, "-c", source],
            env={
                **{
                    key: value
                    for key, value in os.environ.items()
                    if not key.startswith(("ALTIMETER_", "OTEL_"))
                },
                "TEST_ENDPOINT": receiver.endpoint,
            },
            capture_output=True,
            text=True,
            timeout=15,
            check=False,
        )
        assert process.returncode == 0, process.stderr
        assert json.loads(process.stdout) is True
        metrics = {
            metric.name: metric
            for request in receiver.requests
            for resource in request.message.resource_metrics
            for scope in resource.scope_metrics
            for metric in scope.metrics
        }
        assert metrics["control.calls"].sum.data_points[0].as_int == 7
        assert not metrics["control.calls"].sum.data_points[0].attributes
        assert set(metrics) == {*_INSTRUMENTS, "control.calls"}
        assert metrics[_INSTRUMENTS[0]].unit == "{attempt}"
        for name in _INSTRUMENTS:
            stream = getattr(metrics[name], metrics[name].WhichOneof("data"))
            assert stream.aggregation_temporality == 1
            (point,) = stream.data_points
            assert {item.key: item.value.string_value for item in point.attributes} == {
                "messaging.system": "celery",
                "task.name": "reference.work",
                "queue.name": "priority",
                "altimeter.outcome": "success",
            }
            if name.endswith("attempts"):
                assert point.as_int == 3
            else:
                assert metrics[name].unit == "s"
                assert point.count == 3
                assert point.sum == 116
                assert tuple(point.explicit_bounds) == _BUCKETS
                assert point.bucket_counts[-1] == 0
        assert all(
            b"WORKER_CANARY" not in request.message.SerializeToString()
            for request in receiver.requests
        )


def _record(recorder, **changes):
    recorder.record(
        **{
            "task_name": "task.0",
            "queue_name": "queue.0",
            "outcome": "success",
            "duration_seconds": 0.25,
            "started_at": 1004.0,
            "published_at": 1000.0,
            "eligible_at": 1003.0,
            **changes,
        }
    )


def _data(reader):
    data = reader.get_metrics_data()
    return (
        {
            metric.name: metric.data
            for resource in data.resource_metrics
            for scope in resource.scope_metrics
            for metric in scope.metrics
        }
        if data is not None
        else {}
    )


@pytest.fixture
def recording():
    from opentelemetry.sdk.metrics import (
        AlwaysOffExemplarFilter,
        Counter,
        Histogram,
        MeterProvider,
    )
    from opentelemetry.sdk.metrics.export import (
        AggregationTemporality,
        InMemoryMetricReader,
    )
    from opentelemetry.sdk.metrics.view import DropAggregation, View
    from opentelemetry.sdk.resources import Resource

    from django_altimeter.privacy import DEFAULT_POLICY
    from django_altimeter.worker_metrics import (
        WorkerMetrics,
        worker_metric_policy,
        worker_metric_views,
    )

    tasks = frozenset(f"task.{index}" for index in range(128))
    queues = frozenset(f"queue.{index}" for index in range(32))
    policy = worker_metric_policy(DEFAULT_POLICY, task_names=tasks, queue_names=queues)
    reader = InMemoryMetricReader(
        preferred_temporality={
            Counter: AggregationTemporality.DELTA,
            Histogram: AggregationTemporality.DELTA,
        }
    )
    provider = MeterProvider(
        resource=Resource({}),
        metric_readers=[reader],
        shutdown_on_exit=False,
        exemplar_filter=AlwaysOffExemplarFilter(),
        views=[
            View(
                instrument_name="*", attribute_keys=set(), aggregation=DropAggregation()
            ),
            *worker_metric_views(),
        ],
    )
    recorder = WorkerMetrics(
        provider, policy=policy, task_names=tasks, queue_names=queues
    )
    try:
        yield recorder, reader, provider
    finally:
        provider.shutdown()


def test_native_measurements_preserve_each_attempt_and_known_intervals(recording):
    recorder, reader, _ = recording
    for outcome in ("success", "retry", "failure", "unknown"):
        for duration in (20, 36, 60):
            _record(recorder, outcome=outcome, duration_seconds=duration)
    data = _data(reader)
    assert set(data) == set(_INSTRUMENTS)
    for name, stream in data.items():
        assert stream.aggregation_temporality == 1
        assert len(stream.data_points) == 4
        for point in stream.data_points:
            assert set(point.attributes) == {
                "messaging.system",
                "task.name",
                "queue.name",
                "altimeter.outcome",
            }
            assert point.attributes["messaging.system"] == "celery"
            assert point.attributes["task.name"] == "task.0"
            assert point.attributes["queue.name"] == "queue.0"
            assert point.attributes["altimeter.outcome"] in {
                "success",
                "retry",
                "failure",
                "unknown",
            }
            assert not getattr(point, "exemplars", [])
            if name.endswith("attempts"):
                assert point.value == 3
            else:
                assert point.count == 3
                assert tuple(point.explicit_bounds) == _BUCKETS
                assert point.bucket_counts[-1] == 0
                assert (
                    point.sum
                    == {
                        "duration": 116,
                        "enqueue_to_start": 12,
                        "intentional_delay": 9,
                    }[name.rsplit(".", 1)[-1]]
                )


@pytest.mark.parametrize(
    ("changes", "expected"),
    [
        ({"published_at": None}, {}),
        ({"published_at": -1}, {}),
        ({"published_at": float("nan")}, {}),
        ({"published_at": float("inf")}, {}),
        ({"published_at": True}, {}),
        ({"published_at": 2**10000}, {}),
        ({"started_at": None}, {}),
        ({"started_at": float("inf")}, {}),
        ({"started_at": 999}, {}),
        ({"eligible_at": 1005}, {}),
        ({"eligible_at": None}, {"enqueue_to_start": 4}),
        ({"eligible_at": 999}, {"enqueue_to_start": 4}),
        ({"eligible_at": float("nan")}, {"enqueue_to_start": 4}),
        ({"eligible_at": True}, {"enqueue_to_start": 4}),
        (
            {"started_at": 1000, "eligible_at": 1000},
            {"enqueue_to_start": 0, "intentional_delay": 0},
        ),
    ],
)
def test_unavailable_times_are_omitted_without_clamping(recording, changes, expected):
    recorder, reader, _ = recording
    _record(recorder, **changes)
    data = _data(reader)
    assert data[_INSTRUMENTS[0]].data_points[0].value == 1
    assert data[_INSTRUMENTS[1]].data_points[0].sum == 0.25
    for suffix in ("enqueue_to_start", "intentional_delay"):
        if suffix in expected:
            point = data[f"altimeter.worker.{suffix}"].data_points[0]
            assert point.count == 1
            assert point.sum == expected[suffix]
        else:
            assert f"altimeter.worker.{suffix}" not in data
    _record(recorder)
    positive = _data(reader)
    assert positive[_INSTRUMENTS[0]].data_points[0].value == 1
    assert positive[_INSTRUMENTS[2]].data_points[0].sum == 4
    assert positive[_INSTRUMENTS[3]].data_points[0].sum == 3


@pytest.mark.parametrize(
    "duration", [None, -1, True, float("inf"), float("nan"), 2**10000]
)
def test_bad_duration_does_not_lose_attempt_or_other_intervals(recording, duration):
    recorder, reader, _ = recording
    _record(recorder, duration_seconds=duration)
    data = _data(reader)
    assert _INSTRUMENTS[1] not in data
    assert data[_INSTRUMENTS[0]].data_points[0].value == 1
    assert data[_INSTRUMENTS[2]].data_points[0].sum == 4
    _record(recorder, duration_seconds=0)
    positive = _data(reader)
    assert positive[_INSTRUMENTS[0]].data_points[0].value == 1
    assert positive[_INSTRUMENTS[1]].data_points[0].count == 1
    assert positive[_INSTRUMENTS[1]].data_points[0].sum == 0


def test_hostile_and_oversized_values_never_invoke_application_accessors(recording):
    class Hostile:
        def __getattribute__(self, _name):
            raise AssertionError("accessor called")

        def __hash__(self):
            raise AssertionError("hash called")

        def __eq__(self, _other):
            raise AssertionError("equality called")

    class HostileString(str):
        def __len__(self):
            raise AssertionError("length called")

        def __hash__(self):
            raise AssertionError("hash called")

    recorder, reader, _ = recording
    values = [
        Hostile(),
        HostileString("task.0"),
        "private-canary" * 10000,
        "\ud800",
        None,
    ]
    for value in values:
        _record(
            recorder,
            task_name=value,
            queue_name=value,
            outcome=value,
            started_at=value,
            published_at=value,
            eligible_at=value,
            duration_seconds=value,
        )
    _record(recorder)
    data = _data(reader)
    points = data[_INSTRUMENTS[0]].data_points
    assert sum(point.value for point in points) == len(values) + 1
    assert {tuple(sorted(point.attributes.items())) for point in points} == {
        tuple(
            sorted(
                {
                    "messaging.system": "celery",
                    "task.name": "_OTHER",
                    "queue.name": "_OTHER",
                    "altimeter.outcome": "unknown",
                }.items()
            )
        ),
        tuple(
            sorted(
                {
                    "messaging.system": "celery",
                    "task.name": "task.0",
                    "queue.name": "queue.0",
                    "altimeter.outcome": "success",
                }.items()
            )
        ),
    }
    assert data[_INSTRUMENTS[1]].data_points[0].count == 1


def test_admission_remains_bounded_across_delta_collections_and_preserves_outcomes(
    recording,
):
    from collections import Counter
    from itertools import product

    recorder, reader, _ = recording
    outcomes = ("success", "retry", "failure", "unknown")
    queues = [*(f"queue.{index}" for index in range(32)), "_OTHER"]
    combinations = list(
        product((f"task.{index}" for index in range(128)), queues, outcomes)
    )
    reserved = len(queues) * len(outcomes)
    detailed = combinations[: 2000 - reserved]
    for task, queue, outcome in detailed:
        _record(recorder, task_name=task, queue_name=queue, outcome=outcome)
    first = _data(reader)
    assert len(first[_INSTRUMENTS[0]].data_points) == len(detailed)
    assert sum(point.value for point in first[_INSTRUMENTS[0]].data_points) == len(
        detailed
    )
    assert all(
        "altimeter.overflow" not in point.attributes
        for point in first[_INSTRUMENTS[0]].data_points
    )
    # Every coarse partition must still fit after the detailed space is full.
    expected = Counter()
    for queue, outcome in product(queues, outcomes):
        for _ in range(3):
            _record(recorder, task_name="task.127", queue_name=queue, outcome=outcome)
            expected[queue, outcome] += 1
    # An already-admitted detailed series remains detailed after overflow.
    task, queue, outcome = detailed[0]
    _record(recorder, task_name=task, queue_name=queue, outcome=outcome)
    second = _data(reader)
    for name, stream in second.items():
        assert len(stream.data_points) == reserved + 1
        coarse = [
            point
            for point in stream.data_points
            if point.attributes.get("altimeter.overflow")
        ]
        assert len(coarse) == reserved
        for point in coarse:
            assert set(point.attributes) == {
                "messaging.system",
                "queue.name",
                "altimeter.outcome",
                "altimeter.overflow",
            }
            assert point.attributes["altimeter.overflow"] is True
        totals = Counter(
            {
                (
                    point.attributes["queue.name"],
                    point.attributes["altimeter.outcome"],
                ): point.value if name.endswith("attempts") else point.count
                for point in coarse
            }
        )
        assert totals == expected
    assert len(recorder._admitted) + len(recorder._overflow) == 2000


def test_concurrent_admission_counts_every_attempt(recording):
    from concurrent.futures import ThreadPoolExecutor

    recorder, reader, _ = recording

    def run(index):
        for _ in range(100):
            _record(
                recorder,
                task_name=f"task.{index % 128}",
                queue_name=f"queue.{index % 32}",
            )

    with ThreadPoolExecutor(max_workers=8) as executor:
        list(executor.map(run, range(40)))
    data = _data(reader)
    assert sum(point.value for point in data[_INSTRUMENTS[0]].data_points) == 4000
    for name in _INSTRUMENTS[1:]:
        assert sum(point.count for point in data[name].data_points) == 4000


def test_views_reject_foreign_scope_types_units_and_extra_dimensions(recording):
    recorder, reader, provider = recording
    for scope, name, kind, unit in [
        ("foreign", _INSTRUMENTS[0], "create_counter", "{attempt}"),
        ("django_altimeter", _INSTRUMENTS[0], "create_counter", "1"),
        ("django_altimeter", _INSTRUMENTS[0], "create_histogram", "{attempt}"),
        ("django_altimeter", _INSTRUMENTS[1], "create_histogram", "ms"),
        ("django_altimeter", _INSTRUMENTS[1] + ".extra", "create_histogram", "s"),
    ]:
        instrument = getattr(provider.get_meter(scope), kind)(name, unit=unit)
        getattr(instrument, "add" if kind == "create_counter" else "record")(
            99, {"private": "CANARY"}
        )
    _record(recorder)
    data = _data(reader)
    assert set(data) == set(_INSTRUMENTS)
    assert data[_INSTRUMENTS[0]].data_points[0].value == 1
    assert data[_INSTRUMENTS[1]].data_points[0].sum == 0.25


def test_runtime_recorder_is_only_created_for_owned_worker_metrics():
    source = """
import json, sys
from opentelemetry import metrics
from opentelemetry.sdk.metrics import MeterProvider
from django_altimeter.conf import Config, SignalConfig
from django_altimeter.runtime import ProcessRuntime
role, mode = sys.argv[1:]
foreign = MeterProvider(shutdown_on_exit=False) if mode == 'foreign' else None
if foreign is not None: metrics.set_meter_provider(foreign)
config = Config(enabled=mode != 'disabled', role=role,
 integrations=frozenset({'celery'}) if mode != 'no-integration' else frozenset(),
 metrics=SignalConfig(mode != 'signal-disabled',
   'http://127.0.0.1:9/v1/metrics', (), 'collector'))
runtime = ProcessRuntime(config)
result = runtime.initialize()
print(json.dumps({'recorder':runtime.worker_metrics is not None, 'codes':result.codes}))
runtime.shutdown()
if foreign is not None: foreign.shutdown()
"""
    for role, mode in [
        ("celery-prefork", "owned"),
        ("celery-solo", "owned"),
        ("beat", "owned"),
        ("web", "owned"),
        ("celery-prefork", "disabled"),
        ("celery-prefork", "signal-disabled"),
        ("celery-prefork", "foreign"),
        ("celery-prefork", "no-integration"),
    ]:
        result = subprocess.run(
            [sys.executable, "-c", source, role, mode],
            capture_output=True,
            text=True,
            timeout=10,
            check=False,
        )
        assert result.returncode == 0, result.stderr
        decoded = json.loads(result.stdout)
        assert decoded["recorder"] is (
            role in {"celery-prefork", "celery-solo"} and mode == "owned"
        )
        if mode == "foreign":
            assert decoded["codes"] == ["setup.metrics_foreign"]


@pytest.mark.parametrize(
    "field,value",
    [
        ("task_names", frozenset(f"task.{index}" for index in range(129))),
        ("queue_names", frozenset(f"queue.{index}" for index in range(33))),
        ("task_names", frozenset({"x" * 129})),
        ("task_names", frozenset({"é" * 65})),
        ("queue_names", frozenset({" "})),
        ("queue_names", frozenset({"\ud800"})),
        ("task_names", frozenset({1})),
        ("task_names", {"task.0"}),
    ],
)
def test_invalid_name_configuration_fails_boundedly(recording, field, value):
    from opentelemetry.metrics import NoOpMeterProvider

    from django_altimeter.privacy import DEFAULT_POLICY
    from django_altimeter.worker_metrics import WorkerMetrics, worker_metric_policy

    arguments = {
        "task_names": frozenset({"task.0"}),
        "queue_names": frozenset({"queue.0"}),
        field: value,
    }
    with pytest.raises(ValueError, match="^worker_metrics.invalid_configuration$"):
        worker_metric_policy(DEFAULT_POLICY, **arguments)
    with pytest.raises(ValueError, match="^worker_metrics.invalid_configuration$"):
        WorkerMetrics(NoOpMeterProvider(), policy=DEFAULT_POLICY, **arguments)
    recorder, reader, _ = recording
    _record(recorder)
    assert _data(reader)[_INSTRUMENTS[0]].data_points[0].value == 1


def test_task_name_source_is_explicit_bounded_policy_only():
    from dataclasses import replace

    from django_altimeter.privacy import DEFAULT_POLICY
    from django_altimeter.worker_metrics import worker_task_names

    assert worker_task_names(
        replace(
            DEFAULT_POLICY,
            attribute_values=frozenset(
                {
                    ("task.name", "reference.safe"),
                    ("queue.name", "private.queue"),
                    ("other", "CANARY"),
                }
            ),
        )
    ) == frozenset({"reference.safe"})
    assert worker_task_names(DEFAULT_POLICY) == frozenset()
    with pytest.raises(ValueError, match="worker_metrics.invalid_configuration"):
        worker_task_names(
            replace(
                DEFAULT_POLICY,
                attribute_values=frozenset(
                    ("task.name", f"task.{index}") for index in range(129)
                ),
            )
        )


@pytest.mark.parametrize(
    "limit", [{"max_attributes": 3}, {"max_key_bytes": 3}, {"max_string_bytes": 3}]
)
def test_lower_limits_reject_before_native_aggregation(recording, limit):
    from dataclasses import replace

    from django_altimeter.privacy import DEFAULT_LIMITS
    from django_altimeter.worker_metrics import WorkerMetrics

    recorder, reader, provider = recording
    limited = WorkerMetrics(
        provider,
        policy=recorder._policy,
        limits=replace(DEFAULT_LIMITS, **limit),
        task_names=recorder._task_names - {"_OTHER"},
        queue_names=recorder._queue_names - {"_OTHER"},
    )
    _record(limited)
    assert _data(reader) == {}
    _record(recorder)
    assert _data(reader)[_INSTRUMENTS[0]].data_points[0].value == 1


def test_inherited_recorder_returns_before_a_locked_parent_mutex():
    source = """
import os, signal
from opentelemetry.metrics import NoOpMeterProvider
from django_altimeter.privacy import DEFAULT_POLICY
from django_altimeter.worker_metrics import WorkerMetrics, worker_metric_policy
policy = worker_metric_policy(DEFAULT_POLICY,
    task_names=frozenset(), queue_names=frozenset())
recorder = WorkerMetrics(NoOpMeterProvider(), policy=policy)
with recorder._lock:
    child = os.fork()
    if child == 0:
        signal.alarm(3)
        recorder.record(task_name='unknown', queue_name='unknown', outcome='success',
                        duration_seconds=0, started_at=0)
        os._exit(0)
    _, status = os.waitpid(child, 0)
assert os.waitstatus_to_exitcode(status) == 0, status
print('returned without inherited lock')
"""
    result = subprocess.run(
        [sys.executable, "-c", source],
        capture_output=True,
        text=True,
        timeout=7,
        check=False,
    )
    assert result.returncode == 0, result.stderr
    assert result.stdout.strip() == "returned without inherited lock"


def test_application_logging_failure_cannot_escape_or_suppress_safe_neighbor(recording):
    import logging
    from dataclasses import replace

    from django_altimeter.privacy import DEFAULT_LIMITS
    from django_altimeter.worker_metrics import WorkerMetrics

    class BrokenHandler(logging.Handler):
        def emit(self, record):
            raise RuntimeError("LOG_HANDLER_CANARY")

    recorder, reader, provider = recording
    limited = WorkerMetrics(
        provider,
        policy=recorder._policy,
        limits=replace(DEFAULT_LIMITS, max_attributes=3),
    )
    logger = logging.getLogger("django_altimeter.worker_metrics")
    handler = BrokenHandler()
    logger.addHandler(handler)
    try:
        for _ in range(3):
            _record(limited)
        _record(recorder)
    finally:
        logger.removeHandler(handler)
    assert _data(reader)[_INSTRUMENTS[0]].data_points[0].value == 1
    assert limited._reported == {"worker_metrics.invalid_observation"}


def test_real_receiver_keeps_all_four_instruments_when_native_batches_split(recording):
    from collections import Counter

    from opentelemetry.sdk.metrics.export import MetricExportResult

    from django_altimeter.exporting import SanitizingSession, create_metric_exporter

    recorder, reader, _ = recording
    for index in range(100):
        _record(recorder, task_name=f"task.{index}")
    data = reader.get_metrics_data()
    with OTLPReceiver() as receiver:
        session = SanitizingSession(
            "metrics", receiver.endpoint + "/v1/metrics", policy=recorder._policy
        )
        exporter = create_metric_exporter(session, timeout=2)
        try:
            assert exporter.export(data) is MetricExportResult.SUCCESS
        finally:
            exporter.shutdown()
        totals = Counter()
        sizes = []
        for request in receiver.requests:
            size = 0
            for resource in request.message.resource_metrics:
                for scope in resource.scope_metrics:
                    for metric in scope.metrics:
                        assert metric.unit == (
                            "{attempt}" if metric.name.endswith("attempts") else "s"
                        )
                        stream = getattr(metric, metric.WhichOneof("data"))
                        assert stream.aggregation_temporality == 1
                        size += len(stream.data_points)
                        for point in stream.data_points:
                            totals[metric.name] += (
                                point.as_int
                                if metric.name.endswith("attempts")
                                else point.count
                            )
                            assert {item.key for item in point.attributes} == {
                                "messaging.system",
                                "task.name",
                                "queue.name",
                                "altimeter.outcome",
                            }
                            if not metric.name.endswith("attempts"):
                                assert tuple(point.explicit_bounds) == _BUCKETS
            sizes.append(size)
        assert sizes == [128, 128, 128, 16]
        assert totals == {name: 100 for name in _INSTRUMENTS}


def test_reduced_key_limit_never_admits_an_oversized_overflow_marker(recording):
    from dataclasses import replace
    from itertools import product

    from django_altimeter.privacy import DEFAULT_LIMITS
    from django_altimeter.worker_metrics import WorkerMetrics

    recorder, reader, provider = recording
    limited = WorkerMetrics(
        provider,
        policy=recorder._policy,
        limits=replace(DEFAULT_LIMITS, max_key_bytes=17),
        task_names=recorder._task_names - {"_OTHER"},
        queue_names=recorder._queue_names - {"_OTHER"},
    )
    combinations = list(
        product(
            (f"task.{index}" for index in range(128)),
            (f"queue.{index}" for index in range(32)),
            ("success", "retry", "failure", "unknown"),
        )
    )
    for task, queue, outcome in combinations[: limited._capacity]:
        _record(limited, task_name=task, queue_name=queue, outcome=outcome)
    first = _data(reader)
    assert (
        sum(point.value for point in first[_INSTRUMENTS[0]].data_points)
        == limited._capacity
    )
    _record(limited, task_name="task.127", queue_name="queue.31", outcome="failure")
    assert _data(reader) == {}
    _record(limited)
    assert _data(reader)[_INSTRUMENTS[0]].data_points[0].value == 1
