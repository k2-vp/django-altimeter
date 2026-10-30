"""Request aggregation unit evidence plus real runtime/receiver seam checks."""

from __future__ import annotations

import json
import os
import subprocess
import sys

import pytest
from support.otlp_receiver import OTLPReceiver

_BUCKETS = (
    0.005,
    0.01,
    0.025,
    0.05,
    0.075,
    0.1,
    0.25,
    0.5,
    0.75,
    1,
    2.5,
    5,
    7.5,
    10,
    15,
    30,
    45,
    60,
    120,
)


def _attributes(point) -> dict[str, object]:
    return {
        item.key: getattr(item.value, item.value.WhichOneof("value"))
        for item in point.attributes
    }


def test_runtime_request_views_preserve_dimensions_and_long_buckets() -> None:
    # This red exercises existing runtime/SDK behavior, not a missing new import.
    source = """
import json, os
from dataclasses import replace
from django_altimeter.conf import Config, SignalConfig
from django_altimeter.privacy import DEFAULT_POLICY
from django_altimeter.runtime import setup, shutdown, get_process_runtime
pairs = frozenset({
    ('django_altimeter', 'altimeter.http.server.requests'),
    ('django_altimeter', 'http.server.request.duration'),
    ('django_altimeter', 'control.calls'),
})
config = Config(
    enabled=True, service_name='metric-reference', deployment_environment='test',
    release='v1', metrics=SignalConfig(
        True, os.environ['TEST_ENDPOINT'] + '/v1/metrics', (), 'collector'),
    metric_export_interval_millis=60000,
    policy=replace(DEFAULT_POLICY, metric_instruments=pairs,
        attribute_values=frozenset({('http.route', 'items/<item_id>')})),
)
initialized = setup(config)
assert initialized.enabled_signals == ('metrics',), initialized.codes
runtime = get_process_runtime()
assert runtime is not None
meter = runtime.meter_provider.get_meter('django_altimeter')
control = meter.create_counter('control.calls')
control.add(7, {'secret': 'CANARY_PRIVATE'})
count = meter.create_counter('altimeter.http.server.requests', unit='{request}')
duration = meter.create_histogram('http.server.request.duration', unit='s')
attributes = {'http.request.method': 'GET', 'http.route': 'items/<item_id>',
    'altimeter.surface': 'django', 'http.response.status_class': '2xx',
    'altimeter.http.outcome': 'completed', 'altimeter.response_mode': 'buffered',
    'secret': 'CANARY_PRIVATE'}
for seconds in (20, 36, 60):
    count.add(1, attributes)
    duration.record(seconds, attributes)
stopped = shutdown()
print(json.dumps({'completed': stopped.completed, 'codes': stopped.codes}))
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
        assert json.loads(process.stdout) == {"completed": True, "codes": []}
        metrics = {
            metric.name: metric
            for request in receiver.requests
            for resource in request.message.resource_metrics
            for scope in resource.scope_metrics
            for metric in scope.metrics
        }
        assert metrics["control.calls"].sum.data_points[0].as_int == 7
        assert not metrics["control.calls"].sum.data_points[0].attributes
        assert all(
            b"CANARY_PRIVATE" not in request.message.SerializeToString()
            for request in receiver.requests
        )
        counter = metrics["altimeter.http.server.requests"].sum.data_points
        histogram = metrics["http.server.request.duration"].histogram.data_points
        assert len(counter) == len(histogram) == 1
        assert counter[0].as_int == histogram[0].count == 3
        assert tuple(histogram[0].explicit_bounds) == _BUCKETS
        assert histogram[0].bucket_counts[-1] == 0
        assert histogram[0].bucket_counts[15:18] == [1, 1, 1]
        assert (
            _attributes(counter[0])
            == _attributes(histogram[0])
            == {
                "http.request.method": "GET",
                "http.route": "items/<item_id>",
                "altimeter.surface": "django",
                "http.response.status_class": "2xx",
                "altimeter.http.outcome": "completed",
                "altimeter.response_mode": "buffered",
            }
        )


def test_native_metric_export_splits_points_without_losing_totals() -> None:
    from dataclasses import replace

    from opentelemetry.sdk.metrics import Counter, MeterProvider
    from opentelemetry.sdk.metrics.export import (
        AggregationTemporality,
        InMemoryMetricReader,
        MetricExportResult,
    )
    from opentelemetry.sdk.resources import Resource

    from django_altimeter.exporting import SanitizingSession, create_metric_exporter
    from django_altimeter.privacy import DEFAULT_POLICY

    routes = frozenset(f"items/{index}/<item_id>" for index in range(65))
    policy = replace(
        DEFAULT_POLICY,
        metric_instruments=frozenset({("django_altimeter", "control.points")}),
        attribute_values=frozenset(("http.route", route) for route in routes),
    )
    reader = InMemoryMetricReader(
        preferred_temporality={Counter: AggregationTemporality.DELTA}
    )
    provider = MeterProvider(
        metric_readers=[reader], resource=Resource({}), shutdown_on_exit=False
    )
    counter = provider.get_meter("django_altimeter").create_counter("control.points")
    with OTLPReceiver() as receiver:
        session = SanitizingSession(
            "metrics", receiver.endpoint + "/v1/metrics", policy=policy
        )
        exporter = create_metric_exporter(session, timeout=1)
        try:
            counter.add(
                7, {"http.route": "items/0/<item_id>", "http.request.method": "GET"}
            )
            assert (
                exporter.export(reader.get_metrics_data()) is MetricExportResult.SUCCESS
            )
            assert len(receiver.requests) == 1
            for route in sorted(routes):
                for method in ("GET", "POST"):
                    counter.add(1, {"http.route": route, "http.request.method": method})
            data = reader.get_metrics_data()
            assert (
                len(
                    data.resource_metrics[0]
                    .scope_metrics[0]
                    .metrics[0]
                    .data.data_points
                )
                == 130
            )
            assert exporter.export(data) is MetricExportResult.SUCCESS
            assert len(receiver.requests) == 3
            total = 0
            sizes = []
            for request in receiver.requests:
                points = [
                    point
                    for resource in request.message.resource_metrics
                    for scope in resource.scope_metrics
                    for metric in scope.metrics
                    for point in metric.sum.data_points
                ]
                sizes.append(len(points))
                total += sum(point.as_int for point in points)
                assert all(
                    _attributes(point).get("http.route") in routes for point in points
                )
            assert sizes == [1, 128, 2]
            assert total == 137
        finally:
            provider.shutdown()
            exporter.shutdown()


@pytest.fixture
def recording():
    from dataclasses import replace

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

    from django_altimeter.metrics import (
        RequestMetrics,
        request_metric_policy,
        request_metric_views,
    )
    from django_altimeter.privacy import DEFAULT_POLICY

    routes = frozenset({"items/<item_id>", "overflow/<item_id>"}) | frozenset(
        f"items/{index}/<item_id>" for index in range(64)
    )
    policy = request_metric_policy(
        replace(
            DEFAULT_POLICY,
            attribute_values=frozenset(("http.route", route) for route in routes),
        )
    )
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
            *request_metric_views(),
        ],
    )
    recorder = RequestMetrics(provider, policy=policy)
    try:
        yield recorder, reader, provider
    finally:
        provider.shutdown()


def _record(recorder, **changes) -> None:
    values = {
        "duration": 0.25,
        "surface": "django",
        "method": "GET",
        "route": "items/<item_id>",
        "status": 200,
        "outcome": "completed",
        "response_mode": "buffered",
    }
    recorder.record(**(values | changes))


def _data(reader) -> dict:
    data = reader.get_metrics_data()
    if data is None:
        return {}
    return {
        metric.name: metric.data
        for resource in data.resource_metrics
        for scope in resource.scope_metrics
        for metric in scope.metrics
    }


def test_recorder_counts_every_observation_and_does_not_resample(recording) -> None:
    from opentelemetry.sdk.trace import TracerProvider
    from opentelemetry.sdk.trace.sampling import (
        ALWAYS_OFF,
        ALWAYS_ON,
        TraceIdRatioBased,
    )

    recorder, reader, _ = recording
    totals = []
    for sampler in (ALWAYS_ON, TraceIdRatioBased(0.1), ALWAYS_OFF):
        provider = TracerProvider(sampler=sampler, shutdown_on_exit=False)
        try:
            for _ in range(100):
                with provider.get_tracer("control").start_as_current_span("GET"):
                    _record(recorder)
            data = _data(reader)
            counter = data["altimeter.http.server.requests"]
            duration = data["http.server.request.duration"]
            totals.append(
                (
                    sum(p.value for p in counter.data_points),
                    sum(p.count for p in duration.data_points),
                )
            )
            assert (
                counter.aggregation_temporality == duration.aggregation_temporality == 1
            )
        finally:
            provider.shutdown()
    assert totals == [(100, 100), (100, 100), (100, 100)]


def test_duration_values_land_in_finite_long_buckets(recording) -> None:
    recorder, reader, _ = recording
    for duration in (20, 36, 60):
        _record(recorder, duration=duration)
    data = _data(reader)
    point = data["http.server.request.duration"].data_points[0]
    assert tuple(point.explicit_bounds) == _BUCKETS
    assert point.count == 3 and point.sum == 116
    assert point.bucket_counts[15:18] == (1, 1, 1)
    assert point.bucket_counts[-1] == 0
    assert data["altimeter.http.server.requests"].data_points[0].value == 3


def test_views_only_match_exact_owned_instrument_type_scope_and_unit(recording) -> None:
    recorder, reader, provider = recording
    _record(recorder)
    for scope, name, unit in (
        ("unowned", "http.server.request.duration", "s"),
        ("django_altimeter", "http.server.request.duration.child", "s"),
        ("django_altimeter", "http.server.request.duration", "ms"),
        (
            "django_altimeter",
            "altimeter.http.server.requests",
            "{request}",
        ),
    ):
        meter = provider.get_meter(scope)
        meter.create_histogram(name, unit=unit).record(
            100, {"secret": "CANARY_PRIVATE"}
        )
    data = _data(reader)
    assert set(data) == {
        "altimeter.http.server.requests",
        "http.server.request.duration",
    }
    assert sum(p.value for p in data["altimeter.http.server.requests"].data_points) == 1
    assert sum(p.count for p in data["http.server.request.duration"].data_points) == 1


def test_overflow_reserves_all_coarse_series_and_preserves_denominators(
    recording, caplog
) -> None:
    from collections import Counter
    from itertools import islice, product

    recorder, reader, _ = recording
    expected = Counter()
    methods = (
        "GET",
        "POST",
        "PUT",
        "PATCH",
        "DELETE",
        "HEAD",
        "OPTIONS",
        "CONNECT",
        "TRACE",
    )
    statuses = (None, 100, 200, 300, 400, 500)
    modes = ("buffered", "streaming", "unknown")
    outcomes = ("completed", "aborted")

    def record(surface, route, method, status, outcome, mode):
        _record(
            recorder,
            surface=surface,
            route=route,
            method=method,
            status=status,
            outcome=outcome,
            response_mode=mode,
        )
        expected[
            (
                surface,
                "absent" if status is None else f"{status // 100}xx",
                outcome,
                mode,
            )
        ] += 1

    admitted = list(
        islice(
            product(
                ("django", "mcp"),
                (f"items/{n}/<item_id>" for n in range(64)),
                methods,
                statuses,
                outcomes,
                modes,
            ),
            1928,
        )
    )
    for values in admitted:
        record(*values)
    for surface, status, outcome, mode in product(
        ("django", "mcp"), statuses, outcomes, modes
    ):
        record(surface, "overflow/<item_id>", "POST", status, outcome, mode)
    for _ in range(100):
        record("django", "overflow/<item_id>", "POST", 500, "completed", "streaming")
    record(*admitted[0])
    data = _data(reader)
    for name, stream in data.items():
        assert len(stream.data_points) == 2000
        overflow = [
            point
            for point in stream.data_points
            if point.attributes.get("altimeter.overflow")
        ]
        assert len(overflow) == 72
        actual = Counter()
        for point in stream.data_points:
            a = point.attributes
            amount = (
                point.value if name == "altimeter.http.server.requests" else point.count
            )
            actual[
                (
                    a["altimeter.surface"],
                    a["http.response.status_class"],
                    a["altimeter.http.outcome"],
                    a["altimeter.response_mode"],
                )
            ] += amount
            if a.get("altimeter.overflow"):
                assert set(a) == {
                    "altimeter.surface",
                    "http.response.status_class",
                    "altimeter.http.outcome",
                    "altimeter.response_mode",
                    "altimeter.overflow",
                }
        assert actual == expected
        assert sum(actual.values()) == 2101
        detailed_first = [
            p
            for p in stream.data_points
            if dict(p.attributes)
            == {
                "altimeter.surface": "django",
                "http.route": "items/0/<item_id>",
                "http.request.method": "GET",
                "http.response.status_class": "absent",
                "altimeter.http.outcome": "completed",
                "altimeter.response_mode": "buffered",
            }
        ]
        assert len(detailed_first) == 1
        assert (
            detailed_first[0].value
            if name == "altimeter.http.server.requests"
            else detailed_first[0].count
        ) == 2
    codes = [
        record.getMessage()
        for record in caplog.records
        if record.name == "django_altimeter.metrics"
    ]
    assert codes == ["metrics.overflow"]
    # Delta collection must not reset lifetime admission and create new series.
    _record(recorder, route="overflow/<item_id>", status=201)
    after = _data(reader)
    for stream in after.values():
        assert len(stream.data_points) == 1
        assert stream.data_points[0].attributes["altimeter.overflow"] is True


class UnreadableText(str):
    accesses = []

    def __len__(self):
        self.accesses.append("accessor")
        raise AssertionError("user accessor called")

    def __hash__(self):
        self.accesses.append("accessor")
        raise AssertionError("user accessor called")

    def __eq__(self, other):
        self.accesses.append("accessor")
        raise AssertionError("user accessor called")

    def encode(self, *args, **kwargs):
        self.accesses.append("accessor")
        raise AssertionError("user accessor called")


@pytest.mark.parametrize("field", ["method", "route", "status", "response_mode"])
@pytest.mark.parametrize(
    "value",
    [
        UnreadableText("CANARY_PRIVATE"),
        {"secret": "CANARY_PRIVATE"},
        ["CANARY_PRIVATE"],
        "CANARY_PRIVATE" * 100000,
    ],
    ids=["subclass", "dict", "list", "oversized"],
)
def test_unknown_request_values_never_reach_aggregation_and_safe_neighbor_survives(
    recording, field, value
) -> None:
    recorder, reader, _ = recording
    UnreadableText.accesses.clear()
    _record(recorder, **{field: value})
    assert UnreadableText.accesses == []
    _record(recorder)
    data = _data(reader)
    for name, stream in data.items():
        assert (
            sum(
                p.value if name == "altimeter.http.server.requests" else p.count
                for p in stream.data_points
            )
            == 2
        )
        assert all(
            "CANARY_PRIVATE" not in repr(dict(p.attributes)) for p in stream.data_points
        )
        assert any(
            p.attributes.get("http.route") == "items/<item_id>"
            and p.attributes["http.request.method"] == "GET"
            for p in stream.data_points
        )
    attrs = [
        dict(point.attributes)
        for point in data["altimeter.http.server.requests"].data_points
    ]
    key, expected = {
        "method": ("http.request.method", "_OTHER"),
        "route": ("http.route", None),
        "status": ("http.response.status_class", "absent"),
        "response_mode": ("altimeter.response_mode", "unknown"),
    }[field]
    assert any(a.get(key) == expected for a in attrs)


@pytest.mark.parametrize(
    ("field", "value"),
    [
        ("duration", float("nan")),
        ("duration", float("inf")),
        ("duration", -1),
        ("duration", True),
        ("duration", 1 << 100000),
        ("duration", UnreadableText("0.25")),
        ("surface", "CANARY_PRIVATE"),
        ("surface", UnreadableText("django")),
        ("outcome", "CANARY_PRIVATE"),
        ("outcome", UnreadableText("completed")),
    ],
    ids=[
        "nan",
        "inf",
        "negative",
        "bool",
        "huge-int",
        "duration-subclass",
        "surface",
        "surface-subclass",
        "outcome",
        "outcome-subclass",
    ],
)
def test_invalid_observation_drops_before_sdk_and_safe_neighbor_survives(
    recording, field, value
) -> None:
    recorder, reader, _ = recording
    UnreadableText.accesses.clear()
    _record(recorder, **{field: value})
    assert UnreadableText.accesses == []
    _record(recorder)
    data = _data(reader)
    assert data["altimeter.http.server.requests"].data_points[0].value == 1
    assert data["http.server.request.duration"].data_points[0].count == 1
    assert all(len(stream.data_points) == 1 for stream in data.values())


def test_parallel_admission_stays_bounded_and_preserves_every_total(recording) -> None:
    from concurrent.futures import ThreadPoolExecutor

    recorder, reader, _ = recording
    methods = (
        "GET",
        "POST",
        "PUT",
        "PATCH",
        "DELETE",
        "HEAD",
        "OPTIONS",
        "CONNECT",
        "TRACE",
    )

    def record(index):
        _record(
            recorder,
            route=f"items/{index % 64}/<item_id>",
            method=methods[(index // 64) % 9],
            status=100 + 100 * ((index // 576) % 5),
            response_mode="streaming" if index % 2 else "buffered",
        )

    with ThreadPoolExecutor(max_workers=8) as workers:
        list(workers.map(record, range(5000)))
    data = _data(reader)
    for name, stream in data.items():
        assert 1928 < len(stream.data_points) <= 2000
        assert (
            sum(
                p.value if name == "altimeter.http.server.requests" else p.count
                for p in stream.data_points
            )
            == 5000
        )
        assert any(p.attributes.get("altimeter.overflow") for p in stream.data_points)


@pytest.mark.parametrize("mode", ["disabled", "signal-disabled", "foreign", "owned"])
def test_runtime_only_creates_request_recorder_for_owned_metrics(mode) -> None:
    source = """
import json, sys
from opentelemetry import metrics
from opentelemetry.sdk.metrics import MeterProvider
from django_altimeter.conf import Config, SignalConfig
from django_altimeter.runtime import ProcessRuntime
mode = sys.argv[1]
foreign = MeterProvider(shutdown_on_exit=False) if mode == 'foreign' else None
if foreign is not None:
    metrics.set_meter_provider(foreign)
config = Config(enabled=mode != 'disabled', service_name='reference',
    deployment_environment='test', release='v1',
    metrics=SignalConfig(
        mode != 'signal-disabled', 'http://127.0.0.1:9/v1/metrics', (), 'collector'))
runtime = ProcessRuntime(config)
initialized = runtime.initialize()
print(json.dumps({'has_recorder': runtime.request_metrics is not None,
                  'enabled': initialized.enabled_signals, 'codes': initialized.codes}))
runtime.shutdown()
if foreign is not None:
    foreign.shutdown()
"""
    process = subprocess.run(
        [sys.executable, "-c", source, mode],
        capture_output=True,
        text=True,
        timeout=10,
        check=False,
    )
    assert process.returncode == 0, process.stderr
    result = json.loads(process.stdout)
    assert result["has_recorder"] is (mode == "owned")
    assert result["enabled"] == (["metrics"] if mode == "owned" else [])
    if mode == "foreign":
        assert result["codes"] == ["setup.metrics_foreign"]


def test_runtime_full_admission_exports_bounded_batches_with_totals_intact() -> None:
    source = """
import json, os
from dataclasses import replace
from itertools import product, islice
from django_altimeter.conf import Config, SignalConfig
from django_altimeter.privacy import DEFAULT_POLICY
from django_altimeter.runtime import setup, shutdown, get_process_runtime
routes = {f'items/{n}/<item_id>' for n in range(64)} | {'overflow/<item_id>'}
config = Config(enabled=True, service_name='metric-reference',
    deployment_environment='test', release='v1',
    metrics=SignalConfig(
        True, os.environ['TEST_ENDPOINT'] + '/v1/metrics', (), 'collector'),
    policy=replace(DEFAULT_POLICY,
        attribute_values=frozenset(('http.route', route) for route in routes)),
    metric_export_interval_millis=60000, export_timeout=2, shutdown_timeout=2)
initialized = setup(config)
assert initialized.enabled_signals == ('metrics',), initialized.codes
runtime = get_process_runtime()
assert runtime is not None and runtime.request_metrics is not None
recorder = runtime.request_metrics
for route, method, status, outcome, mode in islice(product(
        sorted(routes - {'overflow/<item_id>'}),
        ('GET','POST','PUT','PATCH','DELETE','HEAD','OPTIONS','CONNECT','TRACE'),
        (None,100,200,300,400,500), ('completed','aborted'),
        ('buffered','streaming','unknown')), 1928):
    recorder.record(0.25, surface='django', route=route, method=method,
        status=status, outcome=outcome, response_mode=mode)
for surface, status, outcome, mode in product(
        ('django','mcp'), (None,100,200,300,400,500),
        ('completed','aborted'), ('buffered','streaming','unknown')):
    recorder.record(36, surface=surface, route='overflow/<item_id>', method='POST',
        status=status, outcome=outcome, response_mode=mode)
stopped = shutdown()
print(json.dumps({'completed': stopped.completed, 'codes': stopped.codes,
                  'instance': runtime.instance_id}))
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
        status = json.loads(process.stdout)
        assert status["completed"] is True and status["codes"] == []
        assert "export.batch_count" not in process.stderr
        points = {
            "altimeter.http.server.requests": [],
            "http.server.request.duration": [],
        }
        instances = set()
        batch_sizes = []
        for request in receiver.requests:
            size = 0
            for resource in request.message.resource_metrics:
                instances.add(_attributes(resource.resource)["service.instance.id"])
                for scope in resource.scope_metrics:
                    assert scope.scope.name == "django_altimeter"
                    for metric in scope.metrics:
                        data = getattr(metric, metric.WhichOneof("data"))
                        assert data.aggregation_temporality == 1
                        points[metric.name].extend(data.data_points)
                        size += len(data.data_points)
            batch_sizes.append(size)
        assert len(batch_sizes) == 32 and max(batch_sizes) == 128
        assert min(batch_sizes) > 0 and sum(batch_sizes) == 4000
        assert instances == {status["instance"]}
        for name, data_points in points.items():
            assert len(data_points) == 2000
            assert (
                sum(
                    point.as_int
                    if name == "altimeter.http.server.requests"
                    else point.count
                    for point in data_points
                )
                == 2000
            )
            overflow = [
                point
                for point in data_points
                if _attributes(point).get("altimeter.overflow")
            ]
            assert len(overflow) == 72
            partitions = {_attributes(point)["altimeter.surface"] for point in overflow}
            assert partitions == {"django", "mcp"}
            assert all(
                "http.route" not in _attributes(point)
                and "http.request.method" not in _attributes(point)
                for point in overflow
            )
        assert all(
            tuple(point.explicit_bounds) == _BUCKETS and point.bucket_counts[-1] == 0
            for point in points["http.server.request.duration"]
        )


def test_application_logging_failure_does_not_escape_or_disable_valid_recording(
    recording,
) -> None:
    import logging

    class BrokenHandler(logging.Handler):
        def emit(self, record):
            raise RuntimeError("CANARY_PRIVATE")

    recorder, reader, _ = recording
    logger = logging.getLogger("django_altimeter.metrics")
    handler = BrokenHandler()
    logger.addHandler(handler)
    try:
        for _ in range(10):
            _record(recorder, outcome="CANARY_PRIVATE")
        _record(recorder)
    finally:
        logger.removeHandler(handler)
    data = _data(reader)
    assert data["altimeter.http.server.requests"].data_points[0].value == 1
    assert data["http.server.request.duration"].data_points[0].count == 1


def test_native_request_instruments_require_composed_wire_policy(recording) -> None:
    from opentelemetry.exporter.otlp.proto.common.metrics_encoder import encode_metrics

    from django_altimeter.metrics import REQUEST_INSTRUMENTS, request_metric_policy
    from django_altimeter.privacy import DEFAULT_POLICY, project_metrics

    recorder, reader, _ = recording
    _record(recorder)
    request = encode_metrics(reader.get_metrics_data())
    denied = project_metrics(request, policy=DEFAULT_POLICY)
    assert not any(
        scope.metrics
        for group in denied.resource_metrics
        for scope in group.scope_metrics
    )
    composed = request_metric_policy(DEFAULT_POLICY)
    assert composed is not DEFAULT_POLICY
    assert not DEFAULT_POLICY.metric_instruments
    assert composed.metric_instruments == REQUEST_INSTRUMENTS
    kept = project_metrics(request, policy=composed)
    metrics = {
        metric.name: metric
        for group in kept.resource_metrics
        for scope in group.scope_metrics
        for metric in scope.metrics
    }
    assert set(metrics) == {
        "altimeter.http.server.requests",
        "http.server.request.duration",
    }
    assert metrics["altimeter.http.server.requests"].sum.data_points[0].as_int == 1
    assert metrics["http.server.request.duration"].histogram.data_points[0].count == 1


@pytest.mark.parametrize(
    "limit", [{"max_attributes": 4}, {"max_string_bytes": 2}, {"max_key_bytes": 2}]
)
def test_lower_configured_limits_reject_before_aggregation(recording, limit) -> None:
    from dataclasses import replace

    from django_altimeter.metrics import RequestMetrics, request_metric_policy
    from django_altimeter.privacy import DEFAULT_LIMITS, DEFAULT_POLICY

    safe, reader, provider = recording
    limited = RequestMetrics(
        provider,
        policy=request_metric_policy(DEFAULT_POLICY),
        limits=replace(DEFAULT_LIMITS, **limit),
    )
    _record(limited)
    assert _data(reader) == {}
    _record(safe)
    data = _data(reader)
    assert data["altimeter.http.server.requests"].data_points[0].value == 1
    assert data["http.server.request.duration"].data_points[0].count == 1
