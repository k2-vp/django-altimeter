"""Real gthread traffic reconciles with decoded request metric deltas."""

import json
from collections import Counter
from concurrent.futures import ThreadPoolExecutor

import pytest
from support.gunicorn_server import install_wheel, running_server
from support.otlp_receiver import OTLPReceiver

pytestmark = pytest.mark.integration

COUNT = "altimeter.http.server.requests"
DURATION = "http.server.request.duration"
BOUNDS = (
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
DIMENSIONS = {
    "altimeter.surface",
    "http.request.method",
    "http.route",
    "http.response.status_class",
    "altimeter.http.outcome",
    "altimeter.response_mode",
    "altimeter.overflow",
}


@pytest.fixture(scope="module")
def metrics_wheel(tmp_path_factory):
    return install_wheel(tmp_path_factory.mktemp("wsgi-metrics-wheel"))


def _attributes(values):
    return {
        value.key: getattr(value.value, value.value.WhichOneof("value"))
        for value in values
    }


def _observations(receiver):
    points = {COUNT: [], DURATION: []}
    for request in receiver.requests:
        if request.path != "/v1/metrics":
            continue
        for group in request.message.resource_metrics:
            resource = _attributes(group.resource.attributes)
            assert resource["service.instance.id"]
            assert "altimeter.tenant_id" not in resource
            assert "posthogDistinctId" not in resource
            for scope in group.scope_metrics:
                assert scope.scope.name == "django_altimeter"
                names = [metric.name for metric in scope.metrics]
                assert len(names) == len(set(names))
                for metric in scope.metrics:
                    assert metric.name in points
                    if metric.name == COUNT:
                        assert metric.HasField("sum")
                        assert metric.unit == "{request}"
                        assert metric.sum.is_monotonic
                        data = metric.sum
                    else:
                        assert metric.HasField("histogram")
                        assert metric.unit == "s"
                        data = metric.histogram
                    assert data.aggregation_temporality == 1
                    for point in data.data_points:
                        attributes = _attributes(point.attributes)
                        assert set(attributes) <= DIMENSIONS
                        if metric.name == DURATION:
                            assert tuple(point.explicit_bounds) == BOUNDS
                            assert sum(point.bucket_counts) == point.count
                        points[metric.name].append((resource, attributes, point))
    return points


def _server_spans(receiver):
    return [
        span
        for request in receiver.requests
        if request.path == "/v1/traces"
        for group in request.message.resource_spans
        for scope in group.scope_spans
        for span in scope.spans
        if span.kind == 2
    ]


def _totals(points, name):
    result = Counter()
    for _, attributes, point in points[name]:
        result[tuple(sorted(attributes.items()))] += (
            point.as_int if name == COUNT else point.count
        )
    return result


@pytest.mark.parametrize("sample_rate", ["1", "0.1", "0"])
def test_gthread_sampling_preserves_exact_counts_and_routes(
    metrics_wheel, tmp_path, sample_rate
):
    total = 64
    with OTLPReceiver() as receiver:
        with running_server(
            tmp_path,
            wheel=metrics_wheel,
            receiver=receiver,
            workers=2,
            threads=4,
            extra_env={"ALTIMETER_SAMPLE_RATE": sample_rate},
        ) as server:

            def request(index):
                status, headers, body = server.request(
                    f"/api/items/{index}?secret=metric-query-canary",
                    headers={"sessionId": "metric-session-canary"},
                )
                assert status == 200
                assert json.loads(body)["item_id"] == index
                return {key.lower(): value for key, value in headers.items()}

            with ThreadPoolExecutor(max_workers=8) as clients:
                responses = list(clients.map(request, range(total)))
        points = _observations(receiver)
        counters = _totals(points, COUNT)
        histograms = _totals(points, DURATION)
        assert sum(counters.values()) == total
        assert counters == histograms
        expected = {
            "altimeter.surface": "django",
            "http.request.method": "GET",
            "http.route": "api/items/<int:item_id>",
            "http.response.status_class": "2xx",
            "altimeter.http.outcome": "completed",
            "altimeter.response_mode": "buffered",
        }
        assert counters == {tuple(sorted(expected.items())): total}
        spans = _server_spans(receiver)
        assert len(spans) == sum(
            response["x-altimeter-trace-sampled"] == "1" for response in responses
        )
        assert len({span.trace_id for span in spans}) == len(spans)
        assert {span.trace_id.hex() for span in spans} == {
            response["x-altimeter-trace-id"]
            for response in responses
            if response["x-altimeter-trace-sampled"] == "1"
        }
        if sample_rate == "1":
            assert len(spans) == total
        elif sample_rate == "0":
            assert spans == []
        else:
            assert len(spans) < total
        for request in receiver.requests:
            payload = request.message.SerializeToString()
            assert b"metric-query-canary" not in payload
            assert b"metric-session-canary" not in payload


@pytest.mark.parametrize("sample_rate", ["1", "0.1"])
def test_gthread_status_classes_and_response_modes_preserve_the_denominator(
    metrics_wheel, tmp_path, sample_rate
):
    cases = (
        [("/api/items/7", 200, "api/items/<int:item_id>", "buffered")] * 6
        + [("/api/error", 500, "api/error", "buffered")] * 3
        + [("/api/rejected", 403, None, "buffered")] * 2
        + [("/unmatched-path-canary", 404, None, "buffered")] * 2
        + [("/api/file", 200, "api/file", "streaming")]
        + [("/api/stream", 200, "api/stream", "streaming")]
    )
    with OTLPReceiver() as receiver:
        with running_server(
            tmp_path,
            wheel=metrics_wheel,
            receiver=receiver,
            extra_env={"ALTIMETER_SAMPLE_RATE": sample_rate},
        ) as server:
            for path, status, _, _ in cases:
                actual, _, body = server.request(path)
                assert actual == status, (path, actual, body)
                if path == "/api/file":
                    assert body == b"reference-file\n" * 4096
                if path == "/api/stream":
                    assert body == b"first\nlast\n"
        points = _observations(receiver)
        expected = Counter()
        for _, status, route, mode in cases:
            attributes = {
                "altimeter.surface": "django",
                "http.request.method": "GET",
                "http.response.status_class": f"{status // 100}xx",
                "altimeter.http.outcome": "completed",
                "altimeter.response_mode": mode,
            }
            if route is not None:
                attributes["http.route"] = route
            expected[tuple(sorted(attributes.items()))] += 1
        counts = _totals(points, COUNT)
        assert counts == expected
        assert _totals(points, DURATION) == expected
        assert sum(counts.values()) == len(cases)
        assert (
            sum(
                value
                for dimensions, value in counts.items()
                if dict(dimensions)["http.response.status_class"] == "5xx"
            )
            == 3
        )
        if sample_rate == "1":
            assert len(_server_spans(receiver)) == len(cases)
        for request in receiver.requests:
            payload = request.message.SerializeToString()
            assert b"unmatched-path-canary" not in payload
            assert b"reference-error-canary" not in payload


@pytest.mark.parametrize("sample_rate", ["1", "0.1"])
def test_real_long_gthread_requests_remain_in_finite_buckets(
    metrics_wheel, tmp_path, sample_rate
):
    # Actual wall-clock requests, separate from the predeclared performance gate.
    # A 60s sleep plus real protocol work belongs in the next finite 120s bucket.
    seconds = (20, 36, 60)
    with OTLPReceiver() as receiver:
        with running_server(
            tmp_path,
            wheel=metrics_wheel,
            receiver=receiver,
            threads=4,
            extra_env={"ALTIMETER_SAMPLE_RATE": sample_rate},
        ) as server:

            def request(duration):
                status, _, body = server.request(
                    f"/api/delay?seconds={duration}", timeout=80
                )
                assert status == 200
                assert json.loads(body) == {"seconds": duration}

            with ThreadPoolExecutor(max_workers=3) as clients:
                list(clients.map(request, seconds))
        points = _observations(receiver)
        assert sum(_totals(points, COUNT).values()) == 3
        assert _totals(points, COUNT) == _totals(points, DURATION)
        buckets = [0] * (len(BOUNDS) + 1)
        total_duration = 0.0
        for _, attributes, point in points[DURATION]:
            assert attributes == {
                "altimeter.surface": "django",
                "http.request.method": "GET",
                "http.route": "api/delay",
                "http.response.status_class": "2xx",
                "altimeter.http.outcome": "completed",
                "altimeter.response_mode": "buffered",
            }
            total_duration += point.sum
            for index, count in enumerate(point.bucket_counts):
                buckets[index] += count
        expected = [0] * (len(BOUNDS) + 1)
        for upper in (30, 45, 120):
            expected[BOUNDS.index(upper)] = 1
        assert buckets == expected
        assert 116 <= total_duration < 130
        if sample_rate == "1":
            assert len(_server_spans(receiver)) == 3
