"""Real wheel-installed uvicorn/Ninja behavior and serving-process ownership."""

import json
import time
from http.client import HTTPConnection

import pytest
from support.otlp_receiver import OTLPReceiver
from support.uvicorn_server import install_wheel, running_server

pytestmark = pytest.mark.integration


@pytest.fixture(scope="module")
def wheel(tmp_path_factory):
    return install_wheel(tmp_path_factory.mktemp("uvicorn-wheel"))


def _spans(receiver):
    return [
        (resource.resource, span)
        for request in receiver.requests
        if request.path == "/v1/traces"
        for resource in request.message.resource_spans
        for scope in resource.scope_spans
        for span in scope.spans
    ]


def _attributes(values):
    return {
        value.key: getattr(value.value, value.value.WhichOneof("value"))
        for value in values
    }


def test_real_ninja_request_exports_one_server_span(wheel, tmp_path):
    with OTLPReceiver() as receiver:
        with running_server(tmp_path, wheel=wheel, receiver=receiver) as server:
            status, _, body = server.request("/api/items/42?secret=query-canary")
            assert status == 200 and json.loads(body)["item_id"] == 42
            assert json.loads(body)["instance_id"] is not None
            assert server.lifecycle[0]["codes"] == []
        spans = _spans(receiver)
        assert len(spans) == 1, [str(span) for _, span in spans]
        resource, span = spans[0]
        assert span.kind == 2
        assert _attributes(span.attributes)["http.route"] == "api/items/<item_id>"
        assert (
            _attributes(resource.attributes)["service.instance.id"]
            == json.loads(body)["instance_id"]
        )
        assert "query-canary" not in str(receiver.requests)


def test_spawn_workers_own_setup_and_export_not_supervisor(wheel, tmp_path):
    with OTLPReceiver() as receiver:
        with running_server(
            tmp_path,
            wheel=wheel,
            receiver=receiver,
            workers=2,
        ) as server:
            deadline = time.monotonic() + 10
            while len(server.lifecycle) < 2 and time.monotonic() < deadline:
                time.sleep(0.025)
            startups = server.lifecycle
            assert len(startups) == 2
            assert all(event["event"] == "startup" for event in startups)
            assert all(event["codes"] == [] for event in startups)
            assert all(event["elapsed"] <= 2.2 for event in startups)
            child_pids = {event["pid"] for event in startups}
            instance_ids = {event["instance_id"] for event in startups}
            assert len(child_pids) == len(instance_ids) == 2
            assert server.process.pid not in child_pids
            assert None not in instance_ids
            served = set()
            requests = 0
            for item_id in range(200):
                status, _, body = server.request(f"/api/items/{item_id}")
                assert status == 200
                response = json.loads(body)
                assert response["item_id"] == item_id
                assert response["pid"] in child_pids
                assert response["instance_id"] in instance_ids
                served.add(response["pid"])
                requests += 1
                if served == child_pids:
                    break
            assert served == child_pids
        assert server.supervisor["setup"] == []
        assert server.supervisor["exporters"] == []
        assert server.supervisor["collector_connect"] == []
        assert server.supervisor["profile_events"] > 0
        shutdowns = [
            event for event in server.lifecycle if event["event"] == "shutdown"
        ]
        assert {event["pid"] for event in shutdowns} == child_pids
        assert len(shutdowns) == 2
        assert all(event["elapsed"] <= 2.2 for event in shutdowns)
        spans = _spans(receiver)
        assert len(spans) == requests
        assert all(span.kind == 2 for _, span in spans)
        assert {
            _attributes(resource.attributes)["service.instance.id"]
            for resource, _ in spans
        } == instance_ids


def test_disabled_server_works_without_export(wheel, tmp_path):
    with OTLPReceiver() as receiver:
        with running_server(
            tmp_path,
            wheel=wheel,
            receiver=receiver,
            enabled=False,
        ) as server:
            status, headers, body = server.request("/api/items/7")
            assert status == 200 and json.loads(body)["item_id"] == 7
            assert json.loads(body)["instance_id"] is None
            assert "traceparent" not in {name.lower() for name in headers}
        assert receiver.requests == ()


def test_error_rejection_and_missing_route_keep_http_behavior(wheel, tmp_path):
    with OTLPReceiver() as receiver:
        with running_server(tmp_path, wheel=wheel, receiver=receiver) as server:
            assert server.request("/api/items/1")[0] == 200
            rejected = server.request("/api/rejected")
            assert rejected[0] == 403 and json.loads(rejected[2])["rejected"] is True
            assert server.request("/api/error")[0] == 500
            assert server.request("/unknown-object-secret-canary")[0] == 404
        spans = _spans(receiver)
        assert len(spans) == 4
        attributes = [_attributes(span.attributes) for _, span in spans]
        assert sorted(item["http.response.status_code"] for item in attributes) == [
            200,
            403,
            404,
            500,
        ]
        missing = next(
            item for item in attributes if item["http.response.status_code"] == 404
        )
        assert "http.route" not in missing
        assert "unknown-object-secret-canary" not in str(receiver.requests)
        assert "reference-error-canary" not in str(receiver.requests)


def test_stream_is_not_buffered_and_disconnect_is_aborted(wheel, tmp_path):
    with OTLPReceiver() as receiver:
        with running_server(tmp_path, wheel=wheel, receiver=receiver) as server:
            connection = HTTPConnection("127.0.0.1", server.port, timeout=5)
            started = time.monotonic()
            connection.request("GET", "/api/stream")
            response = connection.getresponse()
            assert response.status == 200
            assert response.read(6) == b"first\n"
            assert time.monotonic() - started < 0.15
            assert response.read() == b"last\n"
            assert time.monotonic() - started >= 0.2
            connection.close()
            connection = HTTPConnection("127.0.0.1", server.port, timeout=5)
            connection.request("GET", "/api/abort")
            response = connection.getresponse()
            assert response.status == 200 and response.read(6) == b"first\n"
            response.close()
            connection.close()
            deadline = time.monotonic() + 3
            while len(_spans(receiver)) < 2 and time.monotonic() < deadline:
                time.sleep(0.025)
        spans = _spans(receiver)
        assert len(spans) == 2
        attributes = [_attributes(span.attributes) for _, span in spans]
        assert {item["altimeter.http.outcome"] for item in attributes} == {
            "completed",
            "aborted",
        }
        assert all(item["http.response.status_code"] == 200 for item in attributes)
        assert all(
            item["altimeter.response_mode"] == "streaming" for item in attributes
        )


def test_hanging_export_drain_has_one_shared_lifespan_budget(wheel, tmp_path):
    with OTLPReceiver() as receiver:
        for _ in range(8):
            receiver.queue_response(hang=True)
        with running_server(
            tmp_path,
            wheel=wheel,
            receiver=receiver,
            extra_env={"ALTIMETER_EXPORT_TIMEOUT": "2"},
        ) as server:
            assert server.request("/api/items/1")[0] == 200
            receiver.wait_for_requests()
            started = time.monotonic()
            server.stop()
            assert time.monotonic() - started < 3.0
        shutdown = [event for event in server.lifecycle if event["event"] == "shutdown"]
        assert len(shutdown) == 1
        assert shutdown[0]["elapsed"] <= 2.2
        assert _spans(receiver)


def test_real_ninja_overflow_preserves_request_denominators(wheel, tmp_path):
    methods = ("GET", "POST", "PUT", "PATCH", "DELETE", "HEAD", "OPTIONS", "TRACE")
    with OTLPReceiver() as receiver:
        with running_server(tmp_path, wheel=wheel, receiver=receiver) as server:
            assert server.request("/api/items/1")[0] == 200
            for index in range(100):
                for method in methods:
                    for status in (200, 404, 503):
                        actual, _, _ = server.request(
                            f"/api/series/{index}?status={status}",
                            method=method,
                        )
                        assert actual == status, (index, method, status, actual)
        assert len(_spans(receiver)) == 2401
        points = {
            "altimeter.http.server.requests": [],
            "http.server.request.duration": [],
        }
        for request in receiver.requests:
            if request.path != "/v1/metrics":
                continue
            point_count = sum(
                len(metric.sum.data_points)
                if metric.HasField("sum")
                else len(metric.histogram.data_points)
                for resource in request.message.resource_metrics
                for scope in resource.scope_metrics
                for metric in scope.metrics
            )
            assert 0 < point_count <= 128
            for resource in request.message.resource_metrics:
                for scope in resource.scope_metrics:
                    for metric in scope.metrics:
                        if metric.name in points:
                            assert (
                                metric.sum.aggregation_temporality
                                if metric.HasField("sum")
                                else metric.histogram.aggregation_temporality
                            ) == 1
                            for point in (
                                metric.sum.data_points
                                if metric.HasField("sum")
                                else metric.histogram.data_points
                            ):
                                points[metric.name].append(
                                    (
                                        _attributes(point.attributes),
                                        point.as_int
                                        if metric.HasField("sum")
                                        else point.count,
                                    )
                                )
        for name, observations in points.items():
            assert sum(count for _, count in observations) == 2401, name
            assert {
                status: sum(
                    count
                    for attrs, count in observations
                    if attrs["http.response.status_class"] == status
                )
                for status in ("2xx", "4xx", "5xx")
            } == {"2xx": 801, "4xx": 800, "5xx": 800}
            distinct = {tuple(sorted(attrs.items())) for attrs, _ in observations}
            assert 1928 <= len(distinct) <= 2000
            overflow = [
                attrs for attrs, _ in observations if attrs.get("altimeter.overflow")
            ]
            assert overflow, name
            assert all(
                "http.route" not in attrs and "http.request.method" not in attrs
                for attrs in overflow
            )
            assert {attrs["http.response.status_class"] for attrs in overflow} == {
                "2xx",
                "4xx",
                "5xx",
            }
            assert all(
                attrs["altimeter.http.outcome"] == "completed" for attrs in overflow
            )
            assert all(attrs["altimeter.surface"] == "django" for attrs in overflow)
            assert all(
                attrs["altimeter.response_mode"] == "buffered" for attrs in overflow
            )
