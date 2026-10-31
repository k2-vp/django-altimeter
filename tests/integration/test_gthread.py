"""Real wheel-installed gunicorn gthread/DRF request and child lifetimes."""

import json
import socket
import struct
import time
from http.client import HTTPConnection

import pytest
from support.gunicorn_server import install_wheel, running_server
from support.otlp_receiver import OTLPReceiver

pytestmark = pytest.mark.integration


@pytest.fixture(scope="module")
def wheel(tmp_path_factory):
    return install_wheel(tmp_path_factory.mktemp("gthread-wheel"))


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


def test_real_drf_request_exports_one_owned_server_span(wheel, tmp_path):
    with OTLPReceiver() as receiver:
        with running_server(tmp_path, wheel=wheel, receiver=receiver) as server:
            status, _, body = server.request("/api/items/42?secret=query-canary")
            assert status == 200 and json.loads(body)["item_id"] == 42
        spans = _spans(receiver)
        assert len(spans) == 1, spans
        resource, span = spans[0]
        assert span.kind == 2
        assert _attributes(span.attributes)["http.route"] == "api/items/<int:item_id>"
        assert (
            _attributes(resource.attributes)["service.instance.id"]
            == json.loads(body)["instance_id"]
        )
        assert b"query-canary" not in b"".join(
            request.message.SerializeToString() for request in receiver.requests
        )


@pytest.mark.parametrize("mode", ["enabled", "disabled", "invalid"])
def test_native_response_behavior_and_sampling_ownership(mode, wheel, tmp_path):
    with OTLPReceiver() as receiver:
        with running_server(
            tmp_path,
            wheel=wheel,
            receiver=receiver,
            enabled=mode != "disabled",
            extra_env={"ALTIMETER_EXPORT_TIMEOUT": "-1"} if mode == "invalid" else {},
        ) as server:
            status, _, body = server.request("/api/items/7")
            assert status == 200 and json.loads(body)["item_id"] == 7
            status, headers, body = server.request("/api/file")
            assert status == 200 and body == b"reference-file\n" * 4096
            assert int(headers["Content-Length"]) == len(body)
            status, _, body = server.request("/api/rejected")
            assert status == 403 and json.loads(body) == {"rejected": True}
            assert server.request("/api/error")[0] == 500
            assert server.request("/private-path-canary")[0] == 404
        spans = _spans(receiver)
        if mode == "enabled":
            assert len(spans) == 5
            values = [_attributes(span.attributes) for _, span in spans]
            assert sorted(value["http.response.status_code"] for value in values) == [
                200,
                200,
                403,
                404,
                500,
            ]
            assert all(
                value["altimeter.http.outcome"] == "completed" for value in values
            )
            missing = next(
                value for value in values if value["http.response.status_code"] == 404
            )
            assert "http.route" not in missing
        else:
            assert receiver.requests == ()
        wire = b"".join(
            request.message.SerializeToString() for request in receiver.requests
        )
        assert b"canary" not in wire


@pytest.mark.parametrize("mode", ["enabled", "disabled", "invalid"])
def test_stream_and_disconnect_preserve_native_iteration(mode, wheel, tmp_path):
    with OTLPReceiver() as receiver:
        with running_server(
            tmp_path,
            wheel=wheel,
            receiver=receiver,
            enabled=mode != "disabled",
            extra_env={"ALTIMETER_EXPORT_TIMEOUT": "-1"} if mode == "invalid" else {},
        ) as server:
            connection = HTTPConnection("127.0.0.1", server.port, timeout=5)
            started = time.monotonic()
            connection.request("GET", "/api/stream")
            response = connection.getresponse()
            assert response.status == 200 and response.read(6) == b"first\n"
            assert time.monotonic() - started < 0.4
            assert response.read() == b"last\n"
            assert time.monotonic() - started >= 0.5
            connection.close()
            with socket.create_connection(
                ("127.0.0.1", server.port), timeout=5
            ) as connection:
                connection.sendall(
                    b"GET /api/abort HTTP/1.1\r\nHost: localhost\r\n\r\n"
                )
                received = b""
                while b"first\n" not in received:
                    chunk = connection.recv(4096)
                    assert chunk, received
                    received += chunk
                connection.setsockopt(
                    socket.SOL_SOCKET, socket.SO_LINGER, struct.pack("ii", 1, 0)
                )
            deadline = time.monotonic() + 5
            closed = []
            while time.monotonic() < deadline:
                path = tmp_path / "close.jsonl"
                closed = (
                    [json.loads(line) for line in path.read_text().splitlines()]
                    if path.exists()
                    else []
                )
                if len(closed) == 2:
                    break
                time.sleep(0.025)
            assert sorted(event["abort"] for event in closed) == [False, True]
            assert server.request("/api/items/8")[0] == 200
        if mode == "enabled":
            spans = _spans(receiver)
            assert len(spans) == 3
            values = [_attributes(span.attributes) for _, span in spans]
            streamed = [
                value
                for value in values
                if value.get("http.route") in {"api/stream", "api/abort"}
            ]
            assert len(streamed) == 2
            assert {value["altimeter.http.outcome"] for value in streamed} == {
                "completed",
                "aborted",
            }
            assert all(
                value["altimeter.response_mode"] == "streaming" for value in streamed
            )
            following = next(
                value
                for value in values
                if value.get("http.route") == "api/items/<int:item_id>"
            )
            assert following["altimeter.http.server.concurrent_at_start"] == 1
        else:
            assert receiver.requests == ()


def test_recycled_children_own_providers_and_drain(wheel, tmp_path):
    with OTLPReceiver() as receiver:
        with running_server(
            tmp_path, wheel=wheel, receiver=receiver, max_requests=3
        ) as server:
            served = []
            # The readiness request counts toward Gunicorn's max_requests.
            # Wait for the next child before opening another client connection;
            # gthread natively closes accepted connections during retirement.
            for item_id in range(2):
                status, _, body = server.request(f"/api/items/{item_id}")
                assert status == 200
                served.append(json.loads(body))
            deadline = time.monotonic() + 8
            while time.monotonic() < deadline:
                if sum(event["event"] == "startup" for event in server.lifecycle) >= 2:
                    break
                time.sleep(0.025)
            assert sum(event["event"] == "startup" for event in server.lifecycle) >= 2
            status, _, body = server.request("/api/items/2")
            assert status == 200
            served.append(json.loads(body))
            assert len({item["pid"] for item in served}) == 2
        startups = [event for event in server.lifecycle if event["event"] == "startup"]
        shutdowns = [
            event for event in server.lifecycle if event["event"] == "shutdown"
        ]
        assert len(startups) >= 2
        assert len({event["instance_id"] for event in startups}) == len(startups)
        assert all(
            event["instance_id"] is not None and event["codes"] == []
            for event in startups
        )
        assert {event["pid"] for event in startups} == {
            event["pid"] for event in shutdowns
        }
        assert all(
            event["elapsed"] <= 2.2 and event["completed"] for event in shutdowns
        )
        assert server.process.pid not in {event["pid"] for event in startups}
        assert all(
            item["instance_id"]
            == next(
                event["instance_id"]
                for event in startups
                if event["pid"] == item["pid"]
            )
            for item in served
        )
        assert len(_spans(receiver)) == len(served)
        assert {
            _attributes(resource.attributes)["service.instance.id"]
            for resource, _ in _spans(receiver)
        } == {item["instance_id"] for item in served}
        assert (
            server.supervisor["setup"]
            == server.supervisor["exporters"]
            == server.supervisor["collector_connect"]
            == []
        )
        assert server.supervisor["profile_events"] > 0


def test_preload_is_diagnosed_without_parent_runtime(wheel, tmp_path):
    with OTLPReceiver() as receiver:
        with running_server(
            tmp_path, wheel=wheel, receiver=receiver, preload=True
        ) as server:
            status, _, body = server.request("/api/items/9")
            assert status == 200 and json.loads(body)["item_id"] == 9
            assert json.loads(body)["instance_id"] is None
        assert "setup.preload_unsupported" in server.output
        assert receiver.requests == ()
        assert (
            server.supervisor["setup"]
            == server.supervisor["exporters"]
            == server.supervisor["collector_connect"]
            == []
        )


def test_hanging_export_respects_worker_exit_budget(wheel, tmp_path):
    with OTLPReceiver() as receiver:
        for _ in range(32):
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
            assert time.monotonic() - started < 4.0
        shutdowns = [
            event for event in server.lifecycle if event["event"] == "shutdown"
        ]
        assert len(shutdowns) == 1 and shutdowns[0]["elapsed"] <= 2.2
        assert _spans(receiver)
