"""Exercise the decoding receiver over an actual loopback HTTP connection."""

from http.client import HTTPConnection
from urllib.parse import urlsplit

import pytest
from google.protobuf.message import Message
from opentelemetry.proto.collector.logs.v1.logs_service_pb2 import (
    ExportLogsServiceRequest,
    ExportLogsServiceResponse,
)
from opentelemetry.proto.collector.metrics.v1.metrics_service_pb2 import (
    ExportMetricsServiceRequest,
    ExportMetricsServiceResponse,
)
from opentelemetry.proto.collector.trace.v1.trace_service_pb2 import (
    ExportTraceServiceRequest,
    ExportTraceServiceResponse,
)
from opentelemetry.proto.logs.v1.logs_pb2 import SEVERITY_NUMBER_INFO

from support.otlp_receiver import OTLPReceiver, Signal


def _post(receiver: OTLPReceiver, path: str, body: bytes) -> tuple[int, str, bytes]:
    address = urlsplit(receiver.endpoint)
    assert address.hostname == "127.0.0.1"
    assert address.port is not None
    connection = HTTPConnection(address.hostname, address.port, timeout=2)
    try:
        connection.request(
            "POST",
            path,
            body=body,
            headers={
                "Content-Type": "application/x-protobuf",
                "X-Receiver-Test": "safe-example",
            },
        )
        response = connection.getresponse()
        return response.status, response.getheader("Content-Type", ""), response.read()
    finally:
        connection.close()


def _trace_request() -> ExportTraceServiceRequest:
    request = ExportTraceServiceRequest()
    span = request.resource_spans.add().scope_spans.add().spans.add()
    span.name = "GET /health"
    span.trace_id = b"\x01" * 16
    span.span_id = b"\x02" * 8
    return request


def _log_request() -> ExportLogsServiceRequest:
    request = ExportLogsServiceRequest()
    record = request.resource_logs.add().scope_logs.add().log_records.add()
    record.body.string_value = "request.complete"
    record.severity_number = SEVERITY_NUMBER_INFO
    return request


def _metric_request() -> ExportMetricsServiceRequest:
    request = ExportMetricsServiceRequest()
    metric = request.resource_metrics.add().scope_metrics.add().metrics.add()
    metric.name = "http.server.request.duration"
    metric.unit = "s"
    metric.gauge.data_points.add(as_double=0.125)
    return request


@pytest.mark.parametrize(
    ("signal", "payload", "response_type"),
    [
        ("traces", _trace_request(), ExportTraceServiceResponse),
        ("logs", _log_request(), ExportLogsServiceResponse),
        ("metrics", _metric_request(), ExportMetricsServiceResponse),
    ],
)
def test_decodes_protobuf_and_captures_headers(
    signal: str, payload: Message, response_type: type[Message]
) -> None:
    with OTLPReceiver() as receiver:
        status, content_type, body = _post(
            receiver, f"/v1/{signal}", payload.SerializeToString()
        )
        assert status == 200
        assert content_type == "application/x-protobuf"
        response = response_type.FromString(body)
        assert response.ListFields() == []

        captured = receiver.wait_for_requests(count=1, timeout=2)
        assert len(captured) == 1
        assert captured[0].path == f"/v1/{signal}"
        assert type(captured[0].message) is type(payload)
        assert captured[0].message == payload
        assert captured[0].headers["x-receiver-test"] == "safe-example"
        assert captured[0].headers["CONTENT-TYPE"] == "application/x-protobuf"
        assert receiver.requests == captured


@pytest.mark.parametrize(
    ("signal", "payload"),
    [
        ("traces", ExportTraceServiceRequest()),
        ("logs", ExportLogsServiceRequest()),
        ("metrics", ExportMetricsServiceRequest()),
    ],
)
def test_empty_request_is_decoded_and_accepted(signal: str, payload: Message) -> None:
    with OTLPReceiver() as receiver:
        status, _, body = _post(receiver, f"/v1/{signal}", payload.SerializeToString())
        assert status == 200
        assert body == b""
        assert receiver.requests[0].message == payload


def test_captures_all_signals_in_order_without_duplicate_records() -> None:
    with OTLPReceiver() as receiver:
        snapshot = receiver.requests
        payloads: tuple[tuple[Signal, Message], ...] = (
            ("traces", _trace_request()),
            ("logs", _log_request()),
            ("metrics", _metric_request()),
        )
        for signal, payload in payloads:
            assert receiver.url(signal) == f"{receiver.endpoint}/v1/{signal}"
            assert (
                _post(receiver, f"/v1/{signal}", payload.SerializeToString())[0] == 200
            )
        captured = receiver.wait_for_requests(count=3, timeout=2)
        assert snapshot == ()
        assert [item.path for item in captured] == [
            "/v1/traces",
            "/v1/logs",
            "/v1/metrics",
        ]
        assert captured[0].message == _trace_request()
        assert captured[1].message == _log_request()
        assert captured[2].message == _metric_request()


def test_waiter_receives_export_and_missing_export_times_out() -> None:
    from concurrent.futures import ThreadPoolExecutor

    with OTLPReceiver() as receiver, ThreadPoolExecutor(max_workers=1) as executor:
        waiting = executor.submit(receiver.wait_for_requests, 1, 2)
        assert (
            _post(receiver, "/v1/traces", _trace_request().SerializeToString())[0]
            == 200
        )
        assert waiting.result(timeout=2)[0].message == _trace_request()
        with pytest.raises(TimeoutError, match="Expected 2 OTLP requests; received 1"):
            receiver.wait_for_requests(count=2, timeout=0.01)


@pytest.mark.parametrize(
    ("path", "body", "expected_status"),
    [("/unknown", b"", 404), ("/v1/traces", b"\xff", 400)],
)
def test_invalid_request_is_not_captured(
    path: str, body: bytes, expected_status: int
) -> None:
    with OTLPReceiver() as receiver:
        assert _post(receiver, path, body)[0] == expected_status
        assert receiver.requests == ()
        assert (
            _post(receiver, "/v1/traces", _trace_request().SerializeToString())[0]
            == 200
        )
        assert receiver.wait_for_requests(timeout=2)[0].message == _trace_request()


def test_context_releases_socket_and_server_thread_even_on_failure() -> None:
    from socket import create_connection
    from threading import enumerate as enumerate_threads

    threads_before = set(enumerate_threads())
    with pytest.raises(RuntimeError, match="test failure"), OTLPReceiver() as receiver:
        address = urlsplit(receiver.endpoint)
        assert address.hostname == "127.0.0.1"
        assert address.port is not None and address.port > 0
        assert _post(receiver, "/v1/logs", _log_request().SerializeToString())[0] == 200
        raise RuntimeError("test failure")
    assert set(enumerate_threads()) == threads_before
    with pytest.raises(OSError):
        with create_connection((address.hostname, address.port), timeout=0.2):
            pytest.fail("Receiver socket remained open after teardown")


def test_teardown_interrupts_a_slow_trickle_request_body() -> None:
    from concurrent.futures import ThreadPoolExecutor
    from threading import Event
    from threading import enumerate as enumerate_threads

    threads_before = set(enumerate_threads())
    receiver = OTLPReceiver().__enter__()
    address = urlsplit(receiver.endpoint)
    assert address.hostname == "127.0.0.1"
    assert address.port is not None
    connection = HTTPConnection(address.hostname, address.port, timeout=2)
    stop_sending = Event()
    trickling = Event()
    payload = _trace_request()
    payload.resource_spans[0].scope_spans[0].spans[0].name = "x" * 256
    body = payload.SerializeToString()

    def send_slowly() -> int:
        sent = 0
        for byte in body:
            try:
                connection.send(bytes((byte,)))
            except OSError:
                break  # Teardown may interrupt the connection mid-body.
            sent += 1
            if sent == 5:
                trickling.set()
            if stop_sending.wait(0.05):
                break
        return sent

    try:
        # A complete request still decodes before the incomplete client arrives.
        assert (
            _post(receiver, "/v1/traces", _trace_request().SerializeToString())[0]
            == 200
        )
        assert receiver.requests[0].message == _trace_request()
        connection.putrequest("POST", "/v1/traces")
        connection.putheader("Content-Type", "application/x-protobuf")
        connection.putheader("Content-Length", str(len(body)))
        connection.endheaders()
        with ThreadPoolExecutor(max_workers=2) as executor:
            sender = executor.submit(send_slowly)
            try:
                assert trickling.wait(timeout=2)
                assert not sender.done()
                teardown = executor.submit(receiver.__exit__, None, None, None)
                try:
                    # Longer than the receiver's idle timeout: ongoing bytes must
                    # not keep fixture teardown alive by renewing that timeout.
                    teardown.result(timeout=3)
                finally:
                    # A regressed receiver must fail the deadline without hanging
                    # pytest: finish the valid body to release its blocked read.
                    stop_sending.set()
                    sent = sender.result(timeout=2)
                    try:
                        connection.send(body[sent:])
                    except OSError:
                        pass  # The fixed receiver has already closed the client.
                    teardown.result(timeout=3)
            finally:
                stop_sending.set()
        assert len(receiver.requests) == 1
    finally:
        connection.close()
        receiver.__exit__(None, None, None)
    assert set(enumerate_threads()) == threads_before


def _payload(signal: Signal) -> Message:
    if signal == "traces":
        return _trace_request()
    if signal == "logs":
        return _log_request()
    return _metric_request()


def _open_post(
    receiver: OTLPReceiver, signal: Signal, *, timeout: float = 2
) -> HTTPConnection:
    address = urlsplit(receiver.endpoint)
    assert address.hostname is not None and address.port is not None
    connection = HTTPConnection(address.hostname, address.port, timeout=timeout)
    connection.request(
        "POST",
        f"/v1/{signal}",
        body=_payload(signal).SerializeToString(),
        headers={"Content-Type": "application/x-protobuf"},
    )
    return connection


@pytest.mark.parametrize("signal", ["traces", "logs", "metrics"])
@pytest.mark.parametrize(
    ("status", "body", "headers", "reason", "send_content_length"),
    [
        pytest.param(200, b"", {}, None, True, id="empty-success"),
        pytest.param(307, b"", {"Location": "/redirected"}, None, True, id="redirect"),
        pytest.param(200, b"\xff", {}, None, True, id="malformed"),
        pytest.param(
            200, b"", {"Content-Type": "text/plain"}, None, True, id="wrong-type"
        ),
        pytest.param(202, b"", {}, None, True, id="unexpected-202"),
        pytest.param(204, b"", {}, None, True, id="unexpected-204"),
        pytest.param(200, b"x" * 65537, {}, None, True, id="oversized"),
        pytest.param(200, b"x" * 65537, {}, None, False, id="oversized-close-framing"),
        pytest.param(
            400,
            b"private-body-canary",
            {},
            "private-reason-canary",
            True,
            id="malicious-prose",
        ),
        pytest.param(
            429, b"private-error", {"Retry-After": "0"}, None, True, id="retryable-429"
        ),
        pytest.param(
            503, b"private-error", {"Retry-After": "1"}, None, True, id="retryable-503"
        ),
    ],
)
def test_scripted_wire_responses_preserve_decoding_and_default_recovery(
    signal: Signal,
    status: int,
    body: bytes,
    headers: dict[str, str],
    reason: str | None,
    send_content_length: bool,
) -> None:
    with OTLPReceiver() as receiver:
        receipt = receiver.queue_response(
            status=status,
            body=body,
            headers=headers,
            reason=reason,
            send_content_length=send_content_length,
        )
        connection = _open_post(receiver, signal)
        try:
            response = connection.getresponse()
            assert response.status == status
            assert response.read() == body
            assert response.getheader("Content-Type") == headers.get(
                "Content-Type", "application/x-protobuf"
            )
            for key, value in headers.items():
                assert response.getheader(key) == value
            if reason is not None:
                assert response.reason == reason
            assert response.getheader("Content-Length") == (
                str(len(body)) if send_content_length else None
            )
        finally:
            connection.close()
        assert receipt.finished.wait(timeout=1)
        assert receipt.headers_sent.is_set()
        assert receipt.body_started.is_set() == bool(body)
        assert receiver.requests[0].message == _payload(signal)
        assert receiver.requests[0].headers["Content-Type"] == "application/x-protobuf"
        assert _post(
            receiver, f"/v1/{signal}", _payload(signal).SerializeToString()
        ) == (200, "application/x-protobuf", b"")
        assert len(receiver.requests) == 2


@pytest.mark.parametrize(
    ("signal", "response_type", "rejected_field"),
    [
        ("traces", ExportTraceServiceResponse, "rejected_spans"),
        ("logs", ExportLogsServiceResponse, "rejected_log_records"),
        ("metrics", ExportMetricsServiceResponse, "rejected_data_points"),
    ],
)
@pytest.mark.parametrize("rejected", [0, 2], ids=["warning", "partial-rejection"])
def test_scripted_partial_success_uses_signal_protobuf(
    signal: Signal,
    response_type: type[
        ExportTraceServiceResponse
        | ExportLogsServiceResponse
        | ExportMetricsServiceResponse
    ],
    rejected_field: str,
    rejected: int,
) -> None:
    expected = response_type()
    partial = expected.partial_success
    setattr(partial, rejected_field, rejected)
    partial.error_message = "private-partial-message"
    with OTLPReceiver() as receiver:
        receiver.queue_response(body=expected.SerializeToString())
        status, content_type, body = _post(
            receiver, f"/v1/{signal}", _payload(signal).SerializeToString()
        )
        assert status == 200 and content_type == "application/x-protobuf"
        assert response_type.FromString(body) == expected
        assert receiver.requests[0].message == _payload(signal)
        assert (
            _post(receiver, f"/v1/{signal}", _payload(signal).SerializeToString())[2]
            == b""
        )
        assert len(receiver.requests) == 2


def test_response_scripts_are_fifo_across_signals_and_ignore_invalid_requests() -> None:
    with OTLPReceiver() as receiver:
        first = receiver.queue_response(status=429, headers={"Retry-After": "0"})
        second = receiver.queue_response(status=503, headers={"Retry-After": "0"})
        assert _post(receiver, "/v1/traces", b"\xff")[0] == 400
        assert _post(receiver, "/unknown", b"")[0] == 404
        assert not first.finished.is_set() and not second.finished.is_set()
        assert _post(receiver, "/v1/logs", _log_request().SerializeToString())[0] == 429
        assert (
            _post(receiver, "/v1/metrics", _metric_request().SerializeToString())[0]
            == 503
        )
        assert (
            _post(receiver, "/v1/traces", _trace_request().SerializeToString())[0]
            == 200
        )
        assert [request.path for request in receiver.requests] == [
            "/v1/logs",
            "/v1/metrics",
            "/v1/traces",
        ]


@pytest.mark.parametrize("signal", ["traces", "logs", "metrics"])
def test_scripted_gzip_response_is_sent_as_encoded_bytes(signal: Signal) -> None:
    from gzip import compress, decompress

    expected = compress(b"", mtime=0)
    with OTLPReceiver() as receiver:
        receipt = receiver.queue_response(
            body=expected, headers={"Content-Encoding": "gzip"}
        )
        connection = _open_post(receiver, signal)
        try:
            response = connection.getresponse()
            assert response.status == 200
            assert response.getheader("Content-Encoding") == "gzip"
            body = response.read()
            assert body == expected and decompress(body) == b""
        finally:
            connection.close()
        assert receipt.finished.wait(timeout=1)
        assert receipt.body_started.is_set()
        assert receiver.requests[0].message == _payload(signal)
        assert (
            _post(receiver, f"/v1/{signal}", _payload(signal).SerializeToString())[2]
            == b""
        )


@pytest.mark.parametrize("signal", ["traces", "logs", "metrics"])
@pytest.mark.parametrize("hang", [False, True], ids=["delayed-headers", "hang"])
def test_unresponsive_script_captures_request_and_detects_client_close(
    signal: Signal, hang: bool
) -> None:
    with OTLPReceiver() as receiver:
        receipt = receiver.queue_response(hang=hang, header_delay=10)
        connection = _open_post(receiver, signal, timeout=0.1)
        try:
            assert receiver.wait_for_requests(timeout=1)[0].message == _payload(signal)
            with pytest.raises(TimeoutError):
                connection.getresponse()
            assert not receipt.headers_sent.is_set()
            assert not receipt.body_started.is_set()
        finally:
            connection.close()
        assert receipt.client_closed.wait(timeout=1)
        assert receipt.finished.wait(timeout=1)
        assert (
            _post(receiver, f"/v1/{signal}", _payload(signal).SerializeToString())[0]
            == 200
        )
        assert len(receiver.requests) == 2


@pytest.mark.parametrize("signal", ["traces", "logs", "metrics"])
@pytest.mark.parametrize("status", [200, 429, 503])
def test_delayed_body_observes_close_before_any_body_write(
    signal: Signal, status: int
) -> None:
    with OTLPReceiver() as receiver:
        receipt = receiver.queue_response(
            status=status,
            body=b"private-body-canary",
            headers={"Content-Encoding": "gzip", "Retry-After": "0"},
            body_delay=10,
        )
        connection = _open_post(receiver, signal)
        try:
            response = connection.getresponse()
            assert response.status == status
            assert response.getheader("Content-Encoding") == "gzip"
            assert response.getheader("Retry-After") == "0"
            assert receipt.headers_sent.wait(timeout=1)
            response.close()
        finally:
            connection.close()
        assert receipt.client_closed.wait(timeout=1)
        assert receipt.finished.wait(timeout=1)
        assert not receipt.body_started.is_set()
        assert receiver.requests[0].message == _payload(signal)
        assert (
            _post(receiver, f"/v1/{signal}", _payload(signal).SerializeToString())[0]
            == 200
        )


@pytest.mark.parametrize("signal", ["traces", "logs", "metrics"])
def test_scripted_slow_body_sends_first_chunk_then_waits(signal: Signal) -> None:
    # The field layout is shared by all three generated partial-success replies.
    body = ExportTraceServiceResponse()
    body.partial_success.error_message = "warning"
    encoded = body.SerializeToString()
    with OTLPReceiver() as receiver:
        receipt = receiver.queue_response(body=encoded, chunk_size=1, chunk_delay=10)
        connection = _open_post(receiver, signal, timeout=0.1)
        try:
            response = connection.getresponse()
            assert response.status == 200
            assert response.read(1) == encoded[:1]
            assert receipt.body_started.is_set()
            with pytest.raises(TimeoutError):
                response.read(1)
            response.close()
        finally:
            connection.close()
        assert receipt.client_closed.wait(timeout=1)
        assert receipt.finished.wait(timeout=1)
        assert receiver.requests[0].message == _payload(signal)
        assert (
            _post(receiver, f"/v1/{signal}", _payload(signal).SerializeToString())[2]
            == b""
        )


@pytest.mark.parametrize("mode", ["headers", "body", "slow", "hang"])
def test_teardown_interrupts_scripted_response_waits(mode: str) -> None:
    from concurrent.futures import ThreadPoolExecutor
    from threading import enumerate as enumerate_threads

    threads_before = set(enumerate_threads())
    receiver = OTLPReceiver().__enter__()
    assert _post(receiver, "/v1/traces", _trace_request().SerializeToString())[0] == 200
    receipt = receiver.queue_response(
        body=b"abcdef",
        header_delay=30 if mode == "headers" else 0,
        body_delay=30 if mode == "body" else 0,
        chunk_delay=30 if mode == "slow" else 0,
        chunk_size=1,
        hang=mode == "hang",
    )
    connection = _open_post(receiver, "logs")
    try:
        assert (
            receiver.wait_for_requests(count=2, timeout=1)[1].message == _log_request()
        )
        if mode in {"body", "slow"}:
            assert receipt.headers_sent.wait(timeout=1)
        if mode == "slow":
            assert receipt.body_started.wait(timeout=1)
        with ThreadPoolExecutor(max_workers=1) as executor:
            teardown = executor.submit(receiver.__exit__, None, None, None)
            try:
                teardown.result(timeout=1)
            finally:
                connection.close()
                teardown.result(timeout=2)
        assert receipt.finished.is_set()
        assert (
            not receipt.client_closed.is_set()
        )  # Local shutdown is not peer EOF evidence.
    finally:
        connection.close()
        receiver.__exit__(None, None, None)
    assert set(enumerate_threads()) == threads_before
