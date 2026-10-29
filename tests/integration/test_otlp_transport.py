"""Real native exporters and decoded loopback HTTP; not live ingestion proof."""

import pytest
from opentelemetry._logs import LogRecord, SeverityNumber
from opentelemetry.sdk._logs import ReadableLogRecord
from opentelemetry.sdk.metrics.export import (
    Gauge,
    Metric,
    MetricsData,
    NumberDataPoint,
    ResourceMetrics,
    ScopeMetrics,
)
from opentelemetry.sdk.resources import Resource
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.util.instrumentation import InstrumentationScope

from django_altimeter.exporting import (
    SanitizingSession,
    create_log_exporter,
    create_metric_exporter,
    create_span_exporter,
)
from django_altimeter.privacy import PrivacyPolicy

pytestmark = pytest.mark.integration
SIGNALS = ("traces", "logs", "metrics")
CANARY = "password=transport-canary"
POLICY = PrivacyPolicy(
    log_bodies=frozenset({"request.complete"}),
    attribute_values=frozenset(
        {("service.name", "transport-test"), ("http.route", "/boards/{board_id}")}
    ),
    metric_instruments=frozenset({("django_altimeter", "http.server.requests")}),
)


def _payload(signal):
    resource = Resource({"service.name": "transport-test", "password": CANARY})
    attributes = {
        "http.route": "/boards/{board_id}",
        "http.response.status_code": 200,
        "password": CANARY,
    }
    scope = InstrumentationScope("django_altimeter")
    if signal == "traces":
        provider = TracerProvider(resource=resource, shutdown_on_exit=False)
        span = provider.get_tracer(scope.name).start_span("GET", attributes=attributes)
        span.end()
        provider.shutdown()
        return (span,)
    if signal == "logs":
        return (
            ReadableLogRecord(
                LogRecord(
                    timestamp=1,
                    observed_timestamp=2,
                    body="request.complete",
                    severity_number=SeverityNumber.INFO,
                    trace_id=1,
                    span_id=2,
                    trace_flags=1,
                    attributes=attributes,
                ),
                resource=resource,
                instrumentation_scope=scope,
            ),
        )
    return MetricsData(
        resource_metrics=(
            ResourceMetrics(
                resource=resource,
                scope_metrics=(
                    ScopeMetrics(
                        scope=scope,
                        metrics=(
                            Metric(
                                name="http.server.requests",
                                description=CANARY,
                                unit="1",
                                data=Gauge(
                                    data_points=(
                                        NumberDataPoint(
                                            attributes=attributes,
                                            start_time_unix_nano=1,
                                            time_unix_nano=2,
                                            value=7,
                                        ),
                                    )
                                ),
                            ),
                        ),
                        schema_url="",
                    ),
                ),
                schema_url="",
            ),
        )
    )


def _exporter(signal, receiver, timeout=2):
    session = SanitizingSession(signal, receiver.url(signal), policy=POLICY)
    factory = {
        "traces": create_span_exporter,
        "logs": create_log_exporter,
        "metrics": create_metric_exporter,
    }[signal]
    return factory(session, timeout=timeout), session


def _records(message, signal):
    if signal == "traces":
        return [
            s for r in message.resource_spans for g in r.scope_spans for s in g.spans
        ]
    if signal == "logs":
        return [
            s
            for r in message.resource_logs
            for g in r.scope_logs
            for s in g.log_records
        ]
    return [
        p
        for r in message.resource_metrics
        for g in r.scope_metrics
        for m in g.metrics
        for p in m.gauge.data_points
    ]


@pytest.mark.parametrize("signal", SIGNALS)
def test_native_exporter_sends_only_projected_safe_payload(signal, otlp_receiver):
    exporter, session = _exporter(signal, otlp_receiver)
    try:
        assert exporter.export(_payload(signal)).name == "SUCCESS"
    finally:
        exporter.shutdown()
    assert session.outcome.code == "export.accepted"
    assert session.outcome.accepted_records == 1
    requests = otlp_receiver.wait_for_requests()
    assert [r.path for r in requests] == [f"/v1/{signal}"]
    request = requests[0]
    records = _records(request.message, signal)
    assert len(records) == 1
    attributes = {a.key: a.value for a in records[0].attributes}
    assert attributes["http.route"].string_value == "/boards/{board_id}"
    assert attributes["http.response.status_code"].int_value == 200
    assert CANARY.encode() not in request.message.SerializeToString()
    assert request.headers.get("Content-Encoding", "identity") == "identity"
    assert request.headers["Accept-Encoding"] == "identity"
    if signal == "traces":
        assert records[0].name == "GET"
        assert records[0].trace_id and records[0].span_id
    elif signal == "logs":
        assert records[0].body.string_value == "request.complete"
    else:
        assert records[0].as_int == 7


def _response_body(signal, rejected=0, warning=""):
    from opentelemetry.proto.collector.logs.v1.logs_service_pb2 import (
        ExportLogsServiceResponse,
    )
    from opentelemetry.proto.collector.metrics.v1.metrics_service_pb2 import (
        ExportMetricsServiceResponse,
    )
    from opentelemetry.proto.collector.trace.v1.trace_service_pb2 import (
        ExportTraceServiceResponse,
    )

    response_type, field = {
        "traces": (ExportTraceServiceResponse, "rejected_spans"),
        "logs": (ExportLogsServiceResponse, "rejected_log_records"),
        "metrics": (ExportMetricsServiceResponse, "rejected_data_points"),
    }[signal]
    response = response_type()
    setattr(response.partial_success, field, rejected)
    response.partial_success.error_message = warning
    return response.SerializeToString()


@pytest.mark.parametrize("signal", SIGNALS)
@pytest.mark.parametrize(
    "rejected,code", [(0, "export.warning"), (1, "export.partial")]
)
def test_partial_and_warning_are_truthful_without_retry(
    signal, rejected, code, otlp_receiver, caplog
):
    otlp_receiver.queue_response(
        body=_response_body(signal, rejected, CANARY), reason=CANARY
    )
    exporter, session = _exporter(signal, otlp_receiver)
    try:
        assert exporter.export(_payload(signal)).name == "SUCCESS"
        assert session.outcome.code == code
        assert session.outcome.rejected_records == rejected
        assert session.outcome.accepted_records == 1 - rejected
        assert session.outcome.local_dropped_records == 0
        assert len(otlp_receiver.requests) == 1
    finally:
        exporter.shutdown()
    assert CANARY not in caplog.text
    assert CANARY not in repr(session.outcome)


@pytest.mark.parametrize("signal", SIGNALS)
@pytest.mark.parametrize(
    "script,code",
    [
        ({"body": b"\xff"}, "export.invalid"),
        ({"headers": {"Content-Type": "text/plain"}}, "export.content_type"),
        ({"status": 202}, "export.status"),
        ({"status": 204}, "export.status"),
        ({"status": 300}, "export.status"),
        ({"body": b"x" * 65537}, "export.response_size"),
        ({"body": b"x" * 65537, "send_content_length": False}, "export.response_size"),
        ({"body": CANARY.encode(), "reason": CANARY}, "export.invalid"),
    ],
    ids=[
        "malformed",
        "wrong-type",
        "202",
        "204",
        "300",
        "oversized-length",
        "oversized-stream",
        "malicious-prose",
    ],
)
def test_protocol_errors_are_not_accepted_or_retried(
    signal, script, code, otlp_receiver, caplog
):
    otlp_receiver.queue_response(**script)
    exporter, session = _exporter(signal, otlp_receiver)
    try:
        assert exporter.export(_payload(signal)).name == "FAILURE"
        assert session.outcome.code == code
        assert session.outcome.accepted_records == session.outcome.rejected_records == 0
        assert len(otlp_receiver.requests) == 1
        # Same exporter/session recovers on the neighboring valid success.
        assert exporter.export(_payload(signal)).name == "SUCCESS"
        assert session.outcome.accepted_records == 1
        assert len(otlp_receiver.requests) == 2
    finally:
        exporter.shutdown()
    assert CANARY not in caplog.text
    assert CANARY not in repr(session.outcome)


@pytest.mark.parametrize("signal", SIGNALS)
@pytest.mark.parametrize("rejected", [-1, 2])
def test_impossible_rejected_counts_fail_closed(signal, rejected, otlp_receiver):
    otlp_receiver.queue_response(body=_response_body(signal, rejected, CANARY))
    exporter, session = _exporter(signal, otlp_receiver)
    try:
        assert exporter.export(_payload(signal)).name == "FAILURE"
        assert session.outcome.code == "export.rejected_count"
        assert session.outcome.accepted_records == 0
        assert len(otlp_receiver.requests) == 1
        assert exporter.export(_payload(signal)).name == "SUCCESS"
        assert session.outcome.accepted_records == 1
    finally:
        exporter.shutdown()


@pytest.mark.parametrize("signal", SIGNALS)
@pytest.mark.parametrize("encoding", ["gzip", "deflate", "br", "unknown"])
def test_encoded_success_is_closed_before_body_offer(signal, encoding, otlp_receiver):
    receipt = otlp_receiver.queue_response(
        headers={"Content-Encoding": encoding}, body=b"x" * 65537, body_delay=2
    )
    exporter, session = _exporter(signal, otlp_receiver)
    try:
        assert exporter.export(_payload(signal)).name == "FAILURE"
        assert session.outcome.code == "export.encoding"
        assert session.outcome.accepted_records == 0
        assert receipt.headers_sent.wait(0.5)
        assert receipt.client_closed.wait(0.5)
        assert not receipt.body_started.is_set()
        assert len(otlp_receiver.requests) == 1
        assert exporter.export(_payload(signal)).name == "SUCCESS"
    finally:
        exporter.shutdown()


@pytest.mark.parametrize("signal", SIGNALS)
def test_redirect_is_closed_before_body_offer_or_second_destination(
    signal, otlp_receiver
):
    from support.otlp_receiver import OTLPReceiver

    with OTLPReceiver() as other:
        receipt = otlp_receiver.queue_response(
            status=307,
            headers={"Location": other.url(signal)},
            body=b"x" * 65537,
            body_delay=2,
        )
        exporter, session = _exporter(signal, otlp_receiver)
        try:
            assert exporter.export(_payload(signal)).name == "FAILURE"
            assert session.outcome.code == "export.status"
            assert session.outcome.accepted_records == 0
            assert receipt.client_closed.wait(0.5)
            assert not receipt.body_started.is_set()
            assert len(otlp_receiver.requests) == 1
            assert other.requests == ()
            assert exporter.export(_payload(signal)).name == "SUCCESS"
        finally:
            exporter.shutdown()


@pytest.mark.parametrize("signal", SIGNALS)
@pytest.mark.parametrize("status", [400, 401, 500, 429, 503])
def test_http_errors_discard_prose_and_keep_native_retry(
    signal, status, otlp_receiver, caplog
):
    from time import monotonic

    retryable = status in (429, 503)
    receipt = otlp_receiver.queue_response(
        status=status,
        headers={"Retry-After": "0", "Set-Cookie": f"secret={CANARY}"},
        reason=CANARY,
        body=CANARY.encode() * 10000,
        body_delay=2,
    )
    exporter, session = _exporter(signal, otlp_receiver, timeout=0.5)
    start = monotonic()
    try:
        assert exporter.export(_payload(signal)).name == (
            "SUCCESS" if retryable else "FAILURE"
        )
        # Native first backoff is at least0.8s. Success within0.5s proves the
        # Retry-After:0 survived, without patching the native retry machinery.
        assert monotonic() - start < 0.5
        assert session.outcome.accepted_records == int(retryable)
        assert session.outcome.code == (
            "export.accepted" if retryable else "export.http_error"
        )
        assert len(otlp_receiver.requests) == (2 if retryable else 1)
        assert receipt.client_closed.wait(0.5)
        assert not receipt.body_started.is_set()
        assert len(session.cookies) == 0
        if not retryable:
            assert exporter.export(_payload(signal)).name == "SUCCESS"
    finally:
        exporter.shutdown()
    assert CANARY not in caplog.text
    assert CANARY not in repr(session.outcome)


@pytest.mark.parametrize("signal", SIGNALS)
@pytest.mark.parametrize("mode", ["hang", "slow-body", "headers-then-body"])
def test_remaining_deadline_interrupts_io_without_leaving_work(
    signal, mode, otlp_receiver, caplog
):
    from threading import enumerate as threads
    from time import monotonic

    if mode == "hang":
        receipt = otlp_receiver.queue_response(hang=True)
    elif mode == "slow-body":
        receipt = otlp_receiver.queue_response(
            body=_response_body(signal, warning=CANARY), chunk_size=1, chunk_delay=0.04
        )
    else:
        receipt = otlp_receiver.queue_response(
            body=_response_body(signal, warning=CANARY),
            header_delay=0.16,
            body_delay=0.16,
        )
    exporter, session = _exporter(signal, otlp_receiver, timeout=0.25)
    start = monotonic()
    try:
        assert exporter.export(_payload(signal)).name == "FAILURE"
        assert monotonic() - start < 0.4
        assert session.outcome.code == "export.deadline"
        assert session.outcome.accepted_records == 0
        assert len(otlp_receiver.requests) == 1
        assert receipt.client_closed.wait(0.5)
        assert not [t for t in threads() if t.name == "altimeter-response-deadline"]
        assert exporter.export(_payload(signal)).name == "SUCCESS"
    finally:
        exporter.shutdown()
    assert CANARY not in caplog.text


def test_metric_per_call_timeout_bounds_hanging_endpoint(otlp_receiver):
    from time import monotonic

    receipt = otlp_receiver.queue_response(hang=True)
    exporter, session = _exporter("metrics", otlp_receiver, timeout=2)
    started = monotonic()
    try:
        assert (
            exporter.export(_payload("metrics"), timeout_millis=100).name == "FAILURE"
        )
        assert monotonic() - started < 0.25
        assert session.outcome.code == "export.deadline"
        assert session.outcome.accepted_records == 0
        assert receipt.client_closed.wait(0.5)
        assert (
            exporter.export(_payload("metrics"), timeout_millis=100).name == "SUCCESS"
        )
    finally:
        exporter.shutdown()


def _wire_message(signal):
    from opentelemetry.exporter.otlp.proto.common._log_encoder import encode_logs
    from opentelemetry.exporter.otlp.proto.common.metrics_encoder import encode_metrics
    from opentelemetry.exporter.otlp.proto.common.trace_encoder import encode_spans

    encoder = {"traces": encode_spans, "logs": encode_logs, "metrics": encode_metrics}[
        signal
    ]
    return encoder(_payload(signal))


def _post(session, body, **options):
    arguments = {
        "method": "POST",
        "url": session.endpoint,
        "headers": {"Content-Type": "application/x-protobuf"},
        "timeout": 1,
        "data": body,
    }
    arguments.update(options)
    return session.request(**arguments)


@pytest.mark.parametrize("signal", SIGNALS)
@pytest.mark.parametrize(
    "options,code",
    [
        ({"method": "GET"}, "export.destination"),
        ({"url": "http://127.0.0.1:1/other"}, "export.destination"),
        ({"headers": {"Content-Type": "text/plain"}}, "export.content_type"),
        (
            {
                "headers": {
                    "Content-Type": "application/x-protobuf",
                    "Content-Encoding": "gzip",
                }
            },
            "export.encoding",
        ),
        ({"params": {"secret": CANARY}}, "export.request"),
        ({"data": bytearray(b"secret")}, "export.request"),
        ({"timeout": float("nan")}, "export.deadline"),
        ({"timeout": 0}, "export.deadline"),
    ],
    ids=[
        "method",
        "endpoint",
        "type",
        "encoding",
        "params",
        "not-bytes",
        "nan-timeout",
        "zero-timeout",
    ],
)
def test_session_rejects_other_request_shapes_before_network(
    signal, options, code, otlp_receiver
):
    from django_altimeter.exporting import ExportProtocolError

    _, session = _exporter(signal, otlp_receiver)
    try:
        with pytest.raises(ExportProtocolError, match=code):
            _post(session, _wire_message(signal).SerializeToString(), **options)
        assert otlp_receiver.requests == ()
        assert session.outcome.code == code
        assert session.outcome.accepted_records == 0
        response = _post(session, _wire_message(signal).SerializeToString())
        assert response.status_code == 200
        assert response.content == b""
        assert response.request is None
        assert response.url is None
        assert len(response.cookies) == len(session.cookies) == 0
        assert response.history == []
        assert response.raw.closed
        assert session.outcome.accepted_records == 1
    finally:
        session.close()


@pytest.mark.parametrize("signal", SIGNALS)
@pytest.mark.parametrize(
    "body,code",
    [
        (b"\xff", "export.invalid"),
        (b"x" * (1024 * 1024 + 1), "export.batch_size"),
        (b"", "export.empty"),
    ],
    ids=["malformed", "oversized", "empty"],
)
def test_invalid_raw_batch_is_not_sent_or_accepted(signal, body, code, otlp_receiver):
    from django_altimeter.exporting import ExportProtocolError

    _, session = _exporter(signal, otlp_receiver)
    try:
        with pytest.raises(ExportProtocolError, match=code):
            _post(session, body)
        assert session.outcome.accepted_records == 0
        assert otlp_receiver.requests == ()
        assert (
            _post(session, _wire_message(signal).SerializeToString()).status_code == 200
        )
        assert session.outcome.accepted_records == 1
    finally:
        session.close()


@pytest.mark.parametrize("signal", SIGNALS)
def test_projection_can_drop_all_records_without_sending_empty_wrappers(
    signal, otlp_receiver
):
    from django_altimeter.exporting import ExportProtocolError

    message = _wire_message(signal)
    record = _records(message, signal)[0]
    for _ in range(33):
        record.attributes.add(key="password").value.string_value = CANARY
    _, session = _exporter(signal, otlp_receiver)
    try:
        with pytest.raises(ExportProtocolError, match="export.empty"):
            _post(session, message.SerializeToString())
        assert session.outcome.local_dropped_records == 1
        assert session.outcome.accepted_records == 0
        assert otlp_receiver.requests == ()
        assert (
            _post(session, _wire_message(signal).SerializeToString()).status_code == 200
        )
    finally:
        session.close()


def _minimal_message(signal):
    message = type(_wire_message(signal))()
    if signal == "traces":
        message.resource_spans.add().scope_spans.add().spans.add(name="X")
    elif signal == "logs":
        record = message.resource_logs.add().scope_logs.add().log_records.add()
        record.body.string_value = "request.complete"
    else:
        scope = message.resource_metrics.add().scope_metrics.add()
        scope.scope.name = "django_altimeter"
        metric = scope.metrics.add(name="http.server.requests", unit="1")
        metric.gauge.data_points.add(as_int=7)
    return message


@pytest.mark.parametrize("signal", SIGNALS)
def test_projection_growth_cannot_cross_post_projection_batch_bound(
    signal, otlp_receiver
):
    from dataclasses import replace

    from django_altimeter.exporting import ExportProtocolError
    from django_altimeter.privacy import (
        DEFAULT_LIMITS,
        project_logs,
        project_metrics,
        project_traces,
    )

    message = _minimal_message(signal)
    project = {
        "traces": project_traces,
        "logs": project_logs,
        "metrics": project_metrics,
    }[signal]
    safe = project(message, policy=POLICY)
    assert safe.ByteSize() > message.ByteSize()  # Added safe wrappers/defaults.
    # Input fits exactly, but reconstructed protobuf would not fit. The pure
    # projector itself returns empty; the transport must never send original.
    limits = replace(DEFAULT_LIMITS, max_batch_bytes=message.ByteSize())
    with SanitizingSession(
        signal, otlp_receiver.url(signal), policy=POLICY, limits=limits
    ) as session:
        with pytest.raises(ExportProtocolError, match="export.empty"):
            _post(session, message.SerializeToString())
        assert session.outcome.accepted_records == 0
        assert otlp_receiver.requests == ()
    limits = replace(DEFAULT_LIMITS, max_batch_bytes=safe.ByteSize())
    with SanitizingSession(
        signal, otlp_receiver.url(signal), policy=POLICY, limits=limits
    ) as session:
        assert _post(session, message.SerializeToString()).status_code == 200
        assert session.outcome.accepted_records == 1
        assert otlp_receiver.requests[0].message == safe
        assert otlp_receiver.requests[0].message.ByteSize() == limits.max_batch_bytes


@pytest.mark.parametrize("signal", SIGNALS)
@pytest.mark.parametrize("container", ["resources", "scopes", "records"])
def test_decoded_batch_container_counts_are_bounded(signal, container, otlp_receiver):
    from django_altimeter.exporting import ExportProtocolError

    message = _minimal_message(signal)
    groups = getattr(message, f"resource_{'spans' if signal == 'traces' else signal}")
    scopes = getattr(groups[0], f"scope_{'spans' if signal == 'traces' else signal}")
    if container == "resources":
        values = groups
    elif container == "scopes":
        values = scopes
    elif signal == "traces":
        values = scopes[0].spans
    elif signal == "logs":
        values = scopes[0].log_records
    else:
        values = scopes[0].metrics[0].gauge.data_points
    for _ in range(128):
        values.add()
    _, session = _exporter(signal, otlp_receiver)
    try:
        with pytest.raises(ExportProtocolError, match="export.batch_count"):
            _post(session, message.SerializeToString())
        assert otlp_receiver.requests == ()
        assert session.outcome.accepted_records == 0
        assert (
            _post(session, _minimal_message(signal).SerializeToString()).status_code
            == 200
        )
    finally:
        session.close()


@pytest.mark.parametrize("signal", SIGNALS)
def test_factory_overrides_ambient_compression(signal, otlp_receiver, monkeypatch):
    monkeypatch.setenv("OTEL_EXPORTER_OTLP_COMPRESSION", "gzip")
    monkeypatch.setenv(f"OTEL_EXPORTER_OTLP_{signal.upper()}_COMPRESSION", "gzip")
    exporter, session = _exporter(signal, otlp_receiver)
    try:
        assert exporter.export(_payload(signal)).name == "SUCCESS"
        assert session.outcome.accepted_records == 1
        assert len(otlp_receiver.requests) == 1
        assert (
            otlp_receiver.requests[0].headers.get("Content-Encoding", "identity")
            == "identity"
        )
    finally:
        exporter.shutdown()


@pytest.mark.parametrize("signal", SIGNALS)
def test_native_no_send_resets_previous_acceptance(signal, otlp_receiver):
    from dataclasses import replace

    from django_altimeter.privacy import DEFAULT_LIMITS

    session = SanitizingSession(
        signal,
        otlp_receiver.url(signal),
        policy=POLICY,
        limits=replace(DEFAULT_LIMITS, max_batch_bytes=1024),
    )
    factory = {
        "traces": create_span_exporter,
        "logs": create_log_exporter,
        "metrics": create_metric_exporter,
    }[signal]
    exporter = factory(session, timeout=1)
    try:
        assert exporter.export(_payload(signal)).name == "SUCCESS"
        assert session.outcome.accepted_records == 1
        payload = _payload(signal)
        if signal == "traces":
            provider = TracerProvider(resource=Resource({}), shutdown_on_exit=False)
            span = provider.get_tracer("django_altimeter").start_span("x" * 2048)
            span.end()
            provider.shutdown()
            payload = (span,)
        elif signal == "logs":
            payload[0].log_record.body = "x" * 2048
        else:
            group = payload.resource_metrics[0]
            scope = group.scope_metrics[0]
            metric = replace(scope.metrics[0], description="x" * 2048)
            scope = replace(scope, metrics=(metric,))
            payload = replace(
                payload, resource_metrics=(replace(group, scope_metrics=(scope,)),)
            )
        assert exporter.export(payload).name == "FAILURE"
        assert len(otlp_receiver.requests) == 1
        assert session.outcome.code == "export.not_sent"
        assert session.outcome.accepted_records == session.outcome.rejected_records == 0
        assert exporter.export(_payload(signal)).name == "SUCCESS"
        assert session.outcome.accepted_records == 1
    finally:
        exporter.shutdown()
    assert exporter.export(_payload(signal)).name == "FAILURE"
    assert session.outcome.code == "export.not_sent"
    assert session.outcome.accepted_records == 0
    assert len(otlp_receiver.requests) == 2


def test_metric_per_call_deadline_also_bounds_native_retry_wait(otlp_receiver):
    from time import monotonic

    otlp_receiver.queue_response(status=503, headers={"Retry-After": "0.5"})
    exporter, session = _exporter("metrics", otlp_receiver, timeout=2)
    started = monotonic()
    try:
        assert (
            exporter.export(_payload("metrics"), timeout_millis=100).name == "FAILURE"
        )
        assert monotonic() - started < 0.25
        assert session.outcome.accepted_records == 0
        assert len(otlp_receiver.requests) == 1
        assert (
            exporter.export(_payload("metrics"), timeout_millis=100).name == "SUCCESS"
        )
    finally:
        exporter.shutdown()


@pytest.mark.parametrize("signal", SIGNALS)
def test_ordinary_error_returns_only_status_retry_after_and_safe_reason(
    signal, otlp_receiver
):
    receipt = otlp_receiver.queue_response(
        status=503,
        headers={"Retry-After": "2", "Set-Cookie": f"secret={CANARY}"},
        reason=CANARY,
        body=CANARY.encode(),
        body_delay=2,
    )
    _, session = _exporter(signal, otlp_receiver)
    try:
        response = _post(session, _wire_message(signal).SerializeToString())
        assert response.status_code == 503
        assert response.headers["Retry-After"] == "2"
        assert set(response.headers) == {"Retry-After"}
        assert response.reason == "OTLP response"
        assert response.content == b""
        assert response.request is response.url is None
        assert response.raw.closed
        assert not response.cookies and not session.cookies
        assert CANARY not in repr(vars(response))
        assert receipt.client_closed.wait(0.5)
        assert not receipt.body_started.is_set()
        assert session.outcome.accepted_records == 0
        assert (
            _post(session, _wire_message(signal).SerializeToString()).status_code == 200
        )
        assert session.outcome.accepted_records == 1
    finally:
        session.close()


@pytest.mark.parametrize("signal", SIGNALS)
def test_valid_response_at_exact_body_limit_is_accepted(signal, otlp_receiver):
    # The wrapper/key/length prefix occupies8bytes with this message length.
    body = _response_body(signal, warning="x" * (65536 - 8))
    assert len(body) == 65536
    otlp_receiver.queue_response(body=body)
    exporter, session = _exporter(signal, otlp_receiver)
    try:
        assert exporter.export(_payload(signal)).name == "SUCCESS"
        assert session.outcome.code == "export.warning"
        assert session.outcome.accepted_records == 1
        assert session.outcome.rejected_records == 0
        assert len(otlp_receiver.requests) == 1
    finally:
        exporter.shutdown()


@pytest.mark.parametrize("signal", SIGNALS)
def test_exact_input_limit_projects_without_unknown_field_copy(signal, otlp_receiver):
    from django_altimeter.exporting import ExportProtocolError

    encoded = _wire_message(signal).SerializeToString()
    padding_length = 1048576 - len(encoded) - 5
    # Unknown field127, length-delimited, followed by a three-byte varint size.
    length = bytes(
        (
            padding_length & 127 | 128,
            padding_length >> 7 & 127 | 128,
            padding_length >> 14,
        )
    )
    raw = encoded + b"\xfa\x07" + length + b"x" * padding_length
    assert len(raw) == 1048576
    _, session = _exporter(signal, otlp_receiver)
    try:
        assert _post(session, raw).status_code == 200
        assert session.outcome.accepted_records == 1
        assert len(otlp_receiver.requests) == 1
        received = otlp_receiver.requests[0].message
        assert len(_records(received, signal)) == 1
        assert received.ByteSize() < 1024
        assert CANARY.encode() not in received.SerializeToString()
        with pytest.raises(ExportProtocolError, match="export.batch_size"):
            _post(session, raw + b"\x00")
        assert session.outcome.accepted_records == 0
        assert len(otlp_receiver.requests) == 1
    finally:
        session.close()


@pytest.mark.parametrize("signal", SIGNALS)
def test_partial_counts_both_accepted_and_rejected_without_resend(
    signal, otlp_receiver
):
    from dataclasses import replace

    payload = _payload(signal)
    second = _payload(signal)
    if signal == "metrics":
        group = payload.resource_metrics[0]
        scope = group.scope_metrics[0]
        metric = scope.metrics[0]
        points = (
            metric.data.data_points
            + second.resource_metrics[0].scope_metrics[0].metrics[0].data.data_points
        )
        metric = replace(metric, data=replace(metric.data, data_points=points))
        scope = replace(scope, metrics=(metric,))
        payload = replace(
            payload, resource_metrics=(replace(group, scope_metrics=(scope,)),)
        )
    else:
        payload += second
    otlp_receiver.queue_response(
        body=_response_body(signal, rejected=1, warning=CANARY)
    )
    exporter, session = _exporter(signal, otlp_receiver)
    try:
        assert exporter.export(payload).name == "SUCCESS"
        assert session.outcome.code == "export.partial"
        assert session.outcome.accepted_records == 1
        assert session.outcome.rejected_records == 1
        assert session.outcome.local_dropped_records == 0
        assert len(otlp_receiver.requests) == 1
        assert len(_records(otlp_receiver.requests[0].message, signal)) == 2
        assert (
            CANARY.encode() not in otlp_receiver.requests[0].message.SerializeToString()
        )
    finally:
        exporter.shutdown()


def _mixed_payload(signal):
    from dataclasses import replace

    payload = _payload(signal)
    excessive = {f"password{i}": CANARY for i in range(33)}
    if signal == "traces":
        provider = TracerProvider(resource=Resource({}), shutdown_on_exit=False)
        span = provider.get_tracer("django_altimeter").start_span(
            "GET", attributes=excessive
        )
        span.end()
        provider.shutdown()
        return payload + (span,)
    if signal == "logs":
        bad = _payload(signal)[0]
        bad.log_record.attributes = excessive
        return payload + (bad,)
    group = payload.resource_metrics[0]
    scope = group.scope_metrics[0]
    metric = scope.metrics[0]
    good = metric.data.data_points[0]
    bad = replace(good, attributes=excessive)
    metric = replace(metric, data=replace(metric.data, data_points=(good, bad)))
    scope = replace(scope, metrics=(metric,))
    return replace(payload, resource_metrics=(replace(group, scope_metrics=(scope,)),))


@pytest.mark.parametrize("signal", SIGNALS)
def test_known_local_drops_survive_failed_response(signal, otlp_receiver):
    otlp_receiver.queue_response(body=b"\xff")
    exporter, session = _exporter(signal, otlp_receiver)
    payload = _mixed_payload(signal)
    try:
        assert exporter.export(payload).name == "FAILURE"
        assert session.outcome.code == "export.invalid"
        assert session.outcome.local_dropped_records == 1
        assert session.outcome.accepted_records == session.outcome.rejected_records == 0
        assert exporter.export(payload).name == "SUCCESS"
        assert session.outcome.accepted_records == 1
        assert session.outcome.local_dropped_records == 1
        assert len(otlp_receiver.requests) == 2
        assert all(
            len(_records(r.message, signal)) == 1 for r in otlp_receiver.requests
        )
    finally:
        exporter.shutdown()


def test_metric_timeout_guard_does_not_patch_missing_native_attribute(
    otlp_receiver, monkeypatch
):
    # Explicit compatibility fault injection, not the real-network timeout proof.
    from opentelemetry.exporter.otlp.proto.http.metric_exporter import (
        OTLPMetricExporter,
    )

    native_init = OTLPMetricExporter.__init__

    def without_timeout(self, *args, **kwargs):
        native_init(self, *args, **kwargs)
        del self._client._timeout

    with monkeypatch.context() as patch:
        patch.setattr(OTLPMetricExporter, "__init__", without_timeout)
        exporter, session = _exporter("metrics", otlp_receiver)
    try:
        assert session.outcome.code == "metrics.timeout_unsupported"
        assert not hasattr(exporter._client, "_timeout")
        assert (
            exporter.export(_payload("metrics"), timeout_millis=100).name == "FAILURE"
        )
        assert session.outcome.code == "metrics.timeout_unsupported"
        assert session.outcome.accepted_records == 0
        assert not hasattr(exporter._client, "_timeout")  # No compatibility patch.
        assert otlp_receiver.requests == ()
        # Restored native behavior is used without silently enabling the patch.
        exporter._client._timeout = 2
        assert (
            exporter.export(_payload("metrics"), timeout_millis=100).name == "SUCCESS"
        )
        assert session.outcome.code == "metrics.timeout_unsupported"
        assert session.outcome.accepted_records == 1
        assert exporter._client._timeout == 2
    finally:
        exporter.shutdown()


def test_metric_export_lock_wait_is_inside_callers_budget(otlp_receiver):
    from threading import Thread
    from time import monotonic

    receipt = otlp_receiver.queue_response(hang=True)
    exporter, session = _exporter("metrics", otlp_receiver, timeout=1)
    first_results = []
    first = Thread(
        target=lambda: first_results.append(
            exporter.export(_payload("metrics"), timeout_millis=300)
        )
    )
    first.start()
    otlp_receiver.wait_for_requests()
    started = monotonic()
    try:
        assert exporter.export(_payload("metrics"), timeout_millis=50).name == "FAILURE"
        assert monotonic() - started < 0.2
        assert session.outcome.code == "export.deadline"
        assert session.outcome.accepted_records == 0
        assert len(otlp_receiver.requests) == 1
        first.join(timeout=0.5)
        assert not first.is_alive()
        assert [r.name for r in first_results] == ["FAILURE"]
        assert receipt.client_closed.wait(0.5)
        assert exporter._client._timeout == 1
        assert (
            exporter.export(_payload("metrics"), timeout_millis=100).name == "SUCCESS"
        )
        assert session.outcome.accepted_records == 1
    finally:
        first.join(timeout=1)
        exporter.shutdown()


@pytest.mark.parametrize("signal", SIGNALS)
def test_conflicting_chunked_framing_cannot_hide_partial_rejection(
    signal, otlp_receiver
):
    body = _response_body(signal, rejected=1)
    framed = f"{len(body):x}\r\n".encode() + body + b"\r\n0\r\n\r\n"
    otlp_receiver.queue_response(
        body=framed,
        headers={"Transfer-Encoding": "chunked", "Content-Length": "0"},
    )
    exporter, session = _exporter(signal, otlp_receiver)
    try:
        assert exporter.export(_payload(signal)).name == "FAILURE"
        assert session.outcome.code == "export.framing"
        assert session.outcome.accepted_records == 0
        assert session.outcome.rejected_records == 0
        assert len(otlp_receiver.requests) == 1
        # An unambiguous, real chunked partial response is understood.
        otlp_receiver.queue_response(
            body=framed,
            headers={"Transfer-Encoding": "chunked"},
            send_content_length=False,
        )
        assert exporter.export(_payload(signal)).name == "SUCCESS"
        assert session.outcome.code == "export.partial"
        assert session.outcome.accepted_records == 0
        assert session.outcome.rejected_records == 1
        assert len(otlp_receiver.requests) == 2
        # The ordinary empty Content-Length0 positive remains valid.
        assert exporter.export(_payload(signal)).name == "SUCCESS"
        assert session.outcome.code == "export.accepted"
        assert session.outcome.accepted_records == 1
        assert len(otlp_receiver.requests) == 3
    finally:
        exporter.shutdown()


@pytest.mark.parametrize("signal", SIGNALS)
def test_local_failure_diagnostic_is_fixed_and_rate_limited(
    signal, otlp_receiver, caplog
):
    exporter, session = _exporter(signal, otlp_receiver)
    try:
        for _ in range(3):
            otlp_receiver.queue_response(body=CANARY.encode(), reason=CANARY)
            assert exporter.export(_payload(signal)).name == "FAILURE"
            assert session.outcome.code == "export.invalid"
            assert session.outcome.accepted_records == 0
        local = [r for r in caplog.records if r.name == "django_altimeter.export"]
        assert [r.getMessage() for r in local] == [f"export.invalid {signal}"]
        assert CANARY not in caplog.text
        assert len(otlp_receiver.requests) == 3
        assert exporter.export(_payload(signal)).name == "SUCCESS"
        assert session.outcome.accepted_records == 1
        assert len(otlp_receiver.requests) == 4
    finally:
        exporter.shutdown()


def test_diagnostic_windows_separate_codes_signals_and_renew(
    otlp_receiver, caplog, monkeypatch
):
    # Unit evidence for the monotonic clock boundary; no wall-clock sleep.
    import django_altimeter.exporting as exporting

    clock = [1000.0]
    monkeypatch.setattr(exporting, "monotonic", lambda: clock[0])
    sessions = [
        SanitizingSession(signal, otlp_receiver.url(signal), policy=POLICY)
        for signal in ("traces", "logs")
    ]
    try:
        for session in sessions:
            for _ in range(2):
                with pytest.raises(
                    exporting.ExportProtocolError, match="export.invalid"
                ):
                    _post(session, b"\xff")
        with pytest.raises(exporting.ExportProtocolError, match="export.empty"):
            _post(sessions[0], b"")
        clock[0] += 61
        with pytest.raises(exporting.ExportProtocolError, match="export.invalid"):
            _post(sessions[0], b"\xff")
        local = [r for r in caplog.records if r.name == "django_altimeter.export"]
        assert [r.getMessage() for r in local] == [
            "export.invalid traces",
            "export.invalid logs",
            "export.empty traces",
            "export.invalid traces",
        ]
        assert all(r.levelname == "WARNING" for r in local)
        assert CANARY not in caplog.text
        assert otlp_receiver.requests == ()
        assert (
            _post(sessions[0], _wire_message("traces").SerializeToString()).status_code
            == 200
        )
        assert sessions[0].outcome.accepted_records == 1
    finally:
        for session in sessions:
            session.close()


@pytest.mark.parametrize("signal", SIGNALS)
def test_logging_handler_failure_does_not_break_export(signal, otlp_receiver, caplog):
    import logging

    class FailingHandler(logging.Handler):
        def emit(self, record):
            raise RuntimeError(CANARY)

    logger = logging.getLogger("django_altimeter.export")
    handler = FailingHandler()
    logger.addHandler(handler)
    exporter, session = _exporter(signal, otlp_receiver)
    try:
        otlp_receiver.queue_response(body=b"\xff")
        assert exporter.export(_payload(signal)).name == "FAILURE"
        assert session.outcome.code == "export.invalid"
        assert session.outcome.accepted_records == 0
        assert exporter.export(_payload(signal)).name == "SUCCESS"
        assert session.outcome.accepted_records == 1
        assert len(otlp_receiver.requests) == 2
        assert CANARY not in caplog.text
    finally:
        exporter.shutdown()
        logger.removeHandler(handler)
        handler.close()


def test_concurrent_identical_diagnostics_emit_once(otlp_receiver, caplog):
    from concurrent.futures import ThreadPoolExecutor

    from django_altimeter.exporting import ExportProtocolError

    with SanitizingSession(
        "traces", otlp_receiver.url("traces"), policy=POLICY
    ) as session:

        def reject(_):
            with pytest.raises(ExportProtocolError, match="export.invalid"):
                _post(session, b"\xff")

        with ThreadPoolExecutor(max_workers=8) as pool:
            list(pool.map(reject, range(32)))
        local = [r for r in caplog.records if r.name == "django_altimeter.export"]
        assert [r.getMessage() for r in local] == ["export.invalid traces"]
        assert otlp_receiver.requests == ()
        assert session.outcome.accepted_records == 0
        assert (
            _post(session, _wire_message("traces").SerializeToString()).status_code
            == 200
        )


@pytest.mark.parametrize("signal", SIGNALS)
@pytest.mark.parametrize("empty", [True, False])
def test_truncated_declared_body_never_counts_as_acceptance(
    signal, empty, otlp_receiver
):
    body = b"" if empty else _response_body(signal, rejected=1)
    otlp_receiver.queue_response(
        body=body, headers={"Content-Length": str(len(body) + 10)}
    )
    exporter, session = _exporter(signal, otlp_receiver)
    try:
        assert exporter.export(_payload(signal)).name == "FAILURE"
        assert session.outcome.code == "export.invalid"
        assert session.outcome.accepted_records == session.outcome.rejected_records == 0
        assert len(otlp_receiver.requests) == 1
        assert exporter.export(_payload(signal)).name == "SUCCESS"
        assert session.outcome.accepted_records == 1
    finally:
        exporter.shutdown()
