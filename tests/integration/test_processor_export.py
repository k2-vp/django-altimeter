"""Local HTTP/decoded-payload evidence; these tests do not claim live ingestion."""

import subprocess
import sys
import textwrap

import pytest
from opentelemetry._logs import LogRecord, SeverityNumber
from opentelemetry.exporter.otlp.proto.http._log_exporter import OTLPLogExporter
from opentelemetry.exporter.otlp.proto.http.trace_exporter import OTLPSpanExporter
from opentelemetry.sdk._logs import LoggerProvider
from opentelemetry.sdk.resources import Resource
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.trace import Status, StatusCode

from django_altimeter.exporting import (
    BoundedSpanProcessor,
    bounded_log_processor,
    native_log_limits,
    native_span_limits,
)
from django_altimeter.privacy import PrivacyPolicy, project_logs, project_traces

POLICY = PrivacyPolicy(
    span_names=frozenset({"request"}),
    event_names=frozenset({"request.complete"}),
    log_bodies=frozenset({"request.complete"}),
    attribute_values=frozenset(
        {("service.name", "admission-test"), ("http.route", "/boards/{board_id}")}
    ),
)

pytestmark = pytest.mark.integration
CANARY = "password=processor-export-canary"


def test_native_trace_export_admits_once_without_bypass(otlp_receiver) -> None:
    provider = TracerProvider(
        resource=Resource({}), shutdown_on_exit=False, span_limits=native_span_limits()
    )
    processor = BoundedSpanProcessor(
        OTLPSpanExporter(endpoint=otlp_receiver.url("traces"), timeout=2),
        schedule_delay_millis=60_000,
    )
    provider.add_span_processor(processor)
    tracer = provider.get_tracer("django_altimeter")
    try:
        with tracer.start_as_current_span(
            "request", attributes={"http.response.status_code": 200}
        ) as safe:
            expected = safe.get_span_context()
        with tracer.start_as_current_span("x" * 257):
            pass
        with tracer.start_as_current_span("request") as unsafe:
            unsafe.set_attribute("password", CANARY)
        with tracer.start_as_current_span("request") as unsafe:
            unsafe.set_status(Status(StatusCode.ERROR, CANARY))
        assert provider.force_flush(timeout_millis=5_000)
    finally:
        provider.shutdown()
    requests = otlp_receiver.wait_for_requests()
    projected = [project_traces(request.message, policy=POLICY) for request in requests]
    assert CANARY.encode() in requests[0].message.SerializeToString()
    spans = [
        span
        for request in projected
        for resource in request.resource_spans
        for scope in resource.scope_spans
        for span in scope.spans
    ]
    assert [request.path for request in requests] == ["/v1/traces"]
    assert len(spans) == 3
    assert len({span.span_id for span in spans}) == 3
    assert spans[0].name == "request"
    assert spans[0].trace_id == expected.trace_id.to_bytes(16, "big")
    assert spans[0].span_id == expected.span_id.to_bytes(8, "big")
    assert spans[0].end_time_unix_nano >= spans[0].start_time_unix_nano > 0
    assert {a.key: a.value.int_value for a in spans[0].attributes} == {
        "http.response.status_code": 200
    }
    assert all(
        CANARY.encode() not in message.SerializeToString() for message in projected
    )


def test_native_log_export_admits_once_without_bypass(otlp_receiver) -> None:
    provider = LoggerProvider(
        resource=Resource({}),
        shutdown_on_exit=False,
        log_record_limits=native_log_limits(),
    )
    processor = bounded_log_processor(
        OTLPLogExporter(endpoint=otlp_receiver.url("logs"), timeout=2),
        schedule_delay_millis=60_000,
    )
    provider.add_log_record_processor(processor)
    logger = provider.get_logger("django_altimeter")
    try:
        logger.emit(
            LogRecord(
                body="request.complete",
                severity_number=SeverityNumber.INFO,
                trace_id=1,
                span_id=2,
                trace_flags=1,
                attributes={"http.response.status_code": 200},
            )
        )
        logger.emit(body="x" * 257)
        logger.emit(body={"password": CANARY})
        logger.emit(body="request.complete", attributes={"password": CANARY})
        assert provider.force_flush(timeout_millis=5_000)
    finally:
        provider.shutdown()
    requests = otlp_receiver.wait_for_requests()
    projected = [project_logs(request.message, policy=POLICY) for request in requests]
    assert CANARY.encode() in requests[0].message.SerializeToString()
    records = [
        record
        for request in projected
        for resource in request.resource_logs
        for scope in resource.scope_logs
        for record in scope.log_records
    ]
    assert [request.path for request in requests] == ["/v1/logs"]
    assert len(records) == 2
    assert records[0].body.string_value == "request.complete"
    assert records[0].trace_id == (1).to_bytes(16, "big")
    assert records[0].span_id == (2).to_bytes(8, "big")
    assert records[0].severity_number == SeverityNumber.INFO.value
    assert {a.key: a.value.int_value for a in records[0].attributes} == {
        "http.response.status_code": 200
    }
    assert all(
        CANARY.encode() not in message.SerializeToString() for message in projected
    )


def test_package_import_starts_no_threads() -> None:
    script = textwrap.dedent("""
        import threading
        starts = []
        original = threading.Thread.start
        def track_start(self, *args, **kwargs):
            starts.append(self.name)
            return original(self, *args, **kwargs)
        threading.Thread.start = track_start
        before = tuple(threading.enumerate())
        import django_altimeter
        import django_altimeter.exporting
        assert not starts, starts
        assert tuple(threading.enumerate()) == before
    """)
    result = subprocess.run(
        [sys.executable, "-c", script],
        capture_output=True,
        text=True,
        timeout=10,
        check=False,
    )
    assert result.returncode == 0, result.stdout + result.stderr


@pytest.mark.parametrize(
    "name,projected_name",
    [("GET", "GET"), (f"SELECT * FROM board WHERE token='{CANARY}'", "SELECT")],
    ids=["safe-route-with-canary", "sql-name-with-canary"],
)
def test_bounds_admission_then_projection_keeps_safe_span_fields(
    otlp_receiver,
    name,
    projected_name,
):
    provider = TracerProvider(
        resource=Resource({}), shutdown_on_exit=False, span_limits=native_span_limits()
    )
    processor = BoundedSpanProcessor(
        OTLPSpanExporter(endpoint=otlp_receiver.url("traces"), timeout=2),
        schedule_delay_millis=60_000,
    )
    provider.add_span_processor(processor)
    tracer = provider.get_tracer("django_altimeter")
    attributes = {
        "http.route": "/boards/{board_id}",
        "http.response.status_code": 200,
        "http.server.request.duration": 0.125,
        "password": CANARY,
    }
    try:
        with tracer.start_as_current_span(name, attributes=attributes) as current:
            expected = current.get_span_context()
        with tracer.start_as_current_span("x" * 257):
            pass
        assert provider.force_flush(timeout_millis=5_000)
    finally:
        provider.shutdown()
    requests = otlp_receiver.requests
    assert len(requests) == 1
    raw = requests[0].message
    raw_spans = raw.resource_spans[0].scope_spans[0].spans
    assert len(raw_spans) == 1
    assert raw_spans[0].name == name
    assert CANARY.encode() in raw.SerializeToString()
    projected = project_traces(raw, policy=POLICY)
    assert CANARY.encode() not in projected.SerializeToString()
    spans = projected.resource_spans[0].scope_spans[0].spans
    assert len(spans) == 1
    safe = spans[0]
    assert safe.name == projected_name
    assert safe.trace_id == expected.trace_id.to_bytes(16, "big")
    assert safe.span_id == expected.span_id.to_bytes(8, "big")
    assert safe.end_time_unix_nano >= safe.start_time_unix_nano > 0
    assert {
        a.key: getattr(a.value, a.value.WhichOneof("value")) for a in safe.attributes
    } == {
        "http.route": "/boards/{board_id}",
        "http.response.status_code": 200,
        "http.server.request.duration": 0.125,
    }


@pytest.mark.parametrize("signal", ["traces", "logs"])
def test_native_size_drop_is_reported_after_admission(signal, otlp_receiver, caplog):
    from django_altimeter.exporting import (
        SanitizingSession,
        create_log_exporter,
        create_span_exporter,
    )

    session = SanitizingSession(signal, otlp_receiver.url(signal), policy=POLICY)
    if signal == "traces":
        provider = TracerProvider(
            resource=Resource({}),
            shutdown_on_exit=False,
            span_limits=native_span_limits(),
        )
        processor = BoundedSpanProcessor(
            create_span_exporter(session), schedule_delay_millis=60_000
        )
        provider.add_span_processor(processor)
        tracer = provider.get_tracer("django_altimeter")

        def emit(attributes):
            with tracer.start_as_current_span("request", attributes=attributes):
                pass
    else:
        provider = LoggerProvider(
            resource=Resource({}),
            shutdown_on_exit=False,
            log_record_limits=native_log_limits(),
        )
        processor = bounded_log_processor(
            create_log_exporter(session), schedule_delay_millis=60_000
        )
        provider.add_log_record_processor(processor)
        logger = provider.get_logger("django_altimeter")

        def emit(attributes):
            logger.emit(body="request.complete", attributes=attributes)

    attributes = {f"unknown{i}": "x" * 250 for i in range(32)}
    try:
        for _ in range(128):
            emit(attributes)
        assert processor.last_diagnostic is None
        assert provider.force_flush(timeout_millis=5_000)
        assert not otlp_receiver.requests
        assert session.outcome.code == "export.local_drop"
        assert session.outcome.local_dropped_records == 128
        assert session.outcome.accepted_records == session.outcome.rejected_records == 0
        diagnostics = [
            r.getMessage()
            for r in caplog.records
            if r.name == "django_altimeter.export"
        ]
        assert diagnostics == [f"export.local_drop {signal}"]
        emit({"http.response.status_code": 200})
        assert provider.force_flush(timeout_millis=5_000)
        assert session.outcome.code == "export.accepted"
        assert session.outcome.accepted_records == 1
        assert session.outcome.local_dropped_records == 0
        assert len(otlp_receiver.requests) == 1
    finally:
        provider.shutdown()


@pytest.mark.parametrize("oversized", [False, True], ids=["accepted", "size-drop"])
def test_shutdown_reports_native_size_loss(otlp_receiver, oversized):
    import json
    import os

    source = """
import json, os
from opentelemetry import trace
from django_altimeter.conf import Config, SignalConfig
from django_altimeter.runtime import setup, shutdown, get_process_runtime
config = Config(
    enabled=True, service_name="size-test", deployment_environment="test", release="v1",
    traces=SignalConfig(True, os.environ["TEST_ENDPOINT"], destination="collector"),
    batch_schedule_delay_millis=60000, export_timeout=0.5,
)
assert setup(config).ok
runtime = get_process_runtime()
tracer = trace.get_tracer("django_altimeter")
with tracer.start_as_current_span("GET", attributes={"http.response.status_code": 200}):
    pass
assert runtime.tracer_provider.force_flush(timeout_millis=1000)
attributes = ({f"unknown{i}": "x" * 250 for i in range(32)}
              if os.environ["OVERSIZED"] == "1" else {"http.response.status_code": 200})
# Less than the batch trigger: the explicit shutdown owns this final flush.
for _ in range(126):
    with tracer.start_as_current_span("GET", attributes=attributes):
        pass
result = shutdown()
outcome = runtime._sessions[0].outcome
print(json.dumps({"completed": result.completed, "codes": result.codes,
                  "code": outcome.code, "dropped": outcome.local_dropped_records}))
"""
    environment = {
        key: value
        for key, value in os.environ.items()
        if not key.startswith(("ALTIMETER_", "OTEL_"))
    }
    process = subprocess.run(
        [sys.executable, "-c", source],
        env={
            **environment,
            "TEST_ENDPOINT": otlp_receiver.url("traces"),
            "OVERSIZED": "1" if oversized else "0",
        },
        capture_output=True,
        text=True,
        timeout=10,
        check=False,
    )
    assert process.returncode == 0, process.stderr
    result = json.loads(process.stdout)
    assert result["completed"] is (not oversized), result
    assert result["codes"] == (["shutdown.export_failed"] if oversized else [])
    assert result["code"] == ("export.local_drop" if oversized else "export.accepted")
    assert result["dropped"] == (126 if oversized else 0)
    spans = [
        span
        for request in otlp_receiver.requests
        for resource in request.message.resource_spans
        for scope in resource.scope_spans
        for span in scope.spans
    ]
    assert len(spans) == (1 if oversized else 127)
    assert all(span.name == "GET" for span in spans)


def test_native_exception_admission_reports_loss_and_keeps_safe_spans(
    otlp_receiver, caplog
):
    from django_altimeter.exporting import SanitizingSession, create_span_exporter

    session = SanitizingSession("traces", otlp_receiver.url("traces"), policy=POLICY)
    provider = TracerProvider(
        resource=Resource({}), shutdown_on_exit=False, span_limits=native_span_limits()
    )
    processor = BoundedSpanProcessor(
        create_span_exporter(session), schedule_delay_millis=60_000
    )
    provider.add_span_processor(processor)
    tracer = provider.get_tracer("django_altimeter")
    try:
        for size in (50, 240, 260, 600, 90):
            message = "界" * size if size == 90 else (CANARY + "x" * size)[:size]
            try:
                with tracer.start_as_current_span("request"):
                    raise RuntimeError(message)
            except RuntimeError as error:
                assert str(error) == message
                assert error.__traceback__ is not None
        with tracer.start_as_current_span("request", attributes={"private": [CANARY]}):
            pass
        with tracer.start_as_current_span(
            "request", attributes={"http.response.status_code": 200}
        ):
            pass
        assert provider.force_flush(timeout_millis=5_000)
    finally:
        provider.shutdown()
    requests = otlp_receiver.wait_for_requests()
    spans = [
        span
        for request in requests
        for resource in request.message.resource_spans
        for scope in resource.scope_spans
        for span in scope.spans
    ]
    assert len(spans) == 3
    assert [span.status.code for span in spans] == [2, 2, 0]
    assert spans[-1].attributes[0].value.int_value == 200
    assert len({span.span_id for span in spans}) == 3
    assert all(span.name == "request" for span in spans)
    assert all(
        CANARY.encode() not in request.message.SerializeToString()
        for request in requests
    )
    diagnostics = [
        record.getMessage()
        for record in caplog.records
        if record.name == "django_altimeter.export"
    ]
    assert diagnostics == ["admission.size traces", "admission.type traces"]
    assert processor.local_dropped_records == 4
    assert CANARY not in caplog.text
