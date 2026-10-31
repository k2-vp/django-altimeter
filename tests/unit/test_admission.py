"""Unit evidence for admission before the native SDK batch queues."""

from collections.abc import Iterator

import pytest
from opentelemetry._logs import LogRecord, SeverityNumber
from opentelemetry.exporter.otlp.proto.common._log_encoder import encode_logs
from opentelemetry.exporter.otlp.proto.common.trace_encoder import encode_spans
from opentelemetry.sdk._logs import LogRecordProcessor, ReadWriteLogRecord
from opentelemetry.sdk._logs.export import InMemoryLogRecordExporter
from opentelemetry.sdk.resources import Resource
from opentelemetry.sdk.trace import Event, ReadableSpan
from opentelemetry.sdk.trace.export.in_memory_span_exporter import InMemorySpanExporter
from opentelemetry.trace import Link, SpanContext, Status, StatusCode, TraceFlags

from django_altimeter.exporting import BoundedSpanProcessor, bounded_log_processor
from django_altimeter.privacy import PrivacyPolicy, project_logs, project_traces

POLICY = PrivacyPolicy(
    span_names=frozenset({"request"}),
    event_names=frozenset({"request.complete"}),
    log_bodies=frozenset({"request.complete"}),
    attribute_values=frozenset(
        {("service.name", "admission-test"), ("http.route", "/boards/{board_id}")}
    ),
)

CONTEXT = SpanContext(1, 2, False, TraceFlags(TraceFlags.SAMPLED))
CANARY = "password=admission-secret-canary"


def span(**changes: object) -> ReadableSpan:
    fields = {
        "name": "request",
        "context": CONTEXT,
        "resource": Resource({}),
        "start_time": 1,
        "end_time": 2,
        "attributes": {"http.response.status_code": 200},
    }
    fields.update(changes)
    return ReadableSpan(**fields)


@pytest.fixture
def span_pipeline() -> Iterator[tuple[BoundedSpanProcessor, InMemorySpanExporter]]:
    exporter = InMemorySpanExporter()
    processor = BoundedSpanProcessor(
        exporter,
        schedule_delay_millis=60_000,
    )
    try:
        yield processor, exporter
    finally:
        processor.shutdown()


@pytest.mark.parametrize(
    "changes,admitted",
    [
        ({"name": "x" * 257}, False),
        ({"name": CANARY}, True),
        ({"attributes": {"password": CANARY}}, True),
        ({"attributes": {"http.route": "x" * 257}}, False),
        ({"status": Status(StatusCode.ERROR, CANARY)}, True),
        ({"resource": Resource({"password": CANARY})}, True),
        ({"events": [Event(CANARY)]}, True),
        ({"links": [Link(CONTEXT, {"password": CANARY})]}, True),
        ({"events": [Event("request")] * 9}, False),
        ({"links": [Link(CONTEXT)] * 5}, False),
    ],
    ids=[
        "name-size",
        "name-secret",
        "attribute-secret",
        "attribute-size",
        "status",
        "resource",
        "event",
        "link",
        "event-count",
        "link-count",
    ],
)
def test_span_bounds_then_projection_preserve_safe_positive(
    span_pipeline: tuple[BoundedSpanProcessor, InMemorySpanExporter],
    changes: dict[str, object],
    admitted: bool,
) -> None:
    processor, exporter = span_pipeline
    safe = span()
    candidate = span(**changes)
    processor.on_end(candidate)
    processor.on_end(safe)
    assert processor.force_flush()
    records = exporter.get_finished_spans()
    assert records == ((candidate, safe) if admitted else (safe,))
    if admitted:
        raw = encode_spans(records)
        assert CANARY.encode() in raw.SerializeToString()
        projected = project_traces(raw, policy=POLICY)
        assert CANARY.encode() not in projected.SerializeToString()
        projected_spans = [
            s
            for r in projected.resource_spans
            for sc in r.scope_spans
            for s in sc.spans
        ]
        assert len(projected_spans) == 2
        assert projected_spans[-1].name == "request"
        assert {a.key: a.value.int_value for a in projected_spans[-1].attributes} == {
            "http.response.status_code": 200
        }
    assert safe.name == "request"
    assert safe.attributes == {"http.response.status_code": 200}


@pytest.fixture
def log_pipeline() -> Iterator[tuple[LogRecordProcessor, InMemoryLogRecordExporter]]:
    exporter = InMemoryLogRecordExporter()
    processor = bounded_log_processor(
        exporter,
        schedule_delay_millis=60_000,
    )
    try:
        yield processor, exporter
    finally:
        processor.shutdown()


def log_record(
    body: object = "request.complete", **changes: object
) -> ReadWriteLogRecord:
    record = LogRecord(
        body=body,
        severity_number=SeverityNumber.INFO,
        attributes={"http.response.status_code": 200},
    )
    for name, value in changes.items():
        setattr(record, name, value)
    return ReadWriteLogRecord(record, resource=Resource({}))


@pytest.mark.parametrize("body", ["x" * 257, CANARY, {"password": CANARY}, [CANARY]])
def test_log_bounds_then_projection_preserve_safe_positive(
    log_pipeline: tuple[LogRecordProcessor, InMemoryLogRecordExporter],
    body: object,
) -> None:
    processor, exporter = log_pipeline
    safe = log_record()
    processor.on_emit(log_record(body))
    processor.on_emit(safe)
    assert processor.force_flush()
    records = exporter.get_finished_logs()
    admitted = body == CANARY
    assert len(records) == (2 if admitted else 1)
    assert records[-1].log_record.body == "request.complete"
    assert records[-1].log_record.attributes == {"http.response.status_code": 200}
    assert safe.log_record.body == "request.complete"
    if admitted:
        raw = encode_logs(records)
        assert CANARY.encode() in raw.SerializeToString()
        projected = project_logs(raw, policy=POLICY)
        assert CANARY.encode() not in projected.SerializeToString()
        projected_logs = [
            r
            for g in projected.resource_logs
            for sc in g.scope_logs
            for r in sc.log_records
        ]
        assert len(projected_logs) == 2
        assert not projected_logs[0].body.WhichOneof("value")
        assert projected_logs[-1].body.string_value == "request.complete"


class HostileValue:
    """Any traversal or string conversion of unknown input is a test failure."""

    touched = False

    def __len__(self):
        self.touched = True
        raise AssertionError("Unknown container was inspected")

    def __iter__(self):
        self.touched = True
        raise AssertionError("Unknown container was walked")

    def __str__(self):
        self.touched = True
        raise AssertionError("Unknown value was formatted")


@pytest.mark.parametrize("surface", ["attributes", "value", "events", "links"])
def test_unknown_span_containers_are_rejected_without_inspection(
    span_pipeline,
    surface,
) -> None:
    processor, exporter = span_pipeline
    hostile = HostileValue()
    changes = (
        {"attributes": {"http.route": hostile}}
        if surface == "value"
        else {surface: hostile}
    )
    processor.on_end(span(**changes))
    safe = span()
    processor.on_end(safe)
    assert processor.force_flush()
    assert exporter.get_finished_spans() == (safe,)
    assert hostile.touched is False


@pytest.mark.parametrize("surface", ["body", "attributes", "value", "exception"])
def test_unknown_log_objects_are_rejected_without_inspection(
    log_pipeline,
    surface,
) -> None:
    processor, exporter = log_pipeline
    hostile = HostileValue()
    unsafe = log_record()
    value = {"http.route": hostile} if surface == "value" else hostile
    setattr(unsafe.log_record, "attributes" if surface == "value" else surface, value)
    processor.on_emit(unsafe)
    processor.on_emit(log_record())
    assert processor.force_flush()
    assert [record.log_record.body for record in exporter.get_finished_logs()] == [
        "request.complete"
    ]
    assert hostile.touched is False


@pytest.mark.parametrize(
    "value",
    [float("inf"), float("nan"), 1 << 100_000, [1], {"a": 1}],
    ids=["infinity", "nan", "oversized-int", "array", "nested"],
)
def test_invalid_scalar_does_not_reach_either_queue(span_pipeline, log_pipeline, value):
    spans, span_exporter = span_pipeline
    logs, log_exporter = log_pipeline
    spans.on_end(span(attributes={"duration_ms": value}))
    unsafe_log = log_record()
    unsafe_log.log_record.attributes = {"duration_ms": value}
    logs.on_emit(unsafe_log)
    safe = span()
    spans.on_end(safe)
    logs.on_emit(log_record())
    assert spans.force_flush() and logs.force_flush()
    assert span_exporter.get_finished_spans() == (safe,)
    assert len(log_exporter.get_finished_logs()) == 1


def test_utf8_ceiling_applies_to_unregistered_names():

    positive = "🙂" * 64
    oversized = "🙂" * 65
    exporter = InMemorySpanExporter()
    processor = BoundedSpanProcessor(
        exporter,
    )
    safe = span(name=positive)
    try:
        processor.on_end(span(name=oversized))
        processor.on_end(safe)
        assert processor.force_flush()
        assert exporter.get_finished_spans() == (safe,)
    finally:
        processor.shutdown()


def test_whole_span_estimate_includes_events_and_resources():
    from dataclasses import replace

    from django_altimeter.privacy import DEFAULT_LIMITS

    exporter = InMemorySpanExporter()
    processor = BoundedSpanProcessor(
        exporter,
        limits=replace(DEFAULT_LIMITS, max_record_bytes=600),
    )
    safe = span()
    unsafe = span(
        resource=Resource({"service.name": "admission-test"}),
        events=[Event("request.complete", {"http.route": "/boards/{board_id}"})] * 8,
    )
    try:
        processor.on_end(unsafe)
        processor.on_end(safe)
        assert processor.force_flush()
        assert exporter.get_finished_spans() == (safe,)
        assert processor.last_diagnostic == "admission.size"
    finally:
        processor.shutdown()


def test_whole_log_estimate_includes_body_and_resource():
    from dataclasses import replace

    from django_altimeter.privacy import DEFAULT_LIMITS

    exporter = InMemoryLogRecordExporter()
    large_body = "a" * 256
    processor = bounded_log_processor(
        exporter,
        limits=replace(DEFAULT_LIMITS, max_record_bytes=400),
    )
    try:
        processor.on_emit(log_record(large_body))
        processor.on_emit(log_record())
        assert processor.force_flush()
        assert len(exporter.get_finished_logs()) == 1
        assert processor.last_diagnostic == "admission.size"
    finally:
        processor.shutdown()


def test_admission_reads_sdk_containers_without_copying_or_mutation(
    span_pipeline,
    monkeypatch,
):
    from opentelemetry.attributes import BoundedAttributes
    from opentelemetry.sdk.util import BoundedList

    processor, exporter = span_pipeline
    attributes = BoundedAttributes(attributes={"http.response.status_code": 200})
    events = BoundedList(8)
    events.append(Event("request.complete"))
    links = BoundedList(4)
    links.append(Link(CONTEXT))
    safe = span(attributes=attributes, events=events, links=links)

    def forbid_copy(*args):
        raise AssertionError("Admission attempted a copy via SDK iterator")

    monkeypatch.setattr(BoundedAttributes, "__iter__", forbid_copy)
    monkeypatch.setattr(BoundedList, "__iter__", forbid_copy)
    processor.on_end(safe)
    assert processor.force_flush()
    assert exporter.get_finished_spans() == (safe,)
    assert safe._attributes is attributes
    assert safe._events is events
    assert safe._links is links
    assert len(events) == 1 and len(links) == 1
    assert attributes["http.response.status_code"] == 200


def test_log_admission_preserves_record_and_context(log_pipeline):
    from opentelemetry.context import Context

    processor, exporter = log_pipeline
    safe = log_record()
    safe.log_record.context = Context({"application_data": "private local context"})
    context = safe.log_record.context
    attributes = safe.log_record.attributes
    processor.on_emit(safe)
    assert processor.force_flush()
    assert safe.log_record.context is context
    assert safe.log_record.attributes is attributes
    assert exporter.get_finished_logs()[0].log_record.context == Context()


@pytest.mark.parametrize("method", ["on_start", "on_end", "force_flush", "shutdown"])
def test_span_public_hooks_absorb_delegate_failures(span_pipeline, monkeypatch, method):
    from opentelemetry.sdk.trace.export import BatchSpanProcessor

    processor, exporter = span_pipeline
    calls = []

    def fail(*args, **kwargs):
        calls.append(method)
        raise RuntimeError(CANARY)

    with monkeypatch.context() as patch:
        patch.setattr(BatchSpanProcessor, method, fail)
        if method in {"on_start", "on_end"}:
            getattr(processor, method)(span())
        else:
            result = getattr(processor, method)()
            if method == "force_flush":
                assert result is False
    assert calls == [method]
    assert processor.last_diagnostic in {
        "span.start_error",
        "span.admission_error",
        "span.flush_error",
        "span.shutdown_error",
    }
    safe = span()
    processor.on_end(safe)
    assert processor.force_flush()
    assert exporter.get_finished_spans() == (safe,)


@pytest.mark.parametrize("method", ["on_emit", "force_flush", "shutdown"])
def test_log_public_hooks_absorb_delegate_failures(log_pipeline, monkeypatch, method):
    from opentelemetry.sdk._logs.export import BatchLogRecordProcessor

    processor, exporter = log_pipeline
    calls = []

    def fail(*args, **kwargs):
        calls.append(method)
        raise RuntimeError(CANARY)

    with monkeypatch.context() as patch:
        patch.setattr(BatchLogRecordProcessor, method, fail)
        if method == "on_emit":
            processor.on_emit(log_record())
        else:
            result = getattr(processor, method)()
            if method == "force_flush":
                assert result is False
    assert calls == [method]
    assert processor.last_diagnostic in {
        "log.admission_error",
        "log.flush_error",
        "log.shutdown_error",
    }
    processor.on_emit(log_record())
    assert processor.force_flush()
    assert len(exporter.get_finished_logs()) == 1


def test_span_lifecycle_forwards_arguments_and_results(span_pipeline, monkeypatch):
    from opentelemetry.context import Context
    from opentelemetry.sdk.trace.export import BatchSpanProcessor

    processor, _ = span_pipeline
    record = span()
    parent = Context({"test": 1})
    calls = []
    with monkeypatch.context() as patch:
        patch.setattr(
            BatchSpanProcessor,
            "on_start",
            lambda self, s, parent_context: calls.append((s, parent_context)),
        )
        patch.setattr(
            BatchSpanProcessor,
            "force_flush",
            lambda self, timeout: calls.append(timeout) or False,
        )
        patch.setattr(
            BatchSpanProcessor, "shutdown", lambda self: calls.append("shutdown")
        )
        processor.on_start(record, parent)
        assert processor.force_flush(123) is False
        processor.shutdown()
    assert calls == [(record, parent), 123, "shutdown"]


def test_log_lifecycle_forwards_timeout_and_shutdown(log_pipeline, monkeypatch):
    from opentelemetry.sdk._logs.export import BatchLogRecordProcessor

    processor, _ = log_pipeline
    calls = []
    with monkeypatch.context() as patch:
        patch.setattr(
            BatchLogRecordProcessor,
            "force_flush",
            lambda self, timeout: calls.append(timeout) or False,
        )
        patch.setattr(
            BatchLogRecordProcessor, "shutdown", lambda self: calls.append("shutdown")
        )
        assert processor.force_flush(456) is False
        processor.shutdown()
    assert calls == [456, "shutdown"]


def test_trace_flags_cannot_retain_oversized_objects(span_pipeline, log_pipeline):
    spans, span_exporter = span_pipeline
    logs, log_exporter = log_pipeline
    unsafe_context = SpanContext(1, 2, False, TraceFlags((1 << 100_000) | 1))
    spans.on_end(span(context=unsafe_context))
    unsafe_log = log_record()
    unsafe_log.log_record.trace_flags = 1 << 100_000
    logs.on_emit(unsafe_log)
    safe = span()
    spans.on_end(safe)
    logs.on_emit(log_record())
    assert spans.force_flush() and logs.force_flush()
    assert span_exporter.get_finished_spans() == (safe,)
    assert len(log_exporter.get_finished_logs()) == 1


@pytest.mark.parametrize("signal", ["span", "log"])
@pytest.mark.parametrize(
    "field,value",
    [("max_record_bytes", 1 << 40), ("max_string_bytes", -1), ("max_queue_size", 513)],
)
def test_invalid_limits_fail_before_starting_batch_thread(
    monkeypatch, signal, field, value
):
    import threading
    from dataclasses import replace

    from django_altimeter.privacy import DEFAULT_LIMITS

    def forbid_thread(*args, **kwargs):
        pytest.fail("Invalid limits started a batch thread")

    monkeypatch.setattr(threading.Thread, "start", forbid_thread)
    limits = replace(DEFAULT_LIMITS, **{field: value})
    with pytest.raises(ValueError, match="limits"):
        if signal == "span":
            BoundedSpanProcessor(InMemorySpanExporter(), limits=limits)
        else:
            bounded_log_processor(InMemoryLogRecordExporter(), limits=limits)


@pytest.mark.parametrize("signal", ["span", "log"])
def test_one_native_batch_uses_limits_dataclass(monkeypatch, signal):
    from dataclasses import replace

    import opentelemetry.sdk._logs.export as log_export

    import django_altimeter.exporting as exporting
    from django_altimeter.privacy import DEFAULT_LIMITS

    monkeypatch.setenv("OTEL_BSP_MAX_QUEUE_SIZE", "9000")
    monkeypatch.setenv("OTEL_BLRP_MAX_QUEUE_SIZE", "9000")
    monkeypatch.setenv("OTEL_BSP_MAX_EXPORT_BATCH_SIZE", "8000")
    monkeypatch.setenv("OTEL_BLRP_MAX_EXPORT_BATCH_SIZE", "8000")
    limits = replace(DEFAULT_LIMITS, max_queue_size=7, max_batch_size=3)
    calls = []
    native_class = (
        exporting.BatchSpanProcessor
        if signal == "span"
        else log_export.BatchLogRecordProcessor
    )
    original = native_class.__init__

    def record_constructor(self, *args, **kwargs):
        calls.append(kwargs)
        original(self, *args, **kwargs)

    monkeypatch.setattr(native_class, "__init__", record_constructor)
    processor = (
        BoundedSpanProcessor(InMemorySpanExporter(), limits=limits)
        if signal == "span"
        else bounded_log_processor(InMemoryLogRecordExporter(), limits=limits)
    )
    try:
        assert len(calls) == 1
        assert calls[0]["max_queue_size"] == 7
        assert calls[0]["max_export_batch_size"] == 3
    finally:
        processor.shutdown()


def test_native_span_and_log_limits_override_sdk_environment(monkeypatch):
    from django_altimeter.exporting import native_log_limits, native_span_limits

    for name in (
        "OTEL_ATTRIBUTE_COUNT_LIMIT",
        "OTEL_ATTRIBUTE_VALUE_LENGTH_LIMIT",
        "OTEL_SPAN_EVENT_COUNT_LIMIT",
        "OTEL_SPAN_LINK_COUNT_LIMIT",
        "OTEL_LOGRECORD_ATTRIBUTE_COUNT_LIMIT",
        "OTEL_LOGRECORD_ATTRIBUTE_VALUE_LENGTH_LIMIT",
    ):
        monkeypatch.setenv(name, "9000")
    span_limits = native_span_limits()
    log_limits = native_log_limits()
    assert span_limits.max_span_attributes == 32
    assert span_limits.max_event_attributes == 32
    assert span_limits.max_link_attributes == 32
    assert span_limits.max_events == 8
    assert span_limits.max_links == 4
    assert span_limits.max_attribute_length == 256
    assert span_limits.max_span_attribute_length == 256
    assert log_limits.max_log_record_attributes == 32
    assert log_limits.max_log_record_attribute_length == 256


@pytest.mark.parametrize(
    "changes",
    [
        {"attributes": {str(i): "a" for i in range(33)}},
        {"resource": Resource({str(i): "a" for i in range(17)})},
        {"attributes": {"a" * 129: "a"}},
        {"events": [Event("request.complete", {"a" * 129: "a"})]},
        {"links": [Link(CONTEXT, {"a" * 129: "a"})]},
    ],
    ids=["attribute-count", "resource-count", "key-bytes", "event-key", "link-key"],
)
def test_all_span_surfaces_enforce_counts_and_key_sizes(span_pipeline, changes):
    processor, exporter = span_pipeline
    processor.on_end(span(**changes))
    safe = span()
    processor.on_end(safe)
    assert processor.force_flush()
    assert exporter.get_finished_spans() == (safe,)
    assert processor.last_diagnostic == "admission.size"


@pytest.mark.parametrize(
    "surface", ["resource", "scope", "event_name", "severity_text", "attributes"]
)
def test_all_log_surfaces_are_admitted_then_projected(log_pipeline, surface):
    from opentelemetry.sdk.util.instrumentation import InstrumentationScope

    processor, exporter = log_pipeline
    unsafe = log_record()
    if surface == "resource":
        unsafe.resource = Resource({"service.name": CANARY})
    elif surface == "scope":
        unsafe.instrumentation_scope = InstrumentationScope(CANARY)
    elif surface == "attributes":
        unsafe.log_record.attributes = {"authorization": CANARY}
    else:
        setattr(unsafe.log_record, surface, CANARY)
    processor.on_emit(unsafe)
    processor.on_emit(log_record())
    assert processor.force_flush()
    records = exporter.get_finished_logs()
    assert len(records) == 2
    raw = encode_logs(records)
    assert CANARY.encode() in raw.SerializeToString()
    projected = project_logs(raw, policy=POLICY)
    assert CANARY.encode() not in projected.SerializeToString()
    projected_logs = [
        r
        for g in projected.resource_logs
        for sc in g.scope_logs
        for r in sc.log_records
    ]
    assert len(projected_logs) == 2
    assert all(r.body.string_value == "request.complete" for r in projected_logs)
    assert {a.key: a.value.int_value for a in projected_logs[-1].attributes} == {
        "http.response.status_code": 200
    }


@pytest.mark.parametrize("signal", ["span", "log"])
def test_bounded_string_encoding_failure_is_fail_open(
    span_pipeline, log_pipeline, signal
):
    if signal == "span":
        processor, exporter = span_pipeline
        processor.on_end(span(name="\ud800"))
        safe = span()
        processor.on_end(safe)
        assert processor.force_flush()
        assert exporter.get_finished_spans() == (safe,)
    else:
        processor, exporter = log_pipeline
        processor.on_emit(log_record("\ud800"))
        processor.on_emit(log_record())
        assert processor.force_flush()
        assert len(exporter.get_finished_logs()) == 1


@pytest.mark.parametrize("surface", ["attributes", "resource", "scope"])
def test_log_retained_surface_sizes_are_bounded(log_pipeline, surface):
    from opentelemetry.sdk.util.instrumentation import InstrumentationScope

    processor, exporter = log_pipeline
    unsafe = log_record()
    if surface == "attributes":
        unsafe.log_record.attributes = {str(i): "x" for i in range(33)}
    elif surface == "resource":
        unsafe.resource = Resource({str(i): "x" for i in range(17)})
    else:
        unsafe.instrumentation_scope = InstrumentationScope("x" * 257)
    processor.on_emit(unsafe)
    processor.on_emit(log_record())
    assert processor.force_flush()
    assert len(exporter.get_finished_logs()) == 1
    assert processor.last_diagnostic == "admission.size"


@pytest.mark.parametrize("location", ["context", "parent", "link"])
def test_non_boolean_context_fields_cannot_bypass_retained_size(
    span_pipeline, location
):
    processor, exporter = span_pipeline
    malformed = SpanContext(1, 2, bytearray(1_048_576), TraceFlags(1))
    changes = (
        {location: malformed} if location != "link" else {"links": [Link(malformed)]}
    )
    processor.on_end(span(**changes))
    safe = span()
    processor.on_end(safe)
    assert processor.force_flush()
    assert exporter.get_finished_spans() == (safe,)
    assert processor.last_diagnostic == "admission.type"


@pytest.mark.parametrize("surface", ["events", "links", "scope", "flags"])
def test_unknown_metaclasses_are_not_consulted_by_type_checks(span_pipeline, surface):
    processor, exporter = span_pipeline
    comparisons = []

    class HostileMeta(type):
        def __eq__(cls, other):
            comparisons.append(other)
            raise RuntimeError("Unknown type equality was invoked")

    class Unknown(metaclass=HostileMeta):
        pass

    value = Unknown()
    if surface == "scope":
        changes = {"instrumentation_scope": value}
    elif surface == "flags":
        changes = {"context": SpanContext(1, 2, False, value)}
    else:
        changes = {surface: value}
    processor.on_end(span(**changes))
    safe = span()
    processor.on_end(safe)
    assert processor.force_flush()
    assert exporter.get_finished_spans() == (safe,)
    assert comparisons == []
    assert processor.last_diagnostic == "admission.type"


def test_bounded_scope_schema_status_and_tracestate_reach_projection(span_pipeline):
    from opentelemetry.sdk.util.instrumentation import InstrumentationScope
    from opentelemetry.trace import TraceState

    processor, exporter = span_pipeline
    state_canary = "state-secret-canary"
    context = SpanContext(
        1, 2, False, TraceFlags(1), TraceState([("vendor", state_canary)])
    )
    candidate = span(
        context=context,
        parent=context,
        links=[Link(context)],
        resource=Resource({"service.name": "admission-test"}, schema_url=CANARY),
        instrumentation_scope=InstrumentationScope("unregistered", CANARY, CANARY),
        status=Status(StatusCode.ERROR, CANARY),
    )
    processor.on_end(candidate)
    assert processor.force_flush()
    assert exporter.get_finished_spans() == (candidate,)
    raw = encode_spans(exporter.get_finished_spans())
    assert CANARY.encode() in raw.SerializeToString()
    assert state_canary.encode() in raw.SerializeToString()
    projected = project_traces(raw, policy=POLICY)
    assert CANARY.encode() not in projected.SerializeToString()
    assert state_canary.encode() not in projected.SerializeToString()
    safe = projected.resource_spans[0].scope_spans[0].spans[0]
    assert safe.name == "request" and safe.status.code == 2
    assert safe.trace_id == (1).to_bytes(16, "big")
    assert safe.parent_span_id == (2).to_bytes(8, "big")
    assert safe.links[0].span_id == (2).to_bytes(8, "big")
    assert {a.key: a.value.int_value for a in safe.attributes} == {
        "http.response.status_code": 200
    }


@pytest.mark.parametrize("limit", ["record", "count", "key", "value"])
def test_tracestate_is_counted_within_limits_and_not_copied(limit):
    from dataclasses import replace

    from opentelemetry.trace import TraceState

    from django_altimeter.privacy import DEFAULT_LIMITS

    limits = DEFAULT_LIMITS
    if limit == "record":
        limits = replace(limits, max_record_bytes=600)
        state = TraceState([("vendor", "x" * 250), ("another", "y" * 250)])
    elif limit == "count":
        limits = replace(limits, max_attributes=1)
        state = TraceState([("vendor", "x"), ("another", "y")])
    elif limit == "key":
        state = TraceState([("v" * 129, "x")])
    else:
        limits = replace(limits, max_string_bytes=32)
        state = TraceState([("vendor", "x" * 33)])
    candidate = span(context=SpanContext(1, 2, False, TraceFlags(1), state))
    safe = span()
    exporter = InMemorySpanExporter()
    processor = BoundedSpanProcessor(exporter, limits=limits)
    try:
        processor.on_end(candidate)
        processor.on_end(safe)
        assert processor.force_flush()
        assert exporter.get_finished_spans() == (safe,)
        assert candidate.context.trace_state is state
        assert processor.last_diagnostic == "admission.size"
    finally:
        processor.shutdown()


@pytest.mark.parametrize(
    "surface",
    [
        "status",
        "resource-schema",
        "scope-name",
        "scope-version",
        "scope-schema",
        "event-name",
    ],
)
def test_newly_admitted_metadata_still_obeys_utf8_size_limit(span_pipeline, surface):
    from opentelemetry.sdk.util.instrumentation import InstrumentationScope

    processor, exporter = span_pipeline
    oversized = "é" * 129
    if surface == "status":
        changes = {"status": Status(StatusCode.ERROR, oversized)}
    elif surface == "resource-schema":
        changes = {"resource": Resource({}, schema_url=oversized)}
    elif surface == "event-name":
        changes = {"events": [Event(oversized)]}
    else:
        names = {
            "scope-name": "name",
            "scope-version": "version",
            "scope-schema": "schema_url",
        }
        values = {"name": "unregistered", names[surface]: oversized}
        changes = {"instrumentation_scope": InstrumentationScope(**values)}
    processor.on_end(span(**changes))
    safe = span()
    processor.on_end(safe)
    assert processor.force_flush()
    assert exporter.get_finished_spans() == (safe,)
    assert processor.last_diagnostic == "admission.size"


@pytest.mark.parametrize(
    "changes,code",
    [
        ({"attributes": {"private": CANARY + "界" * 128}}, "admission.size"),
        ({"attributes": {"private": [CANARY]}}, "admission.type"),
        ({"status": Status(StatusCode.ERROR, CANARY + "x" * 600)}, "admission.size"),
    ],
)
def test_rejected_span_is_counted_and_reports_only_a_bounded_code(
    span_pipeline, caplog, changes, code
):
    processor, exporter = span_pipeline
    rejected = span(**changes)
    safe = span()
    for _ in range(100):
        processor.on_end(rejected)
    processor.on_end(safe)
    assert processor.force_flush()
    assert exporter.get_finished_spans() == (safe,)
    diagnostics = [
        record.getMessage()
        for record in caplog.records
        if record.name == "django_altimeter.export"
    ]
    assert diagnostics == [f"{code} traces"]
    assert processor.local_dropped_records == 100
    assert processor.last_diagnostic == code
    assert CANARY not in caplog.text


def test_admission_drop_diagnostics_are_rate_limited_and_resume(
    span_pipeline, caplog, monkeypatch
):
    import django_altimeter.exporting as exporting

    processor, _ = span_pipeline
    rejected = span(name="x" * 257)
    monkeypatch.setattr(exporting, "monotonic", lambda: 10.0)
    processor.on_end(rejected)
    processor.on_end(rejected)
    monkeypatch.setattr(exporting, "monotonic", lambda: 70.0)
    processor.on_end(rejected)
    diagnostics = [
        record.getMessage()
        for record in caplog.records
        if record.name == "django_altimeter.export"
    ]
    assert diagnostics == ["admission.size traces", "admission.size traces"]
    assert processor.local_dropped_records == 3


def test_admission_drop_count_survives_threads_and_diagnostic_failure(span_pipeline):
    import logging
    from concurrent.futures import ThreadPoolExecutor

    processor, exporter = span_pipeline
    rejected = span(attributes={"private": [CANARY]})
    safe = span()

    class FailingHandler(logging.Handler):
        def emit(self, record):
            raise RuntimeError(CANARY)

    logger = logging.getLogger("django_altimeter.export")
    handler = FailingHandler()
    logger.addHandler(handler)
    try:
        with ThreadPoolExecutor(max_workers=8) as pool:
            list(pool.map(processor.on_end, [rejected] * 1000))
    finally:
        logger.removeHandler(handler)
    processor.on_end(safe)
    assert processor.force_flush()
    assert exporter.get_finished_spans() == (safe,)
    assert processor.local_dropped_records == 1000
    assert processor.last_diagnostic == "admission.type"
