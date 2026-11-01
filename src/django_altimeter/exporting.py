"""Read-only size/type admission before one native batch processor per signal.

Construction is explicit; importing this module creates no processor or thread.
Admission does not sanitize: project generated OTLP before external export.
Only register the wrapper on a provider. The delegate is never registered too.
The estimate includes retained resource/scope/event/link data and fixed overhead;
it is a payload estimate, not a measurement of Python heap usage.
"""

from __future__ import annotations

import logging
from collections.abc import Callable, Mapping, Sequence
from contextvars import ContextVar
from dataclasses import dataclass, replace
from datetime import UTC, datetime
from email.utils import format_datetime, parsedate_to_datetime
from io import BytesIO
from math import isfinite
from os import dup
from socket import SHUT_RDWR, socket
from sys import exception
from threading import Lock, Timer
from time import monotonic
from traceback import clear_frames
from typing import TYPE_CHECKING, Any, Literal, cast
from weakref import ReferenceType, ref

import requests
from google.protobuf.message import Message
from opentelemetry._logs import LogRecord, SeverityNumber
from opentelemetry.attributes import BoundedAttributes
from opentelemetry.context import Context
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
from opentelemetry.sdk.resources import Resource
from opentelemetry.sdk.trace import Event, ReadableSpan, Span, SpanLimits, SpanProcessor
from opentelemetry.sdk.trace.export import (
    BatchSpanProcessor,
    SpanExporter,
    SpanExportResult,
)
from opentelemetry.sdk.util import BoundedList
from opentelemetry.sdk.util.instrumentation import (
    InstrumentationInfo,
    InstrumentationScope,
)
from opentelemetry.sdk.version import __version__ as sdk_version
from opentelemetry.trace import (
    Link,
    SpanContext,
    SpanKind,
    Status,
    StatusCode,
    TraceFlags,
    TraceState,
)
from urllib3.exceptions import ReadTimeoutError, SSLError
from urllib3.util import Timeout

from django_altimeter.privacy import (
    DEFAULT_LIMITS,
    DEFAULT_POLICY,
    PrivacyLimits,
    PrivacyPolicy,
    is_valid_configuration,
    project_logs,
    project_metrics,
    project_traces,
)

if TYPE_CHECKING:
    from opentelemetry.exporter.otlp.proto.http._log_exporter import OTLPLogExporter
    from opentelemetry.exporter.otlp.proto.http.metric_exporter import (
        OTLPMetricExporter,
    )
    from opentelemetry.exporter.otlp.proto.http.trace_exporter import OTLPSpanExporter
    from opentelemetry.metrics import MeterProvider
    from opentelemetry.sdk._logs import (
        LogRecordLimits,
        LogRecordProcessor,
        ReadableLogRecord,
        ReadWriteLogRecord,
    )
    from opentelemetry.sdk._logs.export import LogRecordExporter, LogRecordExportResult
    from opentelemetry.sdk.metrics.export import (
        AggregationTemporality,
        MetricExportResult,
        MetricsData,
    )


class _Rejected(ValueError):
    """Carries only a fixed diagnostic code, never a rejected value."""


def native_span_limits(limits: PrivacyLimits = DEFAULT_LIMITS) -> SpanLimits:
    """Use alongside admission when constructing the native TracerProvider.

    SDK lengths count characters; admission also enforces the UTF-8 byte cap.
    """
    _check_limits(limits)
    return SpanLimits(
        max_attributes=limits.max_attributes,
        max_span_attributes=limits.max_attributes,
        max_event_attributes=limits.max_attributes,
        max_link_attributes=limits.max_attributes,
        max_events=limits.max_events,
        max_links=limits.max_links,
        max_attribute_length=limits.max_string_bytes,
        max_span_attribute_length=limits.max_string_bytes,
    )


def native_log_limits(limits: PrivacyLimits = DEFAULT_LIMITS) -> LogRecordLimits:
    """Use alongside admission when constructing the native LoggerProvider."""
    _check_limits(limits)
    from opentelemetry.sdk._logs import LogRecordLimits

    return LogRecordLimits(
        max_attributes=limits.max_attributes,
        max_log_record_attributes=limits.max_attributes,
        max_attribute_length=limits.max_string_bytes,
        max_log_record_attribute_length=limits.max_string_bytes,
    )


def _check_limits(limits: PrivacyLimits) -> None:
    if not is_valid_configuration(DEFAULT_POLICY, limits):
        raise ValueError("invalid admission limits")


def _text_size(value: object, ceiling: int) -> int:
    if type(value) is not str:
        raise _Rejected("admission.type")
    # A UTF-8 encoding cannot be shorter than the number of code points. Never
    # encode an unbounded value merely to discover that it was too large.
    if len(value) > ceiling:
        raise _Rejected("admission.size")
    size = len(value.encode("utf-8"))
    if size > ceiling:
        raise _Rejected("admission.size")
    return size


def _scalar_size(value: object, limits: PrivacyLimits) -> int:
    if type(value) is str:
        return _text_size(value, limits.max_string_bytes)
    if type(value) is bool:
        return 1
    if type(value) is int and -(1 << 63) <= value < (1 << 63):
        return 8
    if type(value) is float and isfinite(value):
        return 8
    # There is no approved nested schema yet. Reject containers without walking
    # them, including custom mappings/sequences with hostile accessors.
    raise _Rejected("admission.type")


def _within_record(size: int, limits: PrivacyLimits) -> int:
    if size > limits.max_record_bytes:
        raise _Rejected("admission.size")
    return size


def _attributes_size(
    attributes: object,
    limits: PrivacyLimits,
    *,
    resource: bool = False,
) -> int:
    if attributes is None:
        return 0
    # The pinned SDK's mutable BoundedAttributes.__iter__ copies all keys.
    # Read its backing dict without copying or mutating it. Unknown wrappers
    # are refused before invoking any user-defined len/iteration methods.
    if type(attributes) is BoundedAttributes:
        attributes = attributes._dict
    if type(attributes) is not dict:
        raise _Rejected("admission.type")
    ceiling = limits.max_resource_attributes if resource else limits.max_attributes
    if len(attributes) > ceiling:
        raise _Rejected("admission.size")
    values = cast(Mapping[object, object], attributes)
    size = 0
    for index, (key, value) in enumerate(values.items()):
        if index >= ceiling:
            raise _Rejected("admission.size")
        size += 16 + _text_size(key, limits.max_key_bytes) + _scalar_size(value, limits)
        _within_record(size, limits)
    return size


def _resource_size(resource: object, limits: PrivacyLimits) -> int:
    if type(resource) is not Resource:
        raise _Rejected("admission.type")
    size = _text_size(resource.schema_url, limits.max_string_bytes)
    return 32 + size + _attributes_size(resource.attributes, limits, resource=True)


def _scope_size(
    scope: object,
    limits: PrivacyLimits,
) -> int:
    if scope is None:
        return 0
    if (
        type(scope) is not InstrumentationScope
        and type(scope) is not InstrumentationInfo
    ):
        raise _Rejected("admission.type")
    size = _text_size(scope.name, limits.max_string_bytes)
    for value in (scope.version, scope.schema_url):
        if value is not None:
            size += _text_size(value, limits.max_string_bytes)
    if type(scope) is InstrumentationScope:
        size += _attributes_size(scope.attributes, limits)
    return 32 + size


def _context_size(context: object, limits: PrivacyLimits) -> int:
    if context is None:
        return 0
    if type(context) is not SpanContext:
        raise _Rejected("admission.type")
    if type(context.is_remote) is not bool or type(context.is_valid) is not bool:
        raise _Rejected("admission.type")
    state = context.trace_state
    if type(state) is not TraceState or type(state._dict) is not dict:
        raise _Rejected("admission.type")
    entries = state._dict
    if len(entries) > limits.max_attributes:
        raise _Rejected("admission.size")
    size = 32
    for index, (key, value) in enumerate(entries.items()):
        if index >= limits.max_attributes:
            raise _Rejected("admission.size")
        size += 16 + _text_size(key, limits.max_key_bytes)
        size += _text_size(value, limits.max_string_bytes)
        _within_record(size, limits)
    _uint(context.trace_id, 128)
    _uint(context.span_id, 64)
    _flags(context.trace_flags)
    return size


def _flags(value: object) -> None:
    if value is None:
        return
    if (
        type(value) is not int and type(value) is not TraceFlags
    ) or not 0 <= value <= 255:
        raise _Rejected("admission.type")


def _uint(value: object, bits: int, *, optional: bool = False) -> None:
    if value is None and optional:
        return
    if type(value) is not int or not 0 <= value < (1 << bits):
        raise _Rejected("admission.type")


def _sequence(values: object, ceiling: int) -> Sequence[object]:
    if all(type(values) is not kind for kind in (tuple, list, BoundedList)):
        raise _Rejected("admission.type")
    values = cast(Sequence[object], values)
    if len(values) > ceiling:
        raise _Rejected("admission.size")
    return values


def _span_size(span: ReadableSpan, limits: PrivacyLimits) -> int:
    if type(span) is not ReadableSpan:
        raise _Rejected("admission.type")
    size = 256 + _text_size(span.name, limits.max_string_bytes)
    if (
        type(span.status) is not Status
        or type(span.status.status_code) is not StatusCode
    ):
        raise _Rejected("admission.type")
    if span.status.description is not None:
        size += _text_size(span.status.description, limits.max_string_bytes)
    if type(span.kind) is not SpanKind:
        raise _Rejected("admission.type")
    _uint(span.start_time, 64)
    _uint(span.end_time, 64)
    size += _context_size(span.context, limits) + _context_size(span.parent, limits)
    size += _resource_size(span.resource, limits)
    size += _scope_size(span.instrumentation_scope, limits)
    size += _scope_size(span._instrumentation_info, limits)
    size += _attributes_size(span._attributes, limits)
    _within_record(size, limits)
    # ReadableSpan.events/links materialize tuples before callers can inspect
    # their lengths. These narrow reads are pinned to OTel SDK 1.45.x. Indexed
    # traversal also avoids BoundedList.__iter__ creating another list.
    events = _sequence(span._events, limits.max_events)
    for index in range(len(events)):
        event = events[index]
        if type(event) is not Event:
            raise _Rejected("admission.type")
        size += 32 + _text_size(event.name, limits.max_string_bytes)
        _uint(event.timestamp, 64)
        size += _attributes_size(event.attributes, limits)
        _within_record(size, limits)
    links = _sequence(span._links, limits.max_links)
    for index in range(len(links)):
        link = links[index]
        if type(link) is not Link:
            raise _Rejected("admission.type")
        size += 32 + _context_size(link.context, limits)
        size += _attributes_size(link.attributes, limits)
        _within_record(size, limits)
    return size


def _log_size(record: ReadWriteLogRecord, limits: PrivacyLimits) -> int:
    from opentelemetry.sdk._logs import ReadWriteLogRecord

    if (
        type(record) is not ReadWriteLogRecord
        or type(record.log_record) is not LogRecord
    ):
        raise _Rejected("admission.type")
    log = record.log_record
    size = 256
    if log.body is not None:
        size += _text_size(log.body, limits.max_string_bytes)
    if log.event_name is not None:
        size += _text_size(log.event_name, limits.max_string_bytes)
    if log.severity_text is not None:
        size += _text_size(log.severity_text, limits.max_string_bytes)
    if (
        log.severity_number is not None
        and type(log.severity_number) is not SeverityNumber
    ):
        raise _Rejected("admission.type")
    if log.exception is not None:
        raise _Rejected("admission.type")
    _uint(log.timestamp, 64, optional=True)
    _uint(log.observed_timestamp, 64)
    _uint(log.trace_id, 128, optional=True)
    _uint(log.span_id, 64, optional=True)
    _flags(log.trace_flags)
    size += _resource_size(record.resource, limits)
    size += _scope_size(record.instrumentation_scope, limits)
    size += _attributes_size(log.attributes, limits)
    return _within_record(size, limits)


class BoundedSpanProcessor(SpanProcessor):
    """Own exactly one native batch processor; register only this wrapper."""

    def __init__(
        self,
        exporter: SpanExporter,
        *,
        limits: PrivacyLimits = DEFAULT_LIMITS,
        schedule_delay_millis: float = 5000,
        meter_provider: MeterProvider | None = None,
    ) -> None:
        _check_limits(limits)
        self._limits = limits
        self.last_diagnostic: str | None = None
        self._local_dropped_records = 0
        self._drop_lock = Lock()
        self._drop_diagnostics: dict[str, float] = {}
        self._pending_diagnostics, self._report_diagnostics = _defer_export_diagnostics(
            exporter
        )
        self._delegate = BatchSpanProcessor(
            exporter,
            max_queue_size=limits.max_queue_size,
            max_export_batch_size=limits.max_batch_size,
            schedule_delay_millis=schedule_delay_millis,
            meter_provider=meter_provider,
        )

    @property
    def local_dropped_records(self) -> int:
        """Cumulative admission losses for this processor, before batching."""
        with self._drop_lock:
            return self._local_dropped_records

    def _record_drop(self, code: str) -> None:
        if code not in {"admission.size", "admission.type", "span.admission_error"}:
            code = "span.admission_error"
        self.last_diagnostic = code
        try:
            with self._drop_lock:
                self._local_dropped_records += 1
                now = monotonic()
                previous = self._drop_diagnostics.get(code)
                if previous is not None and now - previous < 60:
                    return
                self._drop_diagnostics[code] = now
            # Log outside the lock, after throttling, including for reentrant
            # handlers. Neither the dropped record nor its values are retained.
            _DIAGNOSTIC_LOGGER.warning("%s traces", code)
        except Exception:
            pass  # A diagnostic handler cannot break the application span.

    def on_start(self, span: Span, parent_context: Context | None = None) -> None:
        if self._pending_diagnostics:
            self._report_diagnostics()
        try:
            self._delegate.on_start(span, parent_context=parent_context)
        except Exception:
            self.last_diagnostic = "span.start_error"

    def on_end(self, span: ReadableSpan) -> None:
        if self._pending_diagnostics:
            self._report_diagnostics()
        try:
            _span_size(span, self._limits)
            self._delegate.on_end(span)
        except _Rejected as error:
            self._record_drop(str(error))
        except Exception:
            self._record_drop("span.admission_error")

    def force_flush(self, timeout_millis: int = 30000) -> bool:
        try:
            return self._delegate.force_flush(timeout_millis)
        except Exception:
            self.last_diagnostic = "span.flush_error"
            return False
        finally:
            if self._pending_diagnostics:
                self._report_diagnostics()

    def shutdown(self) -> None:
        try:
            cast(SpanProcessor, self._delegate).shutdown()
        except Exception:
            self.last_diagnostic = "span.shutdown_error"
        finally:
            if self._pending_diagnostics:
                self._report_diagnostics()


def bounded_log_processor(
    exporter: LogRecordExporter,
    *,
    limits: PrivacyLimits = DEFAULT_LIMITS,
    schedule_delay_millis: float = 5000,
    meter_provider: MeterProvider | None = None,
) -> LogRecordProcessor:
    """Construct native log admission explicitly, keeping module imports inert.

    In SDK 1.45.x, importing sdk._logs runs resource detection in worker threads.
    Keep those imports at this construction boundary, never at module import.
    """
    _check_limits(limits)
    from opentelemetry.sdk._logs import LogRecordProcessor
    from opentelemetry.sdk._logs.export import BatchLogRecordProcessor

    class _BoundedLogRecordProcessor(LogRecordProcessor):
        def __init__(self) -> None:
            self._limits = limits
            self.last_diagnostic: str | None = None
            self._pending_diagnostics, self._report_diagnostics = (
                _defer_export_diagnostics(exporter)
            )
            self._delegate = BatchLogRecordProcessor(
                exporter,
                max_queue_size=limits.max_queue_size,
                max_export_batch_size=limits.max_batch_size,
                schedule_delay_millis=schedule_delay_millis,
                meter_provider=meter_provider,
            )

        def on_emit(self, log_record: ReadWriteLogRecord) -> None:
            if self._pending_diagnostics:
                self._report_diagnostics()
            try:
                _log_size(log_record, self._limits)
                self._delegate.on_emit(log_record)
            except _Rejected as error:
                self.last_diagnostic = str(error)
            except Exception:
                self.last_diagnostic = "log.admission_error"

        def force_flush(self, timeout_millis: int = 30000) -> bool:
            try:
                return self._delegate.force_flush(timeout_millis)
            except Exception:
                self.last_diagnostic = "log.flush_error"
                return False
            finally:
                if self._pending_diagnostics:
                    self._report_diagnostics()

        def shutdown(self) -> None:
            try:
                cast(LogRecordProcessor, self._delegate).shutdown()
            except Exception:
                self.last_diagnostic = "log.shutdown_error"
            finally:
                if self._pending_diagnostics:
                    self._report_diagnostics()

    return _BoundedLogRecordProcessor()


# Native exporters call this session once per attempt; they own delivery/retries.
# Keep SDK log exporter imports inside its factory (SDK log imports start a
# resource-detector thread in 1.45.1).
type Signal = Literal["traces", "logs", "metrics"]

_DIAGNOSTIC_LOGGER = logging.getLogger("django_altimeter.export")
_DIAGNOSTIC_CODES = frozenset(
    {
        "export.not_sent",
        "export.batch_count",
        "export.batch_size",
        "export.request",
        "export.destination",
        "export.encoding",
        "export.content_type",
        "export.projection",
        "export.empty",
        "export.http_error",
        "export.status",
        "export.response_size",
        "export.rejected_count",
        "export.partial",
        "export.warning",
        "export.deadline",
        "export.connection",
        "export.invalid",
        "export.failed",
        "export.local_drop",
        "export.framing",
        "metrics.timeout_unsupported",
    }
)


@dataclass(frozen=True)
class ExportOutcome:
    """Latest attempt only, not durable ingestion or a cumulative retry counter.

    Zero accepted on a failure means acceptance is unknown, not proof of loss.
    Counts describe spans, log records or metric data points for this session.
    """

    code: str = "export.not_sent"
    accepted_records: int = 0
    rejected_records: int = 0
    local_dropped_records: int = 0


_OUTCOME_PRIORITY = {
    "export.not_sent": 0,
    "export.empty": 0,
    "export.accepted": 0,
    "export.warning": 1,
    "export.partial": 2,
    "export.local_drop": 3,
}


def _worst_outcome(
    previous: ExportOutcome | None, latest: ExportOutcome
) -> ExportOutcome:
    if previous is None or _OUTCOME_PRIORITY.get(
        latest.code, 3
    ) >= _OUTCOME_PRIORITY.get(previous.code, 3):
        return latest
    return previous


class ExportProtocolError(Exception):
    """Fixed local/protocol code. The native HTTP client must not retry this."""


def _record_count(message: Message, signal: Signal, ceiling: int) -> int:
    """Bound every container before walking even a decoded, byte-bounded batch."""
    suffix = "spans" if signal == "traces" else signal
    groups = getattr(message, f"resource_{suffix}")
    if len(groups) > ceiling:
        raise ExportProtocolError("export.batch_count")
    scopes_left = instruments_left = ceiling
    count = 0
    for group in groups:
        scopes = getattr(group, f"scope_{suffix}")
        if len(scopes) > scopes_left:
            raise ExportProtocolError("export.batch_count")
        scopes_left -= len(scopes)
        for scope in scopes:
            if signal == "metrics":
                if len(scope.metrics) > instruments_left:
                    raise ExportProtocolError("export.batch_count")
                instruments_left -= len(scope.metrics)
                for metric in scope.metrics:
                    kind = metric.WhichOneof("data")
                    if kind is not None:
                        count += len(getattr(metric, kind).data_points)
                        if count > ceiling:
                            raise ExportProtocolError("export.batch_count")
            else:
                count += len(scope.spans if signal == "traces" else scope.log_records)
                if count > ceiling:
                    raise ExportProtocolError("export.batch_count")
    return count


def _safe_retry_after(value: str | None) -> str | None:
    if value is None or len(value) > 128:
        return None
    try:
        seconds = float(value)
    except ValueError:
        try:
            date = parsedate_to_datetime(value)
            if date.tzinfo is None:
                date = date.replace(tzinfo=UTC)
            if (date - datetime.now(UTC)).total_seconds() < 1:
                return None
            return format_datetime(date.astimezone(UTC), usegmt=True)
        except (TypeError, ValueError, OverflowError):
            return None
    return value if isfinite(seconds) and seconds >= 1 else None


def _safe_response(status: int, retry_after: str | None = None) -> requests.Response:
    # A detached response keeps server prose, cookies, request credentials and
    # network references out of the native client's result. Cache only empty bytes.
    result = requests.Response()
    result.status_code = status
    result.reason = "OTLP response"
    if retry_after is not None:
        result.headers["Retry-After"] = retry_after
    result.raw = BytesIO(b"")
    _ = result.content
    result.raw.close()
    result.close()
    return result


def _remaining(deadline: float) -> float:
    remaining = deadline - monotonic()
    if remaining <= 0:
        raise ExportProtocolError("export.deadline")
    return remaining


def _is_protobuf(headers: Mapping[str, str]) -> bool:
    content_type = headers.get("Content-Type", "").split(";", 1)[0]
    return content_type.strip().lower() == "application/x-protobuf"


def _read_response(response: requests.Response, ceiling: int, deadline: float) -> bytes:
    transfer = response.headers.get("Transfer-Encoding")
    if transfer is not None and (
        transfer.strip().lower() != "chunked" or "Content-Length" in response.headers
    ):
        raise ExportProtocolError("export.framing")
    declared = response.headers.get("Content-Length")
    expected: int | None = None
    if declared is not None:
        if len(declared) > 20 or not declared.isascii() or not declared.isdecimal():
            raise ExportProtocolError("export.framing")
        expected = int(declared)
        if expected > ceiling:
            raise ExportProtocolError("export.response_size")
        if expected == 0:
            _remaining(deadline)
            return b""
    # read timeouts measure inactivity, so a slow peer could otherwise keep a
    # read alive indefinitely. A duplicate FD permits shutdown even for TLS and
    # HTTP/1.0 without reaching through urllib3's private socket attributes.
    # Its lifetime ends before releasing the response/connection for reuse.
    with socket(fileno=dup(response.raw.fileno())) as connection:

        def interrupt() -> None:
            try:
                connection.shutdown(SHUT_RDWR)
            except OSError:
                pass  # A completed peer close is already an interrupted read.

        timer = Timer(_remaining(deadline), interrupt)
        timer.daemon = True
        timer.name = "altimeter-response-deadline"
        timer.start()
        try:
            body = bytearray()
            while True:
                _remaining(deadline)
                chunk = response.raw.read(
                    min(8192, ceiling + 1 - len(body)), decode_content=False
                )
                _remaining(deadline)
                if not chunk:
                    # urllib3 1.26 does not enforce Content-Length by default.
                    if expected is not None and len(body) != expected:
                        raise ExportProtocolError("export.invalid")
                    return bytes(body)
                body.extend(chunk)
                if len(body) > ceiling:
                    raise ExportProtocolError("export.response_size")
        finally:
            timer.cancel()
            timer.join()


@dataclass(frozen=True)
class _ProjectedBatch:
    source: bytes
    payload: bytes
    count: int
    dropped: int
    signal: Signal
    policy: PrivacyPolicy
    limits: PrivacyLimits


class _ProjectionScope:
    """Bounded native-export state, explicitly emptied in every copied context."""

    def __init__(self) -> None:
        self.batch: _ProjectedBatch | None = None
        self.terminal_outcome: ExportOutcome | None = None
        self.active = True
        self.requests_started = 0
        # Copied contexts may run on another thread while the export finishes.
        self.lock = Lock()

    def record_terminal(self, outcome: ExportOutcome) -> None:
        with self.lock:
            if self.active:
                self.terminal_outcome = _worst_outcome(self.terminal_outcome, outcome)

    def close(self) -> ExportOutcome | None:
        with self.lock:
            outcome = self.terminal_outcome
            self.active = False
            self.batch = None
            self.terminal_outcome = None
            return outcome


class SanitizingSession(requests.Session):
    """One signal and endpoint, projected bytes, bounded truthful OTLP responses.

    Use only through a native exporter constructed below. Protocol failures are
    fixed exceptions caught by that exporter, not application-facing hooks.
    The budget covers connect/header waits and streamed bodies. A peer that
    continuously trickles header bytes is a known requests limitation: the body
    watchdog starts only once headers exist. No absolute header-drip bound is
    claimed. There is at most one joined watchdog per active response.
    """

    def __init__(
        self,
        signal: str,
        endpoint: str,
        *,
        policy: PrivacyPolicy = DEFAULT_POLICY,
        limits: PrivacyLimits = DEFAULT_LIMITS,
    ) -> None:
        if (
            type(signal) is not str
            or signal not in ("traces", "logs", "metrics")
            or not endpoint
        ):
            raise ValueError("invalid export destination")
        if not is_valid_configuration(policy, limits):
            raise ValueError("invalid export policy or limits")
        super().__init__()
        self.signal = cast(Signal, signal)
        self.endpoint = endpoint
        self.policy = policy
        self.limits = limits
        self.trust_env = False
        self.headers.clear()
        self._configured_headers: dict[str, str] = {}
        self._outcome = ExportOutcome()
        self._diagnostic_times: dict[str, float] = {}
        self._diagnostic_lock = Lock()
        self._pending_diagnostics: dict[str, None] = {}
        self._diagnostics_deferred = False
        self._reporting_diagnostics = False
        self._deadline: ContextVar[float | None] = ContextVar(
            "altimeter_export_deadline", default=None
        )
        # Native HTTP1.45.x returns exceptions whose traceback frames retain the
        # completed serialized batch. Keep only weak references during export;
        # each factory clears completed frames after its one native export call.
        self._failures: ContextVar[tuple[ReferenceType[Exception], ...]] = ContextVar(
            "altimeter_export_failures", default=()
        )
        # Native exports own independent holders; copied contexts share the
        # holder so final cleanup also removes their retained input/output.
        self._projection_scope: ContextVar[_ProjectionScope | None] = ContextVar(
            "altimeter_export_projection", default=None
        )

    def _configure_transport(self, headers: Mapping[str, str] | None) -> None:
        # Configuration owns header validation. Keep its explicit values separate
        # from the native exporter, which merges ambient OTEL_* credentials.
        from django_altimeter.conf import _headers

        self._configured_headers = dict(_headers(headers))
        self.headers.clear()
        # The native constructor writes environment-derived TLS settings here.
        self.verify = True
        self.cert = None

    @property
    def outcome(self) -> ExportOutcome:
        return self._outcome

    def _remember_failure(self, error: Exception) -> None:
        try:
            # The pinned native client makes <=6 retry iterations and at most
            # two submissions each. No unbounded exception/traceback registry.
            self._failures.set((*self._failures.get()[-15:], ref(error)))
        except Exception:
            self._outcome = replace(self._outcome, code="export.failed")

    def _clear_failure_frames(self, caller_exception: BaseException | None) -> None:
        failures = self._failures.get()
        self._failures.set(())
        try:
            for failure in failures:
                error: BaseException | None = failure()
                # Native transport failures can chain through Requests/urllib3.
                # The exception active at export entry predates all of them:
                # never traverse or clear that caller-owned chain or its frames.
                for _ in range(8):
                    if error is None or error is caller_exception:
                        break
                    chained = error.__cause__ or error.__context__
                    if error.__traceback__ is not None:
                        clear_frames(error.__traceback__)
                    error.__traceback__ = None
                    error.__cause__ = None
                    error.__context__ = None
                    error = chained
        except Exception:
            self._outcome = replace(self._outcome, code="export.failed")

    def _export_completed(self, outcome: ExportOutcome | None = None) -> None:
        """Finish native export with its worst terminal batch, after retries."""
        self._emit_diagnostic()
        if not self._diagnostics_deferred and self._projection_scope.get() is None:
            self.report_pending()

    def _emit_diagnostic(self) -> None:
        # Retain fixed codes only; native callers still hold their export locks.
        try:
            code = self._outcome.code
            if code not in _DIAGNOSTIC_CODES:
                return
            with self._diagnostic_lock:
                last = self._diagnostic_times.get(code)
                if last is None or monotonic() - last >= 60:
                    self._pending_diagnostics[code] = None
            if not self._diagnostics_deferred and self._projection_scope.get() is None:
                self.report_pending()
        except Exception:
            pass  # Diagnostic failure must not change export behavior.

    def report_pending(self) -> None:
        """Deliver queued codes only at a boundary outside native export locks.

        Batch workers have no public completion hook. Their owner drains here
        on the next processor call, flush or shutdown. The pending dict is shared
        with processors so their empty request path is one unlocked flag read.
        """
        try:
            with self._diagnostic_lock:
                if self._reporting_diagnostics or not self._pending_diagnostics:
                    return
                codes = tuple(self._pending_diagnostics)
                self._pending_diagnostics.clear()
                now = monotonic()
                for code in codes:
                    self._diagnostic_times[code] = now
                self._reporting_diagnostics = True
            try:
                for code in codes:
                    try:
                        _DIAGNOSTIC_LOGGER.warning("%s %s", code, self.signal)
                    except Exception:
                        pass  # Application handlers cannot change export behavior.
            finally:
                with self._diagnostic_lock:
                    self._reporting_diagnostics = False
        except Exception:
            pass  # Fixed diagnostics are best effort, including during teardown.

    def _report_outcome(self, code: str, local_dropped_records: int = 0) -> None:
        self._outcome = ExportOutcome(code, local_dropped_records=local_dropped_records)
        self._emit_diagnostic()

    def _native_export[B, R](
        self, native_export: Callable[[B], R], batch: B, record_count: int
    ) -> R:
        """Account for native rejection before the transport is reached."""
        scope = self._projection_scope.get()
        if scope is None:
            return native_export(batch)
        with scope.lock:
            before = scope.requests_started
        try:
            return native_export(batch)
        finally:
            with scope.lock:
                unsent = scope.requests_started == before
            if unsent and record_count:
                self._report_outcome("export.local_drop", record_count)
                scope.record_terminal(self.outcome)

    def _export_in_scope[B, R](
        self,
        native_export: Callable[[Sequence[B]], R],
        batch: Sequence[B],
        *,
        count_drops: bool,
    ) -> R:
        """Run one native span or log export in a fresh projection scope."""
        caller_exception = exception()
        self._outcome = ExportOutcome()
        failures_token = self._failures.set(())
        projection_scope = _ProjectionScope()
        projection_token = self._projection_scope.set(projection_scope)
        try:
            return self._native_export(
                native_export, batch, len(batch) if count_drops else 0
            )
        finally:
            projection_scope.close()
            self._projection_scope.reset(projection_token)
            self._clear_failure_frames(caller_exception)
            self._failures.reset(failures_token)
            self._export_completed()

    def _project_for_attempt(self, data: bytes) -> tuple[bytes, int, int]:
        signal, policy, limits = self.signal, self.policy, self.limits
        scope = self._projection_scope.get()
        if scope is not None:
            with scope.lock:
                cached = scope.batch
                if scope.active and cached is not None:
                    if (
                        cached.signal == signal
                        and cached.policy is policy
                        and cached.limits is limits
                        and cached.source == data
                    ):
                        return cached.payload, cached.count, cached.dropped
                    # A different batch replaces the only entry. A failed
                    # projection must leave neither input nor output cached.
                    scope.batch = None
                cached = None
        projected, count, dropped = self._project(data)
        if scope is not None:
            with scope.lock:
                if scope.active:
                    scope.batch = _ProjectedBatch(
                        data,
                        projected,
                        count,
                        dropped,
                        signal,
                        policy,
                        limits,
                    )
        return projected, count, dropped

    def _project(self, data: bytes) -> tuple[bytes, int, int]:
        source: Message
        projected: Message
        if self.signal == "traces":
            source = ExportTraceServiceRequest.FromString(data)
        elif self.signal == "logs":
            source = ExportLogsServiceRequest.FromString(data)
        else:
            source = ExportMetricsServiceRequest.FromString(data)
        before = _record_count(source, self.signal, self.limits.max_batch_size)
        if isinstance(source, ExportTraceServiceRequest):
            projected = project_traces(source, policy=self.policy, limits=self.limits)
        elif isinstance(source, ExportLogsServiceRequest):
            projected = project_logs(source, policy=self.policy, limits=self.limits)
        else:
            projected = project_metrics(source, policy=self.policy, limits=self.limits)
        if type(projected) is not type(source):
            raise ExportProtocolError("export.projection")
        if projected.ByteSize() > self.limits.max_batch_bytes:
            raise ExportProtocolError("export.batch_size")
        after = _record_count(projected, self.signal, self.limits.max_batch_size)
        if after > before:
            raise ExportProtocolError("export.projection")
        if after == 0:
            self._outcome = ExportOutcome("export.empty", local_dropped_records=before)
            raise ExportProtocolError("export.empty")
        return projected.SerializeToString(), after, before - after

    def _response(
        self, response: requests.Response, deadline: float, count: int, dropped: int
    ) -> requests.Response:
        try:
            _remaining(deadline)
            status = response.status_code
            if 400 <= status <= 599:
                self._report_outcome("export.http_error", dropped)
                return _safe_response(
                    status, _safe_retry_after(response.headers.get("Retry-After"))
                )
            if status != 200:
                raise ExportProtocolError("export.status")
            if (
                response.headers.get("Content-Encoding", "identity").strip().lower()
                != "identity"
            ):
                raise ExportProtocolError("export.encoding")
            if not _is_protobuf(response.headers):
                raise ExportProtocolError("export.content_type")
            body = _read_response(response, self.limits.max_response_bytes, deadline)
            if self.signal == "traces":
                trace_partial = ExportTraceServiceResponse.FromString(
                    body
                ).partial_success
                rejected = trace_partial.rejected_spans
                warning = bool(trace_partial.error_message)
            elif self.signal == "logs":
                log_partial = ExportLogsServiceResponse.FromString(body).partial_success
                rejected = log_partial.rejected_log_records
                warning = bool(log_partial.error_message)
            else:
                metric_partial = ExportMetricsServiceResponse.FromString(
                    body
                ).partial_success
                rejected = metric_partial.rejected_data_points
                warning = bool(metric_partial.error_message)
            if not 0 <= rejected <= count:
                raise ExportProtocolError("export.rejected_count")
            code = "export.accepted"
            if rejected:
                code = "export.partial"
            elif warning:
                code = "export.warning"
            outcome = ExportOutcome(code, count - rejected, rejected, dropped)
            self._outcome = outcome
            if self.signal == "metrics" and (scope := self._projection_scope.get()):
                # A validated 200 ends this batch. HTTP failures can still retry;
                # their final outcome is considered when native export finishes.
                scope.record_terminal(outcome)
            self._emit_diagnostic()
            return _safe_response(200)
        finally:
            response.close()

    def request(
        self, method: str, url: str | bytes, *args: Any, **kwargs: Any
    ) -> requests.Response:
        self._outcome = ExportOutcome()
        scope = self._projection_scope.get()
        if scope is not None:
            with scope.lock:
                if scope.active:
                    scope.requests_started += 1
        deadline = monotonic()
        try:
            timeout = kwargs.get("timeout")
            if (
                (type(timeout) is not int and type(timeout) is not float)
                or not isfinite(timeout)
                or timeout <= 0
            ):
                raise ExportProtocolError("export.deadline")
            deadline += timeout
            if (caller_deadline := self._deadline.get()) is not None:
                deadline = min(deadline, caller_deadline)
            _remaining(deadline)
            if args or set(kwargs) - {
                "headers",
                "data",
                "timeout",
                "allow_redirects",
                "stream",
            }:
                raise ExportProtocolError("export.request")
            if method != "POST" or url != self.endpoint:
                raise ExportProtocolError("export.destination")
            headers = requests.structures.CaseInsensitiveDict(kwargs.get("headers"))
            if not _is_protobuf(headers):
                raise ExportProtocolError("export.content_type")
            if headers.get("Content-Encoding", "identity").lower() != "identity":
                raise ExportProtocolError("export.encoding")
            data = kwargs.get("data")
            if type(data) is not bytes:
                raise ExportProtocolError("export.request")
            if len(data) > self.limits.max_batch_bytes:
                raise ExportProtocolError("export.batch_size")
            projected, count, dropped = self._project_for_attempt(data)
            self._outcome = ExportOutcome(local_dropped_records=dropped)
            request_headers = {
                "Content-Type": "application/x-protobuf",
                "User-Agent": f"OTel-OTLP-Exporter-Python/{sdk_version}",
                **self._configured_headers,
                "Accept-Encoding": "identity",
            }

            # A hook runs before requests' redirect-resolution/body buffering
            # (which also runs with allow_redirects=False) and cookie persistence.
            def response_hook(
                response: requests.Response, **_: Any
            ) -> requests.Response:
                return self._response(response, deadline, count, dropped)

            remaining = _remaining(deadline)
            return super().request(
                "POST",
                self.endpoint,
                data=projected,
                headers=request_headers,
                # HTTPAdapter supports urllib3 Timeout; requests' annotation
                # only lists its scalar/tuple shortcuts.
                timeout=cast(
                    Any, Timeout(total=remaining, connect=remaining, read=remaining)
                ),
                allow_redirects=False,
                stream=True,
                hooks={"response": response_hook},
            )
        except ExportProtocolError as error:
            self._remember_failure(error)
            self._report_outcome(str(error), self._outcome.local_dropped_records)
            raise
        except (requests.ConnectionError, requests.Timeout, ReadTimeoutError, SSLError):
            if monotonic() >= deadline:
                self._report_outcome(
                    "export.deadline", self._outcome.local_dropped_records
                )
                failure = ExportProtocolError("export.deadline")
                self._remember_failure(failure)
                raise failure from None
            self._report_outcome(
                "export.connection", self._outcome.local_dropped_records
            )
            connection_failure = requests.ConnectionError("export.connection")
            self._remember_failure(connection_failure)
            raise connection_failure from None
        except Exception:
            code = "export.deadline" if monotonic() >= deadline else "export.invalid"
            self._report_outcome(code, self._outcome.local_dropped_records)
            failure = ExportProtocolError(code)
            self._remember_failure(failure)
            raise failure from None


def _defer_export_diagnostics(
    exporter: object,
) -> tuple[dict[str, None], Callable[[], None]]:
    """Bind our processors/readers without changing arbitrary native exporters."""
    session = getattr(exporter, "_altimeter_session", None)
    if isinstance(session, SanitizingSession):
        session._diagnostics_deferred = True
        return session._pending_diagnostics, session.report_pending
    return {}, lambda: None


def create_span_exporter(
    session: SanitizingSession,
    *,
    timeout: float = 10.0,
    headers: Mapping[str, str] | None = None,
    meter_provider: MeterProvider | None = None,
) -> OTLPSpanExporter:
    from opentelemetry.exporter.otlp.proto.http import Compression
    from opentelemetry.exporter.otlp.proto.http.trace_exporter import OTLPSpanExporter

    if session.signal != "traces":
        raise ValueError("span exporter requires trace session")

    class _SanitizingSpanExporter(OTLPSpanExporter):
        _altimeter_session = session

        def export(self, spans: Sequence[ReadableSpan]) -> SpanExportResult:
            return session._export_in_scope(
                super().export,
                spans,
                # SDK1.45.x: calls after shutdown never start a native batch.
                count_drops=getattr(self, "_shutdown", False) is not True,
            )

    exporter = _SanitizingSpanExporter(
        endpoint=session.endpoint,
        session=session,
        timeout=timeout,
        headers=headers,
        compression=Compression.NoCompression,
        max_request_size=session.limits.max_batch_bytes,
        meter_provider=meter_provider,
    )
    session._configure_transport(headers)
    return exporter


def create_log_exporter(
    session: SanitizingSession,
    *,
    timeout: float = 10.0,
    headers: Mapping[str, str] | None = None,
    meter_provider: MeterProvider | None = None,
) -> OTLPLogExporter:
    from opentelemetry.exporter.otlp.proto.http import Compression
    from opentelemetry.exporter.otlp.proto.http._log_exporter import OTLPLogExporter

    if session.signal != "logs":
        raise ValueError("log exporter requires log session")

    class _SanitizingLogExporter(OTLPLogExporter):
        _altimeter_session = session

        def export(self, batch: Sequence[ReadableLogRecord]) -> LogRecordExportResult:
            return session._export_in_scope(
                super().export,
                batch,
                count_drops=getattr(self, "_shutdown", False) is not True,
            )

    exporter = _SanitizingLogExporter(
        endpoint=session.endpoint,
        session=session,
        timeout=timeout,
        headers=headers,
        compression=Compression.NoCompression,
        max_request_size=session.limits.max_batch_bytes,
        meter_provider=meter_provider,
    )
    session._configure_transport(headers)
    return exporter


def create_metric_exporter(
    session: SanitizingSession,
    *,
    timeout: float = 10.0,
    headers: Mapping[str, str] | None = None,
    meter_provider: MeterProvider | None = None,
    preferred_temporality: dict[type, AggregationTemporality] | None = None,
) -> OTLPMetricExporter:
    from opentelemetry.exporter.otlp.proto.http import Compression
    from opentelemetry.exporter.otlp.proto.http.metric_exporter import (
        OTLPMetricExporter,
    )

    if session.signal != "metrics":
        raise ValueError("metric exporter requires metric session")
    from opentelemetry.sdk.metrics.export import MetricExportResult

    class _DeadlineMetricExporter(OTLPMetricExporter):
        _altimeter_session = session

        def __init__(self) -> None:
            super().__init__(
                endpoint=session.endpoint,
                session=session,
                timeout=timeout,
                headers=headers,
                compression=Compression.NoCompression,
                max_request_size=session.limits.max_batch_bytes,
                max_export_batch_size=session.limits.max_batch_size,
                meter_provider=meter_provider,
                preferred_temporality=preferred_temporality,
            )
            self._export_lock = Lock()
            self._timeout_supported = self._native_timeout() is not None
            if not self._timeout_supported:
                session._report_outcome("metrics.timeout_unsupported")

        def _export_batch(
            self, export_request: ExportMetricsServiceRequest
        ) -> MetricExportResult:
            # SDK1.45.x splits before this hook; count only this native batch.
            count = _record_count(
                export_request, "metrics", session.limits.max_batch_size
            )
            return session._native_export(super()._export_batch, export_request, count)

        def _native_timeout(self) -> float | int | None:
            value = getattr(getattr(self, "_client", None), "_timeout", None)
            if (
                (type(value) is float or type(value) is int)
                and isfinite(value)
                and value > 0
            ):
                return value
            return None

        def export(
            self,
            metrics_data: MetricsData,
            timeout_millis: float | None = 10_000,
            **kwargs: Any,
        ) -> MetricExportResult:
            caller_exception = exception()
            session._outcome = ExportOutcome()
            if (
                timeout_millis is not None
                and type(timeout_millis) is not int
                and type(timeout_millis) is not float
            ):
                session._report_outcome("export.deadline")
                return MetricExportResult.FAILURE
            budget = timeout if timeout_millis is None else timeout_millis / 1000
            if not isfinite(budget) or budget <= 0:
                session._report_outcome("export.deadline")
                return MetricExportResult.FAILURE
            deadline = monotonic() + min(timeout, budget)
            if not self._export_lock.acquire(timeout=max(0, deadline - monotonic())):
                session._report_outcome("export.deadline")
                return MetricExportResult.FAILURE
            token = session._deadline.set(deadline)
            failures_token = session._failures.set(())
            projection_scope = _ProjectionScope()
            projection_token = session._projection_scope.set(projection_scope)
            original = self._native_timeout()
            supported = self._timeout_supported and original is not None
            patched = False
            try:
                remaining = _remaining(deadline)
                if supported:
                    # SDK1.45.x: opentelemetry/exporter/otlp/common/http.py:185
                    # bases native retry/backoff on _timeout; metric export ignores
                    # timeout_millis. Cap only under this lock, restore below.
                    self._client._timeout = min(cast(float, original), remaining)
                    patched = True
                return super().export(metrics_data, timeout_millis, **kwargs)
            except Exception:
                # Includes a missing/changed upstream attribute: keep its
                # native path unpatched, but never expose a telemetry failure.
                session._outcome = ExportOutcome("export.failed")
                return MetricExportResult.FAILURE
            finally:
                terminal_outcome = projection_scope.close()
                if patched:
                    self._client._timeout = cast(float, original)
                if not supported:
                    session._outcome = replace(
                        session._outcome, code="metrics.timeout_unsupported"
                    )
                session._projection_scope.reset(projection_token)
                session._clear_failure_frames(caller_exception)
                session._failures.reset(failures_token)
                session._deadline.reset(token)
                completed_outcome = _worst_outcome(terminal_outcome, session.outcome)
                self._export_lock.release()
                session._export_completed(completed_outcome)

    exporter = _DeadlineMetricExporter()
    session._configure_transport(headers)
    return exporter
