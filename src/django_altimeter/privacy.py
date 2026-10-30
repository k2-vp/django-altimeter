"""Pure, bounded OTLP reconstruction. No SDK object mutation or runtime setup.

Static string values are admitted by exact configured pairs. Opaque identity has
a separate fixed rule on span/log records only. The caller owns the static
configuration; do not populate it from request data.
Projection is the final boundary, not a replacement for admission before batching
or metric Views/cardinality admission before aggregation.
"""

from collections.abc import Sequence
from dataclasses import dataclass, fields
from math import isfinite
from typing import TypeGuard

from opentelemetry.proto.collector.logs.v1.logs_service_pb2 import (
    ExportLogsServiceRequest,
)
from opentelemetry.proto.collector.metrics.v1.metrics_service_pb2 import (
    ExportMetricsServiceRequest,
)
from opentelemetry.proto.collector.trace.v1.trace_service_pb2 import (
    ExportTraceServiceRequest,
)
from opentelemetry.proto.common.v1.common_pb2 import (
    AnyValue,
    InstrumentationScope,
    KeyValue,
)
from opentelemetry.proto.logs.v1.logs_pb2 import LogRecord, SeverityNumber
from opentelemetry.proto.metrics.v1.metrics_pb2 import (
    Exemplar,
    ExponentialHistogramDataPoint,
    HistogramDataPoint,
    Metric,
    NumberDataPoint,
    SummaryDataPoint,
)
from opentelemetry.proto.resource.v1.resource_pb2 import Resource
from opentelemetry.proto.trace.v1.trace_pb2 import Span, Status


@dataclass(frozen=True)
class PrivacyLimits:
    """V1 ceilings, tunable downward. Byte estimates are not Python heap sizes."""

    max_attributes: int = 32
    max_resource_attributes: int = 16
    max_key_bytes: int = 128
    max_string_bytes: int = 256
    max_events: int = 8
    max_links: int = 4
    max_depth: int = 2
    max_children: int = 16
    max_record_bytes: int = 16 * 1024
    max_batch_bytes: int = 1024 * 1024
    max_response_bytes: int = 64 * 1024
    max_queue_size: int = 512
    max_batch_size: int = 128


DEFAULT_LIMITS = PrivacyLimits()
_HTTP_METHODS = frozenset(
    {"GET", "POST", "PUT", "PATCH", "DELETE", "HEAD", "OPTIONS", "CONNECT", "TRACE"}
)
_DB_OPERATIONS = frozenset(
    {
        "SELECT",
        "INSERT",
        "UPDATE",
        "DELETE",
        "CREATE",
        "ALTER",
        "DROP",
        "TRUNCATE",
        "BEGIN",
        "COMMIT",
        "ROLLBACK",
        "SAVEPOINT",
        "RELEASE",
        "EXPLAIN",
        "WITH",
        "CALL",
    }
)
_DB_SYSTEMS = frozenset({"postgresql", "mysql", "sqlite", "redis"})
_SAFE_NAMES = _HTTP_METHODS | _DB_OPERATIONS | {"HTTP", "DB", "SPAN"}


@dataclass(frozen=True)
class PrivacyPolicy:
    """Exact trusted names and values. Unknown keys cannot be opted into here.

    Instrument allow-list entries are (scope name, instrument name). Exemplar
    filtered attributes are always removed. Only native IDs remain for pivots.
    """

    span_names: frozenset[str] = _SAFE_NAMES
    event_names: frozenset[str] = frozenset({"exception"})
    log_bodies: frozenset[str] = frozenset()
    attribute_values: frozenset[tuple[str, str]] = frozenset()
    metric_instruments: frozenset[tuple[str, str]] = frozenset()
    scope_names: frozenset[str] = frozenset({"django_altimeter"})


DEFAULT_POLICY = PrivacyPolicy()
_RESOURCE_KEYS = frozenset(
    {
        "service.name",
        "service.namespace",
        "service.version",
        "service.instance.id",
        "deployment.environment",
        "deployment.environment.name",
        "process.runtime.name",
        "process.runtime.version",
        "telemetry.sdk.name",
        "telemetry.sdk.language",
        "telemetry.sdk.version",
        "altimeter.role",
    }
)
_STRING_KEYS = frozenset(
    {
        "http.route",
        "http.method",
        "http.request.method",
        "db.system",
        "db.system.name",
        "db.operation",
        "db.operation.name",
        "db.collection.name",
        "db.sql.table",
        "task.name",
        "queue.name",
        "altimeter.task.name",
        "altimeter.queue.name",
        "altimeter.operation",
        "altimeter.surface",
        "altimeter.response_mode",
        "altimeter.outcome",
        "messaging.system",
        "messaging.destination.name",
        "messaging.operation.name",
        "exception.type",
        "error.type",
    }
)
_IDENTITY_KEYS = frozenset({"posthogDistinctId", "altimeter.tenant_id"})
_STATUS_KEYS = frozenset({"http.response.status_code", "http.status_code"})
_DURATION_KEYS = frozenset({"http.server.request.duration", "duration_ms"})
_RECORD_KEYS = (
    _STRING_KEYS
    | _STATUS_KEYS
    | _DURATION_KEYS
    | {"trace_id", "span_id", "altimeter.overflow"}
)
_RESOURCE_ATTRIBUTE_KEYS = _RESOURCE_KEYS | {"process.pid", "altimeter.probe"}
_METRIC_KEYS = (
    (_STRING_KEYS - {"exception.type", "error.type"})
    | _STATUS_KEYS
    | {"altimeter.overflow"}
)


def _text(value: object, maximum: int) -> TypeGuard[str]:
    # len(str) is constant time; UTF-8 encoding only sees at most maximum chars.
    return (
        type(value) is str
        and len(value) <= maximum
        and len(value.encode("utf-8")) <= maximum
    )


def is_valid_configuration(policy: PrivacyPolicy, limits: PrivacyLimits) -> bool:
    """Check shared V1 ceilings before explicit processor construction or projection."""
    if type(policy) is not PrivacyPolicy or type(limits) is not PrivacyLimits:
        return False
    for field in fields(PrivacyLimits):
        value = getattr(limits, field.name)
        if type(value) is not int or not 0 <= value <= getattr(
            DEFAULT_LIMITS, field.name
        ):
            return False
    if not 0 < limits.max_batch_size <= limits.max_queue_size:
        return False
    # Only builtin immutable sets: membership must never invoke user containers.
    return all(
        type(getattr(policy, field.name)) is frozenset for field in fields(policy)
    )


def _allowed_attribute(
    key: object,
    value: object,
    policy: PrivacyPolicy,
    limits: PrivacyLimits,
    resource: bool,
) -> bool:
    if not _text(key, limits.max_key_bytes):
        return False
    if resource:
        if key == "altimeter.probe":
            return value is True
        if key == "process.pid":
            return type(value) is int and 0 < value <= 2**31 - 1
        return (
            key in _RESOURCE_KEYS
            and _text(value, limits.max_string_bytes)
            and (key, value) in policy.attribute_values
        )
    if key in _IDENTITY_KEYS:
        return _text(value, min(200, limits.max_string_bytes)) and bool(value.strip())
    if key in _STATUS_KEYS:
        return type(value) is int and 100 <= value <= 599
    if key in _DURATION_KEYS:
        if type(value) is int:
            return 0 <= value <= 2**63 - 1
        if type(value) is float:
            return 0 <= value <= 2**63 - 1 and isfinite(value)
        return False
    if key == "altimeter.overflow":
        return type(value) is bool
    if not _text(value, limits.max_string_bytes):
        return False
    if key in {"trace_id", "span_id"}:
        size = 32 if key == "trace_id" else 16
        return len(value) == size and all(c in "0123456789abcdef" for c in value)
    if key not in _STRING_KEYS:
        return False
    if key in {"http.method", "http.request.method"}:
        return value in _HTTP_METHODS
    if key in {"db.operation", "db.operation.name"}:
        return value in _DB_OPERATIONS or value == "DB"
    if key in {"db.system", "db.system.name"}:
        return value in _DB_SYSTEMS
    return (key, value) in policy.attribute_values


def is_allowed_attribute(
    key: object,
    value: object,
    *,
    policy: PrivacyPolicy = DEFAULT_POLICY,
    limits: PrivacyLimits = DEFAULT_LIMITS,
    resource: bool = False,
) -> bool:
    """Read-only scalar predicate shared by SDK admission and wire projection."""
    try:
        return (
            type(resource) is bool
            and is_valid_configuration(policy, limits)
            and _allowed_attribute(key, value, policy, limits, resource)
        )
    except Exception:
        # Telemetry boundary: unknown/malformed values cannot reach the app.
        return False


def normalize_span_name(
    name: object,
    *,
    policy: PrivacyPolicy = DEFAULT_POLICY,
    limits: PrivacyLimits = DEFAULT_LIMITS,
) -> str:
    """Reduce a bounded SQL/HTTP name to a constant or an exact registered name.

    Never parse SQL literals/comments or URLs. Oversized strings are rejected
    before hashing, splitting or encoding. Unknown names become SPAN.
    """
    try:
        if not is_valid_configuration(policy, limits):
            return "SPAN"
        if not _text(name, limits.max_string_bytes):
            return "SPAN"
        first = name.split(None, 1)[0].upper() if name.strip() else ""
        # SQL's exclusion is unconditional, even if accidentally registered.
        if first in _DB_OPERATIONS:
            return first
        if name.lstrip().startswith(("/*", "--")):
            return "DB"
        if name in policy.span_names or name in _SAFE_NAMES:
            return name
        if first in _HTTP_METHODS:
            return first
        return "SPAN"
    except Exception:
        return "SPAN"


def _attributes(
    source: Sequence[KeyValue],
    policy: PrivacyPolicy,
    limits: PrivacyLimits,
    *,
    resource: bool = False,
    metric: bool = False,
    identity: bool = False,
) -> list[KeyValue]:
    maximum = limits.max_resource_attributes if resource else limits.max_attributes
    if len(source) > maximum:
        return []
    allowed_keys = (
        _RESOURCE_ATTRIBUTE_KEYS
        if resource
        else _METRIC_KEYS
        if metric
        else _RECORD_KEYS | _IDENTITY_KEYS
        if identity
        else _RECORD_KEYS
    )
    result: list[KeyValue] = []
    seen: set[str] = set()
    for item in source:
        key = item.key
        # Generated string_value access materializes the entire Python string.
        # Reject denied keys before touching their values, even their oneof.
        if (
            not _text(key, limits.max_key_bytes)
            or key in seen
            or key not in allowed_keys
        ):
            continue
        kind = item.value.WhichOneof("value")
        if key == "altimeter.probe" and kind != "bool_value":
            continue
        if kind not in {"string_value", "bool_value", "int_value", "double_value"}:
            continue  # Never descend into unapproved structured attributes.
        value = getattr(item.value, kind)
        if _allowed_attribute(key, value, policy, limits, resource):
            result.append(KeyValue(key=key, value=AnyValue(**{kind: value})))
            seen.add(key)
    return result


def _resource(
    source: Resource, policy: PrivacyPolicy, limits: PrivacyLimits
) -> Resource:
    return Resource(
        attributes=_attributes(source.attributes, policy, limits, resource=True)
    )


def _scope(
    source: InstrumentationScope, policy: PrivacyPolicy, limits: PrivacyLimits
) -> InstrumentationScope:
    name = source.name
    if _text(name, limits.max_string_bytes) and name in policy.scope_names:
        return InstrumentationScope(name=name)
    return InstrumentationScope()


def _id(value: bytes, size: int) -> bytes:
    return value if len(value) == size else b""


def _span(source: Span, policy: PrivacyPolicy, limits: PrivacyLimits) -> Span | None:
    if (
        len(source.attributes) > limits.max_attributes
        or len(source.events) > limits.max_events
        or len(source.links) > limits.max_links
        or not _text(source.name, limits.max_string_bytes)
    ):
        return None
    attributes = _attributes(source.attributes, policy, limits, identity=True)
    name = normalize_span_name(source.name, policy=policy, limits=limits)
    if name == "SPAN" and any(
        _text(a.key, limits.max_key_bytes)
        and a.key
        in {
            "db.system",
            "db.system.name",
            "db.statement",
            "db.query.text",
            "db.operation",
            "db.operation.name",
        }
        for a in source.attributes
    ):
        name = next(
            (
                a.value.string_value
                for a in attributes
                if a.key in {"db.operation", "db.operation.name"}
            ),
            "DB",
        )
    if not _text(name, limits.max_string_bytes):
        return None
    result = Span(
        name=name,
        trace_id=_id(source.trace_id, 16),
        span_id=_id(source.span_id, 8),
        parent_span_id=_id(source.parent_span_id, 8),
        flags=source.flags & 0x301,
        kind=source.kind if source.kind in range(6) else Span.SPAN_KIND_UNSPECIFIED,
        start_time_unix_nano=source.start_time_unix_nano,
        end_time_unix_nano=source.end_time_unix_nano,
        attributes=attributes,
    )
    result.status.code = (
        source.status.code
        if source.status.code in (0, 1, 2)
        else Status.STATUS_CODE_UNSET
    )
    for event in source.events:
        if (
            _text(event.name, limits.max_string_bytes)
            and event.name in policy.event_names
            and len(event.attributes) <= limits.max_attributes
        ):
            result.events.add(
                name=event.name,
                time_unix_nano=event.time_unix_nano,
                attributes=_attributes(event.attributes, policy, limits),
            )
    for link in source.links:
        if len(link.attributes) <= limits.max_attributes:
            result.links.add(
                trace_id=_id(link.trace_id, 16),
                span_id=_id(link.span_id, 8),
                flags=link.flags & 0x301,
                attributes=_attributes(link.attributes, policy, limits),
            )
    return result if result.ByteSize() <= limits.max_record_bytes else None


def project_traces(
    request: ExportTraceServiceRequest,
    *,
    policy: PrivacyPolicy = DEFAULT_POLICY,
    limits: PrivacyLimits = DEFAULT_LIMITS,
) -> ExportTraceServiceRequest:
    """Rebuild traces without retaining unknown protobuf fields or free text."""
    result = ExportTraceServiceRequest()
    try:
        if type(request) is not ExportTraceServiceRequest or not is_valid_configuration(
            policy, limits
        ):
            return result
        remaining = limits.max_batch_size
        if len(request.resource_spans) > remaining:
            return result
        scopes_left = remaining
        for group in request.resource_spans:
            if len(group.scope_spans) > scopes_left:
                return ExportTraceServiceRequest()
            scopes_left -= len(group.scope_spans)
            if len(group.resource.attributes) > limits.max_resource_attributes:
                continue
            target_group = result.resource_spans.add(
                resource=_resource(group.resource, policy, limits)
            )
            for scope in group.scope_spans:
                if len(scope.spans) > remaining:
                    return ExportTraceServiceRequest()
                remaining -= len(scope.spans)
                target = target_group.scope_spans.add(
                    scope=_scope(scope.scope, policy, limits)
                )
                for span in scope.spans:
                    projected = _span(span, policy, limits)
                    if projected is not None:
                        target.spans.append(projected)
        if result.ByteSize() > limits.max_batch_bytes:
            return ExportTraceServiceRequest()
        return result
    except Exception:
        return ExportTraceServiceRequest()


def _body(
    source: AnyValue, policy: PrivacyPolicy, limits: PrivacyLimits, depth: int = 0
) -> AnyValue:
    kind = source.WhichOneof("value")
    if kind == "string_value":
        value = source.string_value
        if _text(value, limits.max_string_bytes) and value in policy.log_bodies:
            return AnyValue(string_value=value)
    if kind != "kvlist_value" or depth >= limits.max_depth:
        return AnyValue()
    children = source.kvlist_value.values
    if len(children) > limits.max_children:
        return AnyValue()
    result = AnyValue()
    seen: set[str] = set()
    for child in children:
        key = child.key
        if not _text(key, limits.max_key_bytes) or key in seen:
            continue
        if key == "context" and child.value.WhichOneof("value") == "kvlist_value":
            nested = _body(child.value, policy, limits, depth + 1)
            if nested.WhichOneof("value"):
                result.kvlist_value.values.add(key=key, value=nested)
        else:
            result.kvlist_value.values.extend(_attributes([child], policy, limits))
        seen.add(key)
    return result


def _log(
    source: LogRecord, policy: PrivacyPolicy, limits: PrivacyLimits
) -> LogRecord | None:
    if len(source.attributes) > limits.max_attributes:
        return None
    result = LogRecord(
        time_unix_nano=source.time_unix_nano,
        observed_time_unix_nano=source.observed_time_unix_nano,
        severity_number=source.severity_number
        if source.severity_number in range(25)
        else SeverityNumber.SEVERITY_NUMBER_UNSPECIFIED,
        body=_body(source.body, policy, limits),
        attributes=_attributes(source.attributes, policy, limits, identity=True),
        flags=source.flags & 1,
        trace_id=_id(source.trace_id, 16),
        span_id=_id(source.span_id, 8),
    )
    if (
        _text(source.event_name, limits.max_string_bytes)
        and source.event_name in policy.event_names
    ):
        result.event_name = source.event_name
    return result if result.ByteSize() <= limits.max_record_bytes else None


def project_logs(
    request: ExportLogsServiceRequest,
    *,
    policy: PrivacyPolicy = DEFAULT_POLICY,
    limits: PrivacyLimits = DEFAULT_LIMITS,
) -> ExportLogsServiceRequest:
    """Rebuild log bodies and attributes; arbitrary prose is never exported."""
    result = ExportLogsServiceRequest()
    try:
        if type(request) is not ExportLogsServiceRequest or not is_valid_configuration(
            policy, limits
        ):
            return result
        remaining = limits.max_batch_size
        scopes_left = remaining
        if len(request.resource_logs) > remaining:
            return result
        for group in request.resource_logs:
            if len(group.scope_logs) > scopes_left:
                return ExportLogsServiceRequest()
            scopes_left -= len(group.scope_logs)
            if len(group.resource.attributes) > limits.max_resource_attributes:
                continue
            target_group = result.resource_logs.add(
                resource=_resource(group.resource, policy, limits)
            )
            for scope in group.scope_logs:
                if len(scope.log_records) > remaining:
                    return ExportLogsServiceRequest()
                remaining -= len(scope.log_records)
                target = target_group.scope_logs.add(
                    scope=_scope(scope.scope, policy, limits)
                )
                for record in scope.log_records:
                    projected = _log(record, policy, limits)
                    if projected is not None:
                        target.log_records.append(projected)
        return (
            result
            if result.ByteSize() <= limits.max_batch_bytes
            else ExportLogsServiceRequest()
        )
    except Exception:
        return ExportLogsServiceRequest()


def _exemplars(source: Sequence[Exemplar], limits: PrivacyLimits) -> list[Exemplar]:
    if len(source) > limits.max_children:
        return []
    result = []
    for exemplar in source:
        kind = exemplar.WhichOneof("value")
        if kind not in {"as_int", "as_double"}:
            continue
        value = getattr(exemplar, kind)
        if not isfinite(value):
            continue
        result.append(
            Exemplar(
                time_unix_nano=exemplar.time_unix_nano,
                trace_id=_id(exemplar.trace_id, 16),
                span_id=_id(exemplar.span_id, 8),
                **{kind: value},
            )
        )
    return result


def _number(
    source: NumberDataPoint, policy: PrivacyPolicy, limits: PrivacyLimits
) -> NumberDataPoint | None:
    kind = source.WhichOneof("value")
    if kind not in {"as_int", "as_double"} or not isfinite(getattr(source, kind)):
        return None
    return NumberDataPoint(
        attributes=_attributes(source.attributes, policy, limits, metric=True),
        start_time_unix_nano=source.start_time_unix_nano,
        time_unix_nano=source.time_unix_nano,
        flags=source.flags & 1,
        exemplars=_exemplars(source.exemplars, limits),
        **{kind: getattr(source, kind)},
    )


def _histogram(
    source: HistogramDataPoint, policy: PrivacyPolicy, limits: PrivacyLimits
) -> HistogramDataPoint | None:
    if (
        len(source.bucket_counts) > limits.max_children
        or len(source.explicit_bounds) > limits.max_children
        or len(source.bucket_counts) != len(source.explicit_bounds) + 1
        or any(not isfinite(b) for b in source.explicit_bounds)
    ):
        return None
    result = HistogramDataPoint(
        attributes=_attributes(source.attributes, policy, limits, metric=True),
        start_time_unix_nano=source.start_time_unix_nano,
        time_unix_nano=source.time_unix_nano,
        count=source.count,
        bucket_counts=source.bucket_counts,
        explicit_bounds=source.explicit_bounds,
        flags=source.flags & 1,
        exemplars=_exemplars(source.exemplars, limits),
    )
    for field in ("sum", "min", "max"):
        if source.HasField(field):
            value = getattr(source, field)
            if not isfinite(value):
                return None
            setattr(result, field, value)
    return result


def _exponential(
    source: ExponentialHistogramDataPoint, policy: PrivacyPolicy, limits: PrivacyLimits
) -> ExponentialHistogramDataPoint | None:
    if (
        len(source.positive.bucket_counts) > limits.max_children
        or len(source.negative.bucket_counts) > limits.max_children
        or not isfinite(source.zero_threshold)
    ):
        return None
    result = ExponentialHistogramDataPoint(
        attributes=_attributes(source.attributes, policy, limits, metric=True),
        start_time_unix_nano=source.start_time_unix_nano,
        time_unix_nano=source.time_unix_nano,
        count=source.count,
        scale=source.scale,
        zero_count=source.zero_count,
        zero_threshold=source.zero_threshold,
        flags=source.flags & 1,
        exemplars=_exemplars(source.exemplars, limits),
    )
    result.positive.offset = source.positive.offset
    result.positive.bucket_counts.extend(source.positive.bucket_counts)
    result.negative.offset = source.negative.offset
    result.negative.bucket_counts.extend(source.negative.bucket_counts)
    for field in ("sum", "min", "max"):
        if source.HasField(field):
            value = getattr(source, field)
            if not isfinite(value):
                return None
            setattr(result, field, value)
    return result


def _summary(
    source: SummaryDataPoint, policy: PrivacyPolicy, limits: PrivacyLimits
) -> SummaryDataPoint | None:
    if len(source.quantile_values) > limits.max_children or not isfinite(source.sum):
        return None
    result = SummaryDataPoint(
        attributes=_attributes(source.attributes, policy, limits, metric=True),
        start_time_unix_nano=source.start_time_unix_nano,
        time_unix_nano=source.time_unix_nano,
        count=source.count,
        sum=source.sum,
        flags=source.flags & 1,
    )
    for quantile in source.quantile_values:
        if not 0 <= quantile.quantile <= 1 or not isfinite(quantile.value):
            return None
        result.quantile_values.add(quantile=quantile.quantile, value=quantile.value)
    return result


def project_metrics(
    request: ExportMetricsServiceRequest,
    *,
    policy: PrivacyPolicy = DEFAULT_POLICY,
    limits: PrivacyLimits = DEFAULT_LIMITS,
) -> ExportMetricsServiceRequest:
    """Project exact scope/instrument pairs and strip all exemplar attributes.

    No series are combined here. Callers must admit finite dimensions before SDK
    aggregation; this function cannot undo an already unbounded aggregation.
    """
    result = ExportMetricsServiceRequest()
    try:
        if type(
            request
        ) is not ExportMetricsServiceRequest or not is_valid_configuration(
            policy, limits
        ):
            return result
        groups = request.resource_metrics
        scopes_left = instruments_left = points_left = limits.max_batch_size
        if len(groups) > limits.max_batch_size:
            return result
        for group in groups:
            if len(group.scope_metrics) > scopes_left:
                return ExportMetricsServiceRequest()
            scopes_left -= len(group.scope_metrics)
            if len(group.resource.attributes) > limits.max_resource_attributes:
                continue
            target_group = result.resource_metrics.add(
                resource=_resource(group.resource, policy, limits)
            )
            for scope in group.scope_metrics:
                if len(scope.metrics) > instruments_left:
                    return ExportMetricsServiceRequest()
                instruments_left -= len(scope.metrics)
                name = scope.scope.name
                if (
                    not _text(name, limits.max_string_bytes)
                    or name not in policy.scope_names
                ):
                    continue
                target = target_group.scope_metrics.add(
                    scope=_scope(scope.scope, policy, limits)
                )
                for metric in scope.metrics:
                    if (
                        not _text(metric.name, limits.max_string_bytes)
                        or (name, metric.name) not in policy.metric_instruments
                    ):
                        continue
                    kind = metric.WhichOneof("data")
                    if kind not in {
                        "gauge",
                        "sum",
                        "histogram",
                        "exponential_histogram",
                        "summary",
                    }:
                        continue
                    data = getattr(metric, kind)
                    if len(data.data_points) > points_left:
                        return ExportMetricsServiceRequest()
                    points_left -= len(data.data_points)
                    projected = Metric(
                        name=metric.name,
                        unit=metric.unit
                        if _text(metric.unit, limits.max_string_bytes)
                        and metric.unit in {"s", "ms", "By", "1", "{request}", "{task}"}
                        else "",
                    )
                    output = getattr(projected, kind)
                    if kind in {"sum", "histogram", "exponential_histogram"}:
                        if data.aggregation_temporality not in (1, 2):
                            continue
                        output.aggregation_temporality = data.aggregation_temporality
                    if kind == "sum":
                        output.is_monotonic = data.is_monotonic
                    for point in data.data_points:
                        if len(point.attributes) > limits.max_attributes:
                            continue
                        safe: (
                            NumberDataPoint
                            | HistogramDataPoint
                            | ExponentialHistogramDataPoint
                            | SummaryDataPoint
                            | None
                        ) = None
                        if kind in {"gauge", "sum"}:
                            safe = _number(point, policy, limits)
                        elif kind == "histogram":
                            safe = _histogram(point, policy, limits)
                        elif kind == "exponential_histogram":
                            safe = _exponential(point, policy, limits)
                        elif kind == "summary":
                            safe = _summary(point, policy, limits)
                        if (
                            safe is not None
                            and safe.ByteSize() <= limits.max_record_bytes
                        ):
                            output.data_points.append(safe)
                    if output.data_points:
                        target.metrics.append(projected)
        return (
            result
            if result.ByteSize() <= limits.max_batch_bytes
            else ExportMetricsServiceRequest()
        )
    except Exception:
        return ExportMetricsServiceRequest()
