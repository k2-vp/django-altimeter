"""Privacy contracts exercised against generated OTLP messages, not SDK mocks."""

from dataclasses import replace

import pytest
from opentelemetry.proto.collector.logs.v1.logs_service_pb2 import (
    ExportLogsServiceRequest,
)
from opentelemetry.proto.collector.metrics.v1.metrics_service_pb2 import (
    ExportMetricsServiceRequest,
)
from opentelemetry.proto.collector.trace.v1.trace_service_pb2 import (
    ExportTraceServiceRequest,
)
from opentelemetry.proto.common.v1.common_pb2 import AnyValue, KeyValue

from django_altimeter.privacy import (
    DEFAULT_LIMITS,
    PrivacyPolicy,
    is_allowed_attribute,
    is_valid_configuration,
    normalize_span_name,
    project_logs,
    project_metrics,
    project_traces,
)

POLICY = PrivacyPolicy(
    log_bodies=frozenset({"request.completed"}),
    attribute_values=frozenset(
        {
            ("service.name", "reference"),
            ("http.route", "/items/{id}"),
            ("exception.type", "ValueError"),
        }
    ),
    metric_instruments=frozenset(
        {("django_altimeter", "http.server.request.duration")}
    ),
)

CANARY = "CANARY_PRIVATE_8b92"
TRACE_ID = bytes.fromhex("0123456789abcdef0123456789abcdef")
SPAN_ID = bytes.fromhex("0123456789abcdef")


def attribute(key, value):
    if isinstance(value, str):
        return KeyValue(key=key, value=AnyValue(string_value=value))
    if isinstance(value, int):
        return KeyValue(key=key, value=AnyValue(int_value=value))
    return KeyValue(key=key, value=AnyValue(double_value=value))


def trace_request():
    request = ExportTraceServiceRequest()
    group = request.resource_spans.add(schema_url=CANARY)
    group.resource.attributes.extend(
        [attribute("service.name", "reference"), attribute("secret", CANARY)]
    )
    scope = group.scope_spans.add(schema_url=CANARY)
    scope.scope.name = "django_altimeter"
    scope.scope.version = CANARY
    scope.scope.attributes.append(attribute("secret", CANARY))
    span = scope.spans.add(
        name=f"SELECT '{CANARY}' /* {CANARY} */",
        trace_id=TRACE_ID,
        span_id=SPAN_ID,
        parent_span_id=SPAN_ID,
        trace_state=CANARY,
        start_time_unix_nano=10,
        end_time_unix_nano=30,
        kind=2,
    )
    span.status.code = 2
    span.status.message = CANARY
    span.attributes.extend(
        [
            attribute("http.route", "/items/{id}"),
            attribute("http.response.status_code", 503),
            attribute("http.server.request.duration", 0.25),
            attribute("db.operation.name", "SELECT"),
            attribute("db.statement", f"SELECT '{CANARY}'"),
            attribute("http.route", f"/raw/{CANARY}"),
        ]
    )
    event = span.events.add(name="exception", time_unix_nano=20)
    event.attributes.extend(
        [
            attribute("exception.type", "ValueError"),
            attribute("exception.message", CANARY),
        ]
    )
    span.events.add(name=CANARY)
    link = span.links.add(trace_id=TRACE_ID, span_id=SPAN_ID, trace_state=CANARY)
    link.attributes.extend(
        [attribute("db.operation.name", "SELECT"), attribute("secret", CANARY)]
    )
    return request


def log_request():
    request = ExportLogsServiceRequest()
    scope = request.resource_logs.add().scope_logs.add()
    scope.scope.name = "django_altimeter"
    record = scope.log_records.add(
        trace_id=TRACE_ID,
        span_id=SPAN_ID,
        time_unix_nano=10,
        observed_time_unix_nano=11,
        severity_number=17,
        severity_text=CANARY,
        event_name=CANARY,
    )
    record.body.kvlist_value.values.extend(
        [attribute("http.route", "/items/{id}"), attribute("message", CANARY)]
    )
    nested = record.body.kvlist_value.values.add(key="context")
    nested.value.kvlist_value.values.extend(
        [attribute("http.response.status_code", 503), attribute("password", CANARY)]
    )
    record.attributes.extend(
        [
            attribute("http.response.status_code", 503),
            attribute("exception.stacktrace", CANARY),
        ]
    )
    scope.log_records.add(body=AnyValue(string_value=CANARY))
    scope.log_records.add(body=AnyValue(string_value="request.completed"))
    return request


def metric_request():
    request = ExportMetricsServiceRequest()
    group = request.resource_metrics.add(schema_url=CANARY)
    group.resource.attributes.extend(
        [attribute("service.name", "reference"), attribute("secret", CANARY)]
    )
    scope = group.scope_metrics.add(schema_url=CANARY)
    scope.scope.name = "django_altimeter"
    metric = scope.metrics.add(
        name="http.server.request.duration", unit="s", description=CANARY
    )
    metric.metadata.append(attribute("secret", CANARY))
    metric.histogram.aggregation_temporality = 2
    point = metric.histogram.data_points.add(
        time_unix_nano=30,
        start_time_unix_nano=10,
        count=1,
        sum=0.25,
        bucket_counts=[0, 1],
        explicit_bounds=[0.1],
        min=0.25,
        max=0.25,
    )
    point.attributes.extend(
        [attribute("http.route", "/items/{id}"), attribute("secret", CANARY)]
    )
    exemplar = point.exemplars.add(
        trace_id=TRACE_ID, span_id=SPAN_ID, time_unix_nano=20, as_double=0.25
    )
    exemplar.filtered_attributes.extend(
        [attribute("secret", CANARY), attribute("http.route", f"/{CANARY}")]
    )
    scope.metrics.add(name=CANARY).gauge.data_points.add(as_int=1)
    return request


def test_traces_remove_all_free_text_and_keep_safe_context():
    request = trace_request()
    before = request.SerializeToString()
    assert CANARY.encode() in before  # The negative control really reaches OTLP.
    result = project_traces(request, policy=POLICY)
    assert CANARY.encode() not in result.SerializeToString()
    span = result.resource_spans[0].scope_spans[0].spans[0]
    assert span.name == "SELECT"
    assert span.trace_id == TRACE_ID and span.span_id == SPAN_ID
    assert span.end_time_unix_nano - span.start_time_unix_nano == 20
    assert span.status.code == 2 and not span.status.message
    assert attribute("http.route", "/items/{id}") in span.attributes
    assert attribute("http.response.status_code", 503) in span.attributes
    assert attribute("http.server.request.duration", 0.25) in span.attributes
    assert attribute("db.operation.name", "SELECT") in span.attributes
    assert span.events[0].name == "exception"
    assert span.events[0].attributes == [attribute("exception.type", "ValueError")]
    assert span.links[0].trace_id == TRACE_ID
    assert request.SerializeToString() == before


def test_logs_remove_prose_and_nested_unknowns_keep_safe_body_and_correlation():
    request = log_request()
    before = request.SerializeToString()
    assert CANARY.encode() in before
    result = project_logs(request, policy=POLICY)
    assert CANARY.encode() not in result.SerializeToString()
    records = result.resource_logs[0].scope_logs[0].log_records
    assert records[0].trace_id == TRACE_ID and records[0].severity_number == 17
    assert attribute("http.route", "/items/{id}") in records[0].body.kvlist_value.values
    assert records[-1].body.string_value == "request.completed"
    assert request.SerializeToString() == before


def test_metrics_remove_dimensions_and_exemplar_secrets_keep_measurement():
    request = metric_request()
    before = request.SerializeToString()
    assert CANARY.encode() in before
    result = project_metrics(request, policy=POLICY)
    assert CANARY.encode() not in result.SerializeToString()
    metrics = result.resource_metrics[0].scope_metrics[0].metrics
    assert len(metrics) == 1
    assert metrics[0].name == "http.server.request.duration"
    point = metrics[0].histogram.data_points[0]
    assert point.count == 1 and point.sum == 0.25
    assert attribute("http.route", "/items/{id}") in point.attributes
    assert point.exemplars[0].trace_id == TRACE_ID
    assert not point.exemplars[0].filtered_attributes
    assert request.SerializeToString() == before


# Scalar admission and generated-message projection share the same closed policy.
@pytest.mark.parametrize(
    ("key", "value", "allowed"),
    [
        ("http.request.method", "GET", True),
        ("http.request.method", CANARY, False),
        ("http.route", "/items/{id}", True),
        ("http.route", CANARY, False),
        ("http.response.status_code", 200, True),
        ("http.response.status_code", True, False),
        ("http.response.status_code", "200", False),
        ("http.response.status_code", 999, False),
        ("http.server.request.duration", 0.25, True),
        ("http.server.request.duration", float("nan"), False),
        ("http.server.request.duration", float("inf"), False),
        ("http.server.request.duration", -1, False),
        ("db.operation.name", "SELECT", True),
        ("db.operation.name", f"SELECT '{CANARY}'", False),
        ("db.statement", "SELECT 1", False),
        ("db.system.name", "postgresql", True),
        ("db.system.name", CANARY, False),
        ("trace_id", TRACE_ID.hex(), True),
        ("trace_id", CANARY, False),
        ("trace_id", "a" * 33, False),
        ("span_id", SPAN_ID.hex(), True),
        ("span_id", "g" * 16, False),
        ("altimeter.overflow", True, True),
        ("altimeter.overflow", 1, False),
        ("http.route", ["/items/{id}"], False),
        ("http.route", {"secret": CANARY}, False),
    ],
)
def test_scalar_policy_checks_keys_types_values_and_ranges(key, value, allowed):
    assert is_allowed_attribute(key, value, policy=POLICY) is allowed
    assert is_allowed_attribute("http.response.status_code", 204, policy=POLICY)


def test_configured_pairs_cannot_admit_forbidden_keys_or_wrong_surface():
    policy = replace(
        POLICY,
        attribute_values=POLICY.attribute_values
        | {("db.statement", CANARY), ("password", CANARY), ("db.sql.table", "items")},
    )
    assert not is_allowed_attribute("db.statement", CANARY, policy=policy)
    assert not is_allowed_attribute("password", CANARY, policy=policy)
    assert is_allowed_attribute("db.sql.table", "items", policy=policy)
    assert not is_allowed_attribute("db.sql.table", CANARY, policy=policy)
    assert is_allowed_attribute(
        "service.name", "reference", policy=policy, resource=True
    )
    assert not is_allowed_attribute("service.name", "reference", policy=policy)
    assert not is_allowed_attribute(
        "http.route", "/items/{id}", policy=policy, resource=True
    )


class Uninspectable:
    def __getattribute__(self, name):
        raise KeyboardInterrupt("An unknown object must not be inspected")

    def __len__(self):
        raise KeyboardInterrupt("An unknown container must not be measured")

    def __iter__(self):
        raise KeyboardInterrupt("An unknown container must not be traversed")

    def __str__(self):
        raise KeyboardInterrupt("An unknown object must not be rendered")


class UninspectableString(str):
    def __len__(self):
        raise KeyboardInterrupt("String subclasses must not be inspected")

    def __hash__(self):
        raise KeyboardInterrupt("String subclasses must not be hashed")

    def encode(self, *args, **kwargs):
        raise KeyboardInterrupt("String subclasses must not be encoded")


@pytest.mark.parametrize("kind", [Uninspectable, UninspectableString])
def test_unknown_values_are_rejected_without_calling_user_code(kind):
    value = kind(CANARY) if kind is UninspectableString else kind()
    assert not is_allowed_attribute("http.route", value, policy=POLICY)
    assert not is_allowed_attribute(value, "GET", policy=POLICY)
    assert normalize_span_name(value) == "SPAN"
    for projector in (project_traces, project_logs, project_metrics):
        assert not projector(value).SerializeToString()
    assert is_allowed_attribute("http.request.method", "GET")


@pytest.mark.parametrize(
    ("name", "expected"),
    [
        (f"SELECT '{CANARY}' /* {CANARY} */", "SELECT"),
        (f"\nupdate things set secret='{CANARY}'", "UPDATE"),
        (f"/* {CANARY} */ SELECT 1", "DB"),
        (f"-- {CANARY}\nSELECT 1", "DB"),
        (f"GET /raw/{CANARY}?token={CANARY}", "GET"),
        (f"https://host/{CANARY}", "SPAN"),
        (CANARY, "SPAN"),
        ("SELECT " + "x" * 1_000_000, "SPAN"),
    ],
)
def test_early_names_never_derive_free_text_from_sql_or_url(name, expected):
    assert normalize_span_name(name, policy=POLICY) == expected
    assert normalize_span_name("SELECT", policy=POLICY) == "SELECT"


def test_unknown_database_statement_becomes_db_not_statement_text():
    request = trace_request()
    span = request.resource_spans[0].scope_spans[0].spans[0]
    span.name = f"VENDOR_SQL '{CANARY}'"
    del span.attributes[:]
    span.attributes.append(attribute("db.statement", span.name))
    result = project_traces(request, policy=POLICY)
    assert result.resource_spans[0].scope_spans[0].spans[0].name == "DB"
    assert CANARY.encode() not in result.SerializeToString()


@pytest.mark.parametrize("field", ["attributes", "events", "links"])
def test_oversized_span_collections_drop_record_keep_safe_neighbor(field):
    request = trace_request()
    scope = request.resource_spans[0].scope_spans[0]
    span = scope.spans[0]
    maximum = getattr(DEFAULT_LIMITS, "max_" + field)
    collection = getattr(span, field)
    while len(collection) <= maximum:
        collection.add()
    scope.spans.add(name="DB")
    result = project_traces(request, policy=POLICY)
    assert [s.name for s in result.resource_spans[0].scope_spans[0].spans] == ["DB"]


def test_oversized_resource_drops_group_without_losing_safe_group():
    request = trace_request()
    resource = request.resource_spans[0].resource
    while len(resource.attributes) <= DEFAULT_LIMITS.max_resource_attributes:
        resource.attributes.add(key="secret", value=AnyValue(string_value=CANARY))
    request.resource_spans.add().scope_spans.add().spans.add(name="DB")
    result = project_traces(request, policy=POLICY)
    assert len(result.resource_spans) == 1
    assert result.resource_spans[0].scope_spans[0].spans[0].name == "DB"


def test_oversized_event_and_link_attributes_drop_only_those_children():
    request = trace_request()
    span = request.resource_spans[0].scope_spans[0].spans[0]
    event = span.events[0]
    link = span.links[0]
    for child in (event, link):
        while len(child.attributes) <= DEFAULT_LIMITS.max_attributes:
            child.attributes.add(key="secret", value=AnyValue(string_value=CANARY))
    span.events.add(name="exception")
    span.links.add(trace_id=TRACE_ID, span_id=SPAN_ID)
    safe = (
        project_traces(request, policy=POLICY).resource_spans[0].scope_spans[0].spans[0]
    )
    assert len(safe.events) == len(safe.links) == 1
    assert safe.events[0].name == "exception"
    assert safe.links[0].trace_id == TRACE_ID


def test_utf8_bytes_are_bounded_before_name_body_and_attribute_admission():
    allowed = "é" * 128
    oversized = "é" * 129
    policy = replace(
        POLICY,
        span_names=frozenset({allowed, oversized}),
        log_bodies=frozenset({allowed, oversized}),
        attribute_values=POLICY.attribute_values
        | {("http.route", allowed), ("http.route", oversized)},
    )
    assert is_allowed_attribute("http.route", allowed, policy=policy)
    assert not is_allowed_attribute("http.route", oversized, policy=policy)
    request = log_request()
    scope = request.resource_logs[0].scope_logs[0]
    del scope.log_records[:]
    scope.log_records.add(body=AnyValue(string_value=allowed))
    scope.log_records.add(body=AnyValue(string_value=oversized))
    records = (
        project_logs(request, policy=policy).resource_logs[0].scope_logs[0].log_records
    )
    assert records[0].body.string_value == allowed
    assert not records[1].body.WhichOneof("value")
    assert normalize_span_name(allowed, policy=policy) == allowed
    assert normalize_span_name(oversized, policy=policy) == "SPAN"


def test_log_nesting_count_depth_and_unknown_arrays_fail_closed():
    request = log_request()
    body = request.resource_logs[0].scope_logs[0].log_records[0].body
    # Two kvlist levels are approved; a third never reaches even allowed keys.
    context = body.kvlist_value.values[2].value
    deep = context.kvlist_value.values.add(key="context")
    deep.value.kvlist_value.values.append(attribute("http.route", "/items/{id}"))
    unknown = body.kvlist_value.values.add(key="unrecognized")
    unknown.value.array_value.values.add(string_value=CANARY)
    result = project_logs(request, policy=POLICY)
    values = (
        result.resource_logs[0].scope_logs[0].log_records[0].body.kvlist_value.values
    )
    assert [value.key for value in values] == ["http.route", "context"]
    assert values[1].value.kvlist_value.values == [
        attribute("http.response.status_code", 503)
    ]
    while len(body.kvlist_value.values) <= DEFAULT_LIMITS.max_children:
        body.kvlist_value.values.add(key="secret", value=AnyValue(string_value=CANARY))
    records = (
        project_logs(request, policy=POLICY).resource_logs[0].scope_logs[0].log_records
    )
    assert not records[0].body.WhichOneof("value")
    assert records[-1].body.string_value == "request.completed"


@pytest.mark.parametrize(
    "projector,make",
    [
        (project_traces, trace_request),
        (project_logs, log_request),
        (project_metrics, metric_request),
    ],
)
def test_bad_configuration_and_batch_byte_ceiling_fail_closed(projector, make):
    assert projector(make(), policy=POLICY).SerializeToString()
    assert not projector(
        make(), policy=POLICY, limits=replace(DEFAULT_LIMITS, max_batch_bytes=1)
    ).SerializeToString()
    assert not projector(
        make(), policy=POLICY, limits=replace(DEFAULT_LIMITS, max_depth=-1)
    ).SerializeToString()
    assert not projector(
        make(), policy=POLICY, limits=Uninspectable()
    ).SerializeToString()
    assert not projector(make(), policy=Uninspectable()).SerializeToString()


def test_record_byte_ceiling_drops_oversized_safe_record():
    request = ExportLogsServiceRequest()
    scope = request.resource_logs.add().scope_logs.add()
    scope.log_records.add(body=AnyValue(string_value="a" * 256))
    scope.log_records.add(body=AnyValue(string_value="safe"))
    policy = replace(POLICY, log_bodies=frozenset({"a" * 256, "safe"}))
    result = project_logs(
        request, policy=policy, limits=replace(DEFAULT_LIMITS, max_record_bytes=64)
    )
    assert [
        r.body.string_value for r in result.resource_logs[0].scope_logs[0].log_records
    ] == ["safe"]


@pytest.mark.parametrize(
    "projector,make,groups,scopes,records",
    [
        (project_traces, trace_request, "resource_spans", "scope_spans", "spans"),
        (project_logs, log_request, "resource_logs", "scope_logs", "log_records"),
        (
            project_metrics,
            metric_request,
            "resource_metrics",
            "scope_metrics",
            "metrics",
        ),
    ],
)
def test_batch_container_counts_are_checked_before_iteration(
    projector, make, groups, scopes, records
):
    request = make()
    collection = getattr(getattr(getattr(request, groups)[0], scopes)[0], records)
    while len(collection) <= DEFAULT_LIMITS.max_batch_size:
        collection.add()
    assert not projector(request, policy=POLICY).SerializeToString()
    assert projector(make(), policy=POLICY).SerializeToString()


@pytest.mark.parametrize(
    "projector,make",
    [
        (project_traces, trace_request),
        (project_logs, log_request),
        (project_metrics, metric_request),
    ],
)
def test_unknown_protobuf_wire_fields_cannot_survive_reconstruction(projector, make):
    request = make()
    # Unknown length-delimited field 100. Native protobuf CopyFrom preserves it.
    unknown = b"\xa2\x06" + bytes([len(CANARY)]) + CANARY.encode()
    request.MergeFromString(unknown)
    result = projector(request, policy=POLICY)
    assert CANARY.encode() in request.SerializeToString()
    assert CANARY.encode() not in result.SerializeToString()
    assert result.SerializeToString()


@pytest.mark.parametrize(
    "kind", ["gauge", "sum", "histogram", "exponential_histogram", "summary"]
)
def test_each_metric_shape_preserves_values_and_removes_secrets(kind):
    request = metric_request()
    metric = request.resource_metrics[0].scope_metrics[0].metrics[0]
    metric.ClearField("histogram")
    data = getattr(metric, kind)
    if kind in {"sum", "histogram", "exponential_histogram"}:
        data.aggregation_temporality = 2
    if kind == "sum":
        data.is_monotonic = True
    point = data.data_points.add(time_unix_nano=30, start_time_unix_nano=10)
    point.attributes.extend(
        [attribute("http.route", "/items/{id}"), attribute("secret", CANARY)]
    )
    if kind in {"gauge", "sum"}:
        point.as_double = 0.25
    else:
        point.count = 2
        point.sum = 0.25
    if kind == "histogram":
        point.bucket_counts.extend([1, 1])
        point.explicit_bounds.append(0.1)
    elif kind == "exponential_histogram":
        point.positive.bucket_counts.extend([1, 1])
        point.positive.offset = 1
    elif kind == "summary":
        point.quantile_values.add(quantile=0.5, value=0.125)
    if kind != "summary":
        exemplar = point.exemplars.add(
            as_double=0.125, trace_id=TRACE_ID, span_id=SPAN_ID
        )
        exemplar.filtered_attributes.append(attribute("http.route", CANARY))
    projected = project_metrics(request, policy=POLICY)
    assert CANARY.encode() not in projected.SerializeToString()
    safe = getattr(
        projected.resource_metrics[0].scope_metrics[0].metrics[0], kind
    ).data_points[0]
    assert safe.time_unix_nano == 30
    assert safe.attributes == [attribute("http.route", "/items/{id}")]
    if kind in {"gauge", "sum"}:
        assert safe.as_double == 0.25
    else:
        assert safe.count == 2 and safe.sum == 0.25
    if kind != "summary":
        assert (
            safe.exemplars[0].trace_id == TRACE_ID
            and not safe.exemplars[0].filtered_attributes
        )


def test_metrics_require_exact_scope_and_do_not_copy_correlation_to_dimensions():
    request = metric_request()
    scope = request.resource_metrics[0].scope_metrics[0]
    point = scope.metrics[0].histogram.data_points[0]
    point.attributes.append(attribute("trace_id", TRACE_ID.hex()))
    safe = (
        project_metrics(request, policy=POLICY)
        .resource_metrics[0]
        .scope_metrics[0]
        .metrics[0]
        .histogram.data_points[0]
    )
    assert not any(a.key == "trace_id" for a in safe.attributes)
    assert safe.exemplars[0].trace_id == TRACE_ID
    scope.scope.name = "unknown"
    result = project_metrics(request, policy=POLICY)
    assert not any(
        scope.metrics
        for group in result.resource_metrics
        for scope in group.scope_metrics
    )


def test_oversized_metric_collections_drop_points_without_merging_neighbors():
    request = metric_request()
    histogram = request.resource_metrics[0].scope_metrics[0].metrics[0].histogram
    oversized = histogram.data_points[0]
    oversized.bucket_counts.extend([0] * DEFAULT_LIMITS.max_children)
    safe = histogram.data_points.add(count=1, sum=1, bucket_counts=[1])
    safe.attributes.append(attribute("http.route", "/items/{id}"))
    safe = histogram.data_points.add(count=1, sum=2, bucket_counts=[1])
    safe.attributes.append(attribute("http.route", "/items/{id}"))
    points = (
        project_metrics(request, policy=POLICY)
        .resource_metrics[0]
        .scope_metrics[0]
        .metrics[0]
        .histogram.data_points
    )
    assert [(p.count, p.sum) for p in points] == [(1, 1), (1, 2)]


@pytest.mark.parametrize("signal", ["traces", "logs", "metrics"])
def test_nested_attribute_values_are_dropped_on_every_signal(signal):
    nested = KeyValue(key="http.route")
    nested.value.kvlist_value.values.add(
        key="secret", value=AnyValue(string_value=CANARY)
    )
    # Keep the original route-positive record intact. The new mixed record has
    # no preceding http.route, so duplicate handling cannot hide the type guard.
    if signal == "traces":
        request = trace_request()
        records = request.resource_spans[0].scope_spans[0].spans
        mixed = records.add(name="GET")
        projector = project_traces
    elif signal == "logs":
        request = log_request()
        records = request.resource_logs[0].scope_logs[0].log_records
        mixed = records.add(body=AnyValue(string_value="request.completed"))
        projector = project_logs
    else:
        request = metric_request()
        records = (
            request.resource_metrics[0]
            .scope_metrics[0]
            .metrics[0]
            .histogram.data_points
        )
        mixed = records.add(count=1, sum=0.25, bucket_counts=[1])
        projector = project_metrics
    mixed.attributes.extend([attribute("http.response.status_code", 503), nested])
    assert [a.key for a in mixed.attributes] == [
        "http.response.status_code",
        "http.route",
    ]
    result = projector(request, policy=POLICY)
    assert CANARY.encode() not in result.SerializeToString()
    assert b"/items/{id}" in result.SerializeToString()
    if signal == "traces":
        projected = result.resource_spans[0].scope_spans[0].spans[-1]
        assert projected.name == "GET"
    elif signal == "logs":
        projected = result.resource_logs[0].scope_logs[0].log_records[-1]
        assert projected.body.string_value == "request.completed"
    else:
        projected = (
            result.resource_metrics[0]
            .scope_metrics[0]
            .metrics[0]
            .histogram.data_points[-1]
        )
        assert projected.count == 1 and projected.sum == 0.25
    assert projected.attributes == [attribute("http.response.status_code", 503)]


def test_long_keys_and_bad_strings_cannot_bypass_exact_configuration():
    long_key = "k" * 129
    assert not is_allowed_attribute(
        long_key,
        "safe",
        policy=replace(POLICY, attribute_values=frozenset({(long_key, "safe")})),
    )
    assert not is_allowed_attribute("http.route", "\ud800", policy=POLICY)
    assert not is_allowed_attribute("http.route", "x" * 1_000_000, policy=POLICY)
    assert is_allowed_attribute("http.route", "/items/{id}", policy=POLICY)


def test_small_string_limit_applies_to_generated_fallback_names():
    request = ExportTraceServiceRequest()
    scope = request.resource_spans.add().scope_spans.add()
    scope.spans.add(name="X")
    result = project_traces(request, limits=replace(DEFAULT_LIMITS, max_string_bytes=1))
    assert not result.resource_spans[0].scope_spans[0].spans
    assert (
        project_traces(trace_request(), policy=POLICY)
        .resource_spans[0]
        .scope_spans[0]
        .spans
    )


def test_multiple_resources_preserve_distinct_approved_writer_identity():
    request = metric_request()
    first = request.resource_metrics[0]
    first.resource.attributes.append(attribute("service.instance.id", "writer-a"))
    second = request.resource_metrics.add()
    second.CopyFrom(first)
    second.resource.attributes[-1].value.string_value = "writer-b"
    policy = replace(
        POLICY,
        attribute_values=POLICY.attribute_values
        | {("service.instance.id", "writer-a"), ("service.instance.id", "writer-b")},
    )
    projected = project_metrics(request, policy=policy)
    assert len(projected.resource_metrics) == 2
    assert (
        attribute("service.instance.id", "writer-a")
        in projected.resource_metrics[0].resource.attributes
    )
    assert (
        attribute("service.instance.id", "writer-b")
        in projected.resource_metrics[1].resource.attributes
    )


@pytest.mark.parametrize("level", ["groups", "scopes", "points"])
def test_total_metric_traversal_counts_have_a_batch_ceiling(level):
    request = metric_request()
    if level == "groups":
        collection = request.resource_metrics
    elif level == "scopes":
        collection = request.resource_metrics[0].scope_metrics
    else:
        collection = (
            request.resource_metrics[0]
            .scope_metrics[0]
            .metrics[0]
            .histogram.data_points
        )
    while len(collection) <= DEFAULT_LIMITS.max_batch_size:
        collection.add()
    assert not project_metrics(request, policy=POLICY).SerializeToString()
    assert project_metrics(metric_request(), policy=POLICY).SerializeToString()


def test_unknown_nested_wire_fields_do_not_survive_in_approved_span_fields():
    request = trace_request()
    span = request.resource_spans[0].scope_spans[0].spans[0]
    unknown = b"\xa2\x06" + bytes([len(CANARY)]) + CANARY.encode()
    for message in (
        span,
        span.status,
        span.events[0],
        span.links[0],
        span.attributes[0],
        span.attributes[0].value,
    ):
        message.MergeFromString(unknown)
    result = project_traces(request, policy=POLICY)
    assert CANARY.encode() not in result.SerializeToString()
    assert b"/items/{id}" in result.SerializeToString()


def test_numeric_predicate_does_not_consult_unknown_object_class():
    inspected = []

    class UnknownNumber:
        def __getattribute__(self, name):
            inspected.append(name)
            raise RuntimeError(CANARY)

    assert not is_allowed_attribute("http.server.request.duration", UnknownNumber())
    assert inspected == []
    assert is_allowed_attribute("http.server.request.duration", 0.25)


@pytest.mark.parametrize(
    "limits",
    [
        replace(DEFAULT_LIMITS, max_queue_size=513),
        replace(DEFAULT_LIMITS, max_batch_size=0),
        replace(DEFAULT_LIMITS, max_queue_size=127),
        replace(DEFAULT_LIMITS, max_depth=3),
        replace(DEFAULT_LIMITS, max_children=-1),
        replace(DEFAULT_LIMITS, max_attributes=True),
    ],
)
def test_configuration_rejects_invalid_or_raised_ceilings(limits):
    assert not is_valid_configuration(POLICY, limits)
    assert is_valid_configuration(POLICY, DEFAULT_LIMITS)
    assert is_valid_configuration(
        POLICY, replace(DEFAULT_LIMITS, max_batch_size=1, max_queue_size=1)
    )


def test_explicit_span_registration_cannot_override_sql_text_exclusion():
    name = f"SELECT '{CANARY}' /* {CANARY} */"
    policy = replace(POLICY, span_names=frozenset({name}))
    assert normalize_span_name(name, policy=policy) == "SELECT"
    result = project_traces(trace_request(), policy=policy)
    assert CANARY.encode() not in result.SerializeToString()


@pytest.mark.parametrize("surface", ["span", "log", "metric", "resource"])
def test_denied_key_is_rejected_before_materializing_large_protobuf_value(
    monkeypatch, surface
):
    if surface == "span":
        request = trace_request()
        attributes = request.resource_spans[0].scope_spans[0].spans[0].attributes
        projector, key = project_traces, "db.statement"
    elif surface == "log":
        request = log_request()
        attributes = request.resource_logs[0].scope_logs[0].log_records[0].attributes
        projector, key = project_logs, "password"
    elif surface == "metric":
        request = metric_request()
        attributes = (
            request.resource_metrics[0]
            .scope_metrics[0]
            .metrics[0]
            .histogram.data_points[0]
            .attributes
        )
        # Correlation is allowed on spans/logs, but forbidden as a metric label.
        projector, key = project_metrics, "trace_id"
    else:
        request = trace_request()
        attributes = request.resource_spans[0].resource.attributes
        projector, key = project_traces, "http.route"
    denied = attributes.add(key=key).value
    denied.string_value = CANARY + "x" * 1_000_000
    original_get = AnyValue.__getattribute__
    reads = []

    def guard_get(value, name):
        if value is denied and name == "string_value":
            reads.append(name)
            raise AssertionError("Denied protobuf value was materialized")
        return original_get(value, name)

    # Intercept the external protobuf accessor, keeping real generated messages
    # and the actual projector. The negative control proves the guard is live.
    monkeypatch.setattr(AnyValue, "__getattribute__", guard_get)
    with pytest.raises(AssertionError, match="Denied protobuf value"):
        _ = denied.string_value
    reads.clear()
    result = projector(request, policy=POLICY)
    assert reads == []
    assert CANARY.encode() not in result.SerializeToString()
    assert b"/items/{id}" in result.SerializeToString()


@pytest.mark.parametrize("key", ["posthogDistinctId", "altimeter.tenant_id"])
@pytest.mark.parametrize("value", ["opaque-42", "x" * 200, "é" * 100])
def test_identity_survives_only_span_and_log_record_attributes(key, value):
    traces = trace_request()
    logs = log_request()
    span = traces.resource_spans[0].scope_spans[0].spans[0]
    record = logs.resource_logs[0].scope_logs[0].log_records[0]
    span.attributes.append(attribute(key, value))
    record.attributes.append(attribute(key, value))
    # The identity keys are fixed policy, not configured attribute-value pairs.
    assert (key, value) not in POLICY.attribute_values
    assert is_allowed_attribute(key, value, policy=POLICY)
    projected_trace = project_traces(traces, policy=POLICY)
    projected_log = project_logs(logs, policy=POLICY)
    safe_span = projected_trace.resource_spans[0].scope_spans[0].spans[0]
    safe_log = projected_log.resource_logs[0].scope_logs[0].log_records[0]
    assert attribute(key, value) in safe_span.attributes
    assert attribute(key, value) in safe_log.attributes
    assert attribute("http.response.status_code", 503) in safe_span.attributes
    assert attribute("http.response.status_code", 503) in safe_log.attributes
    assert CANARY.encode() not in projected_trace.SerializeToString()
    assert CANARY.encode() not in projected_log.SerializeToString()


@pytest.mark.parametrize("key", ["posthogDistinctId", "altimeter.tenant_id"])
@pytest.mark.parametrize("value", ["", " \t\n", "x" * 201, "é" * 101, 17, 2.5])
def test_invalid_identity_is_dropped_beside_valid_identity_and_safe_status(key, value):
    traces = trace_request()
    logs = log_request()
    other = "altimeter.tenant_id" if key == "posthogDistinctId" else "posthogDistinctId"
    span = traces.resource_spans[0].scope_spans[0].spans[0]
    record = logs.resource_logs[0].scope_logs[0].log_records[0]
    for target in (span, record):
        target.attributes.extend([attribute(key, value), attribute(other, "opaque-ok")])
    assert not is_allowed_attribute(key, value, policy=POLICY)
    outputs = (project_traces(traces, policy=POLICY), project_logs(logs, policy=POLICY))
    safe_records = (
        outputs[0].resource_spans[0].scope_spans[0].spans[0],
        outputs[1].resource_logs[0].scope_logs[0].log_records[0],
    )
    for target in safe_records:
        assert key not in {a.key for a in target.attributes}
        assert attribute(other, "opaque-ok") in target.attributes
        assert attribute("http.response.status_code", 503) in target.attributes
    assert all(CANARY.encode() not in output.SerializeToString() for output in outputs)


@pytest.mark.parametrize("key", ["posthogDistinctId", "altimeter.tenant_id"])
def test_identity_never_survives_other_locations_even_with_explicit_value_pair(key):
    policy = replace(POLICY, attribute_values=POLICY.attribute_values | {(key, CANARY)})
    traces, logs, metrics = trace_request(), log_request(), metric_request()
    span = traces.resource_spans[0].scope_spans[0].spans[0]
    record = logs.resource_logs[0].scope_logs[0].log_records[0]
    point = (
        metrics.resource_metrics[0].scope_metrics[0].metrics[0].histogram.data_points[0]
    )
    for resource in (
        traces.resource_spans[0].resource,
        logs.resource_logs[0].resource,
        metrics.resource_metrics[0].resource,
    ):
        resource.attributes.append(attribute(key, CANARY))
    span.events[0].attributes.append(attribute(key, CANARY))
    span.links[0].attributes.append(attribute(key, CANARY))
    record.body.kvlist_value.values.append(attribute(key, CANARY))
    point.attributes.append(attribute(key, CANARY))
    point.exemplars[0].filtered_attributes.append(attribute(key, CANARY))
    span.attributes.append(attribute(key, "person-ok"))
    record.attributes.append(attribute(key, "person-ok"))
    outputs = (
        project_traces(traces, policy=policy),
        project_logs(logs, policy=policy),
        project_metrics(metrics, policy=policy),
    )
    assert all(CANARY.encode() not in output.SerializeToString() for output in outputs)
    assert (
        attribute(key, "person-ok")
        in outputs[0].resource_spans[0].scope_spans[0].spans[0].attributes
    )
    assert (
        attribute(key, "person-ok")
        in outputs[1].resource_logs[0].scope_logs[0].log_records[0].attributes
    )
    safe_point = (
        outputs[2]
        .resource_metrics[0]
        .scope_metrics[0]
        .metrics[0]
        .histogram.data_points[0]
    )
    assert safe_point.count == 1 and safe_point.sum == 0.25
    assert attribute("http.route", "/items/{id}") in safe_point.attributes
    assert not safe_point.exemplars[0].filtered_attributes


def test_identity_respects_a_smaller_configured_string_limit():
    limits = replace(DEFAULT_LIMITS, max_string_bytes=12)
    assert is_allowed_attribute("posthogDistinctId", "x" * 12, limits=limits)
    assert not is_allowed_attribute("posthogDistinctId", "x" * 13, limits=limits)


@pytest.mark.parametrize(
    "build,project,groups",
    [
        (trace_request, project_traces, "resource_spans"),
        (log_request, project_logs, "resource_logs"),
        (metric_request, project_metrics, "resource_metrics"),
    ],
)
def test_probe_flag_true_survives_only_resources(build, project, groups):
    request = build()
    resource = getattr(request, groups)[0].resource
    resource.attributes.append(attribute("service.name", "reference"))
    flag = KeyValue(key="altimeter.probe", value=AnyValue(bool_value=True))
    resource.attributes.append(flag)
    if groups == "resource_spans":
        record = request.resource_spans[0].scope_spans[0].spans[0]
        record.attributes.append(flag)
        record.events[0].attributes.append(flag)
        record.links[0].attributes.append(flag)
    elif groups == "resource_logs":
        record = request.resource_logs[0].scope_logs[0].log_records[0]
        record.attributes.append(flag)
        record.body.kvlist_value.values.append(flag)
    else:
        point = (
            request.resource_metrics[0]
            .scope_metrics[0]
            .metrics[0]
            .histogram.data_points[0]
        )
        point.attributes.append(flag)
        point.exemplars[0].filtered_attributes.append(flag)
    assert CANARY.encode() in request.SerializeToString()
    projected = project(request, policy=POLICY)
    safe_resource = getattr(projected, groups)[0].resource
    assert flag in safe_resource.attributes
    assert attribute("service.name", "reference") in safe_resource.attributes
    assert projected.SerializeToString().count(b"altimeter.probe") == 1
    assert CANARY.encode() not in projected.SerializeToString()
    assert b"/items/{id}" in projected.SerializeToString()
    assert is_allowed_attribute("altimeter.probe", True, resource=True)
    assert not is_allowed_attribute("altimeter.probe", True)


@pytest.mark.parametrize(
    "build,project,groups",
    [
        (trace_request, project_traces, "resource_spans"),
        (log_request, project_logs, "resource_logs"),
        (metric_request, project_metrics, "resource_metrics"),
    ],
)
@pytest.mark.parametrize(
    "invalid",
    [
        AnyValue(),
        AnyValue(bool_value=False),
        AnyValue(int_value=1),
        AnyValue(double_value=1.0),
        AnyValue(string_value="true"),
        AnyValue(string_value=CANARY),
        AnyValue(bytes_value=CANARY.encode()),
        AnyValue(array_value={"values": [AnyValue(bool_value=True)]}),
        AnyValue(kvlist_value={"values": [attribute("secret", CANARY)]}),
    ],
)
def test_probe_resource_flag_rejects_other_values_beside_safe_group(
    build, project, groups, invalid
):
    request = build()
    group = getattr(request, groups)[0]
    group.resource.attributes.append(attribute("service.name", "reference"))
    group.resource.attributes.add(key="altimeter.probe", value=invalid)
    neighbor = getattr(request, groups).add()
    neighbor.CopyFrom(group)
    neighbor.resource.attributes[-1].value.CopyFrom(AnyValue(bool_value=True))
    policy = replace(
        POLICY,
        attribute_values=POLICY.attribute_values
        | {("altimeter.probe", "true"), ("altimeter.probe", CANARY)},
    )
    projected = project(request, policy=policy)
    safe_groups = getattr(projected, groups)
    assert len(safe_groups) == 2
    assert "altimeter.probe" not in {a.key for a in safe_groups[0].resource.attributes}
    assert KeyValue(key="altimeter.probe", value=AnyValue(bool_value=True)) in (
        safe_groups[1].resource.attributes
    )
    assert attribute("service.name", "reference") in safe_groups[0].resource.attributes
    assert CANARY.encode() not in projected.SerializeToString()
    assert b"/items/{id}" in projected.SerializeToString()


@pytest.mark.parametrize(
    "build,project,groups",
    [
        (trace_request, project_traces, "resource_spans"),
        (log_request, project_logs, "resource_logs"),
        (metric_request, project_metrics, "resource_metrics"),
    ],
)
def test_probe_resource_rejects_large_string_before_materialization(
    monkeypatch, build, project, groups
):
    request = build()
    attributes = getattr(request, groups)[0].resource.attributes
    denied = attributes.add(key="altimeter.probe").value
    denied.string_value = CANARY + "x" * 1_000_000
    flag = KeyValue(key="altimeter.probe", value=AnyValue(bool_value=True))
    attributes.append(flag)
    reads = []
    original_get = AnyValue.__getattribute__

    def guard_get(value, name):
        if value is denied and name == "string_value":
            reads.append(name)
            raise AssertionError("Non-boolean probe flag was materialized")
        return original_get(value, name)

    monkeypatch.setattr(AnyValue, "__getattribute__", guard_get)
    with pytest.raises(AssertionError, match="Non-boolean probe flag"):
        _ = denied.string_value
    reads.clear()
    projected = project(request, policy=POLICY)
    assert reads == []
    assert flag in getattr(projected, groups)[0].resource.attributes
    assert b"/items/{id}" in projected.SerializeToString()
    assert CANARY.encode() not in projected.SerializeToString()


@pytest.mark.parametrize(
    ("key", "safe", "invalid"),
    [
        ("http.request.method", "_OTHER", CANARY),
        ("http.response.status_class", "5xx", CANARY),
        ("altimeter.http.outcome", "aborted", CANARY),
        ("altimeter.surface", "django", CANARY),
        ("altimeter.response_mode", "streaming", CANARY),
        ("altimeter.http.server.concurrent_at_start", 3, -1),
        ("altimeter.http.response_ready.duration", 0.25, float("inf")),
    ],
)
def test_native_http_closed_values_keep_safe_neighbor_and_drop_canary(
    key, safe, invalid
):
    policy = replace(
        POLICY, attribute_values=POLICY.attribute_values | {(key, str(invalid))}
    )
    request = trace_request()
    spans = request.resource_spans[0].scope_spans[0].spans
    positive = spans.add(name="GET", kind=2)
    positive.attributes.extend(
        [attribute(key, safe), attribute("http.response.status_code", 200)]
    )
    negative = spans.add(name="GET", kind=2)
    negative.attributes.extend(
        [attribute(key, invalid), attribute("http.response.status_code", 503)]
    )
    result = project_traces(request, policy=policy)
    assert CANARY.encode() not in result.SerializeToString()
    projected = result.resource_spans[0].scope_spans[0].spans
    assert projected[-2].attributes == [
        attribute(key, safe),
        attribute("http.response.status_code", 200),
    ]
    assert projected[-1].attributes == [attribute("http.response.status_code", 503)]


def test_request_numeric_details_never_become_metric_dimensions_or_resources():
    request = metric_request()
    group = request.resource_metrics[0]
    point = group.scope_metrics[0].metrics[0].histogram.data_points[0]
    for key, value in (
        ("altimeter.http.server.concurrent_at_start", 3),
        ("altimeter.http.response_ready.duration", 0.25),
    ):
        point.attributes.append(attribute(key, value))
        group.resource.attributes.append(attribute(key, value))
    result = project_metrics(request, policy=POLICY)
    safe = (
        result.resource_metrics[0].scope_metrics[0].metrics[0].histogram.data_points[0]
    )
    assert attribute("http.route", "/items/{id}") in safe.attributes
    assert not any(a.key.startswith("altimeter.http.") for a in safe.attributes)
    assert not any(
        a.key.startswith("altimeter.http.")
        for a in result.resource_metrics[0].resource.attributes
    )


def test_standard_request_histogram_keeps_all_fixed_long_buckets_but_not_other_shapes():
    boundaries = (
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
    request = metric_request()
    scope = request.resource_metrics[0].scope_metrics[0]
    histogram = scope.metrics[0].histogram
    long = histogram.data_points.add(
        count=3,
        sum=116,
        bucket_counts=[0] * 15 + [1, 1, 1, 0, 0],
        explicit_bounds=boundaries,
    )
    long.attributes.extend(
        [attribute("http.route", "/items/{id}"), attribute("secret", CANARY)]
    )
    wrong_shape = histogram.data_points.add()
    wrong_shape.CopyFrom(long)
    wrong_shape.explicit_bounds[-1] = 121
    foreign = scope.metrics.add(name="other.duration", unit="s")
    foreign.histogram.CopyFrom(histogram)
    policy = replace(
        POLICY,
        metric_instruments=POLICY.metric_instruments
        | {("django_altimeter", "other.duration")},
    )
    projected = project_metrics(request, policy=policy)
    assert CANARY.encode() not in projected.SerializeToString()
    metrics = {
        metric.name: metric
        for metric in projected.resource_metrics[0].scope_metrics[0].metrics
    }
    points = metrics["http.server.request.duration"].histogram.data_points
    assert len(points) == 2
    assert points[-1].explicit_bounds == list(boundaries)
    assert points[-1].count == 3 and points[-1].sum == 116
    assert points[-1].bucket_counts[-1] == 0
    assert points[-1].attributes == [attribute("http.route", "/items/{id}")]
    assert len(metrics["other.duration"].histogram.data_points) == 1
    assert DEFAULT_LIMITS.max_children == 16


def test_http_numeric_details_are_server_span_only_and_error_event_is_fixed():
    keys = [
        attribute("altimeter.http.server.concurrent_at_start", 4),
        attribute("altimeter.http.response_ready.duration", 0.5),
    ]
    request = trace_request()
    scope = request.resource_spans[0].scope_spans[0]
    server = scope.spans.add(name="GET", kind=2)
    server.attributes.extend(keys)
    server.events.add(name="exception").attributes.extend(
        keys
        + [
            attribute("error.type", "after_response"),
            attribute("exception.message", CANARY),
        ]
    )
    server.links.add(trace_id=TRACE_ID, span_id=SPAN_ID).attributes.extend(keys)
    internal = scope.spans.add(name="SPAN", kind=1)
    internal.attributes.extend(keys + [attribute("http.response.status_code", 200)])
    result = project_traces(request, policy=POLICY)
    spans = result.resource_spans[0].scope_spans[0].spans
    assert spans[-2].attributes == keys
    assert spans[-2].events[0].attributes == [attribute("error.type", "after_response")]
    assert not spans[-2].links[0].attributes
    assert spans[-1].attributes == [attribute("http.response.status_code", 200)]
    assert CANARY.encode() not in result.SerializeToString()
    logs = log_request()
    logs.resource_logs[0].scope_logs[0].log_records[-1].attributes.extend(keys)
    record = (
        project_logs(logs, policy=POLICY).resource_logs[0].scope_logs[0].log_records[-1]
    )
    assert record.body.string_value == "request.completed"
    assert not any(item.key in {key.key for key in keys} for item in record.attributes)
