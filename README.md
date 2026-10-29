# django-altimeter

This package provides bounded, privacy-preserving OpenTelemetry components for
Django applications: record admission, allow-list projection, and OTLP/HTTP
export.

The current development release supports explicit composition with native
OpenTelemetry providers. Runtime setup (`setup()` and `shutdown()`) and
Django/queue integration are not available yet.

## Install

Build and install the local development wheel:

```sh
uv build
uv pip install dist/django_altimeter-0.1.0.dev0-py3-none-any.whl
```

Queue dependencies are optional extras. For a local checkout:

```sh
uv pip install '.[celery]'
uv pip install '.[rq]'
```

The base package does not install Celery or RQ. Importing `django_altimeter`
performs no setup and starts no threads. The extras supply dependencies.
Queue integrations are not included yet.

## Compatibility

The tested combinations are Python 3.12 / Django 5.2 and Python 3.13 / Django
6.0. Package metadata requires Python >=3.12 and Django >=5.2,<6.1. Other Python
versions have not been tested.

OpenTelemetry API, SDK, and OTLP/HTTP exporter use >=1.45.1,<1.46 with the
matching 0.66b1 semantic conventions. Queue extras use Celery >=5.6.3,<5.7
and Kombu >=5.6.2,<5.7, or RQ >=2.10.0,<2.11. The development lock currently
resolves those families to 1.45.1/0.66b1, 5.6.3/5.6.2, and 2.10.0 respectively.

## Usage

Construct providers explicitly and give each signal its own sanitizing session.
Use a static policy containing only the names and values your application may
export. Creating a batch processor starts its native worker; shut down its
provider when you finish.

Set `OTEL_EXPORTER_OTLP_TRACES_ENDPOINT` to your complete OTLP/HTTP trace endpoint,
for example `http://localhost:4318/v1/traces`. This example sends one span:

```python
import os

from opentelemetry.sdk.resources import Resource
from opentelemetry.sdk.trace import TracerProvider

from django_altimeter.exporting import (
    BoundedSpanProcessor,
    SanitizingSession,
    create_span_exporter,
    native_span_limits,
)
from django_altimeter.privacy import PrivacyPolicy

policy = PrivacyPolicy(
    attribute_values=frozenset(
        {("service.name", "example-app"), ("http.route", "/health")}
    ),
)
session = SanitizingSession(
    "traces", os.environ["OTEL_EXPORTER_OTLP_TRACES_ENDPOINT"], policy=policy
)
provider = TracerProvider(
    resource=Resource({"service.name": "example-app"}),
    span_limits=native_span_limits(),
    shutdown_on_exit=False,
)
provider.add_span_processor(
    BoundedSpanProcessor(create_span_exporter(session, timeout=2.0))
)
try:
    tracer = provider.get_tracer("django_altimeter")
    with tracer.start_as_current_span("GET") as span:
        span.set_attribute("http.route", "/health")
        span.set_attribute("http.response.status_code", 200)
    provider.force_flush(timeout_millis=5_000)
    print(session.outcome)
finally:
    provider.shutdown()
```

For logs, set `OTEL_EXPORTER_OTLP_LOGS_ENDPOINT` to the complete log endpoint,
such as `http://localhost:4318/v1/logs`, and explicitly allow the log body:

```python
import os

from opentelemetry._logs import SeverityNumber
from opentelemetry.sdk._logs import LoggerProvider
from opentelemetry.sdk.resources import Resource

from django_altimeter.exporting import (
    SanitizingSession,
    bounded_log_processor,
    create_log_exporter,
    native_log_limits,
)
from django_altimeter.privacy import PrivacyPolicy

policy = PrivacyPolicy(
    log_bodies=frozenset({"request.complete"}),
    attribute_values=frozenset({("service.name", "example-app")}),
)
session = SanitizingSession(
    "logs", os.environ["OTEL_EXPORTER_OTLP_LOGS_ENDPOINT"], policy=policy
)
provider = LoggerProvider(
    resource=Resource({"service.name": "example-app"}),
    log_record_limits=native_log_limits(),
    shutdown_on_exit=False,
)
provider.add_log_record_processor(
    bounded_log_processor(create_log_exporter(session, timeout=2.0))
)
try:
    provider.get_logger("django_altimeter").emit(
        body="request.complete",
        severity_number=SeverityNumber.INFO,
        attributes={"http.response.status_code": 200},
    )
    provider.force_flush(timeout_millis=5_000)
    print(session.outcome)
finally:
    provider.shutdown()
```

Register only the bounded processor with each provider. Pass authentication in
exporter `headers`, never in resource attributes or policy values. The examples
print the latest transport outcome after flushing; the acceptance and loss
rules below explain how to interpret it.

## Safe projection and admission

`django_altimeter.privacy` supplies frozen `PrivacyPolicy` / `PrivacyLimits`
values and `project_traces`, `project_logs`, and `project_metrics`. Each function
reconstructs a generated OTLP request from permitted fields without mutating
its input. Policy names and string values must come from trusted static
configuration, never request data. Unknown attribute keys remain forbidden even
if configured. Route templates, resource identity, task/queue/operation names,
and log messages require exact approved values. Fixed HTTP/DB enums, bounded
status/duration values, and valid trace/span correlation IDs can survive beside
removed unsafe fields.

Raw paths and queries, SQL text and parameters, bodies, cookies, authentication
headers, task arguments/results, exception prose/locals, and unknown structured
fields are excluded. Status descriptions and stack traces are removed. Metrics
require exact scope/instrument pairs; exemplar filtered attributes are removed
while native trace/span IDs can remain. Default policy includes no application
log bodies or metric instruments. Wire projection does not implement metric
Views or limit series retained before aggregation.

`django_altimeter.exporting` supplies `BoundedSpanProcessor` and the
`bounded_log_processor()` construction function. Each resulting processor owns
one native batch processor, checks retained types and declared limits without
mutation, and delegates an admitted record once. Register only the wrapper with
the provider. Admission drops records only for unsupported types or exceeded
limits, with bounded diagnostic codes. Projection applies the allow-list policy
field by field, preserving permitted route/status/duration/correlation alongside
removed unapproved fields. The exporter factories apply projection before sending.
`native_span_limits()` and `native_log_limits()` supply the corresponding SDK
limits for explicit provider construction. Call `normalize_span_name()` at an
early instrumentation hook to reduce SQL/HTTP names to approved operations
before admission. Provider construction and application integration remain
explicit.

Defaults declared in `PrivacyLimits` and enforced by these components:

| Boundary | Ceiling |
| --- | --- |
| Native span/log queues | 512 records per signal |
| Native export batch | 128 records |
| Record, event, or link attributes | 32 |
| Resource attributes | 16 |
| Attribute key | 128 UTF-8 bytes |
| Allowed string, name, or log body string | 256 UTF-8 bytes |
| Span events / links | 8 / 4 |
| Admitted retained-record payload estimate | 16 KiB |
| Projected record protobuf | 16 KiB |
| Serialized request before decoding and after projection | 1 MiB |
| Uncompressed response body | 64 KiB |
| Approved structured log projection | Depth 2, 16 children per container |

SDK admission accepts scalar attributes and rejects nested containers without
walking them. Structured log reconstruction is a separate projection path.
Projection also bounds groups, scopes, and records/points using the 128-entry
batch ceiling, and metric buckets/quantiles/exemplars using the 16-child ceiling.
The retained-record estimate and protobuf output limit do not measure Python
heap usage or peak transient memory. Steady-state RSS under load has not been
measured. Transport enforces `max_batch_bytes` before parsing and after projection,
and `max_response_bytes` while streaming. Response reads retain at most 64 KiB
plus one byte used to detect overflow.

## OTLP transport, acceptance, and loss

Construct one `SanitizingSession(signal, endpoint, *, policy, limits)` for each
signal (`traces`, `logs`, or `metrics`), then pass it to `create_span_exporter`,
`create_log_exporter`, or `create_metric_exporter`. The factories use the native
OTLP/HTTP exporters with compression off. The session permits only the configured
POST endpoint, sends freshly projected protobuf with `Accept-Encoding: identity`,
disables redirects, and streams responses. Received responses are closed. The
detached response retains only status, a fixed safe reason, and valid bounded
`Retry-After`; original server prose, request credentials, and cookies are
discarded.

Read the frozen `session.outcome` for the latest attempt or local no-send result.
Its `accepted_records`, `rejected_records`, and `local_dropped_records` count
spans, log records, or metric data points. They are separate, noncumulative
counts. Filtering an individual field is not a dropped record. Zero accepted on
a failed attempt means server acceptance is unknown; the server may already
have received the data. SDK `SUCCESS` or `force_flush()` alone does not mean
every record was accepted or durably stored.

| Outcome | Meaning |
| --- | --- |
| `export.accepted` | HTTP 200 with valid signal-specific protobuf, including a valid empty body, and no rejected records or warning. All sent records in this attempt were accepted. |
| `export.partial` | A valid response reports a positive rejected-record count. Accepted and rejected counts remain separate. No retry, including when all records were rejected; replay could duplicate accepted data. |
| `export.warning` | A valid response reports zero rejected records and a warning. The attempt is accepted with this fixed warning code; server warning text is discarded. |
| Fixed protocol error | Redirects, unexpected non-200 success statuses, wrong content type or encoding, malformed/truncated/oversized bodies, ambiguous framing, and invalid rejection counts fail without retry and report zero accepted records. Non-identity HTTP 200 encoding is refused before reading or decompressing its body. |
| `export.http_error` | Ordinary 4xx/5xx bodies are discarded unread. Actual status and a valid bounded `Retry-After` remain available to the native retry code. The pinned client retries 429, 502, 503, and 504, subject to its deadline and retry budget; connection failures also use its native retry path. |
| Local sanitizer rejection | Invalid input, excessive counts/bytes, projection failure, or zero surviving records sends nothing for that attempt and reports zero accepted records. There is no fallback to the original bytes. A native no-send path resets the outcome to `export.not_sent`. |

Request bytes are capped at 1 MiB before protobuf decoding and after projection,
before serialization/send. The record, field, and count limits above also apply.
Uncompressed response bodies are capped at 64 KiB; invalid or oversized replies
are protocol failures. Known local record drops remain separate from remote
rejections even if response handling subsequently fails. Queues remain finite
(512 span/log records per signal, batches of at most 128); limit violations,
queue overflow, shutdown, or exhausted native retries can lose telemetry. There
is no disk spool, outbox, fallback payload, or second retry loop. An ambiguous
network failure can cause a native retry to duplicate data already accepted by
the receiver.

The deadline covers connection establishment, a hang/no response, slow response
bodies, and delays split across headers and body. Request decoding/projection
uses the same remaining budget. The metric factory also honors each export's
`timeout_millis`, capped by its constructor timeout, including lock acquisition
and native retry/backoff. **Continuous trickling of response header bytes is not
covered:** the pinned Requests/urllib3 transport restarts its per-read header
timeout before the body watchdog can run. This is a known transport limitation.

Metric per-call backoff timing depends on the pinned OpenTelemetry 1.45.x private
client field `_client._timeout`. The factory guards that field's presence and
finite positive numeric type, serializes metric exports within the caller's
budget, temporarily caps it, calls the native export once, and restores it in
`finally`. If the guard is unsupported, native state stays unpatched and the
outcome reports `metrics.timeout_unsupported`; the per-call native-backoff
guarantee is then unavailable. Native failures are contained.

Internal diagnostics use the stdlib logger **`django_altimeter.export`** at
`WARNING`. Messages contain only a fixed code and signal name. Each signal
session emits a given code at most once per 60-second monotonic window; different
codes or signal sessions have independent windows. Timestamp updates are
thread-safe, and logging-handler errors do not change export behavior. Full
acceptance (`export.accepted`) emits no warning. Keep this internal logger out of
exported-log handlers to avoid feeding diagnostics back into telemetry.

The closed set of warning codes is:

```text
export.not_sent       export.batch_count    export.batch_size
export.request        export.destination    export.encoding
export.content_type   export.projection     export.empty
export.http_error     export.status         export.response_size
export.rejected_count export.partial        export.warning
export.deadline       export.connection     export.invalid
export.failed         export.framing        metrics.timeout_unsupported
```

## Development

Revalidate the metric timeout compatibility guard and deadline tests before
changing the OTel version bounds.

```sh
uv sync --all-extras --dev
uv run ruff check .
uv run ruff format --check .
uv run mypy src
uv build
uv run pytest tests/unit/test_privacy.py tests/unit/test_admission.py tests/integration/test_wheel.py
uv run pytest tests/integration/test_otlp_transport.py
```

Distribution tests use the existing `dist/` artifacts; rebuild after changing
package files. They inspect both archives, install the wheel without extras into
a clean venv, verify its hash and import origin, and observe thread starts during
import. The integration marker is registered for real-process and built-artifact
tests.

Run the two Linux verification profiles after building, with a unique project
name for each run:

```sh
docker compose -p <unique-asgi-name> -f reference/compose.yaml --profile asgi run --build --rm verify-asgi python -m pytest tests/integration/test_wheel.py
docker compose -p <unique-wsgi-name> -f reference/compose.yaml --profile wsgi run --build --rm verify-wsgi python -m pytest tests/integration/test_wheel.py
```

Verify the native transport against both installed-wheel profiles:

```sh
docker compose -p <unique-asgi-transport-name> -f reference/compose.yaml --profile asgi run --build --rm verify-asgi python -m pytest tests/integration/test_otlp_transport.py
docker compose -p <unique-wsgi-transport-name> -f reference/compose.yaml --profile wsgi run --build --rm verify-wsgi python -m pytest tests/integration/test_otlp_transport.py
```

The ASGI image pins Requests 2.33.1 / urllib3 2.6.3; the WSGI image pins
Requests 2.32.3 / urllib3 1.26.20. Both exercise the same transport contract.

The images install the built wheel, copy both distribution artifacts and tests,
and verify the installed package against that wheel. They contain no source
checkout or editable package installation. The image and its nested clean venv
both use the declared Python/Django profile. These profiles currently verify
packaging and local OTLP behavior; server and worker workloads are not covered
yet.

The function-scoped `otlp_receiver` fixture listens on an ephemeral loopback
port. `.endpoint` gives the base URL and `.url("traces")`, `.url("logs")`, or
`.url("metrics")` gives a signal endpoint. `.requests` is a tuple snapshot;
`.wait_for_requests(count=1, timeout=5.0)` waits for captures or raises
`TimeoutError`. Captures contain `path`, case-insensitive `headers`, and decoded
protobuf `message`. The receiver accepts uncompressed protobuf POSTs up to 1 MiB
and returns an empty successful OTLP response by default. Scripted responses
exercise the transport failure cases. Receiver and processor checks run with:

```sh
uv run pytest tests/support/test_otlp_receiver.py tests/integration/test_processor_export.py
```

Tests run against local receivers. PostHog ingestion and deployed application
behavior remain untested.
