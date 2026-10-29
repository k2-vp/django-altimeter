# django-altimeter

This package provides bounded, privacy-preserving OpenTelemetry components for
Django applications: record admission, allow-list projection, and OTLP/HTTP
export.

The current development release provides explicit runtime setup and shutdown,
safe application spans, correlation, and trusted identity enrichment. Automatic
HTTP and queue instrumentation are not included yet. Telemetry is disabled by
default.

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

## Runtime lifecycle

Add `"django_altimeter"` to Django's `INSTALLED_APPS` to register configuration
checks. `AppConfig.ready()` only registers these checks. Importing the package,
running migrations, and opening a Django shell do not initialize telemetry.
Invalid telemetry settings produce fixed-code `django_altimeter.W001` warnings
without preventing Django startup.

Enable telemetry and set the required resource identity. For a local OTLP/HTTP
collector, configure one full endpoint per signal:

```sh
export ALTIMETER_ENABLED=true
export ALTIMETER_SERVICE_NAME=example-app
export ALTIMETER_DEPLOYMENT_ENVIRONMENT=development
export ALTIMETER_RELEASE=local
export ALTIMETER_TRACES_COLLECTOR_ENDPOINT=http://127.0.0.1:4318/v1/traces
export ALTIMETER_LOGS_COLLECTOR_ENDPOINT=http://127.0.0.1:4318/v1/logs
export ALTIMETER_METRICS_COLLECTOR_ENDPOINT=http://127.0.0.1:4318/v1/metrics
export OTEL_SEMCONV_STABILITY_OPT_IN=http
```

Signals default to enabled when the package is enabled. Set, for example,
`ALTIMETER_LOGS_ENABLED=false` to disable one signal. Disabled signals need no
destination. Endpoints require HTTPS except for literal loopback HTTP collector
addresses. Each signal has one destination; conflicting routes are rejected.
Collector authentication is separate from PostHog ingestion credentials. Set
`ALTIMETER_{TRACES,LOGS,METRICS}_COLLECTOR_HEADERS` for collector authentication;
non-loopback collectors require their own credentials.

For PostHog, use `ALTIMETER_{TRACES,LOGS,METRICS}_ENDPOINT` with the corresponding
`https://us.i.posthog.com/i/v1/{signal}` or EU regional URL, and supply the project
ingestion token in `ALTIMETER_POSTHOG_TOKEN`. Configure only one route per signal.

Environment values control deployment settings. A small `settings.ALTIMETER`
dictionary supplies trusted static policy and callback configuration. Explicit
`setup(role=...)` overrides the environment role; `ALTIMETER_*` values take
precedence over supported standard `OTEL_*` values and defaults. Credentials
never enter resource attributes or displayed configuration. Auto-discovered
native resource detectors are unsupported: a nonempty
`OTEL_EXPERIMENTAL_RESOURCE_DETECTORS` setting disables setup with
`setup.resource_detector_conflict`. Use validated static resource values.
For example:

```python
ALTIMETER = {
    "SPAN_NAMES": frozenset({"report.generate"}),
    "LOG_BODIES": frozenset({"request.complete"}),
    "METRIC_INSTRUMENTS": frozenset({("django_altimeter", "runtime.calls")}),
}
```

`IDENTITY_RESOLVER` accepts a trusted callable or dotted path, loaded only after
Django's app registry is ready. It must use already-resolved in-memory facts and
return `django_altimeter.context.Identity(distinct_id=..., tenant_id=...)` or
`None`; it must not perform authentication, ORM queries, or other I/O.

| Environment setting | Default and range |
| --- | --- |
| `ALTIMETER_SAMPLE_RATE` | `1.0`, from `0` to `1`; metrics are independent |
| `ALTIMETER_TRUST_REMOTE_PARENT` | `false`; supported parent-based OTel sampler settings can opt in |
| `ALTIMETER_EXPORT_TIMEOUT` | `1` second, from `0.001` to `2` |
| `ALTIMETER_SHUTDOWN_TIMEOUT` | `2` seconds, from `0.001` to `2` |
| `ALTIMETER_BATCH_SCHEDULE_DELAY_MILLIS` | `5000`, from `1` to `60000` |
| `ALTIMETER_METRIC_EXPORT_INTERVAL_MILLIS` | `10000`, from `1` to `60000` |

`ALTIMETER_MAX_*` settings match the `PrivacyLimits` field names below and can
only lower their ceilings. Shared standard OTel span/log queue and batch limits
must agree unless an Altimeter value overrides them.

Call `setup(role="web")` explicitly after Django initializes, in the worker that
will serve requests. Enabled setup starts native batch workers. The pinned
urllib3 import may create and bind a local IPv6 socket to `::1` on port `0` to
check IPv6 support; setup makes no outbound connection, sends no data, resolves
no DNS name, and probes no export endpoint. Disabled, invalid, and unowned setup
create no sockets. Inspect `SetupResult.enabled_signals` and `codes`;
`ok` means at least one signal is enabled, so a partial ownership conflict may
still leave other signals working.

| Function | Behavior |
| --- | --- |
| `setup(*, role=None)` | Loads and validates Django/environment configuration, then installs owned providers. Returns frozen `SetupResult(enabled_signals, codes)`. |
| `shutdown(*, timeout=2.0)` | Drains owned signals within one shared deadline. Returns frozen `ShutdownResult(completed, codes)`. |
| `span(name, *, attributes=None)` | Creates a registered span, or an explicit no-op when tracing is unavailable. Pass a plain dictionary of bounded scalar values; unsupported containers are ignored before the SDK can inspect them. Application exceptions propagate unchanged. |
| `get_correlation()` | Returns native trace/span IDs and trace flags only while this worker owns tracing; otherwise returns `None`. |
| `enrich(subject)` | Calls the configured trusted identity resolver for an already-resolved subject. It never authenticates a request or loads a Django model. |

For a script with `DJANGO_SETTINGS_MODULE` already set, initialize Django before
calling the public API:

```python
import django

from django_altimeter import get_correlation, setup, shutdown, span

django.setup()
result = setup(role="web")
try:
    print(result.enabled_signals, result.codes)
    with span("GET", attributes={"http.response.status_code": 200}):
        print(get_correlation())
finally:
    print(shutdown(timeout=2.0))
```

Repeated or concurrent setup with the same effective configuration installs once.
Changing configuration after setup requires a worker restart. Each signal checks
ownership independently: an existing third-party provider is left untouched, and
Altimeter disables its use of that signal. Unowned tracing discloses no
correlation or identity. Every exporting worker has a fresh UUID
`service.instance.id`, including for delta metrics.

Keep setup and shutdown in the application's explicit startup and cleanup
hooks. **Gunicorn preload is unsupported.** Initialize after forking; a child
that inherits initialized providers disables the affected signals. Automatic
HTTP instrumentation and server lifecycle adapters are not included.

Use one shutdown call at worker exit, outside request and task bodies:

```python
from django_altimeter import shutdown

result = shutdown(timeout=2.0)
print(result.completed, result.codes)
```

Two seconds is the total default budget across traces, logs, metrics, HTTP
retries, and cleanup, capped further by configuration or a shorter caller
budget. Native providers have independent exit drains disabled; one coordinator
owns normal-exit cleanup. A timeout can leave pending telemetry unsent. SIGKILL,
a crash, or container termination before the drain can also lose queued data.
There is no disk spool or durable outbox. A completed shutdown does not imply
durable remote ingestion.

To disable telemetry, set the environment value and restart the workers:

```sh
export ALTIMETER_ENABLED=false
```

## Direct component usage

For standalone component use, run these examples in a separate script without
calling `setup()`. Construct providers explicitly and give each signal its own
sanitizing session.
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
while native trace/span IDs can remain. The identity resolver may supply only
`posthogDistinctId` and `altimeter.tenant_id` as nonblank strings of at most
200 UTF-8 bytes, also respecting a smaller configured string limit. These fields
are confined to span/log records; resources, metrics, exemplars, events, and
links exclude them. Session identifiers are excluded. `span(attributes=...)`
cannot supply identity fields; use a trusted resolver through `enrich()`.
Default policy includes no application log bodies or metric instruments. Wire projection does not implement metric
Views or limit series retained before aggregation. Runtime-owned metric Views
select exact scope/instrument pairs, drop all attributes before aggregation,
and disable exemplars. This keeps one series per registered instrument; route,
status, and other metric dimensions are not available yet.

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
heap usage or peak transient memory. Each span/log signal can retain up to
512 queued records plus a batch of 128 in flight. Runtime metric Views keep one
aggregate series per registered instrument and drop dimensions even for rejected
instruments. Python objects, encoding buffers, and allocator behavior also
contribute to RSS; these payload ceilings are not a total process memory limit.
Transport enforces `max_batch_bytes` before parsing and after projection, and
`max_response_bytes` while streaming. Response reads retain at most 64 KiB plus
one byte used to detect overflow.

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
uv run pytest tests/unit/test_conf.py tests/unit/test_context.py
uv run pytest tests/integration/test_django_boundary.py tests/integration/test_context_export.py tests/integration/test_runtime_process.py
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

Test explicit runtime ownership and Django startup in both profiles:

```sh
docker compose -p <unique-asgi-runtime-name> -f reference/compose.yaml --profile asgi run --build --rm verify-asgi python -m pytest tests/integration/test_django_boundary.py tests/integration/test_runtime_process.py
docker compose -p <unique-wsgi-runtime-name> -f reference/compose.yaml --profile wsgi run --build --rm verify-wsgi python -m pytest tests/integration/test_django_boundary.py tests/integration/test_runtime_process.py
```

The ASGI image pins Requests 2.33.1 / urllib3 2.6.3; the WSGI image pins
Requests 2.32.3 / urllib3 1.26.20. Both exercise the same transport contract.

The images install the built wheel, copy both distribution artifacts and tests,
and verify the installed package against that wheel. They contain no package
source checkout or editable installation. The image and its nested clean venv
both use the declared Python/Django profile. The Django boundary tests use the
small reference settings and health callable in fresh subprocesses, including
migrations and shell commands. Tests cover packaging, explicit runtime ownership,
Django startup, and local OTLP behavior. Server and worker workloads are not
covered yet.

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
