# django-altimeter

This package provides bounded, privacy-preserving OpenTelemetry components for
Django applications: record admission, allow-list projection, and OTLP/HTTP
export.

The current development release provides explicit runtime setup and shutdown,
safe application spans, correlation, trusted identity enrichment, and on-demand
configuration and delivery checks. Native request instrumentation supports
Django/Ninja under ASGI and Django/DRF under gunicorn gthread, with explicit
worker startup. Optional database and outgoing
client integrations add safe child spans, and an explicitly attached logging
handler exports approved records without changing stdout. Vendored TypeScript
helpers link board/repair readiness events to current request traces and passive
browser resource timing. Queue instrumentation
is not included yet. Telemetry is disabled by default.

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

Install only the optional request-work libraries your application uses:

```sh
uv pip install '.[psycopg,httpx,requests,redis,structlog]'
```

These extras supply dependencies; explicit setup and logging attachment below
enable observation. The base package does not import optional client libraries.

## Compatibility

The tested combinations are Python 3.12 / Django 5.2 and Python 3.13 / Django
6.0. Package metadata requires Python >=3.12 and Django >=5.2,<6.1. Other Python
versions have not been tested.

OpenTelemetry API, SDK, and OTLP/HTTP exporter use >=1.45.1,<1.46 with the
matching 0.66b1 semantic conventions. Queue extras use Celery >=5.6.3,<5.7
and Kombu >=5.6.2,<5.7, or RQ >=2.10.0,<2.11. The development lock currently
resolves those families to 1.45.1/0.66b1, 5.6.3/5.6.2, and 2.10.0 respectively.

Request-work extras pin psycopg 3.3.4, psycopg-pool 3.3.1, HTTPX 0.28.1 and
structlog 25.5.0. Redis is pinned to 6.1.1 on Python 3.12 and 7.4.0 on Python
3.13. The verification profiles use Requests/urllib3 2.32.3/1.26.20 and
2.33.1/2.6.3 respectively. Do not install a second instrumentor for a library
already observed by Altimeter.

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
| `enrich(subject)` | Calls the trusted identity resolver for an already-resolved subject only when the current recording span belongs to Altimeter. Foreign spans are untouched and do not invoke the resolver. It never authenticates a request or loads a Django model. |

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
Altimeter disables its use of that signal. When tracing is disabled or owned
elsewhere, the ASGI and WSGI boundaries preserve the application's current span
and baggage, so spans from its own tracer retain their parent. Altimeter
discloses no correlation or identity for unowned tracing. Every exporting worker
has a fresh UUID `service.instance.id`, including for delta metrics.

Keep setup and shutdown in the application's explicit startup and cleanup
hooks. **Gunicorn preload is unsupported.** Initialize after forking; a child
that inherits initialized providers disables the affected signals.
Queue lifecycle adapters are not included.

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

## ASGI requests

Install the ASGI server and API dependencies used by the reference application:

```sh
uv pip install 'uvicorn==0.46.0' 'django-ninja==1.6.2'
```

Add `django_altimeter.django.RequestMetadataMiddleware` near the outside of
Django's middleware list, before middleware that may return an early response.
It reads the resolved route template and response metadata. It does not read
`request.user`, authenticate, consume a body, or create a second request span.
Allow only static resolver templates in your Django settings:

```python
MIDDLEWARE = [
    "django_altimeter.django.RequestMetadataMiddleware",
    # Your existing middleware follows.
]
ALTIMETER = {
    "ATTRIBUTE_VALUES": frozenset({("http.route", "api/items/<item_id>")}),
    "EXCLUDED_ROUTES": frozenset({"/health/"}),
    "FRONTEND_ORIGINS": frozenset({"https://frontend.example"}),
}
```

Use the exact `resolver_match.route` spelling; Ninja's source path
`/items/{item_id}` resolves to `api/items/<item_id>` under this URL prefix.
Unknown or unapproved routes omit `http.route`; raw paths, object IDs and query
strings never replace it. `EXCLUDED_ROUTES` contains **exact static request
paths**, checked at entry before creating any span or metric. It accepts no
route templates, wildcards, or regular expressions.
When Altimeter owns tracing, child spans inside an excluded request inherit a
local unsampled context instead of creating orphan traces. Foreign tracing
remains unchanged. WSGI applies this during application and iterable operations;
native file-response limits are described below.

Create Django before setup, then initialize telemetry in each serving worker's
lifespan. The native wrapper looks up the owned runtime per request, so it can
be constructed before lifespan startup. For a Django-only ASGI application:

```python
# your_project/asgi.py; DJANGO_SETTINGS_MODULE must name your Django settings.
from django.core.asgi import get_asgi_application

from django_altimeter import setup, shutdown
from django_altimeter.asgi import instrument


def create_application():
    http_application = instrument(get_asgi_application())

    async def application(scope, receive, send):
        if scope["type"] != "lifespan":
            return await http_application(scope, receive, send)
        while True:
            message = await receive()
            if message["type"] == "lifespan.startup":
                setup(role="web")
                await send({"type": "lifespan.startup.complete"})
            elif message["type"] == "lifespan.shutdown":
                shutdown(timeout=2.0)
                await send({"type": "lifespan.shutdown.complete"})
                return

    return application
```

With the resource identity and endpoints above configured, start two workers:

```sh
export DJANGO_SETTINGS_MODULE=your_project.settings
export ALTIMETER_INTEGRATIONS=asgi,django
export ALTIMETER_LOGS_ENABLED=false
uvicorn your_project.asgi:create_application --factory --workers 2 --lifespan on
```

Uvicorn spawns workers; each calls setup and receives its own
`service.instance.id`. The supervisor does not initialize or export telemetry.
Normal SIGTERM triggers worker lifespan shutdown and the shared two-second
drain; an immediate kill can lose queued records. To run the included reference
application from this checkout, substitute
`DJANGO_SETTINGS_MODULE=altimeter_reference.web_settings` and run:

```sh
uvicorn altimeter_reference.asgi:create_application --app-dir reference --factory --workers 2 --lifespan on
```

For an application that already owns a lifespan, add setup and shutdown to that
existing lifespan instead of replacing it. Wrap each selected HTTP branch once;
a separate MCP HTTP branch uses `instrument(mcp_app, surface="mcp")`. Rewrapping
an Altimeter wrapper returns the same wrapper. Non-HTTP scopes, including
websockets and lifespan, pass through without HTTP observations. Do not also
install `DjangoInstrumentor` or generic OTel ASGI instrumentation on that branch.

The boundary owns one SERVER span, one request counter increment and one duration
observation per eligible request when the corresponding signal is owned. Trace
sampling affects spans, not request totals or histogram counts. Duration is
monotonic application entry through the final successful body/file/trailer send, or
an observed abort. It includes streamed production and send backpressure, and
ends at that send even if application cleanup returns later. It does not measure
client receipt. Native `http.response.pathsend` and final
`http.response.zerocopysend` messages count as body completion; declared trailers
must still finish before the response completes. Response-ready time and
`altimeter.http.server.concurrent_at_start` are span fields, not metric dimensions.
The concurrency count includes sampled and unsampled eligible requests in the
same process and wrapped branch. Aborts keep an already-sent HTTP status; an
abort before response start has no status.

| Instrument | Unit | Dimensions |
| --- | --- | --- |
| `altimeter.http.server.requests` | `{request}` | Method, approved route when available, status class, completed/aborted outcome, surface, buffered/streaming/unknown response mode |
| `http.server.request.duration` | `s` | The same bounded dimensions |

Both instruments use delta temporality. Sum request deltas across workers and
divide by elapsed seconds for requests per second. Compute the 5xx fraction from
completed 5xx requests divided by all completed requests; count aborts separately.
Merge histogram buckets before computing a percentile, rather than averaging
worker percentiles. Missing status uses the fixed class `absent`; unknown methods
use `_OTHER`. The explicit duration buckets, in seconds, are:

```text
0.005, 0.01, 0.025, 0.05, 0.075, 0.1, 0.25, 0.5, 0.75, 1,
2.5, 5, 7.5, 10, 15, 30, 45, 60, 120
```

Admission limits each instrument to 2,000 attribute sets for the process lifetime,
including 72 reserved overflow combinations across the two supported surfaces.
Overflow drops route and method, marks `altimeter.overflow=true`, and preserves
outcome, status class, surface and response mode so denominators remain useful.
Native Views filter dimensions before aggregation. Exemplar collection is off.

Incoming W3C parents are accepted only when both deployment configuration enables
`ALTIMETER_TRUST_REMOTE_PARENT=true` and the trusted network branch explicitly
uses `instrument(..., trusted_ingress=True)`. A request header cannot grant that
trust; public requests cannot force parent-based sampling. Baggage and tracestate
are not propagated by this boundary.

Owned tracing may add `X-Altimeter-Trace-Id` and `X-Altimeter-Trace-Sampled` only
when the response explicitly uses unqualified `Cache-Control: private` or
`no-store`. Public/shared-cache responses omit per-request correlation; existing
cache directives are never relaxed. For configured frontend origins,
`Timing-Allow-Origin` echoes only an exact approved origin and `Vary: Origin`
merges with existing fields. This does not grant application CORS access; retain
your own CORS and authentication policy.

## WSGI requests under gunicorn

The supported WSGI server is gunicorn 23.0.0 with `gthread`, without preload.
The reference app uses Django 5.2 and Django REST Framework 3.16.1:

```sh
uv pip install 'gunicorn==23.0.0' 'djangorestframework==3.16.1'
export ALTIMETER_INTEGRATIONS=wsgi,django
```

Configure `RequestMetadataMiddleware` before Django builds its handler, as in
the ASGI example. Initialize telemetry after Django setup in each serving
child, before opening database connections or pools. A child marker makes an
accidental preloaded master detectably unsupported. In `gunicorn_conf.py`:

```python
import os


def post_fork(server, worker):
    os.environ["APP_SERVING_PID"] = str(worker.pid)


def worker_exit(server, worker):
    from django_altimeter import shutdown

    shutdown(timeout=2.0)
```

In the application's WSGI module, with `DJANGO_SETTINGS_MODULE` already set:

```python
import logging
import os

from django.core.wsgi import get_wsgi_application
from django_altimeter import setup
from django_altimeter.wsgi import instrument

django_application = get_wsgi_application()
if os.environ.get("APP_SERVING_PID") == str(os.getpid()):
    setup(role="web")
else:
    logging.getLogger("django_altimeter.reference").warning("setup.preload_unsupported")
application = instrument(django_application)
```

Start the application with its configuration file and deployment's actual
worker/thread capacity; this example uses two workers and four threads each:

```sh
gunicorn --config gunicorn_conf.py --worker-class gthread --workers 2 --threads 4 project.wsgi:application
```

Do not pass `--preload`. The example guard leaves the application usable but
disables telemetry for a preloaded application and emits the fixed
`setup.preload_unsupported` diagnostic. Recycled children initialize fresh
providers and `service.instance.id` values. The shared two-second `worker_exit`
budget covers all signals; hard kills and undrained queues can lose telemetry.

To disable telemetry, set `ALTIMETER_ENABLED=false` and restart gunicorn with
the same command. The wrapper preserves application responses and exceptions
when telemetry is disabled or configuration is invalid.

For ordinary iterables, WSGI duration runs from application entry to observed
exhaustion or early close. It includes application work, iteration and observed
writes; it does not measure client receipt, download time, or time waiting for a gthread
slot before entry. File and streaming responses remain unbuffered. Context is
attached only during the application, iterator, write and close operations;
it is detached between yielded chunks. `start_response`, its `exc_info`, the
legacy write callable and underlying exceptions retain their WSGI behavior.
Each iterable closes once, and each eligible request finalizes once.

A native `wsgi.file_wrapper` result keeps the server's `sendfile` path. Its
duration ends when the server calls `close()`, before application close work.
If the wrapper cannot accept a close hook, it remains unchanged and finalizes
at application return with the fixed `http.file_wrapper_unobserved` diagnostic.
In that case, file-transfer duration, completion and later aborts are unobserved.

For native `wsgi.file_wrapper` responses served without `sendfile`, the server's
file reads run outside the request context, so logs and spans emitted inside a
custom file object's `read()` are not parented to the request.
These file reads and an unhookable wrapper's `close()` also run outside
route-exclusion suppression.

The wrapper reuses the request counters, histogram buckets, route allow-list,
sampling-independent totals and header trust rules above. The process/branch
concurrency value counts eligible requests already inside the wrapper,
including unsampled requests. It is supporting evidence alongside the actual
thread capacity, not queue length or a wait-time measurement. Aborted responses
retain a status already observed and decrement concurrency during finalization.

The checkout includes an equivalent bootstrap and DRF reference app:

```sh
gunicorn --pythonpath reference --config reference/altimeter_reference/gunicorn_conf.py --worker-class gthread --workers 2 --threads 4 altimeter_reference.wsgi:application
```

## Database and outgoing request work

Select installed client integrations before calling `setup()` in the serving
process. Django opens its database connections lazily; initialize telemetry
before the first query or pool is opened:

```sh
export ALTIMETER_INTEGRATIONS=asgi,django,psycopg,requests,httpx,redis
```

Psycopg observes sync and async Django ORM queries, direct psycopg connections,
and sync/async psycopg pools. Connections already open before setup, including
existing pool members, are not adopted. New pool members created through
`Connection.connect` after setup are instrumented. Missing optional libraries
produce fixed setup gap codes and leave the application running. Repeated setup
does not add another observation around the same operation.

Driver spans use bounded operation names such as `SELECT` or the fallback `DB`.
SQL text, comments, parameters, connection strings, Redis keys, arguments and
results never become telemetry. SQLCommenter is off and SQL sent to the database
is unchanged. Redis commands and pipelines get local child spans; no trace
context is added to Redis commands.

Failed or cancelled work marks its recorded child span as an error, including
async cancellation. The original exception propagates unchanged; normal
generator closure (`GeneratorExit`) does not mark an error.

Use registered application spans around work that actually executes. For
example, materialize a queryset once, then serialize those already loaded rows:

```python
from django_altimeter import span


def render_records(queryset, serialize):
    with span("records.materialize"):
        rows = list(queryset)
    with span("records.serialize"):
        return serialize(rows)
```

Add both names to `ALTIMETER["SPAN_NAMES"]`. Driver time includes client work,
transfer and database waiting; it is not pure database server execution time.
An enclosing materialization span can overlap driver spans. Do not add
overlapping durations or treat residual time as proof of queue wait or CPU use.
Serialization spans cover the serializer call, not client receipt of the body.

Requests and synchronous/asynchronous HTTPX observe physical requests, including
redirect hops. These child spans end when the transport returns response headers;
later response-body consumption belongs to the surrounding application work.
Only exact operator-declared internal origins receive local W3C trace context:

```python
ALTIMETER = {
    "SPAN_NAMES": ("records.materialize", "records.serialize"),
    "INTERNAL_ORIGINS": ("https://catalog.example", "http://inventory:8080"),
}
```

Origins consist of scheme, host and optional port; HTTP and HTTPS are allowed.
Userinfo, paths, queries and wildcards are rejected. Public ingress headers do
not authorize propagation of a remote identity or sampling decision. Baggage,
tracestate and application correlation headers are removed at the outbound
boundary, including `X-PostHog-Session-ID` and `X-PostHog-Distinct-ID`, for both
internal and third-party destinations. An approved origin receives the current
owned `traceparent` only.
Each redirect destination is checked again. Third-party and exporter requests
carry no application trace context. Unrelated application headers and response
bodies remain unchanged; raw URLs, query strings and bodies do not enter spans.

Psycopg supports `execute` and `executemany` on ordinary, custom and named cursor
factories. Psycopg2, connect aliases captured before setup, cursor overrides that
bypass the installed factories, and COPY, stream, fetch or pipeline lifecycle
timing are unsupported. HTTP transports or adapters that bypass the native
parent method are also unsupported. Do not install overlapping automatic
instrumentation.

## Safe application logs

Configure static bodies and approved logger scopes in Django settings, alongside
your other Altimeter policy:

```python
ALTIMETER = {
    "LOG_BODIES": ("records.ready",),
    "EVENT_NAMES": ("records.ready",),
    "SCOPE_NAMES": ("example.work",),
}
```

After Django has configured logging and the serving process has called setup,
attach one export route:

```python
import logging

from django_altimeter import setup
from django_altimeter.logging import install_logging

setup(role="web")
result = install_logging(routes=("example.work",))
logging.getLogger("example.work").info("records.ready")
```

Enable the logs signal and configure its destination using the runtime settings
above. `LoggingSetupResult.ok` reports successful attachment; `codes` contains
fixed diagnostics. Explicitly include relevant nonpropagating loggers. A record
must not reach both a selected child route and a selected parent route. Repeated
installation with the same routes is idempotent. Reattach after application
logging configuration replaces handlers; changing routes requires a new process.

The handler builds a bounded copy for export, preserving application
`LogRecord` objects, stdout bytes, formatters, levels and propagation settings.
It supports stdlib records and structlog `ProcessorFormatter` events before
stdout rendering. Approved static bodies, severity, scope, service and owned
trace/span IDs survive. Unknown free text, rendered JSON, nested values,
exception prose and stack traces are dropped. Internal telemetry loggers cannot
feed export diagnostics back into the export handler. Existing stdout privacy
and error-reporting policies remain the application responsibility.

Use `get_correlation()` at an existing error-reporting boundary before context
detach. Pass the immutable scalar snapshot through that reporter, rather than
passing a native span or looking up context later in a reporting thread.
Altimeter does not create a PostHog client or report another exception occurrence.
See [logging attachment and correlation](docs/logging.md) for topology,
scrubber, nonpropagating logger and post-configuration details.

## Browser journeys

The wheel and sdist include framework-independent TypeScript in
`django_altimeter/browser/`, with `VERSION` matching the Python distribution.
Vendor the pinned release's `journey.ts`, `headers.ts`, `resource_timing.ts` and
the matching `examples/asgi.ts` or `examples/wsgi.ts` into your existing frontend.
There is no published JavaScript package or runtime dependency on React, axios
or an analytics SDK. Find the installed source directory with:

```sh
python -c 'from pathlib import Path; import django_altimeter; print(Path(django_altimeter.__file__).parent / "browser")'
```

Create one `beginJourney` attempt at full-load initialization or navigation
intent. Retain it across renders and effect replay. Supply your existing
`capture` and current `canCapture` functions, a safe release string, and the
required roles: `board`, or up to four of `detail`, `checklist`,
`hidden_documents`, `media` for a repair. Your query/navigation owner decides
which current data makes the screen usable. Placeholder data does not finish a
new navigation; a committed empty result does. Optional work after readiness
does not reopen it.

At the existing axios call, select a fresh in-memory execution key, start the
passive ticket before HTTP, and read headers at axios resolution. For example,
with the application's existing `api`, `capture`, `canCapture` and commit logic:

```typescript
import { beginJourney } from './vendor/altimeter/journey';
import { readTraceHeaders } from './vendor/altimeter/headers';
import { createResourceTiming } from './vendor/altimeter/resource_timing';

const attempt = beginJourney({
  key: navigationKey, screen: 'board', navigationType: 'spa', release,
  roles: ['board'], capture, canCapture, signal: identityLifecycle.signal,
});
const timing = createResourceTiming({ attempt, canCapture, baseUrl: apiBaseUrl });
const selection = { role: 'board' as const, requestKey: executionKey };
attempt.selectRequest(selection);
const ticket = timing.start({ ...selection, url: requestUrl, initiator: 'xmlhttprequest' });
const response = await api.get(requestUrl);
const phases = ticket.complete({ provenance: 'network', responseComplete: performance.now() });
const link = readTraceHeaders({
  headers: { get: name => response.headers[name.toLowerCase()] }, provenance: 'network',
  canCapture: () => attempt.isActive() && canCapture() });
attempt.recordRequest({ ...selection, ...link, ...phases,
  provenance: 'network', contributorCoverage: 'complete' });
// Only when this execution supplies the current committed result:
commitCurrentBoard(response.data);
attempt.finish({ outcome: 'success', itemCount: response.data.items.length });
```

Keep the application's existing error and cancellation behavior. Record a
selected failure response when available, then `finish({outcome: 'failure'})`;
use `cancel('navigation' | 'superseded' | 'aborted')` for genuine departure or
cancellation. Do not cancel on development effect replay. Success, failure and
cancellation each emit at most one terminal event: `board_ready`, `board_failed`,
`board_cancelled` or the corresponding `repair_*` name. Repeated completion and
late callbacks cannot amend it. Duration uses the monotonic browser clock and
ends at the application's committed-data decision, not a guaranteed paint.

Select before recording: obsolete responses cannot select themselves. Retries
replace that role's representative without consuming another role slot. Mark
`contributorCoverage` as `complete`, `partial` or `unknown` according to that
query invocation's known contributing requests. A representative from a
multi-page request group owns only its physical request timing; preserve the
application's existing fan-out/error policy. Coverage, cache provenance,
sampling and backend ingestion are separate facts. Query-cache-only data uses
`cache_only`; uncertain HTTP-cache delivery uses `cache_unverified`. Both retain
null trace/timing rather than reuse a prior response as a new request.

Consent is checked before observation and again before terminal capture. On
revocation, account change or tenant change, call `discard()` or abort the
attempt's lifecycle signal. Discard is permanent even after permission returns;
start a new eligible navigation. The helpers initialize no SDK, identify no
user and install no HTTP interceptor. Keep replay/private-route and sanitizer
policies in the existing application. The ASGI example adds no identity hints.
The WSGI example's optional hints require consent, app policy and an exact
scheme/host/port match on every request. Disable overlapping SDK automatic
header injection and use approved API routes without cross-origin redirects.

Configure `ALTIMETER["FRONTEND_ORIGINS"]` with exact approved frontend origins.
The native server boundary supplies selective `Timing-Allow-Origin` and merges
`Origin` into `Vary`. Your API/proxy CORS policy must expose
`X-Altimeter-Trace-Id` and `X-Altimeter-Trace-Sampled`, and permit any approved
request headers. Preserve authenticated endpoints' `private, no-store` policy;
never replay per-request trace headers from a shared cache. A valid nonzero
lowercase 32-hex ID and `0`/`1` sample flag are correlation hints, not an ingestion
receipt. Missing, unsampled and cache-derived links remain distinct.

| Phase (milliseconds) | Browser boundaries |
| --- | --- |
| `before_request` | `fetchStart` to `requestStart` |
| `first_byte_wait` | `requestStart` to `responseStart` |
| `receive_after_first_byte` | `responseStart` to `responseEnd`, including possible header/interim-response work |
| `post_transfer_until_response_complete` | `responseEnd` to axios resolution, including scheduling and transforms |

These phases are not pure queueing, body-transfer or JSON-parse measurements.
Do not subtract a full SERVER-span lifetime from first-byte wait: their endpoints
differ. Timing requires a unique resolved URL, initiator and active monotonic
request window. Restricted, missing, late, ambiguous, cache-uncertain or
service-worker entries stay null; valid zero intervals remain zero. A retired
in-flight execution makes remaining timing ambiguous because its late response
could match a retry. Correlation still works.

Each attempt holds at most four current role selections; the passive observer
holds at most 32 pending candidates and disconnects on terminal/discard. It
neither clears nor resizes the shared resource buffer and retains no page
history. Already queued entries are drained before terminal capture without
waiting. Timing-only updates fill still-null fields of the current selection
before termination. Durations are finite, nonnegative and at most 86,400,000 ms;
keys are at most 200 characters and release is at most 128 safe characters.
URLs, keys, record IDs, response bodies and exception text never become journey
properties.

Enable web vitals through the existing SDK and its sanitizer allow-list when
your application permits them. Finalized INP/CLS can arrive at page departure;
they are separate from board/repair readiness. Retain only intended bounded
numeric vital fields and a safe page category, removing URL and DOM attribution.
Preserve the SDK's required public project token and distinct-ID ingestion envelope.
The reference uses the real pinned SDK and local HTTP ingestion boundary.
Local browser tests do not verify delivery to a live PostHog project.

## Check configuration and delivery

Run the management command with your application's Django settings and
`django_altimeter` in `INSTALLED_APPS`:

```sh
python manage.py altimeter_check
```

The default local check validates effective configuration, installed integration
dependencies, and provider ownership. It starts no exporter, worker thread, or
network connection. Disabled telemetry is valid; invalid requested configuration,
a provider conflict, or a missing requested dependency returns a failure. Output
contains fixed diagnostic codes and safe service metadata, never credentials,
endpoint URLs, DSNs, or server response text.

Send an explicit probe to the configured destinations:

```sh
python manage.py altimeter_check --probe
python manage.py altimeter_check --probe --signal traces --signal logs --json
```

The probe runs in a short-lived isolated process with role `probe`, a fresh
`service.instance.id`, and resource flag `altimeter.probe=true`. It sends exactly
one record for each selected enabled signal: a span, a log record, or a metric
point. All use the fixed name `altimeter.probe`. The metric has value `1` and
scope `django_altimeter`. Exclude these tagged records from application
dashboards. After imports and provider initialization, the helper shares at most
two seconds across export and drain, capped further by the configured shutdown
timeout or a shorter `runtime.probe(timeout=...)` value. The parent watchdog
allows eight additional seconds for cold startup and reaps the helper if the
combined limit expires. The calling process's providers stay untouched. With no
enabled signal it fails with `nothing_to_probe` and sends nothing. Explicitly selecting a
disabled signal fails with `probe.signal_disabled`. Partial rejection, invalid
responses, authentication errors, and timeouts fail the acceptance check. A
zero-rejection warning is accepted with `export.warning`; the warning text is
discarded.

Read-back also sends a probe, then queries PostHog separately for every requested
signal:

```sh
python manage.py altimeter_check --read-back --timeout 10
python manage.py altimeter_check --read-back --signal metrics --json
```

Supply `ALTIMETER_READBACK_API_KEY` through the operator's environment or secret
manager, together with `ALTIMETER_READBACK_PROJECT_ID`. Use a personal API key
scoped to the target project with `query:read` permission. This key is separate
from the ingestion token and is never loaded into runtime configuration or
passed as a command-line argument. `ALTIMETER_READBACK_HOST` defaults to
`https://us.posthog.com`; use `https://eu.posthog.com` for an EU project.

A match must have the probe's fresh writer ID, fixed resource flag, fixed payload
name, and a timestamp within its bounded send window. Metrics also require the
`django_altimeter` instrumentation scope. Old data, another writer, or a trace
without matching logs and metrics cannot satisfy those other signals. Missing
operator credentials or unsupported query access fails read-back; successful
OTLP acceptance alone does not establish storage or query visibility.

Read-back shares one timeout across all signal queries and backoff, with at most
six query rounds. `--timeout` defaults to 10 seconds and must be finite, greater
than zero, and at most 30 seconds; it does not change the probe's separate startup
allowance or export/drain budget. Query responses are capped at 64 KiB. As with
the OTLP transport below, continuous
trickling of response header bytes is outside the pinned HTTP client's deadline
guarantee.

| State | Meaning |
| --- | --- |
| `configured` | Overall: configuration is valid, including intentionally disabled telemetry. Per signal: that signal is enabled. Dependency and ownership checks can still fail. |
| `accepted` | The receiver accepted the probe's complete nonempty payload for this signal. |
| `observed` | A separate PostHog query found this signal's matching probe record. |

`--json` reports `mode`, `ok`, `configured`, `codes`, `service`, `versions`,
`integrations`, and one row per signal in `signals`. Acceptance and observation
are `null` for stages that were not requested. Exit status is `0` when all
requested checks pass, `1` for a failed check, and `2` for invalid command
arguments. Choose one mode: `--local`, `--probe`, or `--read-back`; the default is
`--local`. Repeated `--signal` options select a subset of configured signals.

Check the disabled configuration with:

```sh
ALTIMETER_ENABLED=false python manage.py altimeter_check
```

Disabling a running application still requires changing its deployment
environment and restarting its workers. A management command checks its own
process; it does not inspect existing workers. These checks run on demand and
start no observer daemon.

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
Default policy includes no application log bodies or application-defined metric
instruments. Wire projection does not implement Views or limit retained series.
The runtime registers the native HTTP request pair with the bounded Views and
admission described above. Other registered scope/instrument pairs drop all
attributes before aggregation, retaining one series per instrument. Unregistered
instruments are dropped and exemplars are disabled.

`django_altimeter.exporting` supplies `BoundedSpanProcessor` and the
`bounded_log_processor()` construction function. Each resulting processor owns
one native batch processor, checks retained types and declared limits without
mutation, and delegates an admitted record once. Register only the wrapper with
the provider. Admission drops records only for unsupported types or exceeded
limits, with bounded diagnostic codes. Projection applies the allow-list policy
field by field, preserving permitted route/status/duration/correlation alongside
removed unapproved fields. The exporter factories apply projection before sending.
Admission inspects the whole retained record: an over-limit field or unsupported
container drops the entire span even if projection would later remove that field.
`BoundedSpanProcessor.local_dropped_records` counts cumulative admission losses
before batching, separately from the session's per-export drop count. It does
not count native batch rejection or SDK queue overflow.
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
The native HTTP duration histogram has one narrow exception: its exact 19
boundaries and 20 bucket counts above are permitted. Native metric export splits
large collections into requests of at most 128 points before projection.
The retained-record estimate and protobuf output limit do not measure Python
heap usage or peak transient memory. Each span/log signal can retain up to
512 queued records plus a batch of 128 in flight. HTTP metrics retain at most
2,000 series per instrument; other registered instruments retain one series,
and unregistered instruments are dropped before aggregation. Python objects,
encoding buffers, and allocator behavior also contribute to RSS; these payload
ceilings are not a total process memory limit.
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

The factories send only their validated `headers=` values and fixed protocol
headers. They do not merge extra `OTEL_EXPORTER_OTLP_HEADERS` or signal-specific
header values from the environment. Ambient OTLP `CERTIFICATE`,
`CLIENT_CERTIFICATE` and `CLIENT_KEY` settings are ignored: TLS peer verification
stays enabled and no client certificate is loaded. Runtime configuration can
still select supported standard `OTEL_*` header variables when no `ALTIMETER_*`
override is set; those values pass configuration validation before reaching
the factory.

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
| `export.local_drop` | A nonempty native batch never reached the sanitized HTTP session, including a native 1 MiB size rejection. `local_dropped_records` counts the unsent records in that batch. Earlier metric batches from the same call may already have been sent. |
| Local sanitizer rejection | Invalid input, excessive counts/bytes, projection failure, or zero surviving records sends nothing for that attempt and reports zero accepted records. There is no fallback to the original bytes. An empty native export retains `export.not_sent`. |

`Retry-After` values below one second are treated as absent, so the SDK uses
its own exponential backoff. This includes numeric values below one and
HTTP-dates less than one second ahead, including past dates. Values of one
second or more remain honored, subject to the same native deadline and retry
budget.

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

Span admission uses the same logger with `admission.size`, `admission.type` or
`span.admission_error`. Each span processor independently limits each code to
one warning per 60 seconds while continuing to count every rejected record.

The closed set of warning codes is:

```text
export.not_sent       export.batch_count    export.batch_size
export.request        export.destination    export.encoding
export.content_type   export.projection     export.empty
export.http_error     export.status         export.response_size
export.rejected_count export.partial        export.warning
export.deadline       export.connection     export.invalid
export.failed         export.local_drop     export.framing
metrics.timeout_unsupported
admission.size        admission.type        span.admission_error
```

## Development

Revalidate the metric timeout compatibility guard and deadline tests before
changing the OTel version bounds.

Browser checks use Bun 1.3.14 and Node 26.12.1. After Python setup and `uv build`,
install the locked development dependencies and Chromium:

```sh
cd tests/browser
bun install --frozen-lockfile
bun run install:browser
bun run typecheck
bun run test:unit
bun run test:e2e resource_timing.spec.ts journey_e2e.spec.ts
```

The browser tests start separate frontend/API origins, real uvicorn from an
installed wheel and a local decoded OTLP receiver, then stop their processes.
Rebuild after changing packaged source. Type checks cover the harness and the
ASGI/WSGI compiler profiles (TypeScript 6.0.3 and 5.9.3). Python asset checks are
`uv run pytest tests/integration/test_browser_asset.py`; they do not replace
the TypeScript or real-browser tests. The development harness is not shipped in
the wheel.

```sh
uv sync --all-extras --dev
uv run ruff check .
uv run ruff format --check .
uv run mypy src
uv build
uv run pytest tests/unit/test_privacy.py tests/unit/test_admission.py tests/integration/test_wheel.py
uv run pytest tests/integration/test_otlp_transport.py
uv run pytest tests/integration/test_check_command.py
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
docker compose -p <unique-asgi-name> -f reference/compose.yaml --profile asgi down --remove-orphans
docker compose -p <unique-wsgi-name> -f reference/compose.yaml --profile wsgi run --build --rm verify-wsgi python -m pytest tests/integration/test_wheel.py
docker compose -p <unique-wsgi-name> -f reference/compose.yaml --profile wsgi down --remove-orphans
```

Run the matching cleanup command after every verification run, including a
failed run. `run --rm` removes only the test container; `down --remove-orphans`
also stops that project's Postgres and Redis and removes its network. In a
script, use an exit trap so failure cannot bypass cleanup. Keep each project
name identical between its run and cleanup commands.

Verify the native transport against both installed-wheel profiles:

```sh
docker compose -p <unique-asgi-transport-name> -f reference/compose.yaml --profile asgi run --build --rm verify-asgi python -m pytest tests/integration/test_otlp_transport.py
docker compose -p <unique-asgi-transport-name> -f reference/compose.yaml --profile asgi down --remove-orphans
docker compose -p <unique-wsgi-transport-name> -f reference/compose.yaml --profile wsgi run --build --rm verify-wsgi python -m pytest tests/integration/test_otlp_transport.py
docker compose -p <unique-wsgi-transport-name> -f reference/compose.yaml --profile wsgi down --remove-orphans
```

Test explicit runtime ownership and Django startup in both profiles:

```sh
docker compose -p <unique-asgi-runtime-name> -f reference/compose.yaml --profile asgi run --build --rm verify-asgi python -m pytest tests/integration/test_django_boundary.py tests/integration/test_runtime_process.py
docker compose -p <unique-asgi-runtime-name> -f reference/compose.yaml --profile asgi down --remove-orphans
docker compose -p <unique-wsgi-runtime-name> -f reference/compose.yaml --profile wsgi run --build --rm verify-wsgi python -m pytest tests/integration/test_django_boundary.py tests/integration/test_runtime_process.py
docker compose -p <unique-wsgi-runtime-name> -f reference/compose.yaml --profile wsgi down --remove-orphans
```

The ASGI image also pins Ninja 1.6.2 and uvicorn 0.46.0. It uses
Requests 2.33.1 / urllib3 2.6.3; the WSGI image pins
gunicorn 23.0.0, DRF 3.16.1, Requests 2.32.3 / urllib3 1.26.20.
Both exercise the same transport contract.

The images install the built wheel, copy both distribution artifacts and tests,
and verify the installed package against that wheel. They contain no package
source checkout or editable installation. The image and its nested clean venv
both use the declared Python/Django profile. The Django boundary tests use the
small reference settings and health callable in fresh subprocesses, including
migrations and shell commands. Tests cover packaging, explicit runtime ownership,
Django startup, local OTLP behavior, real uvicorn/Ninja and gunicorn gthread/DRF
requests, and serving-worker lifecycles. Queue worker workloads are not covered yet.

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

The request-work profiles start dedicated Postgres and Redis services without
published ports. Every test uses a unique database and Redis key prefix; it
cleans up only that namespace and never flushes shared Redis. PostgreSQL trust
authentication is confined to this local, unexposed test network. The reference
app reuses Django's existing ContentType model and adds no model or migration.

The full host test suite also needs real Postgres and Redis services. Point
`ALTIMETER_TEST_POSTGRES_DSN` and `ALTIMETER_TEST_REDIS_URL` at isolated test
services before running `uv run pytest -q`. For example, use
`host=127.0.0.1 port=<postgres-port> dbname=postgres user=postgres` and
`redis://127.0.0.1:<redis-port>/0`. The CI host jobs provision health-checked
services with these settings and pinned image digests. The Compose profiles
provide the settings automatically.

Run real uvicorn request-to-driver/client/log checks in the ASGI image, and the
standalone work in both Python environments, after rebuilding the wheel:

```sh
docker compose -p <unique-asgi-work-name> -f reference/compose.yaml --profile asgi run --build --rm verify-asgi python -m pytest tests/integration/test_reference_work.py
docker compose -p <unique-asgi-work-name> -f reference/compose.yaml --profile asgi down --remove-orphans
docker compose -p <unique-wsgi-work-name> -f reference/compose.yaml --profile wsgi run --build --rm verify-wsgi python -m pytest tests/integration/test_reference_work.py::TestStandaloneReferenceWork tests/integration/test_reference_work.py::test_request_work_extras_install_pinned_clients
docker compose -p <unique-wsgi-work-name> -f reference/compose.yaml --profile wsgi down --remove-orphans
```

The `/work/` endpoints run sync/async ORM queries, direct psycopg, 4,096-row
materialization/serialization, Requests and HTTPX calls, redirects, Redis
commands, and safe/canary logs. They are enabled only by the reference test
launcher. Outgoing checks delay a real HTTP response by 150 ms and require the
application span to contain that delay and parent its client spans, covering
Requests and synchronous/asynchronous HTTPX. The Python 3.12 standalone checks
do not run a WSGI server.

Test the management command from the installed wheel in both profiles:

```sh
docker compose -p <unique-asgi-check-name> -f reference/compose.yaml --profile asgi run --build --rm verify-asgi python -m pytest tests/integration/test_check_command.py
docker compose -p <unique-asgi-check-name> -f reference/compose.yaml --profile asgi down --remove-orphans
docker compose -p <unique-wsgi-check-name> -f reference/compose.yaml --profile wsgi run --build --rm verify-wsgi python -m pytest tests/integration/test_check_command.py
docker compose -p <unique-wsgi-check-name> -f reference/compose.yaml --profile wsgi down --remove-orphans
```

The command tests use real subprocesses and a local OTLP receiver. A local HTTP
stand-in supplies only the external PostHog query API; it does not ingest or
store telemetry in PostHog. Run the credential-gated live read-back test with
real enabled signal destinations and the operator environment described above:

```sh
uv run pytest tests/integration/test_readback_flow.py -m live
```

Without the required operator credentials, the live test is skipped, never
passed. Successful local tests do not verify PostHog ingestion or deployed
application behavior.

Verify ASGI request handling, multiprocess startup and the harness self-tests
with the installed wheel:

```sh
uv build
docker compose -p <unique-asgi-web-name> -f reference/compose.yaml --profile asgi run --build --rm verify-asgi python -m pytest tests/unit/test_metrics.py tests/integration/test_asgi.py tests/integration/test_uvicorn_server.py tests/performance/test_web_overhead.py -s
docker compose -p <unique-asgi-web-name> -f reference/compose.yaml --profile asgi down --remove-orphans
```

Verify WSGI protocol behavior, recycled children and combined request work:

```sh
uv build
docker compose -p <unique-wsgi-web-name> -f reference/compose.yaml --profile wsgi run --build --rm verify-wsgi python -m pytest tests/integration/test_wsgi.py tests/integration/test_gthread.py tests/integration/test_wsgi_headers.py tests/integration/test_driver_spans.py::TestWSGIDriverFlow tests/integration/test_outbound_context.py::TestWSGIPropagation tests/integration/test_logging_topology.py::TestWSGILogFlow tests/integration/test_wsgi_metrics.py
docker compose -p <unique-wsgi-web-name> -f reference/compose.yaml --profile wsgi down --remove-orphans
```

Default test runs, including CI, exclude the wall-clock acceptance tests through
the `performance` marker. The harness self-tests remain in the default host,
ASGI and WSGI suites. Run each timing test explicitly only in its matching local
verification image after `uv build`, using a unique project name:

```sh
docker compose -p <unique> -f reference/compose.yaml --profile asgi run --build --rm verify-asgi python -m pytest -m performance tests/performance/test_web_overhead.py
docker compose -p <unique> -f reference/compose.yaml --profile asgi down --remove-orphans
docker compose -p <unique> -f reference/compose.yaml --profile wsgi run --build --rm verify-wsgi python -m pytest -m performance tests/performance/test_wsgi_overhead.py
docker compose -p <unique> -f reference/compose.yaml --profile wsgi down --remove-orphans
```

The explicit `-m performance` overrides the default selection; each command
selects one timing test. A command that selects zero tests fails.

Each performance test uses one worker, four persistent connections and a static
1 KiB response; gunicorn uses four gthread threads. Each fresh process warms
200 requests, settles for one second,
then runs for ten seconds; three balanced repetitions cover disabled, enabled
and failing-export modes, each requiring at least 1,000 completed requests.
It pools individual latencies for each mode and gates p95 increase against
`max(2 ms, 5% of disabled p95)`. Output includes the wheel hash, versions,
throughput, RSS, errors, aborts and sample counts. A separate 60-second failure
run samples the serving process's Linux resident pages and checks the fixed
plateau thresholds in
the test. These measurements describe that workload and machine, not production
latency or arbitrary-lifetime memory use.
