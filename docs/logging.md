# Safe, correlated logs

Altimeter exports a separate, bounded copy of selected stdlib log records. Your
stdout handlers, formatters, propagation flags, logging levels and original
`LogRecord` are unchanged. Your existing stdout and error-reporting privacy
policies still apply; Altimeter does not sanitize their output or report another
exception occurrence.

Configure static events and approved logger names in Django settings:

```python
ALTIMETER = {
    "LOG_BODIES": ("work.done",),
    "EVENT_NAMES": ("work.done",),
    "SCOPE_NAMES": ("myapp.work", "myapp.audit"),
}
```

Enable the log signal and its single destination using the ordinary Altimeter
configuration. After Django logging configuration and explicit `setup()` in the
serving process, attach the export handler:

```python
from django_altimeter import setup
from django_altimeter.logging import install_logging

setup(role="web")
result = install_logging(routes=("myapp.work", "myapp.audit"))
```

Each route names a stdlib logger; `""` selects root. Explicitly list relevant
nonpropagating loggers. A route must not propagate into another selected route:
for example, root plus `myapp.audit` is valid only when the audit route stops
propagation before root. Invalid topology returns a fixed code and attaches
nothing. Repeating the same installation is idempotent. Configure logging first;
if `dictConfig` replaces handlers, reattach with the same routes afterward.
Changing the declared routes requires a new process. Installation changes none
of the application's logger settings.

Every selected route must already reach an application handler, either directly
or through propagation. Handler-free routes that rely on logging.lastResort
are unsupported: installation returns logging.no_effective_handler and
attaches nothing to any selected route. This preserves ordinary and root logger
fallback output, as well as module-level logging's automatic basicConfig().
Configure Django/Celery logging before attaching Altimeter. A NullHandler
counts as an existing handler because Python also treats it as suppressing
lastResort; handler levels and filters do not change this rule.

For Celery, call the same attachment function after the application's logging
setup hook and after explicit runtime setup in the serving worker process.
Altimeter does not install a Celery hook or take over its stdout configuration.
Do not enable an OTel auto-log handler or send the same stdout stream to the same
telemetry destination: that creates a separate duplicate export route.

`logger.info("work.done")` exports the static body and native severity. With
structlog's `ProcessorFormatter.wrap_for_formatter`, the exact bounded `event`
value is considered before the stdout formatter renders JSON. The adapter never
formats `args`, parses rendered JSON, reads exception messages or stack traces,
or traverses unknown nested data. Unknown bodies are dropped. The application's
stdout may still contain those values, unchanged.

Approved logger names become the OTLP instrumentation scope through
`SCOPE_NAMES`. Other logger names use the fixed `django_altimeter` scope. Service
identity comes from the owned runtime resource. Bounded flat `attributes` on a
stdlib record, or scalar structlog event fields, must satisfy the existing
privacy allow-list. Free text cannot become an allowed field by choosing a
plausible key. DEBUG, INFO, WARNING, ERROR and CRITICAL map to native severities;
other numeric levels are dropped. Package, OTel, Requests and urllib3 internal
logs are never exported by this handler, preventing exporter feedback loops.

An optional trusted `LOG_SCRUBBER` callback receives a newly constructed stdlib
`LogRecord`: bounded message, logger name, severity and already allowed scalar
`attributes`; empty args, filename and exception information. Return a stdlib
`LogRecord`, or another value to drop it. Mutating this copy cannot change the
application record. Its output passes through the same static body and attribute
policy, then the ordinary bounded processor and wire projection. Callback
failures drop that telemetry and produce only a fixed local code.

For an existing asynchronous error reporter, capture the immutable correlation
before leaving its exception boundary and pass those plain fields through its
existing sanitized payload path:

```python
from dataclasses import asdict
from django_altimeter import get_correlation

snapshot = get_correlation()
correlation = asdict(snapshot) if snapshot is not None else None
# Pass correlation to your existing reporter; do not queue a native Span.
```

The snapshot contains only trace ID, span ID and sampled flags. A reporter thread
must use the passed value rather than query its own current context. Correlation
can exist for an unsampled trace; an ID does not prove a trace reached PostHog.
An unowned provider or a stopped/inherited runtime supplies no correlation or
Altimeter log export. Logging is inert until explicitly installed and after
runtime shutdown. No extra provider, exporter queue or background thread is
created by the adapter.

Owned `span()` and ASGI boundaries mark their current native span for the lifetime
of that context, including unsampled spans. Temporarily attaching a foreign span
does not disclose its IDs, and restoring the owned context restores correlation.
For native SDK spans created directly by the owned provider, the pinned SDK's
private processor identity supplies the provider link; absent or changed fields
fail closed. This guarded dependency is covered by a missing-field regression.
Use `snapshot.trace_flags & 1` for the sampled bit; native flags can contain other
bits as well.
