"""Read-only compatibility checks for the pinned OpenTelemetry API providers.

The API has public setters/getters but no public 'unset' operation. Keep proxy
class knowledge here; never reset provider globals or their one-shot guards.
"""

from __future__ import annotations

import os
import sys


def trace_provider_is_unset() -> bool:
    from opentelemetry.trace import ProxyTracerProvider, get_tracer_provider

    # A getter can load a configured provider. Do not initialize somebody else's
    # provider plugin as a side effect of checking ownership.
    return (
        not os.environ.get("OTEL_PYTHON_TRACER_PROVIDER")
        and type(get_tracer_provider()) is ProxyTracerProvider
    )


def log_provider_is_unset() -> bool:
    from opentelemetry._logs import get_logger_provider
    from opentelemetry._logs._internal import ProxyLoggerProvider

    return (
        not os.environ.get("OTEL_PYTHON_LOGGER_PROVIDER")
        and type(get_logger_provider()) is ProxyLoggerProvider
    )


def metric_provider_is_unset() -> bool:
    from opentelemetry.metrics import get_meter_provider
    from opentelemetry.metrics._internal import _ProxyMeterProvider

    return (
        not os.environ.get("OTEL_PYTHON_METER_PROVIDER")
        and type(get_meter_provider()) is _ProxyMeterProvider
    )


def http_semantics_are_compatible() -> bool:
    """Check stable HTTP compatibility without imports or environment writes."""
    key = "OTEL_SEMCONV_STABILITY_OPT_IN"
    configured = os.environ.get(key, "")
    choices = {value.strip() for value in configured.split(",") if value.strip()}
    if "http/dup" in choices:
        return False
    module = sys.modules.get("opentelemetry.instrumentation._semconv")
    stability = getattr(module, "_OpenTelemetrySemanticConventionStability", None)
    if getattr(stability, "_initialized", False):
        signal_type = getattr(module, "_OpenTelemetryStabilitySignalType", None)
        http = getattr(signal_type, "HTTP", None)
        modes = getattr(stability, "_OTEL_SEMCONV_STABILITY_SIGNAL_MAPPING", {})
        if http is None or getattr(modes.get(http), "value", None) != "http":
            return False
    return True


def configure_http_semantics() -> bool:
    """Select stable HTTP before contrib initialization, never reset its state."""
    if not http_semantics_are_compatible():
        return False
    key = "OTEL_SEMCONV_STABILITY_OPT_IN"
    choices = {
        value.strip() for value in os.environ.get(key, "").split(",") if value.strip()
    }
    os.environ[key] = ",".join(sorted(choices | {"http"}))
    return True


def prepare_fork_support() -> None:
    """Load the stdlib executor's fork hooks before SDK resource-detector hooks.

    SDK1.45.x trace providers register a child callback that detects resources
    using ThreadPoolExecutor. If that lazy stdlib class is first imported by a
    later log provider, the SDK callback runs before the executor unlocks its
    inherited global lock, deadlocking inside os.fork(). Resolving the public
    class starts no executor/thread and preserves native callback behavior.
    """
    import concurrent.futures

    _ = concurrent.futures.ThreadPoolExecutor
