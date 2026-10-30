"""Explicit stdlib export routes; application records and stdout stay untouched."""

from __future__ import annotations

import logging
from dataclasses import dataclass
from math import isfinite
from threading import Lock, local
from time import time_ns
from typing import TYPE_CHECKING, TypeGuard

from opentelemetry import trace
from opentelemetry._logs import SeverityNumber, get_logger_provider
from opentelemetry.context import Context

from django_altimeter.context import get_correlation
from django_altimeter.privacy import is_allowed_attribute

if TYPE_CHECKING:
    from django_altimeter.runtime import ProcessRuntime

_MAX_ROUTES = 32
_MAX_HANDLERS = 32
_INTERNAL_NAMES = ("django_altimeter", "opentelemetry", "urllib3", "requests")
_INSTALL_LOCK = Lock()
_Scalar = str | bool | int | float


@dataclass(frozen=True)
class LoggingSetupResult:
    ok: bool = False
    codes: tuple[str, ...] = ()


def _text(value: object, maximum: int) -> TypeGuard[str]:
    return (
        type(value) is str
        and len(value) <= maximum
        and len(value.encode("utf-8")) <= maximum
    )


def _internal(name: str) -> bool:
    return any(
        name == prefix or name.startswith(prefix + ".") for prefix in _INTERNAL_NAMES
    )


def _report(code: str) -> None:
    try:
        logging.getLogger("django_altimeter.logging").warning("%s", code)
    except Exception:
        pass  # An application diagnostic handler cannot change application behavior.


def _owned(runtime: ProcessRuntime) -> bool:
    from django_altimeter.runtime import get_process_runtime

    return (
        get_process_runtime() is runtime
        and "logs" in runtime.result.enabled_signals
        and get_logger_provider() is runtime.logger_provider
    )


def _attributes(values: object, runtime: ProcessRuntime) -> dict[str, _Scalar]:
    limits = runtime.config.limits
    if type(values) is not dict or len(values) > limits.max_attributes:
        return {}
    result: dict[str, _Scalar] = {}
    for key, value in values.items():
        if (
            type(key) is not str
            or len(key) > limits.max_key_bytes
            or key in {"trace_id", "span_id"}
        ):
            continue
        if is_allowed_attribute(
            key, value, policy=runtime.config.policy, limits=limits
        ):
            result[key] = value
    return result


class _SafeLoggingHandler(logging.Handler):
    def __init__(self, runtime: ProcessRuntime, routes: tuple[str, ...]) -> None:
        super().__init__()
        self.runtime = runtime
        self.routes = routes
        self._emitting = local()

    def emit(self, record: logging.LogRecord) -> None:
        # The native Handler lock serializes emit; the thread-local also prevents
        # re-entry from a trusted scrubber or an application's diagnostic handler.
        if getattr(self._emitting, "active", False):
            return
        self._emitting.active = True
        try:
            self._emit(record)
        except Exception:
            _report("logging.emit_failed")
        finally:
            self._emitting.active = False

    def _emit(self, record: logging.LogRecord) -> None:
        runtime = self.runtime
        if type(record) is not logging.LogRecord or not _owned(runtime):
            return
        fields = record.__dict__
        if type(fields) is not dict:
            return
        name = fields.get("name")
        limits = runtime.config.limits
        if not _text(name, limits.max_string_bytes) or _internal(name):
            return
        level = fields.get("levelno")
        if type(level) is not int or level not in (10, 20, 30, 40, 50):
            return
        message = fields.get("msg")
        attributes = fields.get("attributes")
        if type(message) is dict and len(message) <= limits.max_children:
            attributes = message
            message = message.get("event")
        if not _text(message, limits.max_string_bytes):
            return

        correlation = get_correlation()
        # Copy only bounded, known fields. Never copy the original __dict__,
        # interpolate args, render JSON, inspect exceptions or invoke accessors.
        adapted = logging.LogRecord(name, level, "", 0, message, (), None)
        adapted.attributes = _attributes(attributes, runtime)
        scrubber = runtime.config.log_scrubber
        if scrubber is not None:
            # The callback owns this new record, never the application's record.
            scrubbed = scrubber(adapted)
            if type(scrubbed) is not logging.LogRecord:
                return
            adapted = scrubbed
        safe = adapted.__dict__
        if type(safe) is not dict:
            return
        message = safe.get("msg")
        policy = runtime.config.policy
        if (
            not _text(message, limits.max_string_bytes)
            or message not in policy.log_bodies
        ):
            return
        attributes = _attributes(safe.get("attributes"), runtime)
        scope = name if name in policy.scope_names else "django_altimeter"
        # A nonempty context is intentional: OTel replaces a falsey Context()
        # with ambient context. Build a fresh snapshot containing no span object
        # from the application, baggage, or other ambient values.
        captured = trace.INVALID_SPAN_CONTEXT
        if correlation is not None:
            captured = trace.SpanContext(
                trace_id=int(correlation.trace_id, 16),
                span_id=int(correlation.span_id, 16),
                is_remote=False,
                trace_flags=trace.TraceFlags(correlation.trace_flags),
            )
        context = trace.set_span_in_context(trace.NonRecordingSpan(captured), Context())
        created = fields.get("created")
        timestamp = time_ns()
        if (type(created) is float or type(created) is int) and (
            0 <= created < 2**34 and isfinite(created)
        ):
            timestamp = int(created * 1_000_000_000)
        runtime.logger_provider.get_logger(scope).emit(
            timestamp=timestamp,
            context=context,
            severity_number=SeverityNumber((level // 10 - 1) * 4 + 5),
            body=message,
            event_name=message if message in policy.event_names else None,
            attributes=attributes,
        )


def _route_loggers(routes: tuple[str, ...]) -> tuple[logging.Logger, ...] | None:
    if type(routes) is not tuple or not 0 < len(routes) <= _MAX_ROUTES:
        return None
    if any(not _text(name, 128) or _internal(name) for name in routes):
        return None
    if len(set(routes)) != len(routes):
        return None
    loggers = tuple(logging.getLogger(name) for name in routes)
    selected = set(loggers)
    for logger in loggers:
        if len(logger.handlers) > _MAX_HANDLERS:
            return None
        current = logger
        for _ in range(_MAX_ROUTES):
            if not current.propagate or current.parent is None:
                break
            current = current.parent
            if current in selected:
                return None
        else:
            return None
    return loggers


def install_logging(*, routes: tuple[str, ...]) -> LoggingSetupResult:
    """Attach one handler after logging configuration and explicit runtime setup.

    Routes name stdlib loggers (``""`` means root). Explicitly include desired
    nonpropagating loggers. Routes must not propagate into one another. Repeating
    the same installation is harmless; changing its routes requires a new process.
    No logging levels, propagation flags, formatters or stdout handlers are changed.
    """
    from django_altimeter.runtime import get_process_runtime

    acquired = False
    try:
        runtime = get_process_runtime()
        if runtime is None or not _owned(runtime):
            return LoggingSetupResult(codes=("logging.unowned",))
        acquired = _INSTALL_LOCK.acquire(timeout=0.05)
        if not acquired:
            return LoggingSetupResult(codes=("logging.busy",))
        loggers = _route_loggers(routes)
        if loggers is None:
            return LoggingSetupResult(codes=("logging.invalid_routes",))
        existing = runtime.log_handler
        if existing is not None and (
            type(existing) is not _SafeLoggingHandler or existing.routes != routes
        ):
            return LoggingSetupResult(codes=("logging.routes_changed",))
        handler = existing or _SafeLoggingHandler(runtime, routes)
        for logger in loggers:
            logger.addHandler(handler)
        runtime.log_handler = handler
        return LoggingSetupResult(True)
    except Exception:
        _report("logging.install_failed")
        return LoggingSetupResult(codes=("logging.install_failed",))
    finally:
        if acquired:
            _INSTALL_LOCK.release()
