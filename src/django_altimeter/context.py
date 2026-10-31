"""Owned tracing helpers with bounded input before the native SDK sees it."""

import logging
from collections.abc import Iterator, Mapping
from contextlib import AbstractContextManager, contextmanager
from contextvars import ContextVar, Token
from dataclasses import dataclass, field
from math import isfinite
from typing import TypeGuard

from opentelemetry import trace
from opentelemetry.trace import Span, SpanContext, SpanKind, Status, StatusCode

from django_altimeter.privacy import PrivacyLimits, is_allowed_attribute

_Scalar = str | bool | int | float
_DISTINCT_ID_KEY = "posthogDistinctId"
_TENANT_ID_KEY = "altimeter.tenant_id"
_RESOLVER_KEYS = frozenset({_DISTINCT_ID_KEY, _TENANT_ID_KEY})
_OWNED_SPAN: ContextVar[Span | None] = ContextVar("altimeter_owned_span", default=None)


@dataclass(frozen=True)
class Correlation:
    """Current native IDs, disclosed only while Altimeter owns tracing."""

    trace_id: str
    span_id: str
    trace_flags: int


@dataclass(frozen=True)
class Identity:
    """Opaque, already-resolved facts returned by the trusted identity callback."""

    distinct_id: str | None = field(default=None, repr=False)
    tenant_id: str | None = field(default=None, repr=False)


def _bounded_text(value: object, maximum: int) -> TypeGuard[str]:
    if type(value) is not str or len(value) > maximum:
        return False
    try:
        return len(value.encode("utf-8")) <= maximum
    except UnicodeEncodeError:
        return False


def _bounded_scalar(value: object, limits: PrivacyLimits) -> TypeGuard[_Scalar]:
    if type(value) is str:
        return _bounded_text(value, limits.max_string_bytes)
    if type(value) is bool:
        return True
    if type(value) is int:
        return -(2**63) <= value <= 2**63 - 1
    if type(value) is float:
        return isfinite(value)
    return False


def _bounded_attributes(
    attributes: object, limits: PrivacyLimits
) -> dict[str, _Scalar]:
    # Exact builtin types avoid invoking caller-defined len/items/iter methods.
    # Count rejection is O(1); values are never copied or traversed to size them.
    if type(attributes) is not dict or len(attributes) > limits.max_attributes:
        return {}
    result: dict[str, _Scalar] = {}
    for key, value in attributes.items():
        if (
            _bounded_text(key, limits.max_key_bytes)
            and key
            and key not in _RESOLVER_KEYS
            and _bounded_scalar(value, limits)
        ):
            result[key] = value
    return result


def _diagnostic(code: str) -> None:
    try:
        logging.getLogger("django_altimeter.context").warning("%s", code)
    except Exception:
        pass  # An application logging handler cannot break telemetry helpers.


def _mark_owned_span(current: Span) -> Token[Span | None] | None:
    """Mark a span just created by an owned Altimeter boundary, including unsampled."""
    try:
        return _OWNED_SPAN.set(current)
    except Exception:
        _diagnostic("context.mark_failed")
        return None


def _reset_owned_span(token: Token[Span | None] | None) -> None:
    try:
        if token is not None:
            _OWNED_SPAN.reset(token)
    except Exception:
        _diagnostic("context.reset_failed")


def _provider_created_span(current: Span, provider: trace.TracerProvider) -> bool:
    # Pinned SDK 1.45.x has no public Span -> provider link. Its exact native
    # _Span carries the provider's processor object. Missing or changed private
    # state fails closed; unknown Span subclasses never have accessors invoked.
    from opentelemetry.sdk.trace import TracerProvider, _Span

    if type(current) is not _Span or type(provider) is not TracerProvider:
        return False
    span_fields = current.__dict__
    provider_fields = provider.__dict__
    if type(span_fields) is not dict or type(provider_fields) is not dict:
        return False
    processor = span_fields.get("_span_processor")
    return processor is not None and processor is provider_fields.get(
        "_active_span_processor"
    )


def _is_owned_span(current: Span, provider: trace.TracerProvider) -> bool:
    return current is _OWNED_SPAN.get() or _provider_created_span(current, provider)


def get_correlation() -> Correlation | None:
    """Return current native IDs only for this process's owned trace provider."""
    try:
        from django_altimeter.runtime import get_owned_trace_runtime

        runtime = get_owned_trace_runtime()
        if runtime is None:
            return None
        active = trace.get_current_span()
        if not _is_owned_span(active, runtime.tracer_provider):
            return None
        current = active.get_span_context()
        if type(current) is not SpanContext or not current.is_valid:
            return None
        return Correlation(
            trace.format_trace_id(current.trace_id),
            trace.format_span_id(current.span_id),
            int(current.trace_flags),
        )
    except Exception:
        _diagnostic("context.correlation_failed")
        return None


def span(
    name: str, *, attributes: Mapping[str, object] | None = None
) -> AbstractContextManager[Span]:
    """Create a registered native span, usable with `with` in sync/async code.

    Only exact builtin dictionaries and bounded scalar entries reach the SDK.
    Unknown names and unowned tracing yield INVALID_SPAN without changing the
    caller's existing context. Exceptions from the body always propagate.
    """
    return _span(name, attributes, SpanKind.INTERNAL)


def client_span(
    name: str, *, attributes: Mapping[str, object] | None = None
) -> AbstractContextManager[Span]:
    """Use the same bounded native lifecycle for concrete driver/client calls."""
    return _span(name, attributes, SpanKind.CLIENT)


@contextmanager
def _span(
    name: str, attributes: Mapping[str, object] | None, kind: SpanKind
) -> Iterator[Span]:
    manager: AbstractContextManager[Span] | None = None
    current: Span = trace.INVALID_SPAN
    marker: Token[Span | None] | None = None
    try:
        from django_altimeter.runtime import get_owned_trace_runtime

        runtime = get_owned_trace_runtime()
        if (
            runtime is not None
            and _bounded_text(name, runtime.config.limits.max_string_bytes)
            and name in runtime.config.policy.span_names
        ):
            safe = _bounded_attributes(attributes, runtime.config.limits)
            candidate = runtime.tracer_provider.get_tracer(
                "django_altimeter"
            ).start_as_current_span(
                name,
                attributes=safe,
                kind=kind,
                record_exception=False,
                set_status_on_exception=False,
            )
            current = candidate.__enter__()
            manager = candidate
            marker = _mark_owned_span(current)
    except Exception:
        _diagnostic("context.span_start_failed")

    # A generator context manager otherwise retains the original inputs for
    # the whole application body, even when they were rejected before the SDK.
    del attributes, name
    try:
        yield current
    except BaseException as error:
        if manager is not None and not isinstance(error, GeneratorExit):
            try:
                current.set_status(Status(StatusCode.ERROR))
            except Exception:
                _diagnostic("context.span_status_failed")
        raise
    finally:
        if manager is not None:
            try:
                # The original application exception stays outside the native
                # manager, which must neither format nor replace its message.
                manager.__exit__(None, None, None)
            except Exception:
                _diagnostic("context.span_end_failed")
        _reset_owned_span(marker)


def enrich(subject: object) -> None:
    """Apply trusted opaque identity to an owned current recording span.

    The resolver must use facts already available in memory; this helper never
    reads subject properties or performs authentication, ORM access or I/O.
    Resolver results must be exact Identity values. Session IDs are excluded.
    """
    try:
        from django_altimeter.runtime import get_owned_trace_runtime

        runtime = get_owned_trace_runtime()
        if runtime is None or runtime.config.identity_resolver is None:
            return
        current = trace.get_current_span()
        if (
            not _is_owned_span(current, runtime.tracer_provider)
            or not current.is_recording()
        ):
            return
        identity = runtime.config.identity_resolver(subject)
        if type(identity) is not Identity:
            return
        attributes: dict[str, str] = {}
        for key, value in (
            (_DISTINCT_ID_KEY, identity.distinct_id),
            (_TENANT_ID_KEY, identity.tenant_id),
        ):
            if len(attributes) >= runtime.config.limits.max_attributes:
                break
            if _bounded_text(value, runtime.config.limits.max_string_bytes) and (
                is_allowed_attribute(
                    key,
                    value,
                    policy=runtime.config.policy,
                    limits=runtime.config.limits,
                )
            ):
                attributes[key] = value
        if attributes:
            current.set_attributes(attributes)
    except Exception:
        _diagnostic("context.enrich_failed")
