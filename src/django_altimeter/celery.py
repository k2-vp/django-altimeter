"""Native Celery publication, with one scoped producer and a bounded carrier.

Installation registers hooks only. Providers belong to explicit web or worker
startup; this module never imports Celery until installation is requested.
"""

from __future__ import annotations

import json
import logging
import sys
from collections.abc import Callable
from contextlib import AbstractContextManager
from contextvars import ContextVar, Token
from dataclasses import dataclass
from datetime import datetime
from functools import wraps
from math import isfinite
from threading import Lock
from time import time
from types import MethodType
from typing import TYPE_CHECKING, Any

from opentelemetry import context, trace
from opentelemetry.trace import Span, SpanKind, Status, StatusCode

from django_altimeter.context import (
    _mark_owned_span,
    _reset_owned_span,
    get_correlation,
)
from django_altimeter.headers import incoming_context

if TYPE_CHECKING:
    from django_altimeter.runtime import ProcessRuntime

NAMESPACE = "django_altimeter"
MAX_CARRIER_BYTES = 1024
_MAX_EPOCH = 253402300799
_INSTALL_LOCK = Lock()


@dataclass(frozen=True)
class Publication:
    """Validated publication facts; no task IDs, arguments or identity."""

    origin_traceparent: str
    traceparent: str
    published_at: float
    eligible_at: float | None = None
    previous_traceparent: str | None = None


def _diagnostic(code: str) -> None:
    try:
        logging.getLogger("django_altimeter.celery").warning("%s", code)
    except Exception:
        pass


def _fields(
    value: object, required: tuple[str, ...], optional: tuple[str, ...] = ()
) -> dict[str, Any] | None:
    if type(value) is not dict:
        return None
    if not len(required) <= len(value) <= len(required) + len(optional):
        return None
    # Inspect bounded keys before looking up any value; never call user accessors.
    if any(type(key) is not str or key not in required + optional for key in value):
        return None
    if not all(key in value for key in required):
        return None
    return value


def _traceparent(value: object) -> str | None:
    if type(value) is not str or len(value) != 55 or not value.isascii():
        return None
    parent = incoming_context(
        [(b"traceparent", value.encode("ascii"))],
        trusted_ingress=True,
        trust_remote_parent=True,
    )
    return value if trace.get_current_span(parent).get_span_context().is_valid else None


def _epoch(value: object) -> float | None:
    if (
        type(value) is not int and type(value) is not float
    ) or not 0 <= value <= _MAX_EPOCH:
        return None
    return float(value) if isfinite(value) else None


def _wire(publication: Publication) -> dict[str, Any]:
    attempt: dict[str, Any] = {
        "traceparent": publication.traceparent,
        "published_at": publication.published_at,
    }
    if publication.eligible_at is not None:
        attempt["eligible_at"] = publication.eligible_at
    if publication.previous_traceparent is not None:
        attempt["previous_traceparent"] = publication.previous_traceparent
    return {
        "v": 1,
        "origin": {"traceparent": publication.origin_traceparent},
        "attempt": attempt,
    }


def _native_headers(value: object) -> dict[str, Any] | None:
    # A dict lookup can invoke equality on a hostile key with a colliding hash.
    # Validate only a bounded set of exact string keys before any lookup/write.
    if type(value) is not dict or len(value) > 128:
        return None
    if any(type(key) is not str or len(key) > 128 for key in value):
        return None
    return value


def read_publication(headers: object) -> Publication | None:
    """Read only the small, versioned namespace; malformed metadata is absent."""
    headers = _native_headers(headers)
    if headers is None:
        return None
    metadata = _fields(headers.get(NAMESPACE), ("v", "origin", "attempt"))
    if metadata is None or type(metadata["v"]) is not int or metadata["v"] != 1:
        return None
    origin = _fields(metadata["origin"], ("traceparent",))
    attempt = _fields(
        metadata["attempt"],
        ("traceparent", "published_at"),
        ("eligible_at", "previous_traceparent"),
    )
    if origin is None or attempt is None:
        return None
    origin_parent = _traceparent(origin["traceparent"])
    parent = _traceparent(attempt["traceparent"])
    published = _epoch(attempt["published_at"])
    eligible = _epoch(attempt["eligible_at"]) if "eligible_at" in attempt else None
    previous = (
        _traceparent(attempt["previous_traceparent"])
        if "previous_traceparent" in attempt
        else None
    )
    if (
        origin_parent is None
        or parent is None
        or published is None
        or ("eligible_at" in attempt and eligible is None)
        or ("previous_traceparent" in attempt and previous is None)
    ):
        return None
    publication = Publication(origin_parent, parent, published, eligible, previous)
    # Every input has already passed bounded shape/type/length checks.
    if (
        len(json.dumps(_wire(publication), separators=(",", ":")).encode("ascii"))
        > MAX_CARRIER_BYTES
    ):
        return None
    return publication


@dataclass
class _Dispatch:
    runtime: ProcessRuntime | None
    span: Span | None
    active: bool = True


_ACTIVE: ContextVar[_Dispatch | None] = ContextVar(
    "altimeter_celery_dispatch", default=None
)


def _queue_name(
    runtime: ProcessRuntime, exchange: object, routing_key: object, declare: object
) -> str:
    from kombu import Exchange, Queue  # type: ignore[import-untyped]

    if (
        type(exchange) is not str
        or len(exchange) > 128
        or type(routing_key) is not str
        or len(routing_key) > 128
    ):
        return "_OTHER"
    if exchange == "":
        return routing_key if routing_key in runtime.config.queue_names else "_OTHER"
    # Declaring a queue does not mean this message routes there. A named direct
    # exchange must match the single declared binding; other topologies are unknown.
    if (type(declare) is not list and type(declare) is not tuple) or len(declare) != 1:
        return "_OTHER"
    queue = declare[0]
    if type(queue) is not Queue or type(queue.exchange) is not Exchange:
        return "_OTHER"
    name, binding_key = queue.name, queue.routing_key
    binding_exchange, exchange_type = queue.exchange.name, queue.exchange.type
    if (
        type(name) is str
        and len(name) <= 128
        and type(binding_key) is str
        and len(binding_key) <= 128
        and type(binding_exchange) is str
        and len(binding_exchange) <= 128
        and type(exchange_type) is str
        and exchange_type == "direct"
        and binding_exchange == exchange
        and binding_key == routing_key
        and name in runtime.config.queue_names
    ):
        return name
    return "_OTHER"


def _eligible_at(value: object) -> float | None:
    if type(value) is not str or len(value) > 64:
        return None
    try:
        eta = datetime.fromisoformat(value)
        return _epoch(eta.timestamp()) if eta.tzinfo is not None else None
    except (ValueError, OverflowError, OSError):
        return None


def _before_publish(
    headers: object = None,
    exchange: object = None,
    routing_key: object = None,
    declare: object = None,
    **_: Any,
) -> None:
    try:
        from django_altimeter._celery_worker import current_attempt_context
        from django_altimeter.runtime import get_owned_trace_runtime

        dispatch = _ACTIVE.get()
        headers = _native_headers(headers)
        if dispatch is None or not dispatch.active or headers is None:
            return
        runtime, current = dispatch.runtime, dispatch.span
        if (
            runtime is None
            or current is None
            or get_owned_trace_runtime() is not runtime
        ):
            return
        # The consumer's override may open an INTERNAL span while delegating.
        # Publication metadata belongs to this scoped producer, not that child.
        correlation = current.get_span_context()
        if not correlation.is_valid:
            return
        parent = (
            f"00-{correlation.trace_id:032x}-{correlation.span_id:016x}-"
            f"{int(correlation.trace_flags):02x}"
        )
        old = read_publication(headers)
        origin = old.origin_traceparent if old is not None else parent
        previous = None
        attempt = current_attempt_context()
        task_id = headers.get("id")
        if (
            attempt is not None
            and type(task_id) is str
            and len(task_id) <= 256
            and task_id == attempt.task_id
        ):
            previous = attempt.traceparent
            if old is None and attempt.origin_traceparent is not None:
                origin = attempt.origin_traceparent
        published = _epoch(time())
        if published is None:
            return
        publication = Publication(
            origin, parent, published, _eligible_at(headers.get("eta")), previous
        )
        headers[NAMESPACE] = _wire(publication)
        current.set_attribute(
            "queue.name", _queue_name(runtime, exchange, routing_key, declare)
        )
    except Exception:
        _diagnostic("celery.publish_metadata_failed")


def _publish(
    original: Callable[..., Any], args: tuple[Any, ...], kwargs: dict[str, Any]
) -> Any:
    manager: AbstractContextManager[Span] | None = None
    owned_token: Token[Span | None] | None = None
    dispatch_token: Token[_Dispatch | None] | None = None
    dispatch: _Dispatch | None = None
    try:
        try:
            # Shadow the enclosing dispatch before any SDK or configuration work.
            # A nested startup failure must leave its native signal unowned.
            dispatch_token = _ACTIVE.set(None)
            from django_altimeter.runtime import get_owned_trace_runtime
            from django_altimeter.worker_metrics import worker_task_names

            runtime = get_owned_trace_runtime()
            if runtime is not None and "celery" in runtime.config.integrations:
                name = args[0] if args else kwargs.get("name")
                names = worker_task_names(runtime.config.policy)
                name = (
                    name
                    if type(name) is str and len(name) <= 128 and name in names
                    else "_OTHER"
                )
                parent = None
                if get_correlation() is None:
                    parent = trace.set_span_in_context(
                        trace.INVALID_SPAN, context.get_current()
                    )
                candidate = runtime.tracer.start_as_current_span(
                    "celery.publish",
                    context=parent,
                    kind=SpanKind.PRODUCER,
                    attributes={
                        "messaging.system": "celery",
                        "task.name": name,
                        "queue.name": "_OTHER",
                    },
                    record_exception=False,
                    set_status_on_exception=False,
                )
                current = candidate.__enter__()
                manager = candidate
                owned_token = _mark_owned_span(current)
                dispatch = _Dispatch(runtime, current)
                _ACTIVE.set(dispatch)
        except Exception:
            _diagnostic("celery.publish_start_failed")
        return original(*args, **kwargs)
    except BaseException:
        try:
            if dispatch is not None and dispatch.span is not None:
                dispatch.span.set_status(Status(StatusCode.ERROR))
        except Exception:
            _diagnostic("celery.publish_status_failed")
        raise
    finally:
        if dispatch is not None:
            # Copied asyncio contexts retain the holder, never a live operation.
            dispatch.active = False
            dispatch.runtime = None
            dispatch.span = None
        try:
            if dispatch_token is not None:
                _ACTIVE.reset(dispatch_token)
        except Exception:
            _diagnostic("celery.publish_context_failed")
        _reset_owned_span(owned_token)
        try:
            if manager is not None:
                manager.__exit__(None, None, None)
        except Exception:
            _diagnostic("celery.publish_end_failed")


def install(app: object) -> tuple[str, ...]:
    """Register native hooks once, preserving the app's public send_task override."""
    try:
        from celery import Celery, signals  # type: ignore[import-untyped]

        from django_altimeter._celery_worker import install_worker_signals

        if not isinstance(app, Celery):
            return ("celery.invalid_app",)
        instrumentor_module = sys.modules.get("opentelemetry.instrumentation.celery")
        if instrumentor_module is not None:
            instrumentor = getattr(instrumentor_module, "CeleryInstrumentor", None)
            if (
                instrumentor is not None
                and instrumentor().is_instrumented_by_opentelemetry
            ):
                return ("celery.instrumentor_conflict",)
        with _INSTALL_LOCK:
            installed = app.__dict__.get("_altimeter_send_task")
            if installed is None:
                original = app.send_task
                bound = isinstance(original, MethodType)

                @wraps(original.__func__ if bound else original)
                def send_task(*args: Any, **kwargs: Any) -> Any:
                    return _publish(original, args[1:] if bound else args, kwargs)

                wrapped = MethodType(send_task, app) if bound else send_task
                signals.before_task_publish.connect(
                    _before_publish,
                    weak=False,
                    dispatch_uid="django_altimeter.publish",
                )
                app.send_task = wrapped
                app.__dict__["_altimeter_send_task"] = wrapped
            elif app.send_task != installed:
                return ("celery.publication_conflict",)
            return install_worker_signals(app)
    except ImportError:
        return ("celery.unavailable",)
    except Exception:
        _diagnostic("celery.install_failed")
        return ("celery.install_failed",)
