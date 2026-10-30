"""Concrete native client boundaries with per-destination propagation.

HTTP spans cover one physical transport call through response headers, not body
consumption. Redis spans cover one command or executed pipeline, never arguments.
"""

from __future__ import annotations

import logging
import os
import sys
from collections.abc import Iterator, MutableMapping
from contextlib import contextmanager
from contextvars import ContextVar
from functools import wraps
from threading import Lock
from typing import TYPE_CHECKING, Any, cast

from opentelemetry import context, trace
from opentelemetry.trace import Span, Status, StatusCode

from django_altimeter.context import client_span, get_correlation
from django_altimeter.headers import OUTBOUND_CONTEXT_HEADERS, outgoing_headers
from django_altimeter.privacy import is_allowed_attribute

if TYPE_CHECKING:
    from collections.abc import Callable

    from django_altimeter.runtime import ProcessRuntime

_INSTALL_LOCK = Lock()
_EXPORT_REQUEST: ContextVar[bool] = ContextVar(
    "altimeter_export_request", default=False
)


class _RedisOperation:
    """One operation's lifetime, shared by every copied task/thread context."""

    def __init__(self) -> None:
        self.active = True


_REDIS_ACTIVE: ContextVar[_RedisOperation | None] = ContextVar(
    "altimeter_redis_active", default=None
)


def _diagnostic(code: str) -> None:
    try:
        logging.getLogger("django_altimeter.clients").warning("%s", code)
    except Exception:
        pass


def _owned_runtime(integration: str) -> ProcessRuntime | None:
    try:
        from django_altimeter.runtime import get_owned_trace_runtime

        runtime = get_owned_trace_runtime()
        if runtime is not None and integration in runtime.config.integrations:
            return runtime
    except Exception:
        _diagnostic("client.runtime_failed")
    return None


def _suppressed(runtime: ProcessRuntime, url: object = None) -> bool:
    key = getattr(context, "_SUPPRESS_INSTRUMENTATION_KEY", None)
    if key is None or context.get_value(key) or _EXPORT_REQUEST.get():
        return True
    return type(url) is str and url in (
        runtime.config.traces.endpoint,
        runtime.config.logs.endpoint,
        runtime.config.metrics.endpoint,
    )


def _method(value: object) -> str:
    if type(value) is str and len(value) <= 7:
        normalized = value.upper()
        if is_allowed_attribute("http.request.method", normalized):
            return normalized
    return "_OTHER"


_MAX_CONTEXT_HEADER_NAME = max(map(len, OUTBOUND_CONTEXT_HEADERS))


def _context_header_name(key: object) -> str | None:
    # Never normalize a large name or invoke a string subclass's accessors.
    if isinstance(key, str) and str.__len__(key) <= _MAX_CONTEXT_HEADER_NAME:
        name = str.lower(key)
    elif isinstance(key, bytes) and bytes.__len__(key) <= _MAX_CONTEXT_HEADER_NAME:
        try:
            name = bytes.lower(key).decode("ascii")
        except UnicodeDecodeError:
            return None
    else:
        return None
    return name if name in OUTBOUND_CONTEXT_HEADERS else None


class _RequestHeaders(MutableMapping[Any, Any]):
    """A bounded-storage Requests carrier; only the native sender reads values.

    Filtering keys while the sender iterates preserves arbitrarily large business
    maps without a full copy or repeated scans to delete batches of mixed-case
    matches. Redirect copies still use the ordinary Requests mapping protocol.
    """

    def __init__(self, source: Any, generated: dict[str, str]) -> None:
        self._source: Any = (
            source._source if type(source) is _RequestHeaders else source
        )
        self._generated = generated

    def __iter__(self) -> Iterator[Any]:
        for key in self._source:
            if _context_header_name(key) is None:
                yield key
        yield from self._generated

    def __len__(self) -> int:
        return sum(1 for _ in self)

    def __getitem__(self, key: Any) -> Any:
        name = _context_header_name(key)
        if name is not None:
            return self._generated[name]
        return self._source[key]

    def __setitem__(self, key: Any, value: Any) -> None:
        # Application changes to business fields remain visible. Propagation
        # fields never replace the owned context selected for this send.
        self._source[key] = value

    def __delitem__(self, key: Any) -> None:
        name = _context_header_name(key)
        if name is not None:
            del self._generated[name]
        else:
            del self._source[key]

    def copy(self) -> MutableMapping[Any, Any]:
        # Preserve Requests' case-insensitive redirect operations (in particular
        # stripping Authorization and body headers), not merely the wire keys.
        from requests.structures import CaseInsensitiveDict

        return CaseInsensitiveDict(self.items())


def _apply_headers(
    carrier: Any, url: object, runtime: ProcessRuntime, tracing: bool
) -> Any:
    generated = outgoing_headers(
        url,
        internal_origins=runtime.config.internal_origins,
        correlation=get_correlation() if tracing else None,
        owned_tracing=tracing,
    )
    httpx = sys.modules.get("httpx")
    if httpx is not None and isinstance(carrier, httpx.Headers):
        # HTTPX's native carrier is case-insensitive and its transport needs raw
        # header pairs. Deletion, unlike pop(), never fetches a removed value.
        for name in OUTBOUND_CONTEXT_HEADERS:
            try:
                del carrier[name]
            except KeyError:
                pass
        carrier.update(generated)
        return carrier
    return _RequestHeaders(carrier, generated)


def _http_status(span: Span, status: object) -> None:
    try:
        if type(status) is int and 100 <= status <= 599:
            span.set_attribute("http.response.status_code", status)
            if status >= 400:
                span.set_status(Status(StatusCode.ERROR))
    except Exception:
        _diagnostic("client.status_failed")


@contextmanager
def _http_span(
    runtime: ProcessRuntime, method: object, url: object, request: Any
) -> Iterator[Span | None]:
    try:
        tracing = not _suppressed(runtime, url)
        name = _method(method)
    except Exception:
        _diagnostic("client.prepare_failed")
        tracing, name = False, "_OTHER"
    if not tracing:
        try:
            request.headers = _apply_headers(request.headers, url, runtime, False)
        except Exception:
            _diagnostic("client.headers_failed")
        yield None
        return
    with client_span(
        "HTTP" if name == "_OTHER" else name, attributes={"http.request.method": name}
    ) as current:
        try:
            request.headers = _apply_headers(request.headers, url, runtime, True)
        except Exception:
            _diagnostic("client.headers_failed")
        yield current


def _install_requests() -> None:
    import requests

    from django_altimeter.exporting import SanitizingSession

    if getattr(requests.adapters.HTTPAdapter.send, "_altimeter_client_wrapper", False):
        return
    original_send = requests.adapters.HTTPAdapter.send
    original_session_send = requests.Session.send

    @wraps(original_session_send)
    def session_send(self: Any, request: Any, **kwargs: Any) -> Any:
        if not isinstance(self, SanitizingSession):
            return original_session_send(self, request, **kwargs)
        token = _EXPORT_REQUEST.set(True)
        try:
            return original_session_send(self, request, **kwargs)
        finally:
            _EXPORT_REQUEST.reset(token)

    @wraps(original_send)
    def send(self: Any, request: Any, *args: Any, **kwargs: Any) -> Any:
        runtime = _owned_runtime("requests")
        if runtime is None:
            return original_send(self, request, *args, **kwargs)
        with _http_span(runtime, request.method, request.url, request) as current:
            response = original_send(self, request, *args, **kwargs)
            if current is not None:
                _http_status(current, response.status_code)
            return response

    cast(Any, send)._altimeter_client_wrapper = True
    cast(Any, requests.Session).send = session_send
    cast(Any, requests.adapters.HTTPAdapter).send = send


def _install_httpx() -> None:
    import httpx

    if getattr(httpx.HTTPTransport.handle_request, "_altimeter_client_wrapper", False):
        return
    original_sync = httpx.HTTPTransport.handle_request
    original_async = httpx.AsyncHTTPTransport.handle_async_request

    @wraps(original_sync)
    def handle_request(self: Any, request: Any) -> Any:
        runtime = _owned_runtime("httpx")
        if runtime is None:
            return original_sync(self, request)
        with _http_span(runtime, request.method, str(request.url), request) as current:
            response = original_sync(self, request)
            if current is not None:
                _http_status(current, response.status_code)
            return response

    @wraps(original_async)
    async def handle_async_request(self: Any, request: Any) -> Any:
        runtime = _owned_runtime("httpx")
        if runtime is None:
            return await original_async(self, request)
        with _http_span(runtime, request.method, str(request.url), request) as current:
            response = await original_async(self, request)
            if current is not None:
                _http_status(current, response.status_code)
            return response

    cast(Any, handle_request)._altimeter_client_wrapper = True
    cast(Any, httpx.HTTPTransport).handle_request = handle_request
    cast(Any, httpx.AsyncHTTPTransport).handle_async_request = handle_async_request


def _redis_operation(args: tuple[Any, ...]) -> str:
    if args and type(args[0]) in {str, bytes} and len(args[0]) <= 16:
        value = (
            args[0].decode("ascii", errors="replace")
            if type(args[0]) is bytes
            else args[0]
        )
        command = value.upper()
        if is_allowed_attribute("db.operation.name", command):
            return command
    return "DB"


@contextmanager
def _redis_span(operation: str) -> Iterator[None]:
    runtime = _owned_runtime("redis")
    try:
        enclosing = _REDIS_ACTIVE.get()
        tracing = (
            runtime is not None
            and (enclosing is None or not enclosing.active)
            and not _suppressed(runtime)
        )
    except Exception:
        _diagnostic("client.prepare_failed")
        tracing = False
    if not tracing:
        yield
        return
    operation_scope = _RedisOperation()
    token = _REDIS_ACTIVE.set(operation_scope)
    try:
        with client_span(
            "DB", attributes={"db.system.name": "redis", "db.operation.name": operation}
        ):
            yield
    finally:
        # Resetting only this context's token leaves copies suppressed forever.
        # Deactivate the shared holder first, so all copies can start new work.
        operation_scope.active = False
        _REDIS_ACTIVE.reset(token)


def _install_redis() -> None:
    import redis
    import redis.asyncio

    if getattr(redis.Redis.execute_command, "_altimeter_client_wrapper", False):
        return
    sync_command: Callable[..., Any] = redis.Redis.execute_command
    async_command: Callable[..., Any] = redis.asyncio.Redis.execute_command
    sync_pipeline: Callable[..., Any] = redis.client.Pipeline.execute
    async_pipeline: Callable[..., Any] = redis.asyncio.client.Pipeline.execute
    sync_immediate: Callable[..., Any] = redis.client.Pipeline.immediate_execute_command
    async_immediate: Callable[..., Any] = (
        redis.asyncio.client.Pipeline.immediate_execute_command
    )

    @wraps(sync_command)
    def execute_command(self: Any, *args: Any, **kwargs: Any) -> Any:
        with _redis_span(_redis_operation(args)):
            return sync_command(self, *args, **kwargs)

    @wraps(async_command)
    async def execute_async_command(self: Any, *args: Any, **kwargs: Any) -> Any:
        with _redis_span(_redis_operation(args)):
            return await async_command(self, *args, **kwargs)

    @wraps(sync_pipeline)
    def execute_pipeline(self: Any, *args: Any, **kwargs: Any) -> Any:
        with _redis_span("PIPELINE"):
            return sync_pipeline(self, *args, **kwargs)

    @wraps(async_pipeline)
    async def execute_async_pipeline(self: Any, *args: Any, **kwargs: Any) -> Any:
        with _redis_span("PIPELINE"):
            return await async_pipeline(self, *args, **kwargs)

    @wraps(sync_immediate)
    def immediate_command(self: Any, *args: Any, **kwargs: Any) -> Any:
        with _redis_span(_redis_operation(args)):
            return sync_immediate(self, *args, **kwargs)

    @wraps(async_immediate)
    async def immediate_async_command(self: Any, *args: Any, **kwargs: Any) -> Any:
        with _redis_span(_redis_operation(args)):
            return await async_immediate(self, *args, **kwargs)

    cast(Any, execute_command)._altimeter_client_wrapper = True
    cast(Any, redis.Redis).execute_command = execute_command
    cast(Any, redis.asyncio.Redis).execute_command = execute_async_command
    cast(Any, redis.client.Pipeline).execute = execute_pipeline
    cast(Any, redis.asyncio.client.Pipeline).execute = execute_async_pipeline
    cast(Any, redis.client.Pipeline).immediate_execute_command = immediate_command
    cast(
        Any, redis.asyncio.client.Pipeline
    ).immediate_execute_command = immediate_async_command


def install_clients(runtime: ProcessRuntime) -> tuple[str, ...]:
    """Install declared concrete clients once, during explicit owned setup."""
    try:
        if (
            runtime.pid != os.getpid()
            or "traces" not in runtime.result.enabled_signals
            or trace.get_tracer_provider() is not runtime.tracer_provider
            or runtime._shutdown_deadline is not None
        ):
            return ()
    except Exception:
        return ("setup.clients_failed",)
    codes: list[str] = []
    with _INSTALL_LOCK:
        for name, install, instrumentor in (
            ("requests", _install_requests, "RequestsInstrumentor"),
            ("httpx", _install_httpx, "HTTPXClientInstrumentor"),
            ("redis", _install_redis, "RedisInstrumentor"),
        ):
            if name not in runtime.config.integrations:
                continue
            try:
                module = sys.modules.get(f"opentelemetry.instrumentation.{name}")
                existing = getattr(module, instrumentor, None)
                if existing is not None and existing().is_instrumented_by_opentelemetry:
                    codes.append(f"setup.{name}_conflict")
                    continue
                install()
            except ImportError:
                codes.append(f"setup.{name}_missing")
            except Exception:
                codes.append(f"setup.{name}_failed")
    return tuple(codes)
