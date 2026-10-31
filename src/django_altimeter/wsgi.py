"""One native WSGI observation, with context scoped to each application operation.

Duration ends at iterator exhaustion or an observed abort. The WSGI server must
call close(), including after exhaustion: native response completion precedes
application close errors in gunicorn. Servers that abandon an unfinished iterable
without closing it are unsupported. No context remains attached between calls.

Native file wrappers keep their identity and the server's sendfile path. Their
observation ends when the server calls close(), before application close work.
If an instance cannot accept a close hook, it is returned untouched and the
observation ends at app return with http.file_wrapper_unobserved. That fallback
cannot observe file-transfer duration, completion or subsequent aborts.

Excluded requests keep owned child spans non-recording during application,
iterator and hooked close operations. Native server file-transfer code, and an
unhookable close method, remain outside the adapter's context boundary.
"""

from __future__ import annotations

import logging
import os
import sys
from collections.abc import Callable, Iterable, Iterator
from contextlib import contextmanager
from contextvars import Token
from threading import Lock
from time import monotonic, time_ns
from typing import Any, cast
from uuid import uuid4
from wsgiref.types import StartResponse, WSGIApplication, WSGIEnvironment

from opentelemetry import context, metrics, trace
from opentelemetry.trace import Span

from django_altimeter.context import (
    _OWNED_SPAN,
    _reset_owned_span,
    get_correlation,
)
from django_altimeter.headers import (
    incoming_context,
    wsgi_request_headers,
    wsgi_response_headers,
)
from django_altimeter.http import (
    RequestState,
    finish_request,
    request_state,
    start_server_span,
    valid_boundary,
)
from django_altimeter.privacy import _request_method
from django_altimeter.runtime import (
    ProcessRuntime,
    _owns_trace_provider,
    get_process_runtime,
)

_STATE_KEY = "django_altimeter.request_state"
_MISSING = object()
_EMPTY_CONTEXT = context.Context()
_EXCLUDED_CONTEXT_KEY = context.create_key("altimeter_excluded_wsgi")


def _diagnostic(code: str) -> None:
    try:
        logging.getLogger("django_altimeter.wsgi").warning("%s", code)
    except Exception:
        pass  # Application log handlers cannot change protocol behavior.


class _Response:
    """One request and its native iterable; no response bytes are retained."""

    def __init__(
        self,
        owner: _InstrumentedWSGI,
        runtime: ProcessRuntime,
        environ: WSGIEnvironment,
        start_response: StartResponse,
        owned_tracing: bool,
        owned_metrics: bool,
        *,
        excluded: bool = False,
    ) -> None:
        self._owner = owner
        self._counted = False
        self._runtime = runtime
        self._environ = environ
        self._start_response = start_response
        self._owned_tracing = owned_tracing
        self._owned_metrics = owned_metrics
        self._excluded_marker = object() if excluded else None
        self._span: Span = trace.INVALID_SPAN
        self._parent = _EMPTY_CONTEXT
        self._application_context: context.Context | None = None
        self._application_owned_span: Span | None = None
        self._closed = False
        self._failed = False
        self._completed_at: float | None = None
        self._aborted_at: float | None = None
        self._body: Iterable[bytes] | None = None
        self._iterator: Iterator[bytes] | None = None
        self._previous_state = environ.get(_STATE_KEY, _MISSING)
        self._start_time = time_ns()
        self.state = RequestState(
            monotonic(),
            _request_method(environ.get("REQUEST_METHOD")),
            owner.surface,
            runtime.config,
            excluded=excluded,
        )

    def _begin(self) -> None:
        try:
            if self.state.excluded:
                self._span = trace.NonRecordingSpan(
                    trace.SpanContext(uuid4().int, uuid4().int >> 64, False)
                )
                # A response-local marker keeps nested write() calls in their
                # current manual span without exposing an HTTP request state.
                self._parent = context.set_value(
                    _EXCLUDED_CONTEXT_KEY, self._excluded_marker, _EMPTY_CONTEXT
                )
                return
            if self._owned_tracing:
                self.state.concurrent_at_start = self._owner._enter(self._runtime.pid)
                self._counted = True
            self._environ[_STATE_KEY] = self.state
            config = self._runtime.config
            if (
                self._owner.trusted_ingress is True
                and config.trust_remote_parent is True
            ):
                self._parent = incoming_context(
                    wsgi_request_headers(self._environ),
                    trusted_ingress=True,
                    trust_remote_parent=True,
                )
            if self._owned_tracing:
                self._span = start_server_span(
                    self._runtime.tracer, self.state, self._parent, self._start_time
                )
        except Exception:
            _diagnostic("http.span_start_failed")

    @contextmanager
    def _active_context(self) -> Iterator[None]:
        # write() can be called inside app/next/close while a manual span is
        # current. Keep that live scope; the enclosing operation saves it.
        if request_state.get() is self.state or (
            self.state.excluded
            and context.get_value(_EXCLUDED_CONTEXT_KEY) is self._excluded_marker
        ):
            yield
            return
        state_token: Token[RequestState | None] | None = None
        trace_token: Token[context.Context] | None = None
        owned_token: Token[Span | None] | None = None
        try:
            if not self.state.excluded:
                state_token = request_state.set(self.state)
            if self._owned_tracing:
                application_context = self._application_context
                owned_span = self._application_owned_span
                if application_context is None:
                    application_context = trace.set_span_in_context(
                        self._span, self._parent
                    )
                    owned_span = None if self.state.excluded else self._span
                trace_token = context.attach(application_context)
                owned_token = _OWNED_SPAN.set(owned_span)
        except Exception:
            _diagnostic("http.context_failed")
        try:
            yield
        finally:
            if trace_token is not None:
                try:
                    # A generator may hold a span open across yield. Preserve
                    # its exact ownership marker too; foreign spans stay foreign.
                    self._application_context = context.get_current()
                    self._application_owned_span = _OWNED_SPAN.get()
                except Exception:
                    _diagnostic("http.context_failed")
            _reset_owned_span(owned_token)
            if trace_token is not None:
                try:
                    context.detach(trace_token)
                except Exception:
                    _diagnostic("http.context_failed")
            if state_token is not None:
                try:
                    request_state.reset(state_token)
                except Exception:
                    _diagnostic("http.context_failed")

    def invoke(self, application: WSGIApplication) -> Iterable[bytes]:
        self._begin()
        try:
            with self._active_context():
                body = application(self._environ, self._response_start)
                self._body = body
                factory = self._environ.get("wsgi.file_wrapper")
                native_file = isinstance(factory, type) and isinstance(body, factory)
                if not native_file:
                    self._iterator = iter(body)
        except BaseException:
            self._abort()
            try:
                self.close()
            except BaseException:
                # A second close error cannot replace the original app/iter error.
                _diagnostic("http.close_failed")
            raise
        if native_file:
            return self._observe_file(body)
        return self

    def _observe_file(self, body: Iterable[bytes]) -> Iterable[bytes]:
        # The server dispatches sendfile by this exact object's native type.
        # Never interpose an iterable or force a read to observe its lifetime.
        try:
            original_close: Any = getattr(body, "close", _MISSING)
            owner: _Response | None = self

            def close_file() -> Any:
                nonlocal owner, original_close
                if owner is None:
                    return None
                current, owner = owner, None
                current._closed = True
                try:
                    with current._active_context():
                        # Gunicorn calls close from its transfer's finally block.
                        # Observe an active failure without reading its contents.
                        if sys.exception() is not None:
                            current._abort()
                        else:
                            current._completed_at = monotonic()
                        current._finish()
                        if original_close is not _MISSING:
                            return original_close()
                        return None
                finally:
                    original_close = _MISSING
                    current._release_references()

            cast(Any, body).close = close_file
        except Exception:
            _diagnostic("http.file_wrapper_unobserved")
            self._closed = True
            self._completed_at = monotonic()
            try:
                with self._active_context():
                    self._finish()
            finally:
                self._release_references()
        return body

    def _response_start(
        self,
        status: str,
        headers: list[tuple[str, str]],
        exc_info: Any = _MISSING,
    ) -> Callable[[bytes], object]:
        outgoing = headers
        try:
            if not self.state.excluded:
                outgoing = wsgi_response_headers(
                    headers,
                    environ=self._environ,
                    policy=self._runtime.response_header_policy,
                    correlation=get_correlation() if self._owned_tracing else None,
                    owned_tracing=self._owned_tracing,
                )
        except Exception:
            _diagnostic("http.headers_failed")
        try:
            if exc_info is _MISSING:
                write = self._start_response(status, outgoing)
            else:
                write = self._start_response(status, outgoing, exc_info)
        except BaseException:
            self._abort()
            raise
        try:
            if (
                type(status) is str
                and len(status) >= 3
                and (len(status) == 3 or status[3] == " ")
                and status[:3].isascii()
                and status[:3].isdigit()
            ):
                code = int(status[:3])
                if 100 <= code <= 599:
                    self.state.status = code
        except Exception:
            _diagnostic("http.start_observation_failed")

        def observed_write(data: bytes) -> object:
            try:
                with self._active_context():
                    return write(data)
            except BaseException:
                self._abort()
                raise

        return observed_write

    def __iter__(self) -> _Response:
        return self

    def __next__(self) -> bytes:
        if self._closed or self._iterator is None:
            raise StopIteration
        try:
            with self._active_context():
                return next(self._iterator)
        except StopIteration:
            if self._completed_at is None and self._aborted_at is None:
                self._completed_at = monotonic()
            # Completion is known even if a non-compliant server omits close().
            self._release_counter()
            raise
        except BaseException:
            self._abort()
            raise

    def _abort(self) -> None:
        self._failed = True
        if self._aborted_at is None and self._completed_at is None:
            self._aborted_at = monotonic()
        self._release_counter()

    def _release_counter(self) -> None:
        if self._counted:
            self._counted = False
            try:
                self._owner._leave()
            except Exception:
                _diagnostic("http.concurrency_failed")

    def _finish(self) -> None:
        if self.state.excluded:
            self.state.finalized = True
            return
        try:
            finished_at = self._completed_at or self._aborted_at or monotonic()
            duration = max(0.0, finished_at - self.state.started_at)
            finish_request(
                self._span,
                self.state,
                runtime=self._runtime,
                owned_metrics=self._owned_metrics,
                outcome="completed" if self._completed_at is not None else "aborted",
                duration=duration,
                end_time=self._start_time + int(duration * 1_000_000_000),
                failed=self._failed,
            )
        except Exception:
            _diagnostic("http.finalization_failed")
        finally:
            self.state.finalized = True
            self._release_counter()
            try:
                if self._previous_state is _MISSING:
                    self._environ.pop(_STATE_KEY, None)
                else:
                    self._environ[_STATE_KEY] = self._previous_state
            except Exception:
                _diagnostic("http.context_failed")

    def close(self) -> None:
        if self._closed:
            return
        self._closed = True
        if self._completed_at is None and self._aborted_at is None:
            self._aborted_at = monotonic()
        try:
            with self._active_context():
                try:
                    close = getattr(self._body, "close", None)
                    if close is not None:
                        close()
                except BaseException:
                    self._failed = True
                    raise
                finally:
                    self._finish()
        finally:
            self._release_references()

    def _release_references(self) -> None:
        # Break application write-callable cycles without touching its objects.
        self._body = self._iterator = None
        self._application_context = None
        self._application_owned_span = None

    def __del__(self) -> None:
        # An abandoned iterable has no safe export/close boundary. Release only
        # our counter; never run application close() or a telemetry drain here.
        try:
            self._release_counter()
        except Exception:
            pass


class _InstrumentedWSGI:
    def __init__(
        self, application: WSGIApplication, surface: str, trusted_ingress: bool
    ) -> None:
        self.application = application
        self.surface = surface
        self.trusted_ingress = trusted_ingress
        self._pid = os.getpid()
        self._lock = Lock()
        self._active = 0

    def _enter(self, pid: int) -> int:
        # An uninitialized parent can pass this wrapper to a serving child.
        # Initialized inherited providers never reach this owned-runtime branch.
        with self._lock:
            if self._pid != pid:
                self._active = 0
                self._pid = pid
            self._active += 1
            return self._active

    def _leave(self) -> None:
        with self._lock:
            self._active -= 1

    def __call__(
        self, environ: WSGIEnvironment, start_response: StartResponse
    ) -> Iterable[bytes]:
        if request_state.get() is not None:
            return self.application(environ, start_response)
        try:
            runtime = get_process_runtime()
            owned_tracing = runtime is not None and _owns_trace_provider(runtime)
            owned_metrics = (
                runtime is not None
                and "metrics" in runtime.result.enabled_signals
                and metrics.get_meter_provider() is runtime.meter_provider
            )
            if runtime is None or not (owned_tracing or owned_metrics):
                observed = None
            else:
                path = environ.get("PATH_INFO")
                excluded = (
                    type(path) is str
                    and len(path) <= runtime.config.limits.max_string_bytes
                    and path in runtime.config.excluded_routes
                )
                observed = (
                    None
                    if excluded and not owned_tracing
                    else _Response(
                        self,
                        runtime,
                        environ,
                        start_response,
                        owned_tracing,
                        owned_metrics,
                        excluded=excluded,
                    )
                )
        except Exception:
            _diagnostic("http.runtime_failed")
            observed = None
        # Never retry an application call as an instrumentation fallback.
        if observed is None:
            return self.application(environ, start_response)
        return observed.invoke(self.application)


def instrument(
    application: WSGIApplication,
    *,
    surface: str = "django",
    trusted_ingress: bool = False,
) -> WSGIApplication:
    """Wrap one native branch; provider setup belongs to the serving child.

    Repeated wrapping is idempotent. Only explicit deployment policy may set
    trusted_ingress; incoming HTTP headers can never establish trust.
    """
    if isinstance(application, _InstrumentedWSGI):
        return application
    if not valid_boundary(surface, trusted_ingress):
        _diagnostic("http.invalid_boundary")
        return application
    return _InstrumentedWSGI(application, surface, trusted_ingress)
