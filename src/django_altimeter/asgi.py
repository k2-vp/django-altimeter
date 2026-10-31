"""One native HTTP observation per configured ASGI branch.

The adapter forwards protocol messages immediately. Duration ends at the last
successful body/trailer send, or an observed abort, not at client receipt.
"""

from __future__ import annotations

import logging
import os
from asyncio import CancelledError
from collections.abc import Awaitable, Callable
from contextvars import Token
from threading import Lock
from time import monotonic, time_ns
from typing import Any

from opentelemetry import context, metrics, trace
from opentelemetry.trace import Span

from django_altimeter.context import Correlation, _mark_owned_span, _reset_owned_span
from django_altimeter.headers import incoming_context, response_headers, response_mode
from django_altimeter.http import (
    RequestState,
    finish_request,
    request_state,
    start_server_span,
    valid_boundary,
)
from django_altimeter.privacy import _request_method
from django_altimeter.runtime import (
    _owns_trace_provider,
    get_owned_trace_runtime,
    get_process_runtime,
)

type Scope = dict[str, Any]
type Message = dict[str, Any]
type Receive = Callable[[], Awaitable[Message]]
type Send = Callable[[Message], Awaitable[None]]
type ASGIApplication = Callable[[Scope, Receive, Send], Awaitable[None]]

_STATE_KEY = "django_altimeter.request_state"
_MISSING = object()
# OTel Context is immutable; each attached span receives a separate derived context.
_EMPTY_CONTEXT = context.Context()


def _diagnostic(code: str) -> None:
    try:
        logging.getLogger("django_altimeter.asgi").warning("%s", code)
    except Exception:
        pass  # Application logging handlers cannot change request behavior.


class _InstrumentedASGI:
    def __init__(
        self, application: ASGIApplication, surface: str, trusted_ingress: bool
    ) -> None:
        self.application = application
        self.surface = surface
        self.trusted_ingress = trusted_ingress
        self._pid = os.getpid()
        self._lock = Lock()
        self._active = 0

    def _enter(self, pid: int) -> int:
        # A wrapper can be constructed before a serving child starts. An
        # inherited initialized runtime never reaches this counter.
        if self._pid != pid:
            self._lock = Lock()
            self._active = 0
            self._pid = pid
        with self._lock:
            self._active += 1
            return self._active

    def _leave(self) -> None:
        with self._lock:
            self._active -= 1

    async def __call__(self, scope: Scope, receive: Receive, send: Send) -> None:
        if scope.get("type") != "http":
            await self.application(scope, receive, send)
            return
        try:
            runtime = get_process_runtime()
            owned_tracing = runtime is not None and _owns_trace_provider(runtime)
            owned_metrics = (
                runtime is not None
                and "metrics" in runtime.result.enabled_signals
                and metrics.get_meter_provider() is runtime.meter_provider
            )
        except Exception:
            _diagnostic("http.runtime_failed")
            runtime = None
            owned_tracing = owned_metrics = False
        if runtime is None or not (owned_tracing or owned_metrics):
            await self.application(scope, receive, send)
            return

        config = runtime.config
        path = scope.get("path")
        if (
            config.excluded_routes
            and type(path) is str
            and len(path) <= config.limits.max_string_bytes
            and path in config.excluded_routes
        ):
            await self.application(scope, receive, send)
            return

        started_at = monotonic()
        start_time = time_ns()
        state = RequestState(
            started_at, _request_method(scope.get("method")), self.surface, config
        )
        span: Span = trace.INVALID_SPAN
        parent = _EMPTY_CONTEXT
        state_token: Token[RequestState | None] | None = None
        trace_token: Token[context.Context] | None = None
        owned_span_token: Token[Span | None] | None = None
        previous_state = scope.get(_STATE_KEY, _MISSING)
        counted = False
        completed_at: float | None = None
        aborted_at: float | None = None
        expects_trailers = False
        failed = False
        try:
            if owned_tracing:
                state.concurrent_at_start = self._enter(runtime.pid)
                counted = True
            state_token = request_state.set(state)
            scope[_STATE_KEY] = state
            if self.trusted_ingress is True and config.trust_remote_parent is True:
                parent = incoming_context(
                    scope.get("headers", ()),
                    trusted_ingress=True,
                    trust_remote_parent=True,
                )
            if owned_tracing:
                span = start_server_span(runtime.tracer, state, parent, start_time)
        except Exception:
            _diagnostic("http.span_start_failed")
        try:
            # Even when a native start hook fails, do not disclose or adopt an
            # unrelated caller span as this request's SERVER observation.
            trace_token = context.attach(trace.set_span_in_context(span, parent))
            if owned_tracing and span is not trace.INVALID_SPAN:
                owned_span_token = _mark_owned_span(span)
        except Exception:
            _diagnostic("http.context_failed")

        async def observed_receive() -> Message:
            nonlocal aborted_at, failed
            try:
                message = await receive()
            except CancelledError:
                # Applications may cancel a disconnect poll and keep streaming.
                # Cancellation escaping the app is observed by the outer guard.
                raise
            except BaseException:
                failed = True
                if completed_at is None and aborted_at is None:
                    aborted_at = monotonic()
                raise
            if message.get("type") == "http.disconnect" and completed_at is None:
                if aborted_at is None:
                    aborted_at = monotonic()
            return message

        async def observed_send(message: Message) -> None:
            nonlocal completed_at, aborted_at, expects_trailers, failed
            outgoing = message
            if message.get("type") == "http.response.start":
                try:
                    span_context = span.get_span_context()
                    correlation = None
                    if (
                        owned_tracing
                        and get_owned_trace_runtime() is runtime
                        and span_context.is_valid
                    ):
                        correlation = Correlation(
                            trace.format_trace_id(span_context.trace_id),
                            trace.format_span_id(span_context.span_id),
                            int(span_context.trace_flags),
                        )
                    outgoing = {
                        **message,
                        "headers": response_headers(
                            message.get("headers", []),
                            request_headers=scope.get("headers", ()),
                            policy=runtime.response_header_policy,
                            correlation=correlation,
                            owned_tracing=owned_tracing,
                        ),
                    }
                except Exception:
                    _diagnostic("http.headers_failed")
            try:
                await send(outgoing)
            except BaseException:
                failed = True
                if completed_at is None and aborted_at is None:
                    aborted_at = monotonic()
                raise
            sent_at = monotonic()
            try:
                message_type = message.get("type")
                if message_type == "http.response.start":
                    status = message.get("status")
                    if type(status) is int and 100 <= status <= 599:
                        state.status = status
                    expects_trailers = message.get("trailers", False) is True
                    if self.surface == "mcp":
                        state.response_mode = response_mode(message.get("headers", []))
                final_body = (
                    message_type == "http.response.body"
                    and not message.get("more_body", False)
                    and not expects_trailers
                )
                final_trailers = (
                    message_type == "http.response.trailers"
                    and expects_trailers
                    and not message.get("more_trailers", False)
                )
                if (
                    (final_body or final_trailers)
                    and completed_at is None
                    and aborted_at is None
                ):
                    completed_at = sent_at
            except Exception:
                _diagnostic("http.send_observation_failed")

        try:
            await self.application(scope, observed_receive, observed_send)
        except BaseException:
            failed = True
            raise
        finally:
            try:
                finished_at = completed_at or aborted_at or monotonic()
                outcome = "completed" if completed_at is not None else "aborted"
                duration = max(0.0, finished_at - started_at)
                finish_request(
                    span,
                    state,
                    runtime=runtime,
                    owned_metrics=owned_metrics,
                    outcome=outcome,
                    duration=duration,
                    end_time=start_time + int(duration * 1_000_000_000),
                    failed=failed,
                )
            finally:
                _reset_owned_span(owned_span_token)
                if counted:
                    self._leave()
                if trace_token is not None:
                    context.detach(trace_token)
                if state_token is not None:
                    request_state.reset(state_token)
                if previous_state is _MISSING:
                    scope.pop(_STATE_KEY, None)
                else:
                    scope[_STATE_KEY] = previous_state


def instrument(
    application: ASGIApplication,
    *,
    surface: str = "django",
    trusted_ingress: bool = False,
) -> ASGIApplication:
    """Wrap one HTTP branch without starting providers or altering lifespan.

    Only an explicit deployment/network boundary may set trusted_ingress.
    Reapplying this function to its own result preserves the first wrapper.
    """
    if isinstance(application, _InstrumentedASGI):
        return application
    if not valid_boundary(surface, trusted_ingress):
        _diagnostic("http.invalid_boundary")
        return application
    return _InstrumentedASGI(application, surface, trusted_ingress)
