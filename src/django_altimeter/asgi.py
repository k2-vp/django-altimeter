"""One native HTTP observation per configured ASGI branch.

The adapter forwards protocol messages immediately. Duration ends at the last
successful body/trailer send, or an observed abort, not at client receipt.
"""

from __future__ import annotations

import logging
import os
from collections.abc import Awaitable, Callable
from contextvars import Token
from threading import Lock
from time import monotonic, time_ns
from typing import Any

from opentelemetry import context, metrics, trace
from opentelemetry.trace import Span, SpanKind, Status, StatusCode

from django_altimeter.context import Correlation
from django_altimeter.headers import incoming_context, response_headers, response_mode
from django_altimeter.http import RequestState, request_state
from django_altimeter.runtime import get_owned_trace_runtime, get_process_runtime

type Scope = dict[str, Any]
type Message = dict[str, Any]
type Receive = Callable[[], Awaitable[Message]]
type Send = Callable[[Message], Awaitable[None]]
type ASGIApplication = Callable[[Scope, Receive, Send], Awaitable[None]]

_METHODS = frozenset(
    {"GET", "POST", "PUT", "PATCH", "DELETE", "HEAD", "OPTIONS", "CONNECT", "TRACE"}
)
_STATE_KEY = "django_altimeter.request_state"
_MISSING = object()


def _diagnostic(code: str) -> None:
    try:
        logging.getLogger("django_altimeter.asgi").warning("%s", code)
    except Exception:
        pass  # Application logging handlers cannot change request behavior.


def _method(value: object) -> str:
    return (
        value
        if type(value) is str and len(value) <= 7 and value in _METHODS
        else "_OTHER"
    )


def _finish_span(
    span: Span,
    state: RequestState,
    *,
    outcome: str,
    duration: float,
    end_time: int,
    failed: bool,
) -> None:
    try:
        attributes: dict[str, str | int | float] = {
            "altimeter.http.outcome": outcome,
            "altimeter.response_mode": state.response_mode,
        }
        if state.route is not None:
            attributes["http.route"] = state.route
        if state.status is not None:
            attributes["http.response.status_code"] = state.status
        if state.response_ready_at is not None:
            attributes["altimeter.http.response_ready.duration"] = max(
                0.0, min(duration, state.response_ready_at - state.started_at)
            )
        span.set_attributes(attributes)
        if (
            failed
            or outcome == "aborted"
            or (state.status is not None and state.status >= 500)
        ):
            span.set_status(Status(StatusCode.ERROR))
        if failed and outcome == "completed":
            # Preserve the HTTP completion and expose later application failure
            # without reading the exception or retaining its traceback/locals.
            span.add_event("exception", {"error.type": "after_response"})
    except Exception:
        _diagnostic("http.span_enrichment_failed")
    finally:
        try:
            span.end(end_time=end_time)
        except Exception:
            _diagnostic("http.span_end_failed")


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

    def _enter(self) -> int:
        # A wrapper can be constructed before a serving child starts. An
        # inherited initialized runtime never reaches this counter.
        if self._pid != os.getpid():
            self._lock = Lock()
            self._active = 0
            self._pid = os.getpid()
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
            owned_tracing = runtime is not None and get_owned_trace_runtime() is runtime
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

        path = scope.get("path")
        if (
            type(path) is str
            and len(path) <= runtime.config.limits.max_string_bytes
            and path in runtime.config.excluded_routes
        ):
            await self.application(scope, receive, send)
            return

        started_at = monotonic()
        start_time = time_ns()
        state = RequestState(
            started_at, _method(scope.get("method")), self.surface, runtime.config
        )
        span: Span = trace.INVALID_SPAN
        parent = context.Context()
        state_token: Token[RequestState | None] | None = None
        trace_token: Token[context.Context] | None = None
        previous_state = scope.get(_STATE_KEY, _MISSING)
        counted = False
        completed_at: float | None = None
        aborted_at: float | None = None
        expects_trailers = False
        failed = False
        try:
            if owned_tracing:
                state.concurrent_at_start = self._enter()
                counted = True
            state_token = request_state.set(state)
            scope[_STATE_KEY] = state
            parent = incoming_context(
                scope.get("headers", []),
                trusted_ingress=self.trusted_ingress,
                trust_remote_parent=runtime.config.trust_remote_parent,
            )
            if owned_tracing:
                span = runtime.tracer_provider.get_tracer(
                    "django_altimeter"
                ).start_span(
                    state.method if state.method != "_OTHER" else "HTTP",
                    context=parent,
                    kind=SpanKind.SERVER,
                    start_time=start_time,
                    attributes={
                        "http.request.method": state.method,
                        "altimeter.surface": state.surface,
                        "altimeter.http.server.concurrent_at_start": (
                            state.concurrent_at_start
                        ),
                    },
                )
        except Exception:
            _diagnostic("http.span_start_failed")
        try:
            # Even when a native start hook fails, do not disclose or adopt an
            # unrelated caller span as this request's SERVER observation.
            trace_token = context.attach(trace.set_span_in_context(span, parent))
        except Exception:
            _diagnostic("http.context_failed")

        async def observed_receive() -> Message:
            nonlocal aborted_at, failed
            try:
                message = await receive()
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
                            request_headers=scope.get("headers", []),
                            frontend_origins=runtime.config.frontend_origins,
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
                if not state.finalized:
                    state.finalized = True
                    finished_at = completed_at or aborted_at or monotonic()
                    outcome = "completed" if completed_at is not None else "aborted"
                    duration = max(0.0, finished_at - started_at)
                    if not state.excluded and owned_metrics:
                        try:
                            recorder = runtime.request_metrics
                            if recorder is not None:
                                recorder.record(
                                    duration,
                                    surface=state.surface,
                                    method=state.method,
                                    route=state.route,
                                    status=state.status,
                                    outcome=outcome,
                                    response_mode=state.response_mode,
                                )
                        except Exception:
                            _diagnostic("http.metrics_failed")
                    _finish_span(
                        span,
                        state,
                        outcome=outcome,
                        duration=duration,
                        end_time=start_time + int(duration * 1_000_000_000),
                        failed=failed,
                    )
            finally:
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
    if (
        type(surface) is not str
        or len(surface) > 6
        or surface not in {"django", "mcp"}
        or type(trusted_ingress) is not bool
    ):
        _diagnostic("http.invalid_boundary")
        return application
    return _InstrumentedASGI(application, surface, trusted_ingress)
