"""Request-local facts shared by native protocol adapters and metadata hooks."""

from __future__ import annotations

import logging
from contextvars import ContextVar
from dataclasses import dataclass, field
from typing import TYPE_CHECKING

from opentelemetry.trace import Span, Status, StatusCode

from django_altimeter.conf import Config

if TYPE_CHECKING:
    from django_altimeter.runtime import ProcessRuntime

_ERROR_STATUS = Status(StatusCode.ERROR)


@dataclass(slots=True)
class RequestState:
    """Mutable lifetime state; the protocol adapter alone owns finalization.

    Times are monotonic seconds. A resolved route is an approved template,
    never a raw URL. No request, response, body or user object is retained here.
    """

    started_at: float
    method: str
    surface: str = "django"
    config: Config | None = field(default=None, repr=False)
    route: str | None = None
    resolved: bool = False
    excluded: bool = False
    status: int | None = None
    response_ready_at: float | None = None
    response_mode: str = "unknown"
    concurrent_at_start: int = 0
    finalized: bool = False


request_state: ContextVar[RequestState | None] = ContextVar(
    "django_altimeter.request_state", default=None
)


def get_request_state() -> RequestState | None:
    """Read this execution context's request state without touching Django."""
    return request_state.get()


def _diagnostic(code: str) -> None:
    try:
        logging.getLogger("django_altimeter.http").warning("%s", code)
    except Exception:
        pass  # Application logging handlers cannot change request behavior.


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
        if not span.is_recording():
            return
        attributes: dict[str, str | int | float] = {
            "altimeter.http.outcome": outcome,
            "altimeter.response_mode": state.response_mode,
        }
        if state.route is not None:
            attributes["http.route"] = state.route
        status = state.status
        if status is not None:
            attributes["http.response.status_code"] = status
        ready_at = state.response_ready_at
        if ready_at is not None:
            attributes["altimeter.http.response_ready.duration"] = max(
                0.0, min(duration, ready_at - state.started_at)
            )
        span.set_attributes(attributes)
        if failed or outcome == "aborted" or (status is not None and status >= 500):
            span.set_status(_ERROR_STATUS)
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


def finish_request(
    span: Span,
    state: RequestState,
    *,
    runtime: ProcessRuntime,
    owned_metrics: bool,
    outcome: str,
    duration: float,
    end_time: int,
    failed: bool,
) -> None:
    """Finish one protocol observation; adapters retain context/counter cleanup."""
    if state.finalized:
        return
    state.finalized = True
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
        end_time=end_time,
        failed=failed,
    )
