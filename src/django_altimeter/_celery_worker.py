"""Concrete Celery execution signals; registration never initializes providers."""

from __future__ import annotations

import logging
import os
from contextvars import ContextVar, Token
from dataclasses import dataclass
from threading import Lock
from time import monotonic, time, time_ns
from typing import TYPE_CHECKING, Any

from celery import signals  # type: ignore[import-untyped]
from opentelemetry import context, metrics, trace
from opentelemetry.trace import Link, Span, Status, StatusCode

from django_altimeter.context import (
    _bounded_text,
    _mark_owned_span,
    _reset_owned_span,
)
from django_altimeter.headers import incoming_context
from django_altimeter.privacy import is_allowed_attribute
from django_altimeter.runtime import _owns_trace_provider, get_process_runtime

if TYPE_CHECKING:
    from django_altimeter.runtime import ProcessRuntime

_INSTALL_LOCK = Lock()
_EMPTY_CONTEXT = context.Context()


@dataclass(frozen=True)
class AttemptContext:
    """Bounded retry-publication facts; task IDs are never exported."""

    task_id: str
    traceparent: str
    origin_traceparent: str | None
    origin_tracestate: str | None = None


@dataclass
class _Attempt:
    runtime: ProcessRuntime
    task_id: str
    request_id: int
    task_name: str
    queue_name: str
    started_at: float
    started_monotonic: float
    start_time: int
    outcome: str = "unknown"
    span: Span | None = None
    trace_token: Token[context.Context] | None = None
    owned_token: Token[Span | None] | None = None
    active_token: Token[_Attempt | None] | None = None
    publication: AttemptContext | None = None
    published_at: float | None = None
    eligible_at: float | None = None
    active: bool = True


_ACTIVE: ContextVar[_Attempt | None] = ContextVar(
    "altimeter_celery_attempt", default=None
)


def _report(code: str) -> None:
    try:
        logging.getLogger("django_altimeter.celery").warning("%s", code)
    except Exception:
        pass  # Application logging must not turn a receiver into a task failure.


def current_attempt_context() -> AttemptContext | None:
    """Return only this process's still-active owned execution attempt."""
    try:
        attempt = _ACTIVE.get()
        if (
            attempt is not None
            and attempt.active
            and attempt.runtime is get_process_runtime()
            and _owns_trace_provider(attempt.runtime)
        ):
            return attempt.publication
    except Exception:
        _report("celery.context_failed")
    return None


def _parent(traceparent: str) -> context.Context:
    # Y1 already validates its bounded carrier. Reuse the exact traceparent-only
    # ingress parser; neither ambient baggage nor a global propagator participates.
    return incoming_context(
        [(b"traceparent", traceparent.encode("ascii"))],
        trusted_ingress=True,
        trust_remote_parent=True,
    )


def _detach_context(attempt: _Attempt) -> None:
    _reset_owned_span(attempt.owned_token)
    try:
        if attempt.trace_token is not None:
            context.detach(attempt.trace_token)
    except Exception:
        _report("celery.context_failed")
    finally:
        attempt.trace_token = None
        attempt.owned_token = None


def _finish(attempt: _Attempt, *, control_flow: bool = False) -> None:
    if not attempt.active:
        return
    attempt.active = False
    duration = monotonic() - attempt.started_monotonic
    try:
        if attempt.span is not None:
            attempt.span.set_attribute("altimeter.outcome", attempt.outcome)
            if not control_flow and attempt.outcome in {"failure", "unknown"}:
                attempt.span.set_status(Status(StatusCode.ERROR))
            attempt.span.end(
                end_time=attempt.start_time + int(duration * 1_000_000_000)
            )
    except Exception:
        _report("celery.span_finish_failed")
    try:
        runtime = attempt.runtime
        recorder = runtime.worker_metrics
        if (
            recorder is not None
            and metrics.get_meter_provider() is runtime.meter_provider
        ):
            recorder.record(
                task_name=attempt.task_name,
                queue_name=attempt.queue_name,
                outcome=attempt.outcome,
                duration_seconds=duration,
                started_at=attempt.started_at,
                published_at=attempt.published_at,
                eligible_at=attempt.eligible_at,
            )
    except Exception:
        _report("celery.metrics_failed")
    finally:
        _detach_context(attempt)
        try:
            if attempt.active_token is not None:
                _ACTIVE.reset(attempt.active_token)
        except Exception:
            _report("celery.context_failed")
        # A context copied by application callbacks shares this holder. Empty it
        # as well as resetting our token, so it cannot publish a completed retry.
        attempt.publication = None
        attempt.task_id = ""
        attempt.span = None
        attempt.trace_token = attempt.owned_token = attempt.active_token = None


class _WorkerHooks:
    def __init__(self, app: Any) -> None:
        self.app = app
        self.controller_pid: int | None = None
        self.pool: str | None = None
        self.runtime: ProcessRuntime | None = None
        self._reported_missing_metadata = False

    def worker_init(self, sender: Any = None, **_: Any) -> None:
        try:
            if sender.app is not self.app:
                return
            from celery.concurrency.prefork import TaskPool as PreforkPool  # type: ignore[import-untyped]
            from celery.concurrency.solo import TaskPool as SoloPool  # type: ignore[import-untyped]

            pool = sender.pool_cls
            self.controller_pid = os.getpid()
            self.pool = None
            if pool is PreforkPool or (type(pool) is str and pool == "prefork"):
                self.pool = "prefork"
            elif pool is SoloPool or (type(pool) is str and pool == "solo"):
                self.pool = "solo"
            else:
                _report("celery.unsupported_pool")
        except Exception:
            _report("celery.lifecycle_failed")

    def _start(self, role: str) -> None:
        from django_altimeter.django import load_django_config
        from django_altimeter.logging import install_logging
        from django_altimeter.runtime import setup

        result = load_django_config(role=role)
        if result.config is None:
            _report("celery.invalid_config")
            return
        config = result.config
        if not config.enabled or "celery" not in config.integrations:
            return
        if self.runtime is not None and self.runtime is get_process_runtime():
            return
        setup(config)
        runtime = get_process_runtime()
        if runtime is None or runtime.config.role != role:
            return
        self.runtime = runtime
        if "logs" in runtime.result.enabled_signals:
            installed = install_logging(routes=("", "celery.task"))
            if not installed.ok:
                _report("celery.logging_failed")

    def worker_process_init(self, **_: Any) -> None:
        try:
            if self.controller_pid is None:
                return
            pid = os.getpid()
            if self.pool == "prefork" and pid != self.controller_pid:
                self._start("celery-prefork")
            elif self.pool == "solo" and pid == self.controller_pid:
                self._start("celery-solo")
        except Exception:
            _report("celery.lifecycle_failed")

    def beat_init(self, sender: Any = None, **_: Any) -> None:
        try:
            if sender.app is self.app:
                self._start("beat")
                # Beat has no public shutdown signal. The existing runtime's
                # single atexit coordinator owns its normal two-second drain.
        except Exception:
            _report("celery.lifecycle_failed")

    def _owned_runtime(self) -> ProcessRuntime | None:
        runtime = self.runtime
        if (
            runtime is not None
            and runtime is get_process_runtime()
            and runtime.config.role in {"celery-prefork", "celery-solo"}
        ):
            return runtime
        return None

    def shutdown(self, **_: Any) -> None:
        try:
            runtime = self._owned_runtime()
            if runtime is None:
                return
            attempt = _ACTIVE.get()
            if attempt is not None and attempt.runtime is runtime:
                _finish(attempt)
            runtime.shutdown(timeout=2.0)
        except Exception:
            _report("celery.shutdown_failed")

    def task_prerun(self, sender: Any = None, task_id: Any = None, **_: Any) -> None:
        attempt: _Attempt | None = None
        try:
            runtime = self._owned_runtime()
            if runtime is None or sender.app is not self.app:
                return
            previous = _ACTIVE.get()
            if previous is not None and previous.active:
                # A nested/eager trace is not a second worker delivery. Leave its
                # enclosing execution context alone; real workers run one at a time.
                return
            from django_altimeter.celery import read_publication
            from django_altimeter.worker_metrics import worker_task_names

            request = sender.request
            task_name = sender.name
            names = worker_task_names(runtime.config.policy)
            task_name = (
                task_name
                if _bounded_text(task_name, 128) and task_name in names
                else "_OTHER"
            )
            delivery = request.delivery_info
            queue = delivery.get("routing_key") if type(delivery) is dict else None
            queue = (
                queue
                if _bounded_text(queue, 128) and queue in runtime.config.queue_names
                else "_OTHER"
            )
            bounded_id = task_id if _bounded_text(task_id, 256) else ""
            attempt = _Attempt(
                runtime,
                bounded_id,
                id(request),
                task_name,
                queue,
                time(),
                monotonic(),
                time_ns(),
            )
            attempt.active_token = _ACTIVE.set(attempt)
            publication = read_publication(request.headers)
            if publication is not None:
                attempt.published_at = publication.published_at
                attempt.eligible_at = publication.eligible_at
            elif not self._reported_missing_metadata:
                self._reported_missing_metadata = True
                _report("celery.metadata_unavailable")
            if not _owns_trace_provider(runtime):
                return  # Independently owned metrics need no context attachment.
            parent = _parent(publication.traceparent) if publication else _EMPTY_CONTEXT
            links: tuple[Link, ...] = ()
            if publication is not None and publication.previous_traceparent is not None:
                previous_span = trace.get_current_span(
                    _parent(publication.previous_traceparent)
                )
                links = (Link(previous_span.get_span_context()),)
            attributes = {
                "messaging.system": "celery",
                "messaging.operation.name": "process",
                "task.name": task_name,
                "queue.name": queue,
            }
            safe_attributes = {
                key: value
                for key, value in attributes.items()
                if is_allowed_attribute(
                    key, value, policy=runtime._policy, limits=runtime.config.limits
                )
            }
            current = runtime.tracer.start_span(
                "celery.process",
                context=parent,
                kind=trace.SpanKind.CONSUMER,
                attributes=safe_attributes,
                links=links,
                start_time=attempt.start_time,
            )
            attempt.span = current
            attempt.trace_token = context.attach(
                trace.set_span_in_context(current, parent)
            )
            attempt.owned_token = _mark_owned_span(current)
            span_context = current.get_span_context()
            if span_context.is_valid and bounded_id:
                attempt.publication = AttemptContext(
                    bounded_id,
                    f"00-{span_context.trace_id:032x}-{span_context.span_id:016x}-{int(span_context.trace_flags):02x}",
                    publication.origin_traceparent if publication else None,
                )
        except Exception:
            if attempt is not None:
                # A span callback failure must not shorten the independently
                # owned metric lifetime. Postrun still observes the task result.
                _detach_context(attempt)
            _report("celery.start_failed")

    def _attempt(self, sender: Any, task_id: Any = None) -> _Attempt | None:
        runtime = self._owned_runtime()
        if runtime is None or sender.app is not self.app:
            return None
        attempt = _ACTIVE.get()
        if (
            attempt is not None
            and attempt.active
            and attempt.runtime is runtime
            and id(sender.request) == attempt.request_id
            and (
                not attempt.task_id
                or task_id is None
                or (type(task_id) is str and task_id == attempt.task_id)
            )
        ):
            return attempt
        return None

    def task_retry(self, sender: Any = None, request: Any = None, **_: Any) -> None:
        try:
            if self._owned_runtime() is None:
                return
            if (attempt := self._attempt(sender, request.id)) is not None:
                attempt.outcome = "retry"
        except Exception:
            _report("celery.outcome_failed")

    def task_failure(self, sender: Any = None, task_id: Any = None, **_: Any) -> None:
        try:
            if (attempt := self._attempt(sender, task_id)) is not None:
                attempt.outcome = "failure"
        except Exception:
            _report("celery.outcome_failed")

    def task_postrun(
        self, sender: Any = None, task_id: Any = None, state: Any = None, **_: Any
    ) -> None:
        try:
            if (attempt := self._attempt(sender, task_id)) is not None:
                if type(state) is str:
                    attempt.outcome = {
                        "SUCCESS": "success",
                        "RETRY": "retry",
                        "FAILURE": "failure",
                    }.get(state, attempt.outcome)
                # Ignore (including replace) and Reject are deliberate control
                # flow. Keep the bounded unknown outcome without marking ERROR.
                control_flow = type(state) is str and state in {"IGNORED", "REJECTED"}
                _finish(attempt, control_flow=control_flow)
        except Exception:
            _report("celery.finish_failed")

    def task_internal_error(
        self, sender: Any = None, task_id: Any = None, **_: Any
    ) -> None:
        try:
            if (attempt := self._attempt(sender, task_id)) is not None:
                attempt.outcome = "unknown"
                _finish(attempt)
        except Exception:
            _report("celery.finish_failed")


def install_worker_signals(app: Any) -> tuple[str, ...]:
    """Append native receivers once without touching existing receivers/providers."""
    acquired = False
    try:
        acquired = _INSTALL_LOCK.acquire(timeout=0.05)
        if not acquired:
            return ("celery.install_busy",)
        existing = getattr(app, "_altimeter_worker_hooks", None)
        if type(existing) is _WorkerHooks and existing.app is app:
            return ()
        hooks = _WorkerHooks(app)
        for signal, handler in (
            (signals.worker_init, hooks.worker_init),
            (signals.worker_process_init, hooks.worker_process_init),
            (signals.beat_init, hooks.beat_init),
            (signals.worker_process_shutdown, hooks.shutdown),
            (signals.worker_shutdown, hooks.shutdown),
            (signals.task_prerun, hooks.task_prerun),
            (signals.task_retry, hooks.task_retry),
            (signals.task_failure, hooks.task_failure),
            (signals.task_postrun, hooks.task_postrun),
            (signals.task_internal_error, hooks.task_internal_error),
        ):
            signal.connect(
                handler,
                weak=False,
                dispatch_uid=("altimeter.worker", id(app), signal.name),
            )
        app._altimeter_worker_hooks = hooks
        return ()
    except Exception:
        return ("celery.install_failed",)
    finally:
        if acquired:
            _INSTALL_LOCK.release()
