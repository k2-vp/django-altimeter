"""Bounded Celery attempt measurements, admitted before native aggregation."""

from __future__ import annotations

import logging
import os
from collections.abc import Mapping
from dataclasses import replace
from math import isfinite
from threading import Lock
from types import MappingProxyType
from typing import TYPE_CHECKING, TypeGuard

from opentelemetry.metrics import MeterProvider

from django_altimeter.privacy import (
    DEFAULT_LIMITS,
    PrivacyLimits,
    PrivacyPolicy,
    _is_allowed_attribute,
    is_valid_configuration,
)

if TYPE_CHECKING:
    from opentelemetry.sdk.metrics.view import View

WORKER_SCOPE = "django_altimeter"
WORKER_ATTEMPTS = "altimeter.worker.attempts"
WORKER_DURATION = "altimeter.worker.duration"
WORKER_ENQUEUE_TO_START = "altimeter.worker.enqueue_to_start"
WORKER_INTENTIONAL_DELAY = "altimeter.worker.intentional_delay"
WORKER_INSTRUMENTS = frozenset(
    (WORKER_SCOPE, name)
    for name in (
        WORKER_ATTEMPTS,
        WORKER_DURATION,
        WORKER_ENQUEUE_TO_START,
        WORKER_INTENTIONAL_DELAY,
    )
)
WORKER_DURATION_BUCKETS = (
    0.005,
    0.01,
    0.05,
    0.1,
    0.5,
    1,
    5,
    15,
    30,
    60,
    120,
    300,
    900,
    3600,
    86400,
)
WORKER_ATTRIBUTE_KEYS = frozenset(
    {
        "messaging.system",
        "task.name",
        "queue.name",
        "altimeter.outcome",
        "altimeter.overflow",
    }
)
_OUTCOMES = frozenset({"success", "retry", "failure", "unknown"})
_OTHER = "_OTHER"
_SERIES_LIMIT = 2000
_NAME_BYTES = 128
type _Dimensions = tuple[str, str, str]


def _valid_name(value: object) -> bool:
    if type(value) is not str or not 0 < len(value) <= _NAME_BYTES:
        return False
    try:
        return len(value.encode("utf-8")) <= _NAME_BYTES and not value.isspace()
    except UnicodeEncodeError:
        return False


def _validate_names(names: frozenset[str], maximum: int) -> None:
    try:
        if (
            type(names) is frozenset
            and len(names) <= maximum
            and all(_valid_name(name) for name in names)
        ):
            return
    except Exception:
        pass
    raise ValueError("worker_metrics.invalid_configuration")


def worker_task_names(policy: PrivacyPolicy) -> frozenset[str]:
    """Extract explicit names from the validated, bounded settings allow-list."""
    if (
        type(policy) is not PrivacyPolicy
        or type(policy.attribute_values) is not frozenset
    ):
        raise ValueError("worker_metrics.invalid_configuration")
    if len(policy.attribute_values) > 128:
        raise ValueError("worker_metrics.invalid_configuration")
    names: set[str] = set()
    for pair in policy.attribute_values:
        if type(pair) is not tuple or len(pair) != 2 or type(pair[0]) is not str:
            raise ValueError("worker_metrics.invalid_configuration")
        if pair[0] == "task.name":
            if not _valid_name(pair[1]):
                raise ValueError("worker_metrics.invalid_configuration")
            names.add(pair[1])
    return frozenset(names)


def worker_metric_policy(
    policy: PrivacyPolicy,
    *,
    task_names: frozenset[str],
    queue_names: frozenset[str],
) -> PrivacyPolicy:
    """Compose fixed Celery fields after configuration validation, without I/O."""
    _validate_names(task_names, 128)
    _validate_names(queue_names, 32)
    return replace(
        policy,
        metric_instruments=policy.metric_instruments | WORKER_INSTRUMENTS,
        scope_names=policy.scope_names | {WORKER_SCOPE},
        span_names=policy.span_names | {"celery.publish", "celery.process"},
        attribute_values=policy.attribute_values
        | {("task.name", name) for name in task_names | {_OTHER}}
        | {("queue.name", name) for name in queue_names | {_OTHER}}
        | {("altimeter.outcome", outcome) for outcome in _OUTCOMES}
        | {
            ("messaging.system", "celery"),
            ("messaging.operation.name", "publish"),
            ("messaging.operation.name", "process"),
        },
    )


def worker_metric_views() -> tuple[View, ...]:
    """Exact scope, name, type and unit; the runtime supplies the catch-all drop."""
    from opentelemetry.sdk.metrics import Counter, Histogram
    from opentelemetry.sdk.metrics.view import (
        ExplicitBucketHistogramAggregation,
        SumAggregation,
        View,
    )

    return (
        View(
            meter_name=WORKER_SCOPE,
            instrument_name=WORKER_ATTEMPTS,
            instrument_type=Counter,
            instrument_unit="{attempt}",
            attribute_keys=set(WORKER_ATTRIBUTE_KEYS),
            aggregation=SumAggregation(),
        ),
        *(
            View(
                meter_name=WORKER_SCOPE,
                instrument_name=name,
                instrument_type=Histogram,
                instrument_unit="s",
                attribute_keys=set(WORKER_ATTRIBUTE_KEYS),
                aggregation=ExplicitBucketHistogramAggregation(
                    boundaries=WORKER_DURATION_BUCKETS
                ),
            )
            for name in (
                WORKER_DURATION,
                WORKER_ENQUEUE_TO_START,
                WORKER_INTENTIONAL_DELAY,
            )
        ),
    )


def _seconds(value: object) -> TypeGuard[int | float]:
    # Check exact types and magnitude before arithmetic, hashing or encoding.
    if type(value) is int or type(value) is float:
        return 0 <= value <= 2**63 - 1 and isfinite(value)
    return False


class WorkerMetrics:
    """One process-owned recorder, independent of task trace sampling.

    Detailed and reserved overflow series share the 2,000-series ceiling for
    each instrument. Admission survives delta collections because the native
    SDK retains aggregation objects for process life. Unknown names normalize
    before lookup; rejected detailed keys are never cached.
    """

    def __init__(
        self,
        meter_provider: MeterProvider,
        *,
        policy: PrivacyPolicy,
        limits: PrivacyLimits = DEFAULT_LIMITS,
        task_names: frozenset[str] = frozenset(),
        queue_names: frozenset[str] = frozenset(),
    ) -> None:
        _validate_names(task_names, 128)
        _validate_names(queue_names, 32)
        if not is_valid_configuration(policy, limits):
            raise ValueError("worker_metrics.invalid_configuration")
        self._pid = os.getpid()
        self._policy = policy
        self._limits = limits
        self._task_names = task_names | {_OTHER}
        self._queue_names = queue_names | {_OTHER}
        self._capacity = _SERIES_LIMIT - len(_OUTCOMES) * len(self._queue_names)
        self._admitted: dict[_Dimensions, Mapping[str, str | bool]] = {}
        self._overflow: dict[tuple[str, str], Mapping[str, str | bool]] = {}
        self._lock = Lock()
        self._reported: set[str] = set()
        meter = meter_provider.get_meter(WORKER_SCOPE)
        self._counter = meter.create_counter(WORKER_ATTEMPTS, unit="{attempt}")
        self._duration = meter.create_histogram(
            WORKER_DURATION, unit="s", description="Monotonic task-attempt lifetime."
        )
        self._enqueue = meter.create_histogram(
            WORKER_ENQUEUE_TO_START,
            unit="s",
            description="Start minus publication epoch, including intentional delay.",
        )
        self._delay = meter.create_histogram(
            WORKER_INTENTIONAL_DELAY,
            unit="s",
            description="Known eligibility minus publication epoch.",
        )

    def _report(self, code: str) -> None:
        try:
            with self._lock:
                if code in self._reported:
                    return
                self._reported.add(code)
            logging.getLogger("django_altimeter.worker_metrics").warning("%s", code)
        except Exception:
            pass  # Application logging handlers cannot break task execution.

    def _admit(
        self, dimensions: _Dimensions
    ) -> tuple[Mapping[str, str | bool] | None, bool]:
        with self._lock:
            admitted = self._admitted.get(dimensions)
            if admitted is not None:
                return admitted, False
            task, queue, outcome = dimensions
            attributes: dict[str, str | bool] = {
                "messaging.system": "celery",
                "task.name": task,
                "queue.name": queue,
                "altimeter.outcome": outcome,
            }
            if len(attributes) > self._limits.max_attributes or not all(
                _is_allowed_attribute(key, value, self._policy, self._limits)
                for key, value in attributes.items()
            ):
                return None, False
            if len(self._admitted) < self._capacity:
                admitted = MappingProxyType(attributes)
                self._admitted[dimensions] = admitted
                return admitted, False
            coarse = (queue, outcome)
            overflow = self._overflow.get(coarse)
            if overflow is None:
                if not _is_allowed_attribute(
                    "altimeter.overflow", True, self._policy, self._limits
                ):
                    return None, False
                del attributes["task.name"]
                attributes["altimeter.overflow"] = True
                overflow = MappingProxyType(attributes)
                self._overflow[coarse] = overflow
            return overflow, True

    def record(
        self,
        *,
        task_name: str,
        queue_name: str,
        outcome: str,
        duration_seconds: float | None,
        started_at: float,
        published_at: float | None = None,
        eligible_at: float | None = None,
    ) -> None:
        """Count every finalization; omit unavailable intervals without clamping."""
        if os.getpid() != self._pid:
            return  # Never touch an inherited lock or provider after fork.
        try:
            task = (
                task_name
                if type(task_name) is str
                and len(task_name) <= _NAME_BYTES
                and task_name in self._task_names
                else _OTHER
            )
            queue = (
                queue_name
                if type(queue_name) is str
                and len(queue_name) <= _NAME_BYTES
                and queue_name in self._queue_names
                else _OTHER
            )
            outcome = (
                outcome
                if type(outcome) is str and len(outcome) <= 7 and outcome in _OUTCOMES
                else "unknown"
            )
            attributes, overflow = self._admit((task, queue, outcome))
            if attributes is None:
                self._report("worker_metrics.invalid_observation")
                return
            if overflow:
                self._report("worker_metrics.overflow")
            self._counter.add(1, attributes)
            if _seconds(duration_seconds):
                self._duration.record(duration_seconds, attributes)
            if not (_seconds(started_at) and _seconds(published_at)):
                return
            if started_at < published_at:
                return
            if _seconds(eligible_at) and eligible_at > started_at:
                return
            self._enqueue.record(started_at - published_at, attributes)
            if _seconds(eligible_at) and eligible_at >= published_at:
                self._delay.record(eligible_at - published_at, attributes)
        except Exception:
            self._report("worker_metrics.record_failed")
