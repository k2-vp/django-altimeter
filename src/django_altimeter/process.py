"""Opt-in current-process observations on the existing native metric reader.

CPU and GC counters start at their first successful observation, excluding
pre-writer/inherited totals. RSS, threads and FDs are current values, exported as
cumulative non-monotonic sums per the native process conventions. Summing RSS
can double-count shared pages; a new writer is not evidence of a crash or OOM.
"""

from __future__ import annotations

import gc
import logging
import os
from collections.abc import Callable, Mapping
from dataclasses import replace
from math import isfinite
from threading import Lock
from typing import TYPE_CHECKING

from opentelemetry.metrics import CallbackOptions, MeterProvider, Observation

from django_altimeter.privacy import (
    DEFAULT_LIMITS,
    PrivacyLimits,
    PrivacyPolicy,
    _is_allowed_attribute,
    is_valid_configuration,
)

if TYPE_CHECKING:
    from opentelemetry.sdk.metrics.view import View

PROCESS_SCOPE = "django_altimeter"
PROCESS_CPU_TIME = "process.cpu.time"
PROCESS_MEMORY = "process.memory.usage"
PROCESS_THREADS = "process.thread.count"
PROCESS_FDS = "process.unix.file_descriptor.count"
PROCESS_GC_COLLECTIONS = "altimeter.process.gc.collections"
PROCESS_GC_COLLECTED = "altimeter.process.gc.collected"
PROCESS_GC_UNCOLLECTABLE = "altimeter.process.gc.uncollectable"
PROCESS_INSTRUMENTS = frozenset(
    (PROCESS_SCOPE, name)
    for name in (
        PROCESS_CPU_TIME,
        PROCESS_MEMORY,
        PROCESS_THREADS,
        PROCESS_FDS,
        PROCESS_GC_COLLECTIONS,
        PROCESS_GC_COLLECTED,
        PROCESS_GC_UNCOLLECTABLE,
    )
)
_CPU_ATTRIBUTES = ({"cpu.mode": "user"}, {"cpu.mode": "system"})
_GC_ATTRIBUTES = tuple({"altimeter.gc.generation": str(i)} for i in range(3))
_EMPTY_ATTRIBUTES: tuple[Mapping[str, str], ...] = ({},)
_SOURCE_DIAGNOSTICS = frozenset(
    {
        "process.cpu_unavailable",
        "process.memory_unavailable",
        "process.threads_unavailable",
        "process.fd_unavailable",
        "process.gc_unavailable",
    }
)
type _Number = int | float


def process_metric_policy(policy: PrivacyPolicy) -> PrivacyPolicy:
    """Compose only the fixed process instrument and dimension allow-lists."""
    return replace(
        policy,
        metric_instruments=policy.metric_instruments | PROCESS_INSTRUMENTS,
        scope_names=policy.scope_names | {PROCESS_SCOPE},
        attribute_values=policy.attribute_values
        | {("cpu.mode", mode) for mode in ("user", "system")}
        | {("altimeter.gc.generation", str(generation)) for generation in range(3)},
    )


def process_metric_views() -> tuple[View, ...]:
    """Exact native types/units; runtime retains the catch-all drop View."""
    from opentelemetry.sdk.metrics import ObservableCounter, ObservableUpDownCounter
    from opentelemetry.sdk.metrics.view import SumAggregation, View

    return (
        View(
            meter_name=PROCESS_SCOPE,
            instrument_name=PROCESS_CPU_TIME,
            instrument_type=ObservableCounter,
            instrument_unit="s",
            attribute_keys={"cpu.mode"},
            aggregation=SumAggregation(),
        ),
        *(
            View(
                meter_name=PROCESS_SCOPE,
                instrument_name=name,
                instrument_type=ObservableUpDownCounter,
                instrument_unit=unit,
                attribute_keys=set(),
                aggregation=SumAggregation(),
            )
            for name, unit in (
                (PROCESS_MEMORY, "By"),
                (PROCESS_THREADS, "{thread}"),
                (PROCESS_FDS, "{file_descriptor}"),
            )
        ),
        *(
            View(
                meter_name=PROCESS_SCOPE,
                instrument_name=name,
                instrument_type=ObservableCounter,
                instrument_unit=unit,
                attribute_keys={"altimeter.gc.generation"},
                aggregation=SumAggregation(),
            )
            for name, unit in (
                (PROCESS_GC_COLLECTIONS, "{collection}"),
                (PROCESS_GC_COLLECTED, "{object}"),
                (PROCESS_GC_UNCOLLECTABLE, "{object}"),
            )
        ),
    )


class ProcessMetrics:
    """Seven fixed instruments, at most fourteen series, no sampling thread.

    Source reads return only bounded scalars. An unavailable source omits its
    observations independently. The PID check precedes any inherited lock or
    source access; the enclosing runtime owns the provider and writer UUID.
    """

    def __init__(
        self,
        meter_provider: MeterProvider,
        *,
        policy: PrivacyPolicy,
        limits: PrivacyLimits = DEFAULT_LIMITS,
    ) -> None:
        import psutil  # type: ignore[import-untyped]

        if not is_valid_configuration(policy, limits):
            raise ValueError("process.invalid_configuration")
        self._pid = os.getpid()
        self._process = psutil.Process(self._pid)
        if self._process.pid != self._pid:
            raise ValueError("process.invalid_source")
        self._policy = policy
        self._limits = limits
        self._lock = Lock()
        self._reported: set[str] = set()
        self._pending: set[str] = set()
        # Four fixed keys, each retaining two CPU or three GC scalars only.
        self._baselines: dict[str, tuple[_Number, ...]] = {}
        self._previous: dict[str, tuple[_Number, ...]] = {}
        meter = meter_provider.get_meter(PROCESS_SCOPE)
        meter.create_observable_counter(
            PROCESS_CPU_TIME,
            callbacks=[self._cpu],
            unit="s",
            description="CPU seconds since first observation, excluding child CPU.",
        )
        meter.create_observable_up_down_counter(
            PROCESS_MEMORY, callbacks=[self._memory], unit="By"
        )
        meter.create_observable_up_down_counter(
            PROCESS_THREADS, callbacks=[self._threads], unit="{thread}"
        )
        meter.create_observable_up_down_counter(
            PROCESS_FDS, callbacks=[self._fds], unit="{file_descriptor}"
        )
        meter.create_observable_counter(
            PROCESS_GC_COLLECTIONS, callbacks=[self._collections], unit="{collection}"
        )
        meter.create_observable_counter(
            PROCESS_GC_COLLECTED, callbacks=[self._collected], unit="{object}"
        )
        meter.create_observable_counter(
            PROCESS_GC_UNCOLLECTABLE, callbacks=[self._uncollectable], unit="{object}"
        )

    def report_pending(self) -> None:
        """Deliver fixed codes only after native collection/export locks release."""
        if os.getpid() != self._pid or not self._lock.acquire(blocking=False):
            return
        try:
            codes = tuple(sorted(self._pending))
            self._pending.clear()
            # Mark before calling handlers: a handler may reenter collection.
            self._reported.update(codes)
        finally:
            self._lock.release()
        for code in codes:
            try:
                logging.getLogger("django_altimeter.process").warning("%s", code)
            except Exception:
                pass  # Application handlers must not break the native reader.

    def _observe(
        self,
        options: CallbackOptions,
        read: Callable[[], tuple[_Number, ...]],
        attributes: tuple[Mapping[str, str], ...],
        code: str,
        *,
        counter: str | None = None,
    ) -> tuple[Observation, ...]:
        if os.getpid() != self._pid or options.timeout_millis <= 0:
            return ()
        if not self._lock.acquire(blocking=False):
            return ()
        try:
            values = read()
            if type(values) is not tuple or len(values) != len(attributes):
                raise ValueError("process.invalid_observation")
            for value, labels in zip(values, attributes, strict=True):
                if (
                    type(value) not in (int, float)
                    or (counter != PROCESS_CPU_TIME and type(value) is not int)
                    or not 0 <= value <= 2**63 - 1
                    or not isfinite(value)
                    or len(labels) > self._limits.max_attributes
                    or not all(
                        _is_allowed_attribute(key, label, self._policy, self._limits)
                        for key, label in labels.items()
                    )
                ):
                    raise ValueError("process.invalid_observation")
            if counter is not None:
                previous = self._previous.get(counter, values)
                if any(
                    now < prior for now, prior in zip(values, previous, strict=True)
                ):
                    raise ValueError("process.invalid_observation")
                self._previous[counter] = values
                baseline = self._baselines.setdefault(counter, values)
                values = tuple(
                    value - start for value, start in zip(values, baseline, strict=True)
                )
            return tuple(
                Observation(value, labels)
                for value, labels in zip(values, attributes, strict=True)
            )
        except Exception:
            # This callback runs under the SDK's non-reentrant collection lock.
            # Store at most five fixed codes; never call application handlers here.
            if code in _SOURCE_DIAGNOSTICS and code not in self._reported:
                self._pending.add(code)
        finally:
            self._lock.release()
        return ()

    def _cpu_values(self) -> tuple[_Number, ...]:
        times = self._process.cpu_times()
        return times.user, times.system

    def _cpu(self, options: CallbackOptions) -> tuple[Observation, ...]:
        return self._observe(
            options,
            self._cpu_values,
            _CPU_ATTRIBUTES,
            "process.cpu_unavailable",
            counter=PROCESS_CPU_TIME,
        )

    def _memory(self, options: CallbackOptions) -> tuple[Observation, ...]:
        return self._observe(
            options,
            lambda: (self._process.memory_info().rss,),
            _EMPTY_ATTRIBUTES,
            "process.memory_unavailable",
        )

    def _threads(self, options: CallbackOptions) -> tuple[Observation, ...]:
        return self._observe(
            options,
            lambda: (self._process.num_threads(),),
            _EMPTY_ATTRIBUTES,
            "process.threads_unavailable",
        )

    def _fds(self, options: CallbackOptions) -> tuple[Observation, ...]:
        return self._observe(
            options,
            lambda: (self._process.num_fds(),),
            _EMPTY_ATTRIBUTES,
            "process.fd_unavailable",
        )

    @staticmethod
    def _gc_values(statistic: str) -> tuple[_Number, ...]:
        stats = gc.get_stats()
        if type(stats) is not list or len(stats) != 3:
            raise ValueError("process.invalid_observation")
        if any(type(row) is not dict or len(row) != 3 for row in stats):
            raise ValueError("process.invalid_observation")
        return tuple(row[statistic] for row in stats)

    def _collections(self, options: CallbackOptions) -> tuple[Observation, ...]:
        return self._observe(
            options,
            lambda: self._gc_values("collections"),
            _GC_ATTRIBUTES,
            "process.gc_unavailable",
            counter=PROCESS_GC_COLLECTIONS,
        )

    def _collected(self, options: CallbackOptions) -> tuple[Observation, ...]:
        return self._observe(
            options,
            lambda: self._gc_values("collected"),
            _GC_ATTRIBUTES,
            "process.gc_unavailable",
            counter=PROCESS_GC_COLLECTED,
        )

    def _uncollectable(self, options: CallbackOptions) -> tuple[Observation, ...]:
        return self._observe(
            options,
            lambda: self._gc_values("uncollectable"),
            _GC_ATTRIBUTES,
            "process.gc_unavailable",
            counter=PROCESS_GC_UNCOLLECTABLE,
        )
