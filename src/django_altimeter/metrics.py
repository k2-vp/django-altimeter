"""One process-owned request recorder, with admission before SDK aggregation."""

from __future__ import annotations

import logging
import os
from collections.abc import Mapping
from dataclasses import replace
from math import isfinite
from threading import Lock
from types import MappingProxyType
from typing import TYPE_CHECKING

from opentelemetry.metrics import MeterProvider

from django_altimeter.privacy import (
    DEFAULT_LIMITS,
    HTTP_ATTRIBUTE_VALUES,
    HTTP_REQUEST_DURATION_BUCKETS,
    PrivacyLimits,
    PrivacyPolicy,
    _is_allowed_attribute,
    _request_method,
    is_valid_configuration,
)

if TYPE_CHECKING:
    from opentelemetry.sdk.metrics.view import View

REQUEST_SCOPE = "django_altimeter"
REQUEST_COUNT = "altimeter.http.server.requests"
REQUEST_DURATION = "http.server.request.duration"
REQUEST_INSTRUMENTS = frozenset(
    {(REQUEST_SCOPE, REQUEST_COUNT), (REQUEST_SCOPE, REQUEST_DURATION)}
)
REQUEST_SURFACES = frozenset({"django", "mcp"})
REQUEST_DURATION_BUCKETS = HTTP_REQUEST_DURATION_BUCKETS
REQUEST_ATTRIBUTE_KEYS = frozenset(
    {
        "altimeter.surface",
        "http.request.method",
        "http.route",
        "http.response.status_class",
        "altimeter.http.outcome",
        "altimeter.response_mode",
        "altimeter.overflow",
    }
)
_OUTCOMES = frozenset({"completed", "aborted"})
_RESPONSE_MODES = frozenset({"buffered", "streaming", "unknown"})
_SERIES_LIMIT = 2000
_OVERFLOW_PER_SURFACE = 2 * 6 * 3
type _Dimensions = tuple[str, str, str, str, str, str | None]


def request_metric_policy(policy: PrivacyPolicy) -> PrivacyPolicy:
    """Compose native HTTP names and fixed categories without admitting routes."""
    return replace(
        policy,
        metric_instruments=policy.metric_instruments | REQUEST_INSTRUMENTS,
        scope_names=policy.scope_names | {REQUEST_SCOPE},
        attribute_values=policy.attribute_values | HTTP_ATTRIBUTE_VALUES,
    )


def request_metric_views() -> tuple[View, ...]:
    """Exact native instruments only; the runtime supplies its catch-all drop."""
    from opentelemetry.sdk.metrics import Counter, Histogram
    from opentelemetry.sdk.metrics.view import (
        ExplicitBucketHistogramAggregation,
        SumAggregation,
        View,
    )

    return (
        View(
            meter_name=REQUEST_SCOPE,
            instrument_name=REQUEST_COUNT,
            instrument_type=Counter,
            instrument_unit="{request}",
            attribute_keys=set(REQUEST_ATTRIBUTE_KEYS),
            aggregation=SumAggregation(),
        ),
        View(
            meter_name=REQUEST_SCOPE,
            instrument_name=REQUEST_DURATION,
            instrument_type=Histogram,
            instrument_unit="s",
            attribute_keys=set(REQUEST_ATTRIBUTE_KEYS),
            aggregation=ExplicitBucketHistogramAggregation(
                boundaries=REQUEST_DURATION_BUCKETS
            ),
        ),
    )


class RequestMetrics:
    """Paired observations share one lifetime admission set across all branches.

    Configuration is trusted, validated startup data. Request values are not.
    The fixed overflow partition is reserved inside the 2,000-series ceiling,
    even before an overflow observation occurs. Delta collection never frees
    admission slots: the SDK can retain aggregation objects for process life.
    """

    def __init__(
        self,
        meter_provider: MeterProvider,
        *,
        policy: PrivacyPolicy,
        limits: PrivacyLimits = DEFAULT_LIMITS,
        surfaces: frozenset[str] = REQUEST_SURFACES,
    ) -> None:
        if (
            not is_valid_configuration(policy, limits)
            or type(surfaces) is not frozenset
            or not 0 < len(surfaces) <= len(REQUEST_SURFACES)
            or any(
                type(surface) is not str
                or len(surface) > 6
                or surface not in REQUEST_SURFACES
                for surface in surfaces
            )
        ):
            raise ValueError("metrics.invalid_configuration")
        self._policy = policy
        self._limits = limits
        self._surfaces = surfaces
        self._pid = os.getpid()
        self._capacity = _SERIES_LIMIT - _OVERFLOW_PER_SURFACE * len(surfaces)
        self._admitted: dict[_Dimensions, Mapping[str, str | bool]] = {}
        self._overflow: dict[tuple[str, str, str, str], Mapping[str, str | bool]] = {}
        self._lock = Lock()
        self._reported: set[str] = set()
        meter = meter_provider.get_meter(REQUEST_SCOPE)
        self._counter = meter.create_counter(REQUEST_COUNT, unit="{request}")
        self._duration = meter.create_histogram(
            REQUEST_DURATION,
            unit="s",
            description=(
                "Application entry to observed response completion or abort; "
                "not client receipt."
            ),
        )

    def _report(self, code: str) -> None:
        try:
            with self._lock:
                if code in self._reported:
                    return
                self._reported.add(code)
            logging.getLogger("django_altimeter.metrics").warning("%s", code)
        except Exception:
            pass  # A broken application logging handler cannot affect requests.

    def _admit(
        self, dimensions: _Dimensions
    ) -> tuple[Mapping[str, str | bool] | None, bool]:
        with self._lock:
            admitted = self._admitted.get(dimensions)
            if admitted is not None:
                return admitted, False
            surface, status_class, outcome, mode, method, route = dimensions
            attributes: dict[str, str | bool] = {
                "altimeter.surface": surface,
                "http.response.status_class": status_class,
                "altimeter.http.outcome": outcome,
                "altimeter.response_mode": mode,
                "http.request.method": method,
            }
            if route is not None:
                attributes["http.route"] = route
            if len(attributes) > self._limits.max_attributes or not all(
                _is_allowed_attribute(key, value, self._policy, self._limits)
                for key, value in attributes.items()
            ):
                return None, False
            if len(self._admitted) < self._capacity:
                admitted = MappingProxyType(attributes)
                self._admitted[dimensions] = admitted
                return admitted, False
            # Never cache a rejected detailed key. Only the reserved, bounded
            # coarse partition may grow once detailed admission is exhausted.
            coarse = dimensions[:4]
            overflow = self._overflow.get(coarse)
            if overflow is None:
                del attributes["http.request.method"]
                attributes.pop("http.route", None)
                attributes["altimeter.overflow"] = True
                overflow = MappingProxyType(attributes)
                self._overflow[coarse] = overflow
            return overflow, True

    def record(
        self,
        duration: float,
        *,
        surface: str,
        method: str,
        route: str | None,
        status: int | None,
        outcome: str,
        response_mode: str,
    ) -> None:
        """Record once at native finalization, independently of trace sampling."""
        # Never touch an inherited lock/provider after a fork.
        if os.getpid() != self._pid:
            return
        try:
            if (
                type(duration) not in (int, float)
                or not 0 <= duration <= 2**63 - 1
                or not isfinite(duration)
                or type(surface) is not str
                or len(surface) > 6
                or surface not in self._surfaces
                or type(outcome) is not str
                or len(outcome) > 9
                or outcome not in _OUTCOMES
            ):
                self._report("metrics.invalid_observation")
                return
            status_class = (
                f"{status // 100}xx"
                if type(status) is int and 100 <= status <= 599
                else "absent"
            )
            mode = (
                response_mode
                if type(response_mode) is str
                and len(response_mode) <= 9
                and response_mode in _RESPONSE_MODES
                else "unknown"
            )
            method = _request_method(method)
            route = (
                route
                if _is_allowed_attribute(
                    "http.route", route, self._policy, self._limits
                )
                else None
            )
            attributes, overflow = self._admit(
                (surface, status_class, outcome, mode, method, route)
            )
            if attributes is None:
                self._report("metrics.invalid_observation")
                return
            if overflow:
                self._report("metrics.overflow")
            self._counter.add(1, attributes)
            self._duration.record(duration, attributes)
        except Exception:
            self._report("metrics.record_failed")
