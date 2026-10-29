"""One explicit, process-owned composition of the native OpenTelemetry SDK.

Setup performs no network work. Native batch workers start only on explicit
setup; importing this module does not construct an SDK provider. Shutdown uses
one existing daemon coordinator, including at interpreter exit (where Python
3.12+ forbids starting threads). Pending telemetry may be lost at the deadline.
"""

from __future__ import annotations

import atexit
import logging
import os
from dataclasses import dataclass, replace
from glob import escape
from math import isfinite
from threading import Event, Lock, Thread
from time import monotonic
from typing import TYPE_CHECKING, Any
from uuid import uuid4

from opentelemetry import metrics, trace
from opentelemetry._logs import NoOpLoggerProvider
from opentelemetry.metrics import NoOpMeterProvider
from opentelemetry.trace import NoOpTracerProvider

from django_altimeter import _otel_compat
from django_altimeter.conf import Config, validate_config

if TYPE_CHECKING:
    import requests
    from opentelemetry._logs import LoggerProvider as APILoggerProvider
    from opentelemetry.sdk._logs import LoggerProvider
    from opentelemetry.sdk.metrics import MeterProvider
    from opentelemetry.sdk.trace import TracerProvider

    from django_altimeter.conf import SignalConfig
    from django_altimeter.exporting import SanitizingSession


@dataclass(frozen=True)
class SetupResult:
    enabled_signals: tuple[str, ...] = ()
    codes: tuple[str, ...] = ()

    @property
    def ok(self) -> bool:
        return bool(self.enabled_signals)


@dataclass(frozen=True)
class ShutdownResult:
    completed: bool = True
    codes: tuple[str, ...] = ()


def _report(code: str) -> None:
    try:
        logging.getLogger("django_altimeter.runtime").warning("%s", code)
    except Exception:
        # A broken application logging handler must not change application flow.
        pass


def _process_session(
    runtime: ProcessRuntime, signal: str, endpoint: str
) -> SanitizingSession:
    # requests/urllib3 performs a local IPv6 capability bind at import. Keep
    # that import behind explicit, enabled signal construction as well.
    from django_altimeter.exporting import ExportProtocolError, SanitizingSession

    class ProcessSession(SanitizingSession):
        """Apply writer ownership and the shared exit budget before native HTTP."""

        def request(
            self, method: str, url: str | bytes, *args: Any, **kwargs: Any
        ) -> requests.Response:
            if os.getpid() != runtime.pid:
                failure = ExportProtocolError("export.owner")
                self._remember_failure(failure)
                raise failure
            deadline = runtime._shutdown_deadline
            if deadline is not None:
                remaining = deadline - monotonic()
                if remaining <= 0 or runtime._finished.is_set():
                    failure = ExportProtocolError("export.deadline")
                    self._remember_failure(failure)
                    raise failure
                timeout = kwargs.get("timeout", runtime.config.export_timeout)
                if type(timeout) not in (int, float):
                    failure = ExportProtocolError("export.deadline")
                    self._remember_failure(failure)
                    raise failure
                kwargs["timeout"] = min(timeout, remaining)
            return super().request(method, url, *args, **kwargs)

    return ProcessSession(
        signal, endpoint, policy=runtime._policy, limits=runtime.config.limits
    )


class ProcessRuntime:
    """Explicit native providers and one shutdown lifetime, owned by one PID."""

    def __init__(self, config: Config) -> None:
        self.config = config
        self.pid = os.getpid()
        self.instance_id = str(uuid4())
        self.tracer_provider: trace.TracerProvider = NoOpTracerProvider()
        self.logger_provider: APILoggerProvider = NoOpLoggerProvider()
        self.meter_provider: metrics.MeterProvider = NoOpMeterProvider()
        self._trace_provider: TracerProvider | None = None
        self._log_provider: LoggerProvider | None = None
        self._metric_provider: MeterProvider | None = None
        self._sessions: list[SanitizingSession] = []
        self._policy = replace(
            config.policy,
            attribute_values=config.policy.attribute_values
            | frozenset(config.resource_attributes)
            | {("service.instance.id", self.instance_id)},
        )
        self.result = SetupResult()
        self._shutdown_deadline: float | None = None
        self._shutdown_result = ShutdownResult(False, ("shutdown.pending",))
        self._shutdown_lock = Lock()
        self._stop = Event()
        self._finished = Event()
        self._coordinator: Thread | None = None

    def _session(self, signal: str, config: SignalConfig) -> SanitizingSession:
        if config.endpoint is None:
            raise ValueError("setup.destination")
        session = _process_session(self, signal, config.endpoint)
        self._sessions.append(session)
        return session

    def _install_traces(self) -> None:
        from opentelemetry.sdk.resources import Resource
        from opentelemetry.sdk.trace import TracerProvider
        from opentelemetry.sdk.trace.sampling import ParentBased, TraceIdRatioBased

        from django_altimeter.exporting import (
            BoundedSpanProcessor,
            create_span_exporter,
            native_span_limits,
        )

        root = TraceIdRatioBased(self.config.sample_rate)
        sampler = (
            ParentBased(root)
            if self.config.trust_remote_parent
            else ParentBased(
                root, remote_parent_sampled=root, remote_parent_not_sampled=root
            )
        )
        provider = TracerProvider(
            resource=Resource(self._resource_attributes()),
            sampler=sampler,
            span_limits=native_span_limits(self.config.limits),
            shutdown_on_exit=False,
            meter_provider=NoOpMeterProvider(),
        )
        exporter = create_span_exporter(
            self._session("traces", self.config.traces),
            timeout=self._export_timeout(),
            headers=dict(self.config.traces.headers),
            meter_provider=NoOpMeterProvider(),
        )
        provider.add_span_processor(
            BoundedSpanProcessor(
                exporter,
                limits=self.config.limits,
                schedule_delay_millis=self.config.batch_schedule_delay_millis,
                meter_provider=NoOpMeterProvider(),
            )
        )
        self._trace_provider = provider
        trace.set_tracer_provider(provider)
        if trace.get_tracer_provider() is not provider:
            raise RuntimeError("setup.traces_race")
        self.tracer_provider = provider

    def _install_logs(self) -> None:
        from opentelemetry._logs import get_logger_provider, set_logger_provider
        from opentelemetry.sdk._logs import LoggerProvider
        from opentelemetry.sdk.resources import Resource

        from django_altimeter.exporting import (
            bounded_log_processor,
            create_log_exporter,
            native_log_limits,
        )

        provider = LoggerProvider(
            resource=Resource(self._resource_attributes()),
            shutdown_on_exit=False,
            log_record_limits=native_log_limits(self.config.limits),
            meter_provider=NoOpMeterProvider(),
        )
        exporter = create_log_exporter(
            self._session("logs", self.config.logs),
            timeout=self._export_timeout(),
            headers=dict(self.config.logs.headers),
            meter_provider=NoOpMeterProvider(),
        )
        provider.add_log_record_processor(
            bounded_log_processor(
                exporter,
                limits=self.config.limits,
                schedule_delay_millis=self.config.batch_schedule_delay_millis,
                meter_provider=NoOpMeterProvider(),
            )
        )
        self._log_provider = provider
        set_logger_provider(provider)
        if get_logger_provider() is not provider:
            raise RuntimeError("setup.logs_race")
        self.logger_provider = provider

    def _install_metrics(self) -> None:
        from opentelemetry.sdk.metrics import (
            Counter,
            Histogram,
            MeterProvider,
            ObservableCounter,
        )
        from opentelemetry.sdk.metrics.export import (
            AggregationTemporality,
            PeriodicExportingMetricReader,
        )
        from opentelemetry.sdk.metrics.view import DropAggregation, View
        from opentelemetry.sdk.resources import Resource

        from django_altimeter.exporting import create_metric_exporter

        # Until the request recorder supplies its finite dimensions, each
        # registered instrument has one aggregate series. Unknown instruments
        # never allocate aggregation series. Writer identity stays on resource.
        # Even DropAggregation retains an internal entry per attribute set in
        # SDK1.45.x. Filter its keys too, before the SDK hashes measurements.
        views = [
            View(
                instrument_name="*", aggregation=DropAggregation(), attribute_keys=set()
            )
        ]
        views.extend(
            View(meter_name=scope, instrument_name=escape(name), attribute_keys=set())
            for scope, name in sorted(self.config.policy.metric_instruments)
        )
        exporter = create_metric_exporter(
            self._session("metrics", self.config.metrics),
            timeout=self._export_timeout(),
            headers=dict(self.config.metrics.headers),
            meter_provider=NoOpMeterProvider(),
            preferred_temporality={
                Counter: AggregationTemporality.DELTA,
                Histogram: AggregationTemporality.DELTA,
                ObservableCounter: AggregationTemporality.DELTA,
            },
        )
        reader = PeriodicExportingMetricReader(
            exporter,
            export_interval_millis=self.config.metric_export_interval_millis,
            export_timeout_millis=self._export_timeout() * 1000,
        )
        from opentelemetry.sdk.metrics import AlwaysOffExemplarFilter

        provider = MeterProvider(
            resource=Resource(self._resource_attributes()),
            metric_readers=[reader],
            views=views,
            exemplar_filter=AlwaysOffExemplarFilter(),
            shutdown_on_exit=False,
        )
        self._metric_provider = provider
        metrics.set_meter_provider(provider)
        if metrics.get_meter_provider() is not provider:
            raise RuntimeError("setup.metrics_race")
        self.meter_provider = provider

    def _resource_attributes(self) -> dict[str, str]:
        return {
            **dict(self.config.resource_attributes),
            "service.instance.id": self.instance_id,
        }

    def _export_timeout(self) -> float:
        # An export already in progress when shutdown begins cannot consume an
        # unbounded exit budget. Later attempts additionally share its deadline.
        return min(self.config.export_timeout, self.config.shutdown_timeout, 2.0)

    def initialize(self, inherited_signals: tuple[str, ...] = ()) -> SetupResult:
        if not self.config.enabled:
            self.result = SetupResult(codes=("setup.disabled",))
            return self.result
        # sdk._logs constructs a default Resource at import, before our
        # explicit provider resource can take effect. Extra native detectors
        # may perform arbitrary I/O; unsupported ambient plugins fail open.
        if os.environ.get("OTEL_EXPERIMENTAL_RESOURCE_DETECTORS"):
            self.result = SetupResult(codes=("setup.resource_detector_conflict",))
            _report("setup.resource_detector_conflict")
            return self.result
        if not _otel_compat.configure_http_semantics():
            self.result = SetupResult(codes=("setup.semconv_conflict",))
            _report("setup.semconv_conflict")
            return self.result
        _otel_compat.prepare_fork_support()
        enabled: list[str] = []
        codes: list[str] = []
        # A fixed three-element composition, not a provider registry.
        for signal, config, available, install in (
            (
                "traces",
                self.config.traces,
                _otel_compat.trace_provider_is_unset,
                self._install_traces,
            ),
            (
                "logs",
                self.config.logs,
                _otel_compat.log_provider_is_unset,
                self._install_logs,
            ),
            (
                "metrics",
                self.config.metrics,
                _otel_compat.metric_provider_is_unset,
                self._install_metrics,
            ),
        ):
            if not config.enabled:
                continue
            try:
                if signal in inherited_signals:
                    code = f"setup.{signal}_inherited"
                elif not available():
                    code = f"setup.{signal}_foreign"
                else:
                    install()
                    enabled.append(signal)
                    continue
            except Exception:
                code = f"setup.{signal}_failed"
            codes.append(code)
            _report(code)
        self.result = SetupResult(tuple(enabled), tuple(codes))
        # Start once at setup, not at atexit. Even unsuccessful partial
        # construction can leave native workers that need bounded cleanup.
        if self._sessions:
            self._coordinator = Thread(
                target=self._coordinate_shutdown,
                name="altimeter-shutdown",
                daemon=True,
            )
            self._coordinator.start()
        return self.result

    def _coordinate_shutdown(self) -> None:
        self._stop.wait()
        codes: list[str] = []
        deadline = self._shutdown_deadline
        if deadline is None:
            return
        providers = (self._trace_provider, self._log_provider, self._metric_provider)
        try:
            for provider in providers:
                if provider is None:
                    continue
                remaining = deadline - monotonic()
                if remaining <= 0:
                    codes.append("shutdown.deadline")
                    break
                try:
                    if not provider.force_flush(timeout_millis=int(remaining * 1000)):
                        codes.append("shutdown.flush_failed")
                except Exception:
                    codes.append("shutdown.flush_failed")
            for session in self._sessions:
                if session.outcome.code == "export.partial":
                    codes.append("shutdown.partial")
                elif session.outcome.code not in {
                    "export.not_sent",
                    "export.accepted",
                    "export.warning",
                    "export.empty",
                }:
                    codes.append("shutdown.export_failed")
            for provider in providers:
                if provider is None:
                    continue
                remaining = deadline - monotonic()
                if remaining <= 0:
                    codes.append("shutdown.deadline")
                    break
                try:
                    if provider is self._metric_provider:
                        self._metric_provider.shutdown(timeout_millis=remaining * 1000)
                    else:
                        provider.shutdown()
                except Exception:
                    codes.append("shutdown.cleanup_failed")
            for session in self._sessions:
                if monotonic() >= deadline:
                    codes.append("shutdown.deadline")
                    break
                session.close()
            if monotonic() >= deadline:
                codes.append("shutdown.deadline")
        except Exception:
            codes.append("shutdown.cleanup_failed")
        finally:
            fixed_codes = tuple(dict.fromkeys(codes))
            self._shutdown_result = ShutdownResult(not fixed_codes, fixed_codes)
            self._finished.set()

    def shutdown(self, *, timeout: float = 2.0) -> ShutdownResult:
        if os.getpid() != self.pid:
            return ShutdownResult(False, ("shutdown.inherited",))
        if type(timeout) not in (int, float) or not isfinite(timeout) or timeout < 0:
            return ShutdownResult(False, ("shutdown.invalid_timeout",))
        deadline = monotonic() + min(timeout, self.config.shutdown_timeout, 2.0)
        if not self._shutdown_lock.acquire(timeout=max(0, deadline - monotonic())):
            return ShutdownResult(False, ("shutdown.deadline",))
        try:
            if self._shutdown_deadline is None:
                self._shutdown_deadline = deadline
                self._stop.set()
                if self._coordinator is None:
                    self._shutdown_result = ShutdownResult()
                    self._finished.set()
            deadline = min(deadline, self._shutdown_deadline)
        finally:
            self._shutdown_lock.release()
        if not self._finished.wait(max(0, deadline - monotonic())):
            return ShutdownResult(False, ("shutdown.deadline",))
        return self._shutdown_result


_runtime: ProcessRuntime | None = None
_setup_lock = Lock()
_atexit_registered = False


def _after_fork() -> None:
    global _setup_lock
    # Only our lock is replaced. SDK globals and inherited providers stay intact.
    _setup_lock = Lock()


if hasattr(os, "register_at_fork"):
    os.register_at_fork(after_in_child=_after_fork)


def setup(config: Config) -> SetupResult:
    global _runtime, _atexit_registered
    try:
        if type(config) is not Config:
            return SetupResult(codes=("setup.invalid_config",))
        validated = validate_config(config)
        if validated.config is None:
            return SetupResult(codes=validated.codes)
        config = validated.config
        with _setup_lock:
            inherited: tuple[str, ...] = ()
            if _runtime is not None:
                if _runtime.pid == os.getpid():
                    if _runtime.config != config:
                        _report("setup.config_conflict")
                        return SetupResult(codes=("setup.config_conflict",))
                    if _runtime._shutdown_deadline is not None:
                        return SetupResult(codes=("setup.stopped",))
                    return _runtime.result
                inherited = _runtime.result.enabled_signals
            runtime = ProcessRuntime(config)
            result = runtime.initialize(inherited)
            _runtime = runtime
            if not _atexit_registered and runtime._coordinator is not None:
                atexit.register(shutdown)
                _atexit_registered = True
            return result
    except Exception:
        _report("setup.failed")
        return SetupResult(codes=("setup.failed",))


def shutdown(*, timeout: float = 2.0) -> ShutdownResult:
    try:
        runtime = _runtime
        if runtime is None:
            return ShutdownResult(codes=("shutdown.not_started",))
        return runtime.shutdown(timeout=timeout)
    except Exception:
        _report("shutdown.failed")
        return ShutdownResult(False, ("shutdown.failed",))


def get_owned_trace_runtime() -> ProcessRuntime | None:
    runtime = _runtime
    if (
        runtime is None
        or runtime.pid != os.getpid()
        or runtime._shutdown_deadline is not None
        or "traces" not in runtime.result.enabled_signals
    ):
        return None
    try:
        if trace.get_tracer_provider() is runtime.tracer_provider:
            return runtime
        return None
    except Exception:
        return None


def get_process_runtime() -> ProcessRuntime | None:
    """Return the live current-process composition, never an inherited runtime."""
    runtime = _runtime
    if (
        runtime is None
        or runtime.pid != os.getpid()
        or runtime._shutdown_deadline is not None
        or not runtime.result.enabled_signals
    ):
        return None
    return runtime
