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
from dataclasses import asdict, dataclass, replace
from datetime import UTC, datetime
from glob import escape
from math import isfinite
from threading import Event, Lock, Thread, get_ident
from time import monotonic, time_ns
from typing import TYPE_CHECKING, Any, NoReturn
from uuid import uuid4

from opentelemetry import metrics, trace
from opentelemetry._logs import NoOpLoggerProvider
from opentelemetry.metrics import NoOpMeterProvider
from opentelemetry.trace import NoOpTracerProvider

from django_altimeter import _otel_compat
from django_altimeter.conf import Config, SignalConfig, validate_config
from django_altimeter.privacy import PrivacyLimits, PrivacyPolicy

if TYPE_CHECKING:
    import requests
    from opentelemetry._logs import LoggerProvider as APILoggerProvider
    from opentelemetry.sdk._logs import LoggerProvider
    from opentelemetry.sdk.metrics import MeterProvider
    from opentelemetry.sdk.trace import TracerProvider

    from django_altimeter.exporting import ExportOutcome, SanitizingSession
    from django_altimeter.headers import ResponseHeaderPolicy
    from django_altimeter.metrics import RequestMetrics


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

        def _export_completed(self, outcome: ExportOutcome | None = None) -> None:
            runtime._record_shutdown_export((outcome or self.outcome).code)
            super()._export_completed(outcome)

        def request(
            self, method: str, url: str | bytes, *args: Any, **kwargs: Any
        ) -> requests.Response:
            if os.getpid() != runtime.pid:
                self._refuse("export.owner")
            deadline = runtime._shutdown_deadline
            if deadline is not None:
                remaining = deadline - monotonic()
                if remaining <= 0 or runtime._finished.is_set():
                    self._refuse("export.deadline")
                timeout = kwargs.get("timeout", runtime.config.export_timeout)
                if type(timeout) not in (int, float):
                    self._refuse("export.deadline")
                kwargs["timeout"] = min(timeout, remaining)
            return super().request(method, url, *args, **kwargs)

        def _refuse(self, code: str) -> NoReturn:
            failure = ExportProtocolError(code)
            self._remember_failure(failure)
            raise failure

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
        self.tracer: trace.Tracer = trace.NoOpTracer()
        self.logger_provider: APILoggerProvider = NoOpLoggerProvider()
        self.log_handler: logging.Handler | None = None
        self.meter_provider: metrics.MeterProvider = NoOpMeterProvider()
        self.request_metrics: RequestMetrics | None = None
        self.response_header_policy: ResponseHeaderPolicy | None = None
        self._trace_provider: TracerProvider | None = None
        self._log_provider: LoggerProvider | None = None
        self._metric_provider: MeterProvider | None = None
        self._sessions: list[SanitizingSession] = []
        self._policy = config.policy
        self.result = SetupResult()
        self._initializing_thread = get_ident()
        self._initialized = Event()
        self._shutdown_deadline: float | None = None
        self._shutdown_result = ShutdownResult(False, ("shutdown.pending",))
        self._shutdown_lock = Lock()
        self._export_failure_lock = Lock()
        self._shutdown_export_failures: list[str] = []
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
        from opentelemetry.sdk.trace.sampling import (
            ALWAYS_OFF,
            ALWAYS_ON,
            ParentBased,
            Sampler,
            TraceIdRatioBased,
        )

        from django_altimeter.exporting import (
            BoundedSpanProcessor,
            create_span_exporter,
            native_span_limits,
        )

        rate = self.config.sample_rate
        root: Sampler
        if rate == 1:
            root = ALWAYS_ON
        elif rate == 0:
            root = ALWAYS_OFF
        else:
            root = TraceIdRatioBased(rate)
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
        self.tracer = provider.get_tracer("django_altimeter")

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
            AlwaysOffExemplarFilter,
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
        from django_altimeter.metrics import (
            REQUEST_INSTRUMENTS,
            RequestMetrics,
            request_metric_views,
        )

        # The native request pair admits finite dimensions before aggregation.
        # Other registered instruments keep one aggregate series until their
        # owning recorder supplies an explicit bounded schema.
        # Even DropAggregation retains an internal entry per attribute set in
        # SDK1.45.x. Filter its keys too, before the SDK hashes measurements.
        views = [
            View(
                instrument_name="*", aggregation=DropAggregation(), attribute_keys=set()
            )
        ]
        views.extend(request_metric_views())
        views.extend(
            View(meter_name=scope, instrument_name=escape(name), attribute_keys=set())
            for scope, name in sorted(
                self.config.policy.metric_instruments - REQUEST_INSTRUMENTS
            )
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
        self.request_metrics = RequestMetrics(
            provider,
            policy=self._policy,
            limits=self.config.limits,
        )

    def _resource_attributes(self) -> dict[str, str | bool]:
        attributes: dict[str, str | bool] = {
            **dict(self.config.resource_attributes),
            "service.instance.id": self.instance_id,
        }
        if self.config.role == "probe":
            attributes["altimeter.probe"] = True
        return attributes

    def _export_timeout(self) -> float:
        # An export already in progress when shutdown begins cannot consume an
        # unbounded exit budget. Later attempts additionally share its deadline.
        return min(self.config.export_timeout, self.config.shutdown_timeout, 2.0)

    def initialize(self, inherited_signals: tuple[str, ...] = ()) -> SetupResult:
        try:
            return self._initialize(inherited_signals)
        except Exception:
            self.result = SetupResult(codes=("setup.failed",))
            return self.result
        finally:
            self._initialized.set()

    def _initialize(self, inherited_signals: tuple[str, ...]) -> SetupResult:
        if not self.config.enabled:
            self.result = SetupResult(codes=("setup.disabled",))
            return self.result
        # sdk._logs constructs a default Resource at import, before our
        # explicit provider resource can take effect. Extra native detectors
        # may perform arbitrary I/O; unsupported ambient plugins fail open.
        if os.environ.get("OTEL_EXPERIMENTAL_RESOURCE_DETECTORS"):
            self.result = SetupResult(codes=("setup.resource_detector_conflict",))
            return self.result
        if not _otel_compat.configure_http_semantics():
            self.result = SetupResult(codes=("setup.semconv_conflict",))
            return self.result
        _otel_compat.prepare_fork_support()
        from django_altimeter.headers import prepare_response_header_policy
        from django_altimeter.metrics import request_metric_policy

        self.response_header_policy = prepare_response_header_policy(
            self.config.frontend_origins
        )
        request_policy = request_metric_policy(self.config.policy)
        self._policy = replace(
            request_policy,
            attribute_values=request_policy.attribute_values
            | frozenset(self.config.resource_attributes)
            | {("service.instance.id", self.instance_id)},
        )
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
        self.result = SetupResult(tuple(enabled), tuple(codes))
        if "traces" in enabled:
            if self.config.integrations & {"psycopg", "psycopg2"}:
                try:
                    from django_altimeter.psycopg import install as install_psycopg

                    codes.extend(install_psycopg(self).codes)
                except Exception:
                    codes.append("setup.psycopg_failed")
            if self.config.integrations & {"requests", "httpx", "redis"}:
                try:
                    from django_altimeter.clients import install_clients

                    codes.extend(install_clients(self))
                except Exception:
                    codes.append("setup.clients_failed")
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

    def _record_shutdown_export(self, code: str) -> None:
        if os.getpid() != self.pid or self._shutdown_deadline is None:
            return
        if code == "export.partial":
            failure = "shutdown.partial"
        elif code not in {
            "export.not_sent",
            "export.accepted",
            "export.warning",
            "export.empty",
        }:
            failure = "shutdown.export_failed"
        else:
            return
        # Native workers and the coordinator can finish exports concurrently.
        # This fixed two-code history cannot be overwritten by a later success.
        with self._export_failure_lock:
            if failure not in self._shutdown_export_failures:
                self._shutdown_export_failures.append(failure)

    def _shutdown_export_codes(self) -> tuple[str, ...]:
        for session in self._sessions:
            self._record_shutdown_export(session.outcome.code)
        with self._export_failure_lock:
            return tuple(self._shutdown_export_failures)

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
            codes.extend(self._shutdown_export_codes())
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
                finally:
                    codes.extend(self._shutdown_export_codes())
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
            codes.extend(self._shutdown_export_codes())
            fixed_codes = tuple(dict.fromkeys(codes))
            self._shutdown_result = ShutdownResult(not fixed_codes, fixed_codes)
            self._finished.set()

    def shutdown(self, *, timeout: float = 2.0) -> ShutdownResult:
        if os.getpid() != self.pid:
            return ShutdownResult(False, ("shutdown.inherited",))
        if type(timeout) not in (int, float) or not isfinite(timeout) or timeout < 0:
            return ShutdownResult(False, ("shutdown.invalid_timeout",))
        deadline = monotonic() + min(timeout, self.config.shutdown_timeout, 2.0)
        if not self._initialized.is_set():
            if self._initializing_thread == get_ident():
                return ShutdownResult(False, ("shutdown.pending",))
            if not self._initialized.wait(max(0, deadline - monotonic())):
                return ShutdownResult(False, ("shutdown.deadline",))
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
            runtime = _runtime
            install = runtime is None or runtime.pid != os.getpid()
            if install:
                inherited = () if runtime is None else runtime.result.enabled_signals
                runtime = ProcessRuntime(config)
                # Claim the single writer before native construction, which can
                # invoke logging handlers on this thread or an SDK helper thread.
                _runtime = runtime
        assert runtime is not None
        if install:
            result = runtime.initialize(inherited)
            with _setup_lock:
                if not _atexit_registered and runtime._coordinator is not None:
                    atexit.register(shutdown)
                    _atexit_registered = True
        else:
            if not runtime._initialized.is_set():
                if runtime._initializing_thread == get_ident():
                    return SetupResult(codes=("setup.in_progress",))
                # A native constructor may be waiting on a handler in another
                # thread. Bound that reentry instead of waiting on each other.
                if not runtime._initialized.wait(1.0):
                    return SetupResult(codes=("setup.in_progress",))
            # Callback objects may implement equality; compare outside locks.
            if runtime.config != config:
                result = SetupResult(codes=("setup.config_conflict",))
            elif runtime._shutdown_deadline is not None:
                return SetupResult(codes=("setup.stopped",))
            else:
                return runtime.result
        # Publish initialization before diagnostics, so identical reentry sees
        # the completed result and does not recursively emit the same codes.
        for code in result.codes:
            if code != "setup.disabled":
                _report(code)
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


def _owns_trace_provider(runtime: ProcessRuntime) -> bool:
    """Check provider ownership after get_process_runtime validates the lifetime."""
    return (
        "traces" in runtime.result.enabled_signals
        and trace.get_tracer_provider() is runtime.tracer_provider
    )


def get_owned_trace_runtime() -> ProcessRuntime | None:
    runtime = get_process_runtime()
    try:
        return (
            runtime if runtime is not None and _owns_trace_provider(runtime) else None
        )
    except Exception:
        return None


def get_process_runtime() -> ProcessRuntime | None:
    """Return the live current-process composition, never an inherited runtime."""
    runtime = _runtime
    if (
        runtime is None
        or runtime.pid != os.getpid()
        or not runtime._initialized.is_set()
        or runtime._shutdown_deadline is not None
        or not runtime.result.enabled_signals
    ):
        return None
    return runtime


def validate_runtime(config: Config) -> tuple[str, ...]:
    """Read the same configuration/ownership boundaries without starting work."""
    try:
        validation = validate_config(config)
        if validation.config is None:
            return validation.codes
        if not config.enabled:
            return ()
        codes: list[str] = []
        if os.environ.get("OTEL_EXPERIMENTAL_RESOURCE_DETECTORS"):
            codes.append("setup.resource_detector_conflict")
        if not _otel_compat.http_semantics_are_compatible():
            codes.append("setup.semconv_conflict")
        current = _runtime
        if current is not None and current.pid == os.getpid():
            if not current._initialized.is_set():
                return (*codes, "setup.in_progress")
            # Validation must not invoke callback objects' custom equality.
            callbacks_match = (
                current.config.identity_resolver is config.identity_resolver
                and current.config.log_scrubber is config.log_scrubber
            )
            values_match = current.config.enabled == config.enabled and replace(
                current.config, identity_resolver=None, log_scrubber=None
            ) == replace(config, identity_resolver=None, log_scrubber=None)
            if not callbacks_match or not values_match:
                codes.append("setup.config_conflict")
            if current._shutdown_deadline is not None:
                codes.append("setup.stopped")
        for name, available in (
            ("traces", _otel_compat.trace_provider_is_unset),
            ("logs", _otel_compat.log_provider_is_unset),
            ("metrics", _otel_compat.metric_provider_is_unset),
        ):
            if not getattr(config, name).enabled:
                continue
            if current is not None and name in current.result.enabled_signals:
                if current.pid != os.getpid():
                    codes.append(f"setup.{name}_inherited")
                continue
            if not available():
                codes.append(f"setup.{name}_foreign")
        return tuple(dict.fromkeys(codes))
    except Exception:
        return ("setup.failed",)


@dataclass(frozen=True)
class ProbeSignalResult:
    signal: str
    code: str = "export.not_sent"
    accepted_records: int = 0
    rejected_records: int = 0
    local_dropped_records: int = 0

    @property
    def accepted(self) -> bool:
        return (
            self.code in {"export.accepted", "export.warning"}
            and self.accepted_records == 1
            and self.rejected_records == 0
            and self.local_dropped_records == 0
        )


@dataclass(frozen=True)
class ProbeResult:
    instance_id: str | None
    started_at: datetime
    finished_at: datetime
    signals: tuple[ProbeSignalResult, ...] = ()
    codes: tuple[str, ...] = ()

    @property
    def ok(self) -> bool:
        return (
            bool(self.signals)
            and not self.codes
            and all(signal.accepted for signal in self.signals)
        )


_PROBE_SIGNALS = ("traces", "logs", "metrics")
_PROBE_NAME = "altimeter.probe"
_PROBE_INPUT_LIMIT = 256 * 1024
_PROBE_OUTPUT_LIMIT = 16 * 1024


def _probe_configuration(config: Config, signals: tuple[str, ...]) -> dict[str, Any]:
    # Transfer only the deployment values needed by the existing composition.
    # Callbacks, request policy and operator/read-back credentials never cross.
    return {
        "service_name": config.service_name,
        "deployment_environment": config.deployment_environment,
        "release": config.release,
        "traces": asdict(config.traces if "traces" in signals else SignalConfig()),
        "logs": asdict(config.logs if "logs" in signals else SignalConfig()),
        "metrics": asdict(config.metrics if "metrics" in signals else SignalConfig()),
        "limits": asdict(config.limits),
        "export_timeout": config.export_timeout,
        "shutdown_timeout": config.shutdown_timeout,
    }


def _probe_config_from_json(values: dict[str, Any]) -> Config:
    def signal(name: str) -> SignalConfig:
        data = values[name]
        return SignalConfig(
            enabled=data["enabled"],
            endpoint=data["endpoint"],
            headers=tuple(tuple(pair) for pair in data["headers"]),
            destination=data["destination"],
        )

    # This is a private value transfer, not another configuration parser:
    # validate_config below applies the same public deployment contract.
    return Config(
        enabled=True,
        service_name=values["service_name"],
        deployment_environment=values["deployment_environment"],
        release=values["release"],
        role="probe",
        traces=signal("traces"),
        logs=signal("logs"),
        metrics=signal("metrics"),
        limits=PrivacyLimits(**values["limits"]),
        policy=PrivacyPolicy(
            span_names=frozenset({_PROBE_NAME}),
            log_bodies=frozenset({_PROBE_NAME}),
            metric_instruments=frozenset({("django_altimeter", _PROBE_NAME)}),
        ),
        export_timeout=values["export_timeout"],
        shutdown_timeout=values["shutdown_timeout"],
        # One explicit drain owns collection. Probe sampling is always on.
        batch_schedule_delay_millis=60000,
        metric_export_interval_millis=60000,
    )


def _perform_probe(
    config: Config, started_at: datetime, deadline: float
) -> ProbeResult:
    from opentelemetry._logs import SeverityNumber
    from opentelemetry.context import Context

    requested = tuple(name for name in _PROBE_SIGNALS if getattr(config, name).enabled)
    validation = validate_config(config)
    if validation.config is None:
        return ProbeResult(None, started_at, datetime.now(UTC), codes=validation.codes)
    runtime = ProcessRuntime(config)
    codes: list[str] = []
    try:
        initialized = runtime.initialize()
        codes.extend(initialized.codes)
        if monotonic() >= deadline:
            codes.append("probe.deadline")
        else:
            if "traces" in initialized.enabled_signals:
                span = runtime.tracer_provider.get_tracer(
                    "django_altimeter"
                ).start_span(_PROBE_NAME, context=Context())
                span.end()
            if "logs" in initialized.enabled_signals:
                runtime.logger_provider.get_logger("django_altimeter").emit(
                    timestamp=time_ns(),
                    observed_timestamp=time_ns(),
                    body=_PROBE_NAME,
                    severity_number=SeverityNumber.INFO,
                )
            if "metrics" in initialized.enabled_signals:
                runtime.meter_provider.get_meter("django_altimeter").create_counter(
                    _PROBE_NAME, unit="1"
                ).add(1)
    except Exception:
        codes.append("probe.failed")
    finally:
        stopped = runtime.shutdown(timeout=max(0, deadline - monotonic()))
        codes.extend(stopped.codes)
    results = []
    for name in requested:
        session = next(
            (item for item in runtime._sessions if item.signal == name), None
        )
        if session is None:
            results.append(ProbeSignalResult(name))
        else:
            outcome = session.outcome
            results.append(
                ProbeSignalResult(
                    name,
                    outcome.code,
                    outcome.accepted_records,
                    outcome.rejected_records,
                    outcome.local_dropped_records,
                )
            )
    return ProbeResult(
        runtime.instance_id,
        started_at,
        datetime.now(UTC),
        tuple(results),
        tuple(dict.fromkeys(codes)),
    )


def _probe_child() -> None:
    """Private, one-request helper; never load a Django app or operator key."""
    import json
    import sys

    started = datetime.now(UTC)
    try:
        raw = sys.stdin.buffer.read(_PROBE_INPUT_LIMIT + 1)
        if len(raw) > _PROBE_INPUT_LIMIT:
            raise ValueError("probe.input_size")
        request = json.loads(raw)
        started = datetime.fromisoformat(request["started_at"])
        result = _perform_probe(
            _probe_config_from_json(request["config"]), started, request["deadline"]
        )
    except Exception:
        result = ProbeResult(None, started, datetime.now(UTC), codes=("probe.failed",))
    output = {
        "instance_id": result.instance_id,
        "started_at": result.started_at.isoformat(),
        "finished_at": result.finished_at.isoformat(),
        "signals": [asdict(signal) for signal in result.signals],
        "codes": result.codes,
    }
    sys.stdout.write(json.dumps(output))
    sys.stdout.flush()


def probe(
    config: Config,
    *,
    signals: tuple[str, ...] | None = None,
    timeout: float = 2.0,
) -> ProbeResult:
    """Send one isolated safe probe per selected signal within a total budget.

    A fresh helper owns all providers, native workers and at-fork hooks. Its
    bounded stdin contains validated values, never command-line credentials.
    No application provider or module runtime is installed in the caller.
    """
    import json
    import subprocess
    import sys

    started = datetime.now(UTC)
    requested: tuple[str, ...] = ()

    def failure(*codes: str) -> ProbeResult:
        return ProbeResult(
            None,
            started,
            datetime.now(UTC),
            tuple(ProbeSignalResult(name) for name in requested),
            tuple(codes),
        )

    try:
        if type(timeout) not in (int, float) or not isfinite(timeout) or timeout <= 0:
            return failure("probe.invalid_timeout")
        deadline = monotonic() + min(timeout, 2.0)
        validation = validate_config(config)
        if validation.config is None:
            return failure(*validation.codes)
        if signals is not None and (
            type(signals) is not tuple
            or len(signals) > 3
            or any(
                type(name) is not str or name not in _PROBE_SIGNALS for name in signals
            )
        ):
            return failure("probe.invalid_signals")
        enabled = tuple(
            name
            for name in _PROBE_SIGNALS
            if config.enabled and getattr(config, name).enabled
        )
        requested = (
            enabled
            if signals is None
            else tuple(name for name in _PROBE_SIGNALS if name in signals)
        )
        if not enabled or not requested:
            return failure("nothing_to_probe")
        if any(name not in enabled for name in requested):
            return failure("probe.signal_disabled")
        deadline = min(deadline, monotonic() + config.shutdown_timeout)
        request = json.dumps(
            {
                "config": _probe_configuration(config, requested),
                "started_at": started.isoformat(),
                "deadline": deadline,
            },
            ensure_ascii=False,
        ).encode("utf-8")
        if len(request) > _PROBE_INPUT_LIMIT:
            return failure("probe.input_size")
        environment = {
            key: value
            for key, value in os.environ.items()
            if not key.startswith(("ALTIMETER_", "OTEL_"))
        }
        # The helper emits exactly one bounded JSON value. Native diagnostics
        # stay private: only its fixed result codes are exposed to the caller.
        with subprocess.Popen(
            [
                sys.executable,
                "-I",
                "-c",
                "from django_altimeter.runtime import _probe_child; _probe_child()",
            ],
            stdin=subprocess.PIPE,
            stdout=subprocess.PIPE,
            stderr=subprocess.DEVNULL,
            env=environment,
        ) as child:
            try:
                try:
                    output, _ = child.communicate(
                        request, timeout=max(0, deadline - monotonic())
                    )
                except subprocess.TimeoutExpired:
                    child.kill()
                    child.communicate()
                    return failure("probe.deadline")
            except BaseException:
                # Cancellation must stop the helper before its caller resumes
                # or exits. Preserve the original interrupt after reaping.
                try:
                    child.terminate()
                    child.wait(timeout=min(0.1, max(0, deadline - monotonic())))
                except BaseException:
                    # A second interrupt while waiting must still force exit.
                    child.kill()
                finally:
                    child.wait()
                raise
        if child.returncode != 0 or len(output) > _PROBE_OUTPUT_LIMIT:
            return failure("probe.failed")
        result = json.loads(output)
        return ProbeResult(
            result["instance_id"],
            datetime.fromisoformat(result["started_at"]),
            datetime.fromisoformat(result["finished_at"]),
            tuple(ProbeSignalResult(**row) for row in result["signals"]),
            tuple(result["codes"]),
        )
    except Exception:
        return failure("probe.failed")
