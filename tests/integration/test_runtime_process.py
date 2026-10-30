"""Real process ownership, decoded export and bounded shutdown/RSS checks.

The reference command runs this file against the installed wheel in both
verification images. Host runs are local subprocess evidence, not Linux proof.
"""

from __future__ import annotations

import json
import os
import subprocess
import sys
import textwrap
from time import monotonic
from uuid import UUID

import pytest
from support.otlp_receiver import OTLPReceiver

_IMPORTS = """
import json, os, sys, time
from dataclasses import replace
from opentelemetry import trace, metrics
from opentelemetry._logs import LogRecord, SeverityNumber, get_logger_provider
from django_altimeter.conf import Config, SignalConfig
from django_altimeter.privacy import DEFAULT_POLICY
from django_altimeter.runtime import (
    setup, shutdown, get_owned_trace_runtime, get_process_runtime,
)

def configuration(signals=('traces', 'logs', 'metrics'), **changes):
    def signal(name):
        return SignalConfig(
            enabled=name in signals,
            endpoint=os.environ['TEST_ENDPOINT'] + '/v1/' + name,
            destination='collector',
        )
    config = Config(
        enabled=True, service_name='reference', deployment_environment='test',
        release='v1', role='web', traces=signal('traces'), logs=signal('logs'),
        metrics=signal('metrics'), export_timeout=0.5,
        policy=replace(
            DEFAULT_POLICY,
            log_bodies=frozenset({'runtime.ready'}),
            metric_instruments=frozenset({('django_altimeter', 'runtime.calls')}),
        ),
    )
    return replace(config, **changes)

def emit(runtime):
    with runtime.tracer_provider.get_tracer('django_altimeter').start_as_current_span(
        'GET', attributes={'http.request.method': 'GET', 'secret': 'CANARY_PRIVATE'}
    ):
        pass
    runtime.logger_provider.get_logger('django_altimeter').emit(LogRecord(
        body='runtime.ready', severity_number=SeverityNumber.INFO,
        attributes={'http.request.method': 'GET', 'secret': 'CANARY_PRIVATE'},
    ))
    runtime.meter_provider.get_meter('django_altimeter').create_counter(
        'runtime.calls'
    ).add(7, {'secret': 'CANARY_PRIVATE'})
"""


def _environment(receiver: OTLPReceiver, **extra: str) -> dict[str, str]:
    environment = {
        key: value
        for key, value in os.environ.items()
        if not key.startswith(("ALTIMETER_", "OTEL_"))
    }
    return {**environment, "TEST_ENDPOINT": receiver.endpoint, **extra}


def _run(
    receiver: OTLPReceiver,
    source: str,
    *,
    environment: dict[str, str] | None = None,
    timeout: float = 15,
) -> dict[str, object]:
    process = subprocess.run(
        [sys.executable, "-c", _IMPORTS + textwrap.dedent(source)],
        env=environment or _environment(receiver),
        capture_output=True,
        text=True,
        timeout=timeout,
        check=False,
    )
    assert process.returncode == 0, process.stderr
    return json.loads(process.stdout)


def _signals(receiver: OTLPReceiver) -> dict[str, list[object]]:
    result: dict[str, list[object]] = {"traces": [], "logs": [], "metrics": []}
    for request in receiver.requests:
        assert b"CANARY" not in request.message.SerializeToString()
        signal = request.path.rsplit("/", 1)[-1]
        result[signal].append(request.message)
    return result


def test_real_exporters_send_each_safe_record_once(otlp_receiver: OTLPReceiver) -> None:
    result = _run(
        otlp_receiver,
        """
        config = configuration()
        result = setup(config)
        runtime = get_process_runtime()
        assert runtime is not None
        emit(runtime)
        stopped = shutdown()
        print(json.dumps({'enabled': result.enabled_signals,
                          'complete': stopped.completed, 'codes': stopped.codes,
                          'instance': runtime.instance_id}))
    """,
        environment=_environment(
            otlp_receiver,
            OTEL_EXPORTER_OTLP_METRICS_TEMPORALITY_PREFERENCE="CUMULATIVE",
            OTEL_RESOURCE_ATTRIBUTES="secret=CANARY_PRIVATE,service.name=CANARY_PRIVATE",
        ),
    )
    assert result["enabled"] == ["traces", "logs", "metrics"]
    assert result["complete"] is True, result
    assert str(UUID(result["instance"])) == result["instance"]
    signals = _signals(otlp_receiver)
    assert {key: len(value) for key, value in signals.items()} == {
        "traces": 1,
        "logs": 1,
        "metrics": 1,
    }
    trace_resource = signals["traces"][0].resource_spans[0]
    spans = trace_resource.scope_spans[0].spans
    assert len(spans) == 1
    assert spans[0].name == "GET"
    assert {a.key: a.value.string_value for a in spans[0].attributes} == {
        "http.request.method": "GET",
    }
    log_resource = signals["logs"][0].resource_logs[0]
    logs = log_resource.scope_logs[0].log_records
    assert len(logs) == 1
    assert logs[0].body.string_value == "runtime.ready"
    metric_resource = signals["metrics"][0].resource_metrics[0]
    metric = metric_resource.scope_metrics[0].metrics[0]
    assert metric.name == "runtime.calls"
    assert metric.sum.aggregation_temporality == 1  # OTLP DELTA
    assert len(metric.sum.data_points) == 1
    assert metric.sum.data_points[0].as_int == 7
    assert not metric.sum.data_points[0].attributes
    for resource in (trace_resource, log_resource, metric_resource):
        attributes = {a.key: a.value.string_value for a in resource.resource.attributes}
        assert attributes == {
            "service.name": "reference",
            "deployment.environment.name": "test",
            "service.version": "v1",
            "altimeter.role": "web",
            "service.instance.id": result["instance"],
        }


def test_initialized_parent_never_exports_from_fork_child(
    otlp_receiver: OTLPReceiver,
) -> None:
    result = _run(
        otlp_receiver,
        """
        config = configuration()
        assert setup(config).ok
        runtime = get_process_runtime()
        providers = (runtime.tracer_provider, runtime.logger_provider,
                     runtime.meter_provider)
        tracer = providers[0].get_tracer('django_altimeter')
        logger = providers[1].get_logger('django_altimeter')
        counter = providers[2].get_meter('django_altimeter').create_counter(
            'runtime.calls'
        )
        emit(runtime)
        for provider in providers:
            provider.force_flush()
        child = os.fork()
        if child == 0:
            assert get_owned_trace_runtime() is None
            inherited = setup(config)
            assert not inherited.enabled_signals
            assert set(inherited.codes) == {
                'setup.traces_inherited', 'setup.logs_inherited',
                'setup.metrics_inherited',
            }, inherited
            with tracer.start_as_current_span('GET'):
                pass
            logger.emit(LogRecord(body='runtime.ready'))
            counter.add(99)
            for provider in providers:
                provider.force_flush()
            os._exit(0)
        _, status = os.waitpid(child, 0)
        stopped = shutdown()
        print(json.dumps({'child_exit': os.waitstatus_to_exitcode(status),
                          'complete': stopped.completed}))
    """,
    )
    assert result["child_exit"] == 0
    assert result["complete"] is True
    signals = _signals(otlp_receiver)
    assert {signal: len(requests) for signal, requests in signals.items()} == {
        "traces": 1,
        "logs": 1,
        "metrics": 1,
    }
    spans = signals["traces"][0].resource_spans[0].scope_spans[0].spans
    assert len(spans) == 1
    assert spans[0].name == "GET"


def test_uninitialized_parent_allows_fresh_forked_writers(
    otlp_receiver: OTLPReceiver,
) -> None:
    result = _run(
        otlp_receiver,
        """
        original = trace.get_tracer_provider()
        statuses = []
        for _ in range(2):
            child = os.fork()
            if child == 0:
                assert setup(configuration()).ok
                emit(get_process_runtime())
                assert shutdown().completed
                os._exit(0)
            _, status = os.waitpid(child, 0)
            statuses.append(os.waitstatus_to_exitcode(status))
        assert trace.get_tracer_provider() is original
        assert get_process_runtime() is None
        print(json.dumps({'statuses': statuses}))
    """,
    )
    assert result["statuses"] == [0, 0]
    signals = _signals(otlp_receiver)
    assert {signal: len(requests) for signal, requests in signals.items()} == {
        "traces": 2,
        "logs": 2,
        "metrics": 2,
    }
    identities = {
        attribute.value.string_value
        for request in signals["metrics"]
        for resource in request.resource_metrics
        for attribute in resource.resource.attributes
        if attribute.key == "service.instance.id"
    }
    assert len(identities) == 2
    assert all(str(UUID(identity)) == identity for identity in identities)
    for request in signals["metrics"]:
        metric = request.resource_metrics[0].scope_metrics[0].metrics[0]
        assert metric.sum.aggregation_temporality == 1
        assert metric.sum.data_points[0].as_int == 7


def test_repeated_concurrent_setup_installs_once_and_config_conflicts(
    otlp_receiver: OTLPReceiver,
) -> None:
    result = _run(
        otlp_receiver,
        """
        from concurrent.futures import ThreadPoolExecutor
        import threading
        config = configuration()
        with ThreadPoolExecutor(max_workers=16) as executor:
            results = list(executor.map(lambda _: setup(config), range(64)))
        assert all(result is results[0] for result in results)
        runtime = get_process_runtime()
        original = trace.get_tracer_provider()
        conflict = setup(replace(config, sample_rate=0.25))
        assert trace.get_tracer_provider() is original
        emit(runtime)
        threads = [thread.name for thread in threading.enumerate()]
        stopped = shutdown()
        again = shutdown()
        print(json.dumps({'codes': conflict.codes, 'threads': threads,
                          'complete': stopped.completed, 'repeat': again == stopped,
                          'restart': setup(config).codes}))
    """,
    )
    assert result["codes"] == ["setup.config_conflict"]
    assert result["complete"] is True
    assert result["repeat"] is True
    assert result["restart"] == ["setup.stopped"]
    assert result["threads"].count("OtelBatchSpanRecordProcessor") == 1
    assert result["threads"].count("OtelBatchLogRecordProcessor") == 1
    assert result["threads"].count("OtelPeriodicExportingMetricReader") == 1
    assert result["threads"].count("altimeter-shutdown") == 1
    assert all(len(requests) == 1 for requests in _signals(otlp_receiver).values())


@pytest.mark.parametrize("foreign", ["traces", "logs", "metrics"])
def test_foreign_provider_is_untouched_and_other_signals_export(
    otlp_receiver: OTLPReceiver,
    foreign: str,
) -> None:
    result = _run(
        otlp_receiver,
        f"""
        from opentelemetry.sdk.resources import Resource
        from opentelemetry.sdk.trace import TracerProvider
        from opentelemetry.sdk._logs import LoggerProvider
        from opentelemetry.sdk.metrics import MeterProvider
        from opentelemetry._logs import set_logger_provider
        if {foreign!r} == 'traces':
            provider = TracerProvider(resource=Resource({{}}), shutdown_on_exit=False)
            trace.set_tracer_provider(provider)
        elif {foreign!r} == 'logs':
            provider = LoggerProvider(resource=Resource({{}}), shutdown_on_exit=False)
            set_logger_provider(provider)
        else:
            class ForeignMeterProvider(MeterProvider):
                calls = 0
                def get_meter(self, *args, **kwargs):
                    self.calls += 1
                    return super().get_meter(*args, **kwargs)
            provider = ForeignMeterProvider(
                resource=Resource({{}}), shutdown_on_exit=False
            )
            metrics.set_meter_provider(provider)
        result = setup(configuration())
        runtime = get_process_runtime()
        assert runtime is not None
        if {foreign!r} == 'traces':
            assert trace.get_tracer_provider() is provider
            assert get_owned_trace_runtime() is None
            assert type(runtime.tracer_provider) is trace.NoOpTracerProvider
        elif {foreign!r} == 'logs':
            from opentelemetry._logs import NoOpLoggerProvider
            assert get_logger_provider() is provider
            assert type(runtime.logger_provider) is NoOpLoggerProvider
        else:
            assert metrics.get_meter_provider() is provider
            assert provider.calls == 0, provider.calls
            assert type(runtime.meter_provider) is metrics.NoOpMeterProvider
        emit(runtime)
        stopped = shutdown()
        provider.shutdown()
        print(json.dumps({{'enabled': result.enabled_signals,
                           'codes': result.codes, 'complete': stopped.completed}}))
    """,
        environment=_environment(
            otlp_receiver, OTEL_PYTHON_SDK_INTERNAL_METRICS_ENABLED="true"
        ),
    )
    assert set(result["enabled"]) == {"traces", "logs", "metrics"} - {foreign}
    assert result["codes"] == [f"setup.{foreign}_foreign"]
    assert result["complete"] is True
    signals = _signals(otlp_receiver)
    assert signals[foreign] == []
    assert all(len(records) == 1 for key, records in signals.items() if key != foreign)


def test_setup_opens_no_network_and_import_starts_no_threads(
    otlp_receiver: OTLPReceiver,
) -> None:
    source = """
import json, sys, threading
started = []
original = threading.Thread.start
def observe(self):
    started.append(self.name)
    return original(self)
threading.Thread.start = observe
socket_events = []
sys.addaudithook(lambda event, args: socket_events.append(event)
                if event.startswith('socket.') else None)
import django_altimeter.runtime
assert not started, started
assert not socket_events, socket_events
probe = threading.Thread(target=lambda: None)
probe.start()
probe.join()
assert len(started) == 1
connections = []
sys.addaudithook(lambda event, args: connections.append(event)
                if event in ('socket.connect', 'socket.getaddrinfo') else None)
"""
    process = subprocess.run(
        [
            sys.executable,
            "-c",
            source
            + _IMPORTS
            + """
result = setup(configuration())
assert not connections, connections
assert result.ok
assert shutdown().completed
print(json.dumps({'enabled': result.enabled_signals}))
""",
        ],
        env=_environment(otlp_receiver),
        capture_output=True,
        text=True,
        timeout=10,
        check=False,
    )
    assert process.returncode == 0, process.stderr
    assert json.loads(process.stdout)["enabled"] == ["traces", "logs", "metrics"]
    assert not otlp_receiver.requests


@pytest.mark.parametrize("mode", ["disabled", "invalid", "semconv"])
def test_refused_configuration_preserves_application(
    otlp_receiver: OTLPReceiver,
    mode: str,
) -> None:
    result = _run(
        otlp_receiver,
        f"""
        original = trace.get_tracer_provider()
        config = configuration()
        if {mode!r} == 'disabled':
            config = replace(config, enabled=False)
        elif {mode!r} == 'invalid':
            config = object()
        else:
            os.environ['OTEL_SEMCONV_STABILITY_OPT_IN'] = 'http/dup'
        result = setup(config)
        assert trace.get_tracer_provider() is original
        assert get_owned_trace_runtime() is None
        calls = []
        def application():
            calls.append(1)
            return 'working'
        print(json.dumps({{'enabled': result.enabled_signals, 'codes': result.codes,
                           'value': application(), 'calls': calls}}))
    """,
    )
    assert result["enabled"] == []
    assert result["codes"] == [
        {
            "disabled": "setup.disabled",
            "invalid": "setup.invalid_config",
            "semconv": "setup.semconv_conflict",
        }[mode]
    ]
    assert result["value"] == "working"
    assert result["calls"] == [1]
    assert not otlp_receiver.requests


def test_shutdown_shares_one_deadline_across_signals(
    otlp_receiver: OTLPReceiver,
) -> None:
    otlp_receiver.queue_response(header_delay=0.15)
    otlp_receiver.queue_response(hang=True)
    result = _run(
        otlp_receiver,
        """
        assert setup(configuration(export_timeout=1)).ok
        emit(get_process_runtime())
        start = time.monotonic()
        result = shutdown(timeout=0.3)
        elapsed = time.monotonic() - start
        print(json.dumps({'elapsed': elapsed, 'complete': result.completed,
                          'codes': result.codes}))
    """,
    )
    assert 0.24 <= result["elapsed"] < 0.6, result
    assert result["complete"] is False
    assert "shutdown.deadline" in result["codes"]
    assert 1 <= len(otlp_receiver.requests) <= 2
    assert all(request.path != "/v1/metrics" for request in otlp_receiver.requests)


def test_normal_exit_uses_the_existing_bounded_coordinator(
    otlp_receiver: OTLPReceiver,
) -> None:
    otlp_receiver.queue_response(hang=True)
    source = (
        _IMPORTS
        + """
assert setup(configuration(export_timeout=2)).ok
emit(get_process_runtime())
print('EXIT_READY', flush=True)
"""
    )
    process = subprocess.Popen(
        [sys.executable, "-c", source],
        env=_environment(otlp_receiver),
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
    )
    assert process.stdout is not None
    try:
        assert process.stdout.readline().strip() == "EXIT_READY"
        start = monotonic()
        _, stderr = process.communicate(timeout=3)
        elapsed = monotonic() - start
        assert process.returncode == 0, stderr
        assert elapsed < 2.7
        assert "cannot start new thread" not in stderr
        assert len(otlp_receiver.requests) >= 1
    finally:
        if process.poll() is None:
            process.kill()
            process.wait()


def test_hanging_export_retention_has_a_declared_rss_plateau(
    otlp_receiver: OTLPReceiver,
) -> None:
    # Declared before measurement: two queues of 512, one in-flight batch of
    # <=128 per signal, <=16KiB admitted estimates; one aggregate metric series.
    # Python objects exceed wire estimates. Budget: <256MiB total RSS and <16MiB
    # extra peak after 4096-record warmup, despite 3x then 6x additional records.
    for _ in range(100):
        otlp_receiver.queue_response(hang=True)
    result = _run(
        otlp_receiver,
        """
        import resource
        assert setup(configuration(export_timeout=1)).ok
        runtime = get_process_runtime()
        tracer = runtime.tracer_provider.get_tracer('django_altimeter')
        logger = runtime.logger_provider.get_logger('django_altimeter')
        counter = runtime.meter_provider.get_meter('django_altimeter').create_counter(
            'runtime.calls'
        )
        def rss():
            value = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
            return value if sys.platform == 'darwin' else value * 1024
        def load(count):
            for index in range(count):
                attributes = {f'private.{key}': str(index) + 'x' * 220
                              for key in range(30)}
                with tracer.start_as_current_span('GET', attributes=attributes):
                    pass
                logger.emit(LogRecord(body='runtime.ready', attributes=attributes))
                counter.add(1, {'unbounded.dimension': str(index)})
        load(4096)
        samples = [rss()]
        for count in (12288, 24576):
            load(count)
            samples.append(rss())
        stopped = shutdown(timeout=0.1)
        print(json.dumps({'rss': samples,
                          'queues': runtime.config.limits.max_queue_size,
                          'batch': runtime.config.limits.max_batch_size}))
    """,
        timeout=45,
    )
    samples = result["rss"]
    assert result["queues"] == 512
    assert result["batch"] == 128
    assert max(samples) < 256 * 1024 * 1024, samples
    assert max(samples[1:]) - samples[0] < 16 * 1024 * 1024, samples
    assert otlp_receiver.requests


def test_provider_detection_is_read_only(otlp_receiver: OTLPReceiver) -> None:
    source = """
import os
from opentelemetry import trace, metrics
from opentelemetry import _logs
from django_altimeter import _otel_compat
for key, getter, setter, noop, check in (
    ('OTEL_PYTHON_TRACER_PROVIDER', trace.get_tracer_provider,
     trace.set_tracer_provider, trace.NoOpTracerProvider,
     _otel_compat.trace_provider_is_unset),
    ('OTEL_PYTHON_METER_PROVIDER', metrics.get_meter_provider,
     metrics.set_meter_provider, metrics.NoOpMeterProvider,
     _otel_compat.metric_provider_is_unset),
    ('OTEL_PYTHON_LOGGER_PROVIDER', _logs.get_logger_provider,
     _logs.set_logger_provider, _logs.NoOpLoggerProvider,
     _otel_compat.log_provider_is_unset),
):
    original = getter()
    assert check()
    assert getter() is original
    os.environ[key] = 'CANARY_MISSING_PLUGIN'
    assert not check()  # Must not load an unowned provider plugin.
    del os.environ[key]
    assert getter() is original
    concrete = noop()
    setter(concrete)
    assert not check()
    assert getter() is concrete
"""
    result = subprocess.run(
        [sys.executable, "-c", source],
        env=_environment(otlp_receiver),
        capture_output=True,
        text=True,
        timeout=10,
        check=False,
    )
    assert result.returncode == 0, result.stderr
    assert not otlp_receiver.requests


@pytest.mark.parametrize("signal", ["traces", "logs", "metrics"])
def test_shutdown_retry_backoff_cannot_start_a_send_after_deadline(
    otlp_receiver: OTLPReceiver,
    signal: str,
) -> None:
    otlp_receiver.queue_response(status=503, headers={"Retry-After": "1"})
    result = _run(
        otlp_receiver,
        f"""
        assert setup(configuration(signals=({signal!r},), export_timeout=2)).ok
        emit(get_process_runtime())
        start = time.monotonic()
        result = shutdown(timeout=0.2)
        elapsed = time.monotonic() - start
        # Keep the process alive beyond native backoff: a late retry must not
        # open another request after the one shared shutdown deadline.
        time.sleep(1.1)
        print(json.dumps({{'elapsed': elapsed, 'complete': result.completed,
                           'codes': result.codes}}))
    """,
    )
    assert result["elapsed"] < 0.5
    assert result["complete"] is False
    assert len(otlp_receiver.requests) == 1
    assert otlp_receiver.requests[0].path == f"/v1/{signal}"


def test_failed_export_releases_batch_without_waiting_for_cyclic_gc(
    otlp_receiver: OTLPReceiver,
) -> None:
    otlp_receiver.queue_response()  # Warm the real connection/export path.
    for _ in range(6):
        otlp_receiver.queue_response(hang=True)
    result = _run(
        otlp_receiver,
        """
        import gc, tracemalloc, weakref
        from opentelemetry.sdk.resources import Resource
        from opentelemetry.sdk.trace import TracerProvider
        from opentelemetry.sdk.trace.export import SpanExportResult
        from django_altimeter.exporting import SanitizingSession, create_span_exporter
        provider = TracerProvider(resource=Resource({}), shutdown_on_exit=False)
        tracer = provider.get_tracer('django_altimeter')
        exporter = create_span_exporter(
            SanitizingSession('traces', os.environ['TEST_ENDPOINT'] + '/v1/traces'),
            # Leave time for projection and the receiver to observe each batch
            # under tracemalloc; this test measures retention, not latency.
            timeout=0.5,
        )
        with tracer.start_as_current_span('GET') as warm:
            pass
        assert exporter.export([warm]) is SpanExportResult.SUCCESS
        del warm
        gc.collect()
        gc.disable()
        tracemalloc.start()
        baseline = tracemalloc.get_traced_memory()[0]
        last = None
        def send_batch():
            global last
            batch = []
            for index in range(96):
                with tracer.start_as_current_span('GET', attributes={
                    f'private.{key}': f'CANARY_{index}_{key}_' + 'x' * 200
                    for key in range(30)
                }) as span:
                    batch.append(span)
            last = weakref.ref(batch[-1])
            assert exporter.export(batch) is SpanExportResult.FAILURE
        for _ in range(6):
            send_batch()
        retained = tracemalloc.get_traced_memory()[0] - baseline
        alive = last() is not None
        tracemalloc.stop()
        gc.enable()
        exporter.shutdown()
        provider.shutdown()
        print(json.dumps({'retained': retained, 'record_alive': alive}))
    """,
    )
    # Threshold declared before the first red: six failed ~700KiB native
    # batches must leave <256KiB bookkeeping, without a cyclic GC collection.
    assert result["retained"] < 256 * 1024, result
    assert result["record_alive"] is False
    requests = _signals(otlp_receiver)["traces"]
    assert len(requests) == 7
    assert len(requests[0].resource_spans[0].scope_spans[0].spans) == 1
    assert all(
        len(request.resource_spans[0].scope_spans[0].spans) == 96
        for request in requests[1:]
    )


def test_setup_does_not_load_or_contact_ambient_resource_detector(
    otlp_receiver: OTLPReceiver,
    tmp_path,
) -> None:
    # A real discoverable distribution/entry point, not a mocked loader. The
    # detector attempts an actual loopback connection if the SDK imports it.
    distribution = tmp_path / "reference_detector-1.0.dist-info"
    distribution.mkdir()
    (distribution / "METADATA").write_text(
        "Name: reference-detector\nVersion: 1.0\n", encoding="utf-8"
    )
    (distribution / "entry_points.txt").write_text(
        "[opentelemetry_resource_detector]\n"
        "reference_detector = reference_detector:Detector\n",
        encoding="utf-8",
    )
    (tmp_path / "reference_detector.py").write_text(
        """
import os
import socket
from urllib.parse import urlsplit
from opentelemetry.sdk.resources import Resource, ResourceDetector
class Detector(ResourceDetector):
    def detect(self):
        endpoint = urlsplit(os.environ['TEST_ENDPOINT'])
        with socket.create_connection((endpoint.hostname, endpoint.port), timeout=0.2):
            pass
        return Resource({})
""",
        encoding="utf-8",
    )
    result = _run(
        otlp_receiver,
        """
        network = []
        sys.addaudithook(lambda event, args: network.append(event)
                        if event in ('socket.connect', 'socket.getaddrinfo') else None)
        result = setup(configuration())
        shutdown()
        print(json.dumps({'network': network, 'enabled': result.enabled_signals,
                          'codes': result.codes,
                          'imported': 'reference_detector' in sys.modules,
                          'application': (lambda: 'working')()}))
    """,
        environment=_environment(
            otlp_receiver,
            PYTHONPATH=str(tmp_path),
            OTEL_EXPERIMENTAL_RESOURCE_DETECTORS="reference_detector",
        ),
    )
    assert result["network"] == []
    assert result["imported"] is False
    assert result["enabled"] == []
    assert result["codes"] == ["setup.resource_detector_conflict"]
    assert result["application"] == "working"
    assert not otlp_receiver.requests


@pytest.mark.parametrize("signal", ["traces", "logs", "metrics"])
@pytest.mark.parametrize("malformed", [False, True], ids=["success", "malformed"])
def test_export_preserves_application_exception_and_releases_record(
    otlp_receiver: OTLPReceiver, signal: str, malformed: bool
) -> None:
    otlp_receiver.queue_response(body=b"\xff" if malformed else b"")
    result = _run(
        otlp_receiver,
        """
        import gc, weakref
        from opentelemetry.sdk._logs import ReadableLogRecord
        from opentelemetry.sdk.metrics.export import (
            Gauge, Metric, MetricsData, NumberDataPoint, ResourceMetrics, ScopeMetrics,
        )
        from opentelemetry.sdk.resources import Resource
        from opentelemetry.sdk.trace import TracerProvider
        from opentelemetry.sdk.util.instrumentation import InstrumentationScope
        from django_altimeter.exporting import (
            SanitizingSession, create_span_exporter, create_log_exporter,
            create_metric_exporter,
        )
        signal = os.environ['TEST_SIGNAL']
        resource = Resource({})
        scope = InstrumentationScope('django_altimeter')
        provider = TracerProvider(resource=resource, shutdown_on_exit=False)
        session = SanitizingSession(
            signal, os.environ['TEST_ENDPOINT'] + '/v1/' + signal,
            policy=configuration().policy,
        )
        factory = {'traces': create_span_exporter, 'logs': create_log_exporter,
                   'metrics': create_metric_exporter}[signal]
        exporter = factory(session, timeout=0.5)

        def export_record():
            attributes = {'http.response.status_code': 200, 'secret': 'CANARY_PRIVATE'}
            if signal == 'traces':
                record = provider.get_tracer(scope.name).start_span(
                    'GET', attributes=attributes,
                )
                record.end()
                payload = (record,)
            elif signal == 'logs':
                record = ReadableLogRecord(
                    LogRecord(body='runtime.ready', severity_number=SeverityNumber.INFO,
                              attributes=attributes),
                    resource=resource, instrumentation_scope=scope,
                )
                payload = (record,)
            else:
                record = MetricsData(resource_metrics=(ResourceMetrics(
                    resource=resource, schema_url='', scope_metrics=(ScopeMetrics(
                        scope=scope, schema_url='', metrics=(Metric(
                            name='runtime.calls', description='', unit='1',
                            data=Gauge(data_points=(NumberDataPoint(
                                attributes=attributes, start_time_unix_nano=1,
                                time_unix_nano=2, value=7,
                            ),)),
                        ),),
                    ),),
                ),))
                payload = record
            reference = weakref.ref(record)
            return exporter.export(payload).name, reference

        def application():
            local_marker = object()
            try:
                raise LookupError('CANARY_application_context')
            except LookupError:
                raise RuntimeError('CANARY_application_error') from ValueError(
                    'CANARY_application_cause'
                )

        gc.collect()
        gc.disable()
        try:
            try:
                application()
            except RuntimeError as caught:
                original_traceback = caught.__traceback__
                frame = original_traceback.tb_next.tb_frame
                original_locals = dict(frame.f_locals)
                chain = (caught, caught.__cause__, caught.__context__)
                original = [(error.__traceback__, error.__cause__, error.__context__)
                            for error in chain]
                exported, reference = export_record()
                unchanged = all(
                    error.__traceback__ is saved[0]
                    and error.__cause__ is saved[1]
                    and error.__context__ is saved[2]
                    for error, saved in zip(chain, original)
                )
                locals_unchanged = frame.f_locals == original_locals
                record_alive = reference() is not None
        finally:
            gc.enable()
            exporter.shutdown()
            provider.shutdown()
        print(json.dumps({'exported': exported, 'unchanged': unchanged,
                          'locals_unchanged': locals_unchanged,
                          'record_alive': record_alive,
                          'outcome': session.outcome.code}))
        """,
        environment=_environment(otlp_receiver, TEST_SIGNAL=signal),
    )
    assert result["exported"] == ("FAILURE" if malformed else "SUCCESS")
    assert result["outcome"] == ("export.invalid" if malformed else "export.accepted")
    assert result["unchanged"] is True, result
    assert result["locals_unchanged"] is True, result
    assert result["record_alive"] is False, result
    (request,) = _signals(otlp_receiver)[signal]
    if signal == "traces":
        (span,) = request.resource_spans[0].scope_spans[0].spans
        assert span.name == "GET"
        assert span.attributes[0].value.int_value == 200
    elif signal == "logs":
        (record,) = request.resource_logs[0].scope_logs[0].log_records
        assert record.body.string_value == "runtime.ready"
        assert record.attributes[0].value.int_value == 200
    else:
        (metric,) = request.resource_metrics[0].scope_metrics[0].metrics
        assert metric.name == "runtime.calls"
        assert metric.gauge.data_points[0].as_int == 7


@pytest.mark.parametrize("malformed", [False, True], ids=["success", "malformed"])
def test_runtime_metric_flush_preserves_application_exception(
    otlp_receiver: OTLPReceiver, malformed: bool
) -> None:
    otlp_receiver.queue_response(body=b"\xff" if malformed else b"")
    result = _run(
        otlp_receiver,
        """
        assert setup(configuration(signals=('metrics',))).ok
        runtime = get_process_runtime()
        runtime.meter_provider.get_meter('django_altimeter').create_counter(
            'runtime.calls'
        ).add(7)
        def application():
            local_marker = object()
            raise RuntimeError('CANARY_application_error')
        try:
            application()
        except RuntimeError as caught:
            original_traceback = caught.__traceback__
            frame = original_traceback.tb_next.tb_frame
            original_locals = dict(frame.f_locals)
            runtime.meter_provider.force_flush()
            unchanged = caught.__traceback__ is original_traceback
            locals_unchanged = frame.f_locals == original_locals
        outcome = runtime._sessions[0].outcome.code
        shutdown()
        print(json.dumps({'unchanged': unchanged, 'locals_unchanged': locals_unchanged,
                          'outcome': outcome}))
        """,
    )
    assert result["outcome"] == ("export.invalid" if malformed else "export.accepted")
    assert result["unchanged"] is True, result
    assert result["locals_unchanged"] is True, result
    (request,) = _signals(otlp_receiver)["metrics"]
    (metric,) = request.resource_metrics[0].scope_metrics[0].metrics
    assert metric.name == "runtime.calls"
    assert metric.sum.data_points[0].as_int == 7


@pytest.mark.parametrize(
    ("first", "last", "codes"),
    [
        ("accepted", "accepted", []),
        ("accepted", "malformed", ["shutdown.export_failed"]),
        ("malformed", "accepted", ["shutdown.export_failed"]),
        ("accepted", "partial", ["shutdown.partial"]),
        ("partial", "accepted", ["shutdown.partial"]),
        ("malformed", "partial", ["shutdown.export_failed", "shutdown.partial"]),
        ("partial", "malformed", ["shutdown.partial", "shutdown.export_failed"]),
    ],
)
def test_shutdown_accumulates_final_metric_collection_failures(
    otlp_receiver: OTLPReceiver, first: str, last: str, codes: list[str]
) -> None:
    from opentelemetry.proto.collector.metrics.v1.metrics_service_pb2 import (
        ExportMetricsServiceResponse,
    )

    partial = ExportMetricsServiceResponse()
    partial.partial_success.rejected_data_points = 1
    partial.partial_success.error_message = "CANARY_server_prose"
    bodies = {
        "accepted": b"",
        "malformed": b"\xff",
        "partial": partial.SerializeToString(),
    }
    otlp_receiver.queue_response(body=bodies[first])
    otlp_receiver.queue_response(body=bodies[last])
    result = _run(
        otlp_receiver,
        """
        from opentelemetry.metrics import Observation
        assert setup(configuration(signals=('metrics',))).ok
        runtime = get_process_runtime()
        runtime.meter_provider.get_meter('django_altimeter').create_observable_gauge(
            'runtime.calls', callbacks=[lambda _: [Observation(7)]]
        )
        stopped = shutdown()
        print(json.dumps({'complete': stopped.completed, 'codes': stopped.codes,
                          'last': runtime._sessions[0].outcome.code}))
        """,
    )
    assert result["last"] == "export." + ("invalid" if last == "malformed" else last)
    assert result["codes"] == codes, result
    assert result["complete"] is (not codes), result
    requests = _signals(otlp_receiver)["metrics"]
    assert len(requests) == 2
    for request in requests:
        (metric,) = request.resource_metrics[0].scope_metrics[0].metrics
        assert metric.name == "runtime.calls"
        assert metric.gauge.data_points[0].as_int == 7


@pytest.mark.parametrize(
    "case",
    [
        "config_conflict",
        "traces_foreign",
        "semconv_conflict",
        "resource_detector_conflict",
    ],
)
def test_setup_diagnostic_handler_can_reenter_without_deadlocking(
    otlp_receiver: OTLPReceiver, case: str
) -> None:
    result = _run(
        otlp_receiver,
        """
        import faulthandler, logging
        config = configuration(signals=('traces',))
        case = os.environ['TEST_CASE']
        if case == 'config_conflict':
            assert setup(config).ok
        elif case == 'traces_foreign':
            trace.set_tracer_provider(trace.NoOpTracerProvider())
        elif case == 'semconv_conflict':
            os.environ['OTEL_SEMCONV_STABILITY_OPT_IN'] = 'http/dup'
        else:
            os.environ['OTEL_EXPERIMENTAL_RESOURCE_DETECTORS'] = 'CANARY_not_loaded'
        calls = []
        class ReenterOnce(logging.Handler):
            busy = False
            def emit(self, record):
                if self.busy:
                    return
                self.busy = True
                try:
                    nested = setup(config)
                    calls.append((record.getMessage(), nested.enabled_signals,
                                  nested.codes))
                finally:
                    self.busy = False
        logger = logging.getLogger('django_altimeter.runtime')
        handler = ReenterOnce()
        logger.addHandler(handler)
        # Both guards are real time bounds, so a deadlock fails the subprocess
        # rather than leaving the test runner or another lane blocked.
        faulthandler.dump_traceback_later(2, exit=True)
        try:
            if case == 'config_conflict':
                logger.warning('control-outside-lock')
            changed = replace(config, role='beat')
            called = setup(changed if case == 'config_conflict' else config)
            stopped = shutdown(timeout=0.2)
        finally:
            faulthandler.cancel_dump_traceback_later()
            logger.removeHandler(handler)
        print(json.dumps({'codes': called.codes, 'calls': calls,
                          'stopped': stopped.completed}))
        """,
        environment=_environment(otlp_receiver, TEST_CASE=case),
        timeout=5,
    )
    expected = "setup." + case
    assert result["codes"] == [expected]
    assert any(call[0] == expected for call in result["calls"])
    if case == "config_conflict":
        assert result["calls"][0][0] == "control-outside-lock"
        assert all(call[1] == ["traces"] and call[2] == [] for call in result["calls"])
    else:
        assert all(call[1] == [] and call[2] == [expected] for call in result["calls"])
    assert result["stopped"] is True
    assert not otlp_receiver.requests


def test_native_setup_diagnostic_runs_outside_setup_lock(
    otlp_receiver: OTLPReceiver,
) -> None:
    result = _run(
        otlp_receiver,
        """
        import faulthandler, logging
        config = configuration(signals=('logs',))
        os.environ['OTEL_RESOURCE_ATTRIBUTES'] = 'malformed-pair'
        nested = []
        class Reenter(logging.Handler):
            def emit(self, record):
                result = setup(config)
                nested.append(result.codes)
        logger = logging.getLogger('opentelemetry.sdk.resources')
        handler = Reenter()
        logger.addHandler(handler)
        faulthandler.dump_traceback_later(2, exit=True)
        try:
            result = setup(config)
            stopped = shutdown(timeout=0.2)
        finally:
            faulthandler.cancel_dump_traceback_later()
            logger.removeHandler(handler)
        print(json.dumps({'enabled': result.enabled_signals, 'codes': result.codes,
                          'nested': nested, 'stopped': stopped.completed}))
        """,
        timeout=5,
    )
    assert result["enabled"] == ["logs"]
    assert result["codes"] == []
    assert result["nested"] == [["setup.in_progress"]]
    assert result["stopped"] is True
    assert not otlp_receiver.requests


def test_configuration_callback_equality_runs_outside_setup_lock(
    otlp_receiver: OTLPReceiver,
) -> None:
    result = _run(
        otlp_receiver,
        """
        import faulthandler
        calls = []
        class Resolver:
            def __call__(self, subject):
                raise AssertionError('setup must not invoke the resolver')
            def __eq__(self, other):
                calls.append(setup(config).enabled_signals)
                return False
        config = configuration(signals=('traces',), identity_resolver=Resolver())
        assert setup(config).ok
        faulthandler.dump_traceback_later(2, exit=True)
        try:
            result = setup(replace(config, identity_resolver=Resolver()))
            stopped = shutdown(timeout=0.2)
        finally:
            faulthandler.cancel_dump_traceback_later()
        print(json.dumps({'codes': result.codes, 'calls': calls,
                          'stopped': stopped.completed}))
        """,
        timeout=5,
    )
    assert result["codes"] == ["setup.config_conflict"]
    assert result["calls"] == [["traces"]]
    assert result["stopped"] is True
    assert not otlp_receiver.requests


def test_shutdown_keeps_failure_before_a_later_batch_succeeds(
    otlp_receiver: OTLPReceiver,
) -> None:
    otlp_receiver.queue_response(body=b"\xff", body_delay=0.5)
    otlp_receiver.queue_response()
    source = _IMPORTS + textwrap.dedent(
        """
        import faulthandler, logging
        faulthandler.dump_traceback_later(6, exit=True)
        assert setup(configuration(signals=('traces',), export_timeout=2,
                                   batch_schedule_delay_millis=60000)).ok
        runtime = get_process_runtime()
        during_shutdown = []
        class ObserveFailure(logging.Handler):
            def emit(self, record):
                if record.getMessage() == 'export.invalid traces':
                    during_shutdown.append(runtime._shutdown_deadline is not None)
        logging.getLogger('django_altimeter.export').addHandler(ObserveFailure())
        tracer = runtime.tracer_provider.get_tracer('django_altimeter')
        for _ in range(129):
            with tracer.start_as_current_span(
                'GET', attributes={'http.response.status_code': 200}
            ):
                pass
        print('READY', flush=True)
        assert sys.stdin.readline().strip() == 'SHUTDOWN'
        stopped = shutdown()
        print(json.dumps({'complete': stopped.completed, 'codes': stopped.codes,
                          'during_shutdown': during_shutdown,
                          'last': runtime._sessions[0].outcome.code}))
        faulthandler.cancel_dump_traceback_later()
        """
    )
    process = subprocess.Popen(
        [sys.executable, "-c", source],
        env=_environment(otlp_receiver),
        stdin=subprocess.PIPE,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
    )
    assert process.stdout is not None
    try:
        assert process.stdout.readline().strip() == "READY"
        otlp_receiver.wait_for_requests(1, timeout=2)
        stdout, stderr = process.communicate("SHUTDOWN\n", timeout=5)
        assert process.returncode == 0, stderr
        result = json.loads(stdout)
    finally:
        if process.poll() is None:
            process.kill()
            process.wait()
    assert result["during_shutdown"] == [True]
    assert result["last"] == "export.accepted"
    assert result["complete"] is False, result
    assert result["codes"] == ["shutdown.export_failed"], result
    requests = _signals(otlp_receiver)["traces"]
    assert len(requests) == 2
    spans = [
        span
        for request in requests
        for resource in request.resource_spans
        for scope in resource.scope_spans
        for span in scope.spans
    ]
    assert len(spans) == 129
    assert all(span.name == "GET" for span in spans)
    assert all(span.attributes[0].value.int_value == 200 for span in spans)


@pytest.mark.parametrize(
    "case",
    ["accepted", "retry_recovered", "partial_first", "partial_middle", "partial_last"],
)
def test_shutdown_preserves_partial_across_native_metric_batches(
    otlp_receiver: OTLPReceiver, case: str
) -> None:
    from opentelemetry.proto.collector.metrics.v1.metrics_service_pb2 import (
        ExportMetricsServiceResponse,
    )

    partial = ExportMetricsServiceResponse()
    partial.partial_success.rejected_data_points = 1
    partial.partial_success.error_message = "CANARY_response_prose"
    partial_index = {
        "partial_first": 0,
        "partial_middle": 1,
        "partial_last": 2,
    }.get(case)
    if case == "retry_recovered":
        otlp_receiver.queue_response(status=503, headers={"Retry-After": "1"})
    for index in range(3):
        otlp_receiver.queue_response(
            body=partial.SerializeToString() if index == partial_index else b""
        )
    source = """
        routes = frozenset(f'items/{index}/<item_id>' for index in range(65))
        config = configuration(
            signals=('metrics',), metric_export_interval_millis=60000,
            export_timeout=1.5,
            policy=replace(DEFAULT_POLICY, attribute_values=frozenset(
                ('http.route', route) for route in routes)),
        )
        assert setup(config).ok
        runtime = get_process_runtime()
        assert runtime is not None and runtime.request_metrics is not None
        for route in sorted(routes):
            for method in ('GET', 'POST'):
                runtime.request_metrics.record(
                    0.25, surface='django', method=method, route=route, status=200,
                    outcome='completed', response_mode='buffered',
                )
        stopped = shutdown(timeout=2)
        again = shutdown()
        print(json.dumps({'complete': stopped.completed, 'codes': stopped.codes,
                          'again': again == stopped,
                          'latest': runtime._sessions[0].outcome.code}))
    """
    process = subprocess.run(
        [sys.executable, "-c", _IMPORTS + textwrap.dedent(source)],
        env=_environment(otlp_receiver),
        capture_output=True,
        text=True,
        timeout=10,
        check=False,
    )
    assert process.returncode == 0, process.stderr
    assert "CANARY" not in process.stdout + process.stderr
    result = json.loads(process.stdout)
    requests = _signals(otlp_receiver)["metrics"]
    sizes = [
        sum(
            len(getattr(metric, metric.WhichOneof("data")).data_points)
            for resource in request.resource_metrics
            for scope in resource.scope_metrics
            for metric in scope.metrics
        )
        for request in requests
    ]
    assert sizes == ([128, 128, 128, 4] if case == "retry_recovered" else [128, 128, 4])
    if case == "retry_recovered":
        assert requests[0].SerializeToString() == requests[1].SerializeToString()
        requests = requests[1:]
    totals = {"altimeter.http.server.requests": 0, "http.server.request.duration": 0}
    for request in requests:
        for resource in request.resource_metrics:
            for scope in resource.scope_metrics:
                for metric in scope.metrics:
                    kind = metric.WhichOneof("data")
                    for point in getattr(metric, kind).data_points:
                        totals[metric.name] += (
                            point.as_int if kind == "sum" else point.count
                        )
                        attributes = {
                            item.key: getattr(
                                item.value, item.value.WhichOneof("value")
                            )
                            for item in point.attributes
                        }
                        assert attributes["http.route"].startswith("items/")
                        assert attributes["http.request.method"] in {"GET", "POST"}
    assert totals == {
        "altimeter.http.server.requests": 130,
        "http.server.request.duration": 130,
    }
    assert result["latest"] == (
        "export.partial" if case == "partial_last" else "export.accepted"
    )
    assert result["again"] is True
    assert result["codes"] == (
        ["shutdown.partial"] if partial_index is not None else []
    )
    assert result["complete"] is (partial_index is None)
