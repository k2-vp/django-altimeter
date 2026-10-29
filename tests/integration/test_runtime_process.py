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
            timeout=0.05,
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
