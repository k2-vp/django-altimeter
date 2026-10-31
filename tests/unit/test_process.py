"""Process opt-in and source admission; real-process proof lives in integration."""

import json
import os
import subprocess
import sys
from dataclasses import replace

import pytest
from opentelemetry.proto.collector.metrics.v1.metrics_service_pb2 import (
    ExportMetricsServiceRequest,
)
from support.otlp_receiver import OTLPReceiver

from django_altimeter.privacy import DEFAULT_POLICY, project_metrics


@pytest.fixture
def otlp_receiver():
    with OTLPReceiver() as receiver:
        yield receiver


def _run(source, receiver, **environment):
    env = {
        key: value
        for key, value in os.environ.items()
        if not key.startswith(("OTEL_", "ALTIMETER_"))
    }
    result = subprocess.run(
        [sys.executable, "-c", source],
        env={**env, "TEST_ENDPOINT": receiver.endpoint, **environment},
        capture_output=True,
        text=True,
        timeout=15,
    )
    print(result.stderr, end="")
    assert result.returncode == 0, result.stdout + result.stderr
    return json.loads(result.stdout)


@pytest.mark.parametrize("integration", ["", "process"])
def test_opt_in_keeps_existing_metrics_and_application_working(
    otlp_receiver, integration
):
    output = _run(
        """
import json, os
from django_altimeter.conf import load_config
from django_altimeter.runtime import setup, shutdown, get_process_runtime
config = load_config(environ={
    'ALTIMETER_ENABLED': 'true',
    'ALTIMETER_SERVICE_NAME': 'safe-process',
    'ALTIMETER_DEPLOYMENT_ENVIRONMENT': 'test',
    'ALTIMETER_RELEASE': 'v1',
    'ALTIMETER_INTEGRATIONS': os.environ['TEST_INTEGRATION'],
    'ALTIMETER_TRACES_ENABLED': 'false',
    'ALTIMETER_LOGS_ENABLED': 'false',
    'ALTIMETER_METRICS_COLLECTOR_ENDPOINT': os.environ['TEST_ENDPOINT'] + '/v1/metrics',
}, values={'METRIC_INSTRUMENTS': [('django_altimeter', 'safe.calls')]})
if config.config is not None:
    setup(config.config)
runtime = get_process_runtime()
if runtime is not None:
    meter = runtime.meter_provider.get_meter('django_altimeter')
    meter.create_counter('safe.calls').add(1, {'private': 'process-canary'})
    runtime.meter_provider.force_flush()
shutdown()
print(json.dumps({'valid': config.ok, 'application': 6 * 7}))
""",
        otlp_receiver,
        TEST_INTEGRATION=integration,
    )
    assert output == {"valid": True, "application": 42}
    observed = {
        metric.name
        for request in otlp_receiver.requests
        for resource in request.message.resource_metrics
        for scope in resource.scope_metrics
        for metric in scope.metrics
    }
    assert "safe.calls" in observed
    assert ("process.memory.usage" in observed) == (integration == "process")
    assert "process-canary" not in str(otlp_receiver.requests)


def test_process_policy_preserves_closed_dimensions_and_units():
    policy = replace(
        DEFAULT_POLICY,
        metric_instruments=frozenset(
            {
                ("django_altimeter", "process.cpu.time"),
                ("django_altimeter", "process.thread.count"),
            }
        ),
        attribute_values=frozenset({("cpu.mode", "user")}),
    )
    payload = ExportMetricsServiceRequest()
    scope = payload.resource_metrics.add().scope_metrics.add()
    scope.scope.name = "django_altimeter"
    cpu = scope.metrics.add(name="process.cpu.time", unit="s")
    point = cpu.sum.data_points.add(as_double=0.25)
    cpu.sum.aggregation_temporality = 1
    cpu.sum.is_monotonic = True
    point.attributes.add(key="cpu.mode").value.string_value = "user"
    point.attributes.add(key="tenant").value.string_value = "process-canary"
    threads = scope.metrics.add(name="process.thread.count", unit="{thread}")
    threads.sum.data_points.add(as_int=3)
    threads.sum.aggregation_temporality = 2
    projected = project_metrics(payload, policy=policy)
    metrics = projected.resource_metrics[0].scope_metrics[0].metrics
    assert metrics[0].sum.data_points[0].attributes[0].key == "cpu.mode"
    assert metrics[0].sum.data_points[0].attributes[0].value.string_value == "user"
    assert metrics[1].unit == "{thread}"
    assert "process-canary" not in str(projected)


@pytest.fixture
def recorder():
    from opentelemetry.metrics import NoOpMeterProvider

    from django_altimeter.process import ProcessMetrics, process_metric_policy

    return ProcessMetrics(
        NoOpMeterProvider(), policy=process_metric_policy(DEFAULT_POLICY)
    )


@pytest.mark.parametrize(
    "value", [None, True, -1, float("nan"), float("inf"), 2**10000]
)
def test_invalid_source_values_are_omitted_beside_real_safe_value(
    recorder, monkeypatch, value
):
    from opentelemetry.metrics import CallbackOptions

    options = CallbackOptions()
    positive = recorder._threads(options)
    assert len(positive) == 1 and positive[0].value > 0
    with monkeypatch.context() as patch:
        patch.setattr(recorder._process, "num_threads", lambda: value)
        assert recorder._threads(options) == ()
    recovered = recorder._threads(options)
    assert len(recovered) == 1 and recovered[0].value > 0


def test_source_and_logging_failures_do_not_escape(recorder, monkeypatch, caplog):
    import logging

    from opentelemetry.metrics import CallbackOptions

    def denied():
        raise PermissionError("source-secret-canary")

    options = CallbackOptions()
    monkeypatch.setattr(recorder._process, "num_fds", denied)
    assert recorder._fds(options) == ()
    assert recorder._fds(options) == ()
    assert [record.getMessage() for record in caplog.records] == [
        "process.fd_unavailable"
    ]
    assert "canary" not in caplog.text
    assert recorder._memory(options)[0].value > 0
    monkeypatch.setattr(recorder._process, "num_threads", denied)
    monkeypatch.setattr(
        logging.getLogger("django_altimeter.process"), "warning", denied
    )
    assert recorder._threads(options) == ()
    assert recorder._memory(options)[0].value > 0


def test_counter_baseline_and_regression_never_create_a_negative_delta(
    recorder, monkeypatch
):
    from types import SimpleNamespace

    from opentelemetry.metrics import CallbackOptions

    readings = iter([(20.0, 10.0), (20.5, 10.25), (19.0, 9.0), (21.0, 10.5)])

    def cpu_times():
        user, system = next(readings)
        return SimpleNamespace(user=user, system=system)

    monkeypatch.setattr(recorder._process, "cpu_times", cpu_times)
    options = CallbackOptions()
    assert [point.value for point in recorder._cpu(options)] == [0, 0]
    assert [point.value for point in recorder._cpu(options)] == [0.5, 0.25]
    assert recorder._cpu(options) == ()
    assert [point.value for point in recorder._cpu(options)] == [1, 0.5]
    assert set(recorder._baselines) == {"process.cpu.time"}


@pytest.mark.parametrize("stats", [None, [], [{}, {}, {}], [{"collections": 1}] * 4])
def test_unknown_gc_containers_are_omitted_without_losing_other_sources(
    recorder, monkeypatch, stats
):
    import gc

    from opentelemetry.metrics import CallbackOptions

    options = CallbackOptions()
    assert len(recorder._collections(options)) == 3
    monkeypatch.setattr(gc, "get_stats", lambda: stats)
    assert recorder._collections(options) == ()
    assert recorder._collected(options) == ()
    assert recorder._uncollectable(options) == ()
    assert recorder._memory(options)[0].value > 0


def test_unknown_container_is_not_traversed(recorder, monkeypatch):
    import gc

    from opentelemetry.metrics import CallbackOptions

    class HostileList(list):
        def __len__(self):
            raise AssertionError("must not read unknown container length")

        def __iter__(self):
            raise AssertionError("must not traverse unknown container")

    monkeypatch.setattr(gc, "get_stats", lambda: HostileList())
    assert recorder._collections(CallbackOptions()) == ()


@pytest.mark.parametrize(
    ("key", "bad"),
    [("cpu.mode", "process-canary"), ("altimeter.gc.generation", "3")],
)
def test_process_dimensions_cannot_be_expanded_by_configuration(key, bad):
    from django_altimeter.privacy import is_allowed_attribute
    from django_altimeter.process import process_metric_policy

    policy = process_metric_policy(DEFAULT_POLICY)
    policy = replace(policy, attribute_values=policy.attribute_values | {(key, bad)})
    assert not is_allowed_attribute(key, bad, policy=policy)
    good = "user" if key == "cpu.mode" else "0"
    assert is_allowed_attribute(key, good, policy=policy)


@pytest.mark.parametrize("mode", ["disabled", "metrics_off", "foreign"])
def test_inactive_process_integration_never_imports_psutil(otlp_receiver, mode):
    output = _run(
        """
import json, os, sys
from opentelemetry import metrics
from django_altimeter.conf import Config, SignalConfig
from django_altimeter.runtime import setup, shutdown
mode = os.environ['TEST_MODE']
if mode == 'foreign':
    metrics.set_meter_provider(metrics.NoOpMeterProvider())
config = Config(
    enabled=mode != 'disabled', service_name='safe',
    deployment_environment='test', release='v1',
    integrations=frozenset({'process'}),
    metrics=SignalConfig(
        enabled=mode != 'metrics_off',
        endpoint=os.environ['TEST_ENDPOINT'] + '/v1/metrics', destination='collector',
    ),
)
setup(config)
shutdown()
print(json.dumps({'loaded': 'psutil' in sys.modules, 'application': 42}))
""",
        otlp_receiver,
        TEST_MODE=mode,
    )
    assert output == {"loaded": False, "application": 42}
    assert otlp_receiver.requests == ()


def test_exact_process_views_reject_other_scope_type_and_unit(otlp_receiver):
    output = _run(
        """
import json, os
from opentelemetry.metrics import Observation
from django_altimeter.conf import Config, SignalConfig
from django_altimeter.runtime import setup, shutdown, get_process_runtime
config = Config(
    enabled=True, service_name='safe', deployment_environment='test', release='v1',
    integrations=frozenset({'process'}),
    metrics=SignalConfig(
        enabled=True, endpoint=os.environ['TEST_ENDPOINT'] + '/v1/metrics',
        destination='collector',
    ),
)
assert setup(config).ok
runtime = get_process_runtime()
meter = runtime.meter_provider.get_meter('django_altimeter')
bad = lambda options: [Observation(123456789, {'cpu.mode': 'process-canary'})]
meter.create_observable_counter('process.cpu.time', callbacks=[bad], unit='ms')
meter.create_counter('process.memory.usage', unit='By').add(123456789)
runtime.meter_provider.get_meter('process-canary').create_observable_counter(
    'process.cpu.time', callbacks=[bad], unit='s')
assert runtime.meter_provider.force_flush()
shutdown()
print(json.dumps({'application': 42}))
""",
        otlp_receiver,
    )
    assert output == {"application": 42}
    observed = [
        metric
        for request in otlp_receiver.requests
        for resource in request.message.resource_metrics
        for scope in resource.scope_metrics
        for metric in scope.metrics
    ]
    assert {metric.name for metric in observed} >= {
        "process.cpu.time",
        "process.memory.usage",
        "process.thread.count",
    }
    assert all(
        metric.unit == "s" for metric in observed if metric.name == "process.cpu.time"
    )
    assert "123456789" not in str(otlp_receiver.requests)
    assert "process-canary" not in str(otlp_receiver.requests)
