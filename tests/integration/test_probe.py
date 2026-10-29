"""Real isolated probe processes and decoded OTLP, never live ingestion proof."""

import json
import os
import subprocess
import sys
import textwrap

import pytest
from support.otlp_receiver import OTLPReceiver

_IMPORTS = """
import json, os, time
from dataclasses import asdict, replace
from django_altimeter.conf import Config, SignalConfig
from django_altimeter.runtime import probe

def configuration(signals=('traces', 'logs', 'metrics'), **changes):
    def signal(name):
        return SignalConfig(
            name in signals, os.environ['TEST_ENDPOINT'] + '/v1/' + name,
            destination='collector')
    return replace(Config(
        enabled=True, service_name='probe-test', deployment_environment='test',
        release='v1', traces=signal('traces'), logs=signal('logs'),
        metrics=signal('metrics'),
    ), **changes)

def snapshot(result):
    return {'ok': result.ok, 'instance_id': result.instance_id, 'codes': result.codes,
            'started_at': result.started_at.isoformat(),
            'finished_at': result.finished_at.isoformat(),
            'signals': [{**asdict(row), 'accepted': row.accepted}
                        for row in result.signals]}
"""


def _run(receiver: OTLPReceiver, source: str, *, timeout: float = 10) -> dict:
    environment = {
        key: value
        for key, value in os.environ.items()
        if not key.startswith(("ALTIMETER_", "OTEL_"))
    }
    process = subprocess.run(
        [sys.executable, "-c", _IMPORTS + textwrap.dedent(source)],
        env={**environment, "TEST_ENDPOINT": receiver.endpoint},
        capture_output=True,
        text=True,
        timeout=timeout,
        check=False,
    )
    assert process.returncode == 0, process.stderr
    return json.loads(process.stdout)


def test_probe_sends_one_tagged_safe_record_for_each_signal(
    otlp_receiver: OTLPReceiver,
) -> None:
    result = _run(
        otlp_receiver,
        """
        import json, os
        from django_altimeter.conf import Config, SignalConfig
        from django_altimeter.privacy import PrivacyPolicy
        from django_altimeter.runtime import probe
        def signal(name):
            return SignalConfig(True, os.environ['TEST_ENDPOINT'] + '/v1/' + name,
                                destination='collector')
        config = Config(
            enabled=True, service_name='probe-test', deployment_environment='test',
            release='v1', traces=signal('traces'), logs=signal('logs'),
            metrics=signal('metrics'), policy=PrivacyPolicy(
                span_names=frozenset({'altimeter.probe'}),
                log_bodies=frozenset({'altimeter.probe'}),
                metric_instruments=frozenset({('django_altimeter', 'altimeter.probe')}),
            ),
        )
        result = probe(config)
        print(json.dumps({'ok': result.ok, 'instance_id': result.instance_id,
                          'codes': result.codes}))
        """,
    )
    assert result["ok"] is True, result
    captures = otlp_receiver.wait_for_requests(3, timeout=2)
    assert len(captures) == 3
    assert {capture.path for capture in captures} == {
        "/v1/traces",
        "/v1/logs",
        "/v1/metrics",
    }
    for capture in captures:
        if capture.path == "/v1/traces":
            (resource,) = capture.message.resource_spans
            (scope,) = resource.scope_spans
            (record,) = scope.spans
            assert record.name == "altimeter.probe"
            assert record.trace_id and record.span_id
        elif capture.path == "/v1/logs":
            (resource,) = capture.message.resource_logs
            (scope,) = resource.scope_logs
            (record,) = scope.log_records
            assert record.body.string_value == "altimeter.probe"
            assert record.severity_number == 9
        else:
            (resource,) = capture.message.resource_metrics
            (scope,) = resource.scope_metrics
            (metric,) = scope.metrics
            (record,) = metric.sum.data_points
            assert metric.name == "altimeter.probe"
            assert record.as_int == 1
            assert metric.sum.aggregation_temporality == 1
        assert scope.scope.name == "django_altimeter"
        assert all(item.key != "altimeter.probe" for item in record.attributes)
        flags = {item.key: item.value for item in resource.resource.attributes}
        assert flags["altimeter.probe"].bool_value is True
        resources = {
            item.key: item.value.string_value for item in resource.resource.attributes
        }
        assert resources["service.instance.id"] == result["instance_id"]
        assert resources["service.name"] == "probe-test"
        assert resources["deployment.environment.name"] == "test"
        assert resources["service.version"] == "v1"
        assert resources["altimeter.role"] == "probe"


def test_runtime_validation_is_read_only(otlp_receiver: OTLPReceiver) -> None:
    result = _run(
        otlp_receiver,
        """
        import json, os, sys, threading
        from django_altimeter.conf import Config, SignalConfig
        from django_altimeter.runtime import validate_runtime
        config = configuration(signals=('traces',))
        before = dict(os.environ)
        threads = tuple(threading.enumerate())
        sockets = []
        sys.addaudithook(lambda event, args: sockets.append(event)
                        if event.startswith('socket.') else None)
        compatible = not validate_runtime(config)
        print(json.dumps({'compatible': compatible,
                          'environment_same': dict(os.environ) == before,
                          'threads_same': tuple(threading.enumerate()) == threads,
                          'sockets': sockets}))
        """,
    )
    assert result["compatible"] is True
    assert result["environment_same"] is True
    assert result["threads_same"] is True
    assert result["sockets"] == []
    assert not otlp_receiver.requests


def _partial_response(signal: str, rejected: int) -> bytes:
    from opentelemetry.proto.collector.logs.v1.logs_service_pb2 import (
        ExportLogsServiceResponse,
    )
    from opentelemetry.proto.collector.metrics.v1.metrics_service_pb2 import (
        ExportMetricsServiceResponse,
    )
    from opentelemetry.proto.collector.trace.v1.trace_service_pb2 import (
        ExportTraceServiceResponse,
    )

    if signal == "traces":
        response = ExportTraceServiceResponse()
        response.partial_success.rejected_spans = rejected
    elif signal == "logs":
        response = ExportLogsServiceResponse()
        response.partial_success.rejected_log_records = rejected
    else:
        response = ExportMetricsServiceResponse()
        response.partial_success.rejected_data_points = rejected
    response.partial_success.error_message = "CANARY_VENDOR_SECRET"
    return response.SerializeToString()


@pytest.mark.parametrize("signal", ["traces", "logs", "metrics"])
@pytest.mark.parametrize(
    ("case", "code", "accepted"),
    [
        ("accepted", "export.accepted", True),
        ("warning", "export.warning", True),
        ("partial", "export.partial", False),
        ("malformed", "export.invalid", False),
        ("unauthorized", "export.http_error", False),
        ("redirect", "export.status", False),
        ("wrong_type", "export.content_type", False),
    ],
)
def test_probe_reports_each_real_transport_outcome(
    otlp_receiver: OTLPReceiver, signal: str, case: str, code: str, accepted: bool
) -> None:
    if case in {"warning", "partial"}:
        otlp_receiver.queue_response(
            body=_partial_response(signal, int(case == "partial"))
        )
    elif case == "malformed":
        otlp_receiver.queue_response(body=b"\xffCANARY_VENDOR_SECRET")
    elif case == "unauthorized":
        otlp_receiver.queue_response(
            status=401, reason="CANARY_VENDOR_SECRET", body=b"CANARY_VENDOR_SECRET"
        )
    elif case == "redirect":
        otlp_receiver.queue_response(
            status=302,
            headers={"Location": otlp_receiver.url("logs")},
            body=b"CANARY_VENDOR_SECRET",
        )
    elif case == "wrong_type":
        otlp_receiver.queue_response(
            headers={"Content-Type": "text/plain"}, body=b"CANARY_VENDOR_SECRET"
        )
    result = _run(
        otlp_receiver,
        f"""
        result = probe(configuration(), signals=({signal!r},))
        print(json.dumps(snapshot(result)))
        """,
    )
    assert result["ok"] is accepted, result
    (row,) = result["signals"]
    assert row["signal"] == signal
    assert row["code"] == code
    assert row["accepted"] is accepted
    assert row["accepted_records"] == int(accepted)
    assert row["rejected_records"] == int(case == "partial")
    assert row["local_dropped_records"] == 0
    assert "CANARY" not in json.dumps(result)
    (request,) = otlp_receiver.wait_for_requests(1, timeout=2)
    assert request.path == f"/v1/{signal}"
    assert b"altimeter.probe" in request.message.SerializeToString()


@pytest.mark.parametrize("signal", ["traces", "logs", "metrics"])
def test_probe_preserves_native_retry_then_acceptance(
    otlp_receiver: OTLPReceiver, signal: str
) -> None:
    otlp_receiver.queue_response(status=503, headers={"Retry-After": "0"})
    otlp_receiver.queue_response()
    result = _run(
        otlp_receiver,
        f"""
        print(json.dumps(snapshot(probe(configuration(), signals=({signal!r},)))))
        """,
    )
    assert result["ok"] is True, result
    assert result["signals"][0]["code"] == "export.accepted"
    captures = otlp_receiver.wait_for_requests(2, timeout=2)
    assert len(captures) == 2
    assert all(capture.path == f"/v1/{signal}" for capture in captures)
    assert captures[0].message == captures[1].message


@pytest.mark.parametrize("enabled", [False, True])
def test_nothing_enabled_never_starts_a_probe(
    otlp_receiver: OTLPReceiver, enabled: bool
) -> None:
    result = _run(
        otlp_receiver,
        f"""
        import sys, threading
        started = []
        sys.addaudithook(lambda event, args: started.append(event)
                        if event == 'subprocess.Popen' or event.startswith('socket.')
                        else None)
        threads = tuple(threading.enumerate())
        result = probe(configuration(signals=(), enabled={enabled!r}))
        print(json.dumps({{'result': snapshot(result), 'started': started,
                           'threads_same': threads == tuple(threading.enumerate())}}))
        """,
    )
    assert result["result"]["ok"] is False
    assert result["result"]["codes"] == ["nothing_to_probe"]
    assert result["result"]["instance_id"] is None
    assert result["result"]["signals"] == []
    assert result["started"] == []
    assert result["threads_same"] is True
    assert not otlp_receiver.requests


def test_selected_disabled_signal_cannot_pass(otlp_receiver: OTLPReceiver) -> None:
    result = _run(
        otlp_receiver,
        """
        result = probe(configuration(signals=('traces',)), signals=('logs',))
        print(json.dumps(snapshot(result)))
        """,
    )
    assert result["ok"] is False
    assert result["codes"] == ["probe.signal_disabled"]
    assert result["signals"] == [
        {
            "signal": "logs",
            "code": "export.not_sent",
            "accepted_records": 0,
            "rejected_records": 0,
            "local_dropped_records": 0,
            "accepted": False,
        }
    ]
    assert not otlp_receiver.requests


def test_probe_cannot_accept_a_locally_empty_payload(
    otlp_receiver: OTLPReceiver,
) -> None:
    result = _run(
        otlp_receiver,
        """
        config = configuration(signals=('metrics',))
        limits = replace(config.limits, max_resource_attributes=1)
        config = replace(config, limits=limits)
        print(json.dumps(snapshot(probe(config))))
        """,
    )
    assert result["ok"] is False
    assert result["signals"][0]["code"] == "export.empty"
    assert result["signals"][0]["accepted"] is False
    assert result["signals"][0]["accepted_records"] == 0
    assert not otlp_receiver.requests


def test_probe_is_independent_of_application_sampling(
    otlp_receiver: OTLPReceiver,
) -> None:
    result = _run(
        otlp_receiver,
        """
        config = configuration(signals=('traces',), sample_rate=0)
        print(json.dumps(snapshot(probe(config))))
        """,
    )
    assert result["ok"] is True, result
    (capture,) = otlp_receiver.wait_for_requests(1, timeout=2)
    assert (
        capture.message.resource_spans[0].scope_spans[0].spans[0].name
        == "altimeter.probe"
    )


@pytest.mark.parametrize("application_started", [False, True])
def test_probe_preserves_application_runtime_and_native_providers(
    otlp_receiver: OTLPReceiver, application_started: bool
) -> None:
    result = _run(
        otlp_receiver,
        f"""
        import threading
        from opentelemetry import trace, metrics
        from opentelemetry._logs import get_logger_provider
        from django_altimeter.runtime import setup, shutdown, get_process_runtime
        def providers():
            return (trace.get_tracer_provider(), get_logger_provider(),
                    metrics.get_meter_provider())
        config = configuration(signals=('traces',))
        if {application_started!r}:
            assert setup(config).ok
        original_runtime = get_process_runtime()
        original_providers = providers()
        original_threads = tuple(threading.enumerate())
        original_environment = dict(os.environ)
        result = probe(config)
        unchanged = (original_runtime is get_process_runtime()
                     and all(a is b for a, b in zip(original_providers, providers()))
                     and original_threads == tuple(threading.enumerate())
                     and original_environment == dict(os.environ))
        assert setup(config).ok
        application = get_process_runtime()
        tracer = application.tracer_provider.get_tracer('django_altimeter')
        span = tracer.start_span('GET')
        span.end()
        stopped = shutdown()
        print(json.dumps({{'probe': snapshot(result), 'unchanged': unchanged,
                           'application_instance': application.instance_id,
                           'shutdown': asdict(stopped)}}))
        """,
    )
    assert result["probe"]["ok"] is True, result
    assert result["unchanged"] is True
    assert result["shutdown"] == {"completed": True, "codes": []}
    assert result["application_instance"] != result["probe"]["instance_id"]
    probe_request, app_request = otlp_receiver.wait_for_requests(2, timeout=2)
    probe_resource = probe_request.message.resource_spans[0]
    app_resource = app_request.message.resource_spans[0]
    assert probe_resource.scope_spans[0].spans[0].name == "altimeter.probe"
    assert app_resource.scope_spans[0].spans[0].name == "GET"
    assert all(
        item.key != "altimeter.probe" for item in app_resource.resource.attributes
    )
    assert {
        item.key: item.value.string_value for item in app_resource.resource.attributes
    }["altimeter.role"] == "web"


def test_probe_leaves_foreign_providers_untouched(otlp_receiver: OTLPReceiver) -> None:
    result = _run(
        otlp_receiver,
        """
        from opentelemetry import trace, metrics
        from opentelemetry._logs import (
            NoOpLoggerProvider, get_logger_provider, set_logger_provider,
        )
        from django_altimeter.runtime import get_process_runtime, validate_runtime
        foreign = (trace.NoOpTracerProvider(), NoOpLoggerProvider(),
                   metrics.NoOpMeterProvider())
        trace.set_tracer_provider(foreign[0])
        set_logger_provider(foreign[1])
        metrics.set_meter_provider(foreign[2])
        codes = validate_runtime(configuration())
        result = probe(configuration())
        print(json.dumps({'probe': snapshot(result), 'codes': codes,
                          'runtime_absent': get_process_runtime() is None,
                          'foreign_same': all(a is b for a, b in zip(foreign, (
                              trace.get_tracer_provider(), get_logger_provider(),
                              metrics.get_meter_provider())))}))
        """,
    )
    assert result["probe"]["ok"] is True, result
    assert result["codes"] == [
        "setup.traces_foreign",
        "setup.logs_foreign",
        "setup.metrics_foreign",
    ]
    assert result["runtime_absent"] is True
    assert result["foreign_same"] is True
    assert len(otlp_receiver.wait_for_requests(3, timeout=2)) == 3


def test_repeated_probes_have_fresh_writer_ids_and_bounded_utc_windows(
    otlp_receiver: OTLPReceiver,
) -> None:
    from datetime import UTC, datetime
    from uuid import UUID

    before = datetime.now(UTC)
    result = _run(
        otlp_receiver,
        """
        config = configuration(signals=('traces',))
        results = [snapshot(probe(config)), snapshot(probe(config))]
        print(json.dumps({'results': results}))
        """,
    )
    after = datetime.now(UTC)
    first, second = result["results"]
    assert first["ok"] is second["ok"] is True
    assert first["instance_id"] != second["instance_id"]
    captures = otlp_receiver.wait_for_requests(2, timeout=2)
    for row, capture in zip((first, second), captures, strict=True):
        assert UUID(row["instance_id"]).version == 4
        started = datetime.fromisoformat(row["started_at"])
        finished = datetime.fromisoformat(row["finished_at"])
        assert before <= started <= finished <= after
        assert (finished - started).total_seconds() <= 2
        resource = capture.message.resource_spans[0].resource
        assert {item.key: item.value.string_value for item in resource.attributes}[
            "service.instance.id"
        ] == row["instance_id"]


@pytest.mark.parametrize("signal", ["traces", "logs", "metrics"])
@pytest.mark.parametrize("mode", ["hang", "slow_body"])
def test_probe_deadline_reaps_helper_and_allows_a_fresh_probe(
    otlp_receiver: OTLPReceiver, signal: str, mode: str
) -> None:
    if mode == "hang":
        receipt = otlp_receiver.queue_response(hang=True)
    else:
        receipt = otlp_receiver.queue_response(
            body=_partial_response(signal, 0), chunk_size=1, chunk_delay=0.1
        )
    result = _run(
        otlp_receiver,
        f"""
        import threading
        original_threads = tuple(threading.enumerate())
        config = configuration(signals=({signal!r},))
        started = time.monotonic()
        result = probe(config, timeout=0.75)
        elapsed = time.monotonic() - started
        try:
            os.waitpid(-1, os.WNOHANG)
            reaped = False
        except ChildProcessError:
            reaped = True
        recovery = probe(config)
        print(json.dumps({{'result': snapshot(result), 'elapsed': elapsed,
                           'reaped': reaped, 'recovery': snapshot(recovery),
                           'threads_same':
                               original_threads == tuple(threading.enumerate())}}))
        """,
    )
    assert result["result"]["ok"] is False, result
    assert not any(row["accepted"] for row in result["result"]["signals"])
    assert {"probe.deadline", "shutdown.deadline"}.intersection(
        result["result"]["codes"]
    )
    assert 0.6 <= result["elapsed"] < 1.25
    assert result["reaped"] is True
    assert result["threads_same"] is True
    assert receipt.client_closed.wait(1)
    assert result["recovery"]["ok"] is True, result
    captures = otlp_receiver.wait_for_requests(2, timeout=2)
    assert len(captures) == 2
    assert all(capture.path == f"/v1/{signal}" for capture in captures)


def test_probe_keeps_credentials_callbacks_and_ambient_state_out_of_payloads(
    otlp_receiver: OTLPReceiver,
) -> None:
    result = _run(
        otlp_receiver,
        """
        import sys
        def callback(*args, **kwargs):
            raise RuntimeError('CANARY_CALLBACK_MUST_NOT_RUN')
        config = configuration(signals=('traces',), identity_resolver=callback,
                               log_scrubber=callback)
        config = replace(config, traces=replace(config.traces,
            headers=(('authorization', 'Bearer CANARY_INGEST_CREDENTIAL'),)))
        os.environ['ALTIMETER_READBACK_API_KEY'] = 'CANARY_OPERATOR_CREDENTIAL'
        os.environ['OTEL_RESOURCE_ATTRIBUTES'] = 'service.name=CANARY_AMBIENT'
        observed = []
        def audit(event, args):
            if event == 'subprocess.Popen':
                observed.append({'argv_secret': 'CANARY' in repr(args[1]),
                                 'ambient_keys': [key for key in args[3]
                                     if key.startswith(('ALTIMETER_', 'OTEL_'))]})
        sys.addaudithook(audit)
        result = probe(config)
        print(json.dumps({'result': snapshot(result), 'observed': observed,
                          'operator_unchanged': os.environ['ALTIMETER_READBACK_API_KEY']
                              == 'CANARY_OPERATOR_CREDENTIAL'}))
        """,
    )
    assert result["result"]["ok"] is True, result
    assert result["observed"] == [{"argv_secret": False, "ambient_keys": []}]
    assert result["operator_unchanged"] is True
    assert "CANARY" not in json.dumps(result)
    (capture,) = otlp_receiver.wait_for_requests(1, timeout=2)
    assert capture.headers["Authorization"] == "Bearer CANARY_INGEST_CREDENTIAL"
    assert b"CANARY" not in capture.message.SerializeToString()
    resource = capture.message.resource_spans[0]
    assert resource.scope_spans[0].spans[0].name == "altimeter.probe"
    assert {item.key: item.value.string_value for item in resource.resource.attributes}[
        "service.name"
    ] == "probe-test"


@pytest.mark.parametrize(
    ("expression", "code"),
    [
        ("probe(configuration(service_name=''))", "config.invalid_value"),
        ("probe(configuration(), timeout=0)", "probe.invalid_timeout"),
        ("probe(configuration(), timeout=float('nan'))", "probe.invalid_timeout"),
        (
            "probe(configuration(), signals=('CANARY_UNKNOWN',))",
            "probe.invalid_signals",
        ),
    ],
)
def test_invalid_probe_inputs_do_not_start_work(
    otlp_receiver: OTLPReceiver, expression: str, code: str
) -> None:
    result = _run(
        otlp_receiver,
        f"""
        import sys
        work = []
        sys.addaudithook(lambda event, args: work.append(event)
                        if event == 'subprocess.Popen' or event.startswith('socket.')
                        else None)
        result = {expression}
        print(json.dumps({{'result': snapshot(result), 'work': work}}))
        """,
    )
    assert result["result"]["ok"] is False
    assert result["result"]["codes"] == [code]
    assert result["result"]["instance_id"] is None
    assert result["work"] == []
    assert "CANARY" not in json.dumps(result)
    assert not otlp_receiver.requests


@pytest.mark.parametrize(
    ("key", "value", "code"),
    [
        ("OTEL_SEMCONV_STABILITY_OPT_IN", "http/dup", "setup.semconv_conflict"),
        (
            "OTEL_EXPERIMENTAL_RESOURCE_DETECTORS",
            "CANARY_DETECTOR",
            "setup.resource_detector_conflict",
        ),
    ],
)
def test_local_validation_reports_conflicts_without_mutation(
    otlp_receiver: OTLPReceiver, key: str, value: str, code: str
) -> None:
    result = _run(
        otlp_receiver,
        f"""
        import sys, threading
        from django_altimeter.runtime import validate_runtime
        os.environ[{key!r}] = {value!r}
        before = dict(os.environ)
        threads = tuple(threading.enumerate())
        work = []
        sys.addaudithook(lambda event, args: work.append(event)
                        if event == 'subprocess.Popen' or event.startswith('socket.')
                        else None)
        codes = validate_runtime(configuration())
        print(json.dumps({{'codes': codes,
                           'environment_same': before == dict(os.environ),
                           'threads_same': threads == tuple(threading.enumerate()),
                           'work': work}}))
        """,
    )
    assert result["codes"] == [code]
    assert result["environment_same"] is True
    assert result["threads_same"] is True
    assert result["work"] == []
    assert "CANARY" not in json.dumps(result)
    assert not otlp_receiver.requests


@pytest.mark.parametrize("failed_signal", ["traces", "logs", "metrics"])
def test_probe_reports_mixed_signal_results_independently(
    otlp_receiver: OTLPReceiver, failed_signal: str
) -> None:
    for signal in ("traces", "logs", "metrics"):
        otlp_receiver.queue_response(body=b"\xff" if signal == failed_signal else b"")
    result = _run(
        otlp_receiver,
        "print(json.dumps(snapshot(probe(configuration()))))",
    )
    assert result["ok"] is False
    assert len(result["signals"]) == 3
    for row in result["signals"]:
        failed = row["signal"] == failed_signal
        assert row["accepted"] is not failed
        assert row["code"] == ("export.invalid" if failed else "export.accepted")
        assert row["accepted_records"] == int(not failed)
    assert len(otlp_receiver.wait_for_requests(3, timeout=2)) == 3


def test_local_validation_uses_current_runtime_ownership_and_config(
    otlp_receiver: OTLPReceiver,
) -> None:
    result = _run(
        otlp_receiver,
        """
        from django_altimeter.runtime import setup, shutdown, validate_runtime
        class Callback:
            def __call__(self, *args):
                raise AssertionError('CANARY_CALLBACK_MUST_NOT_RUN')
            def __eq__(self, other):
                raise AssertionError('CANARY_EQUALITY_MUST_NOT_RUN')
        config = configuration(signals=('traces',), identity_resolver=Callback())
        assert setup(config).ok
        same = validate_runtime(config)
        different = validate_runtime(replace(config, role='beat'))
        stopped = shutdown()
        after_stop = validate_runtime(config)
        print(json.dumps({'same': same, 'different': different,
                          'after_stop': after_stop, 'shutdown': asdict(stopped)}))
        """,
    )
    assert result["same"] == []
    assert result["different"] == ["setup.config_conflict"]
    assert result["after_stop"] == ["setup.stopped"]
    assert result["shutdown"] == {"completed": True, "codes": []}
    assert "CANARY" not in json.dumps(result)
    assert not otlp_receiver.requests
