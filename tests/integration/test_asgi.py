"""Native ASGI lifetime checks with real processes and decoded OTLP exports.

Protocol tests drive the adapter's receive/send boundary directly; real uvicorn
and Ninja cases separately prove server behavior from the installed wheel.
"""

import json
import os
import subprocess
import sys
import textwrap
from http.client import HTTPConnection
from time import monotonic, sleep

import pytest
from support.otlp_receiver import OTLPReceiver

_IMPORTS = """
import asyncio, json, os, time
from dataclasses import replace
from opentelemetry import trace
from django_altimeter.conf import Config, SignalConfig
from django_altimeter.privacy import DEFAULT_POLICY
from django_altimeter.runtime import setup, shutdown, get_process_runtime

def configure(**changes):
    def signal(name):
        return SignalConfig(True, os.environ['TEST_ENDPOINT'] + '/v1/' + name,
                            destination='collector')
    config = replace(Config(
        enabled=True, service_name='asgi-test', deployment_environment='test',
        release='v1', traces=signal('traces'), metrics=signal('metrics'),
        export_timeout=0.5,
        integrations=frozenset({'asgi', 'django'}),
        policy=replace(DEFAULT_POLICY,
            attribute_values=frozenset({('http.route', '/safe/')})),
    ), **changes)
    assert setup(config).ok
    return get_process_runtime()

SCOPE = {'type': 'http', 'asgi': {'version': '3.0', 'spec_version': '2.4'},
         'http_version': '1.1', 'method': 'GET', 'scheme': 'http',
         'path': '/safe/', 'raw_path': b'/safe/', 'query_string': b'CANARY_QUERY',
         'root_path': '', 'headers': [(b'cookie', b'CANARY_COOKIE')],
         'server': ('127.0.0.1', 8000), 'client': ('127.0.0.1', 12345)}
"""


def _run(receiver: OTLPReceiver, source: str) -> dict:
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
        timeout=15,
        check=False,
    )
    assert process.returncode == 0, process.stderr
    return json.loads(process.stdout)


def _spans(receiver: OTLPReceiver) -> list:
    return [
        span
        for request in receiver.requests
        if request.path == "/v1/traces"
        for resource in request.message.resource_spans
        for scope in resource.scope_spans
        for span in scope.spans
    ]


def _metric_count(receiver: OTLPReceiver, name: str) -> int:
    return sum(
        point.as_int if metric.HasField("sum") else point.count
        for request in receiver.requests
        if request.path == "/v1/metrics"
        for resource in request.message.resource_metrics
        for scope in resource.scope_metrics
        for metric in scope.metrics
        if metric.name == name
        for point in (
            metric.sum.data_points
            if metric.HasField("sum")
            else metric.histogram.data_points
        )
    )


def test_http_request_is_observed_once(otlp_receiver: OTLPReceiver) -> None:
    result = _run(
        otlp_receiver,
        """
        runtime = configure()
        calls = 0
        messages = []
        async def app(scope, receive, send):
            global calls
            calls += 1
            await send({'type': 'http.response.start', 'status': 200, 'headers': []})
            await send({'type': 'http.response.body', 'body': b'safe'})
        async def receive():
            return {'type': 'http.request', 'body': b'', 'more_body': False}
        async def send(message):
            messages.append(message)
        from django_altimeter.asgi import instrument
        observed = instrument(instrument(app))
        asyncio.run(observed(dict(SCOPE), receive, send))
        tracer = runtime.tracer_provider.get_tracer('django_altimeter')
        with tracer.start_as_current_span('SPAN'):
            pass
        stopped = shutdown()
        print(json.dumps({'calls': calls, 'body': messages[-1]['body'].decode(),
                          'status': messages[0]['status'],
                          'stopped': stopped.completed}))
        """,
    )
    assert result == {"calls": 1, "body": "safe", "status": 200, "stopped": True}
    spans = _spans(otlp_receiver)
    assert len([span for span in spans if span.name == "SPAN"]) == 1
    server_spans = [span for span in spans if span.kind == 2]
    assert len(server_spans) == 1
    assert server_spans[0].name == "GET"
    assert _metric_count(otlp_receiver, "altimeter.http.server.requests") == 1
    assert _metric_count(otlp_receiver, "http.server.request.duration") == 1
    for request in otlp_receiver.requests:
        assert b"CANARY" not in request.message.SerializeToString()


def _attributes(record) -> dict:
    return {
        entry.key: getattr(entry.value, entry.value.WhichOneof("value"))
        for entry in record.attributes
    }


@pytest.mark.parametrize("trailers", [False, True])
def test_final_send_time_survives_later_application_return(
    otlp_receiver: OTLPReceiver, trailers: bool
) -> None:
    result = _run(
        otlp_receiver,
        f"        TRAILERS = {trailers!r}\n"
        + """
        from django_altimeter.asgi import instrument
        from django_altimeter.http import get_request_state, request_state
        configure()
        sent = []
        state_seen = None
        async def app(scope, receive, send):
            global state_seen
            state_seen = get_request_state()
            state_seen.route = '/safe/'
            state_seen.response_mode = 'streaming'
            state_seen.response_ready_at = time.monotonic()
            await send({'type': 'http.response.start', 'status': 200,
                        'headers': [], 'trailers': TRAILERS})
            await send({'type': 'http.response.body', 'body': b'first',
                        'more_body': True})
            assert sent[-1]['body'] == b'first'
            await asyncio.sleep(0.08)
            await send({'type': 'http.response.body', 'body': b'last'})
            if TRAILERS:
                await asyncio.sleep(0.08)
                await send({'type': 'http.response.trailers',
                            'headers': [(b'x-safe', b'value')]})
            await asyncio.sleep(0.25)
        async def receive():
            return {'type': 'http.request', 'body': b''}
        async def send(message):
            sent.append(message)
        scope = dict(SCOPE)
        previous_scope_state = object()
        scope['django_altimeter.request_state'] = previous_scope_state
        async def run():
            await instrument(app)(scope, receive, send)
            assert get_request_state() is None
            assert not trace.get_current_span().get_span_context().is_valid
        asyncio.run(run())
        assert scope['django_altimeter.request_state'] is previous_scope_state
        assert state_seen.finalized is True
        assert shutdown().completed
        print(json.dumps({'body': ''.join(m.get('body', b'').decode() for m in sent),
                          'final_type': sent[-1]['type']}))
        """,
    )
    assert result["body"] == "firstlast"
    assert result["final_type"] == (
        "http.response.trailers" if trailers else "http.response.body"
    )
    (span,) = _spans(otlp_receiver)
    duration = (span.end_time_unix_nano - span.start_time_unix_nano) / 1e9
    assert (0.14 if trailers else 0.07) <= duration
    assert duration < (0.30 if trailers else 0.22)
    assert _attributes(span)["altimeter.response_mode"] == "streaming"
    assert _attributes(span)["altimeter.http.outcome"] == "completed"
    assert _metric_count(otlp_receiver, "altimeter.http.server.requests") == 1
    assert _metric_count(otlp_receiver, "http.server.request.duration") == 1


@pytest.mark.parametrize(
    ("failure_point", "expected_status", "outcome"),
    [
        ("application", None, "aborted"),
        ("receive", None, "aborted"),
        ("start", None, "aborted"),
        ("body", 200, "aborted"),
        ("trailers", 200, "aborted"),
        ("cancel", 200, "aborted"),
        ("after_response", 200, "completed"),
    ],
)
def test_protocol_failures_propagate_once_and_cleanup_context(
    otlp_receiver: OTLPReceiver,
    failure_point: str,
    expected_status: int | None,
    outcome: str,
) -> None:
    result = _run(
        otlp_receiver,
        f"        FAILURE_POINT = {failure_point!r}\n"
        + """
        from django_altimeter.asgi import instrument
        from django_altimeter.http import get_request_state
        configure()
        failure = (asyncio.CancelledError('CANARY_CANCEL')
                   if FAILURE_POINT == 'cancel' else RuntimeError('CANARY_ERROR'))
        calls = 0
        active_at_entry = []
        states = []
        async def app(scope, receive, send):
            global calls
            calls += 1
            state = get_request_state()
            states.append(state)
            active_at_entry.append(state.concurrent_at_start)
            if calls == 1 and FAILURE_POINT == 'application':
                raise failure
            await receive()
            await send({'type': 'http.response.start', 'status': 200,
                        'headers': [], 'trailers': FAILURE_POINT == 'trailers'})
            if calls == 1 and FAILURE_POINT == 'cancel':
                raise failure
            await send({'type': 'http.response.body', 'body': b'safe'})
            if FAILURE_POINT == 'trailers':
                await send({'type': 'http.response.trailers', 'headers': []})
            if calls == 1 and FAILURE_POINT == 'after_response':
                raise failure
        async def receive():
            if calls == 1 and FAILURE_POINT == 'receive':
                raise failure
            return {'type': 'http.request', 'body': b''}
        async def send(message):
            if calls == 1 and message['type'] == 'http.response.' + FAILURE_POINT:
                raise failure
        observed = instrument(app)
        scope = dict(SCOPE)
        async def run():
            try:
                await observed(scope, receive, send)
            except BaseException as caught:
                assert caught is failure
            else:
                raise AssertionError('original application failure was lost')
            assert get_request_state() is None
            assert not trace.get_current_span().get_span_context().is_valid
            assert 'django_altimeter.request_state' not in scope
            await observed(scope, receive, send)
            assert get_request_state() is None
            assert not trace.get_current_span().get_span_context().is_valid
            assert 'django_altimeter.request_state' not in scope
        asyncio.run(run())
        assert all(s.finalized for s in states)
        assert shutdown().completed
        print(json.dumps({'calls': calls, 'concurrency': active_at_entry}))
        """,
    )
    assert result == {"calls": 2, "concurrency": [1, 1]}
    failed, positive = _spans(otlp_receiver)
    attrs = _attributes(failed)
    assert attrs.get("http.response.status_code") == expected_status
    assert attrs["altimeter.http.outcome"] == outcome
    assert failed.status.code == 2
    if failure_point == "after_response":
        assert len(failed.events) == 1
        assert failed.events[0].name == "exception"
        assert _attributes(failed.events[0]) == {"error.type": "after_response"}
    else:
        assert not failed.events
    assert _attributes(positive)["altimeter.http.outcome"] == "completed"
    assert _attributes(positive)["http.response.status_code"] == 200
    assert _metric_count(otlp_receiver, "altimeter.http.server.requests") == 2
    assert _metric_count(otlp_receiver, "http.server.request.duration") == 2
    for request in otlp_receiver.requests:
        assert b"CANARY" not in request.message.SerializeToString()


@pytest.mark.parametrize("sample_rate", [1.0, 0.1, 0.0])
def test_concurrency_counts_unsampled_requests_and_metrics_keep_totals(
    otlp_receiver: OTLPReceiver, sample_rate: float
) -> None:
    result = _run(
        otlp_receiver,
        f"        SAMPLE_RATE = {sample_rate!r}\n"
        + """
        from django_altimeter.asgi import instrument
        from django_altimeter.http import get_request_state
        configure(sample_rate=SAMPLE_RATE)
        entries = []
        states = []
        gate = asyncio.Event()
        async def app(scope, receive, send):
            state = get_request_state()
            states.append(state)
            entries.append(state.concurrent_at_start)
            if len(entries) == 200:
                gate.set()
            if scope['method'] == 'GET':
                await gate.wait()
            await send({'type': 'http.response.start', 'status': 200, 'headers': []})
            await send({'type': 'http.response.body', 'body': b'safe'})
        async def receive():
            return {'type': 'http.request', 'body': b''}
        async def send(message):
            return None
        observed = instrument(app)
        async def run():
            await asyncio.gather(*(
                observed(dict(SCOPE), receive, send) for _ in range(200)
            ))
            assert get_request_state() is None
            await observed({**SCOPE, 'method': 'POST'}, receive, send)
        asyncio.run(run())
        assert all(state.finalized for state in states)
        assert shutdown().completed
        print(json.dumps({'entries': entries}))
        """,
    )
    assert result["entries"] == [*range(1, 201), 1]
    spans = _spans(otlp_receiver)
    if sample_rate == 1.0:
        assert len(spans) == 201
        assert sorted(
            _attributes(span)["altimeter.http.server.concurrent_at_start"]
            for span in spans
            if span.name == "GET"
        ) == list(range(1, 201))
    elif sample_rate == 0.0:
        assert not spans
    else:
        assert 0 < len(spans) < 100
    assert _metric_count(otlp_receiver, "altimeter.http.server.requests") == 201
    assert _metric_count(otlp_receiver, "http.server.request.duration") == 201
    for request in otlp_receiver.requests:
        if request.path == "/v1/metrics":
            assert b"concurrent_at_start" not in request.message.SerializeToString()


@pytest.mark.parametrize("application_fails", [False, True])
def test_native_telemetry_failure_cannot_retry_app_or_leak_counter(
    otlp_receiver: OTLPReceiver,
    application_fails: bool,
) -> None:
    result = _run(
        otlp_receiver,
        f"        APPLICATION_FAILS={application_fails!r}\n"
        + """
        from opentelemetry.sdk.trace import SpanProcessor
        from django_altimeter.asgi import instrument
        from django_altimeter.http import get_request_state
        runtime = configure()
        class FailingProcessor(SpanProcessor):
            def on_end(self, span):
                raise RuntimeError('CANARY_PROCESSOR_ERROR')
        runtime.tracer_provider.add_span_processor(FailingProcessor())
        entries = []
        calls = 0
        application_failure = RuntimeError('CANARY_APPLICATION')
        sent = []
        async def app(scope, receive, send):
            global calls
            calls += 1
            entries.append(get_request_state().concurrent_at_start)
            await send({'type': 'http.response.start', 'status': 200, 'headers': []})
            if APPLICATION_FAILS and calls == 1:
                raise application_failure
            await send({'type': 'http.response.body', 'body': b'safe'})
        async def receive():
            return {'type': 'http.request', 'body': b''}
        async def send(message):
            sent.append(message)
        observed = instrument(app)
        async def run():
            for _ in range(2):
                try:
                    await observed(dict(SCOPE), receive, send)
                except RuntimeError as caught:
                    assert caught is application_failure
                else:
                    assert not (APPLICATION_FAILS and calls == 1)
                assert get_request_state() is None
                assert not trace.get_current_span().get_span_context().is_valid
        asyncio.run(run())
        assert shutdown().completed
        print(json.dumps({'entries': entries, 'calls': calls,
                          'bodies': [m['body'].decode() for m in sent if 'body' in m]}))
        """,
    )
    assert result == {
        "entries": [1, 1],
        "calls": 2,
        "bodies": ["safe"] if application_fails else ["safe", "safe"],
    }
    assert len(_spans(otlp_receiver)) == 2
    assert _metric_count(otlp_receiver, "altimeter.http.server.requests") == 2
    assert _metric_count(otlp_receiver, "http.server.request.duration") == 2
    for request in otlp_receiver.requests:
        assert b"CANARY" not in request.message.SerializeToString()


def test_websocket_and_lifespan_are_forwarded_without_http_observations(
    otlp_receiver: OTLPReceiver,
) -> None:
    result = _run(
        otlp_receiver,
        """
        from django_altimeter.asgi import instrument
        from django_altimeter.http import get_request_state
        runtime = configure()
        scopes = []
        async def app(scope, receive, send):
            scopes.append(scope['type'])
            assert get_request_state() is None
            message = await receive()
            await send(message)
        async def receive():
            return {'type': 'custom.event', 'value': 'safe'}
        sent = []
        async def send(message):
            sent.append(message)
        observed = instrument(app, surface='mcp')
        assert instrument(observed) is observed
        async def run():
            await observed({'type': 'lifespan'}, receive, send)
            await observed({'type': 'websocket'}, receive, send)
        asyncio.run(run())
        tracer = runtime.tracer_provider.get_tracer('django_altimeter')
        with tracer.start_as_current_span('SPAN'):
            pass
        assert shutdown().completed
        print(json.dumps({'scopes': scopes, 'sent': sent}))
        """,
    )
    assert result == {
        "scopes": ["lifespan", "websocket"],
        "sent": [{"type": "custom.event", "value": "safe"}] * 2,
    }
    assert [span.name for span in _spans(otlp_receiver)] == ["SPAN"]
    assert _metric_count(otlp_receiver, "altimeter.http.server.requests") == 0
    assert _metric_count(otlp_receiver, "http.server.request.duration") == 0


def test_actual_task_cancellation_cleans_counter_and_context(
    otlp_receiver: OTLPReceiver,
) -> None:
    result = _run(
        otlp_receiver,
        """
        from django_altimeter.asgi import instrument
        from django_altimeter.http import get_request_state
        configure()
        entered = asyncio.Event()
        calls = 0
        entries = []
        states = []
        cancelled = None
        async def app(scope, receive, send):
            global calls, cancelled
            calls += 1
            state = get_request_state()
            states.append(state)
            entries.append(state.concurrent_at_start)
            await send({'type': 'http.response.start', 'status': 200, 'headers': []})
            if calls == 1:
                entered.set()
                try:
                    await asyncio.Event().wait()
                except asyncio.CancelledError as exc:
                    cancelled = exc
                    raise
            await send({'type': 'http.response.body', 'body': b'safe'})
        async def receive():
            return {'type': 'http.request', 'body': b''}
        async def send(message):
            return None
        observed = instrument(app)
        async def child():
            try:
                await observed(dict(SCOPE), receive, send)
            except asyncio.CancelledError as exc:
                assert exc is cancelled
                assert get_request_state() is None
                assert not trace.get_current_span().get_span_context().is_valid
                raise
        async def run():
            task = asyncio.create_task(child())
            await entered.wait()
            task.cancel('CANARY_CANCELLATION')
            try:
                await task
            except asyncio.CancelledError:
                pass
            else:
                raise AssertionError('task cancellation swallowed')
            await observed(dict(SCOPE), receive, send)
        asyncio.run(run())
        assert all(state.finalized for state in states)
        assert shutdown().completed
        print(json.dumps({'calls': calls, 'entries': entries}))
        """,
    )
    assert result == {"calls": 2, "entries": [1, 1]}
    aborted, completed = _spans(otlp_receiver)
    assert _attributes(aborted)["altimeter.http.outcome"] == "aborted"
    assert _attributes(aborted)["http.response.status_code"] == 200
    assert _attributes(completed)["altimeter.http.outcome"] == "completed"
    assert _metric_count(otlp_receiver, "altimeter.http.server.requests") == 2
    assert _metric_count(otlp_receiver, "http.server.request.duration") == 2
    for request in otlp_receiver.requests:
        assert b"CANARY" not in request.message.SerializeToString()


def test_branch_concurrency_is_thread_safe_and_separate(
    otlp_receiver: OTLPReceiver,
) -> None:
    result = _run(
        otlp_receiver,
        """
        from concurrent.futures import ThreadPoolExecutor
        from threading import Barrier, Lock
        from django_altimeter.asgi import instrument
        from django_altimeter.http import get_request_state
        configure()
        barrier = Barrier(8, timeout=5)
        entries = {'django': [], 'mcp': []}
        lock = Lock()
        async def app(scope, receive, send):
            state = get_request_state()
            with lock:
                entries[state.surface].append(state.concurrent_at_start)
            if scope['method'] == 'GET':
                barrier.wait()
            await send({'type': 'http.response.start', 'status': 200, 'headers': []})
            await send({'type': 'http.response.body', 'body': b'safe'})
        async def receive():
            return {'type': 'http.request', 'body': b''}
        async def send(message):
            return None
        django = instrument(app)
        mcp = instrument(app, surface='mcp')
        async def one(application, method):
            await application({**SCOPE, 'method': method}, receive, send)
            assert get_request_state() is None
            assert not trace.get_current_span().get_span_context().is_valid
        def worker(application):
            asyncio.run(one(application, 'GET'))
        with ThreadPoolExecutor(max_workers=8) as pool:
            futures = [pool.submit(worker, django) for _ in range(5)]
            futures += [pool.submit(worker, mcp) for _ in range(3)]
            for future in futures:
                future.result(timeout=8)
        asyncio.run(one(django, 'POST'))
        asyncio.run(one(mcp, 'POST'))
        assert shutdown().completed
        print(json.dumps(entries))
        """,
    )
    assert sorted(result["django"][:-1]) == list(range(1, 6))
    assert sorted(result["mcp"][:-1]) == list(range(1, 4))
    assert result["django"][-1] == result["mcp"][-1] == 1
    assert len(_spans(otlp_receiver)) == 10
    assert _metric_count(otlp_receiver, "altimeter.http.server.requests") == 10
    assert _metric_count(otlp_receiver, "http.server.request.duration") == 10


def test_asgi_import_starts_no_threads_or_sockets(otlp_receiver: OTLPReceiver) -> None:
    result = _run(
        otlp_receiver,
        """
        import threading, sys
        before = tuple(threading.enumerate())
        sockets = []
        sys.addaudithook(lambda event, args: sockets.append(event)
                        if event.startswith('socket.') else None)
        import django_altimeter.asgi
        print(json.dumps({'threads_same': tuple(threading.enumerate()) == before,
                          'sockets': sockets}))
        """,
    )
    assert result == {"threads_same": True, "sockets": []}
    assert not otlp_receiver.requests


@pytest.mark.parametrize("mode", ["disabled", "foreign_tracing", "foreign_metrics"])
def test_signal_ownership_is_independent_and_app_still_runs(
    otlp_receiver: OTLPReceiver, mode: str
) -> None:
    result = _run(
        otlp_receiver,
        f"        MODE = {mode!r}\n"
        + """
        from opentelemetry import metrics
        from django_altimeter.asgi import instrument
        from django_altimeter.http import get_request_state
        if MODE == 'disabled':
            setup(Config())
        else:
            if MODE == 'foreign_tracing':
                foreign = trace.NoOpTracerProvider()
                trace.set_tracer_provider(foreign)
            else:
                foreign = metrics.NoOpMeterProvider()
                metrics.set_meter_provider(foreign)
            configure()
        calls = 0
        sent = []
        async def app(scope, receive, send):
            global calls
            calls += 1
            await send({'type': 'http.response.start', 'status': 200,
                        'headers': [(b'cache-control', b'private, no-store')]})
            await send({'type': 'http.response.body', 'body': b'safe'})
        async def receive():
            return {'type': 'http.request', 'body': b''}
        async def send(message):
            sent.append(message)
        asyncio.run(instrument(app)(dict(SCOPE), receive, send))
        if MODE == 'foreign_tracing':
            assert trace.get_tracer_provider() is foreign
        elif MODE == 'foreign_metrics':
            assert metrics.get_meter_provider() is foreign
        assert get_request_state() is None
        assert shutdown().completed
        print(json.dumps({'calls': calls, 'body': sent[-1]['body'].decode(),
                          'headers': [[k.decode(),v.decode()]
                                      for k,v in sent[0]['headers']]}))
        """,
    )
    assert result["calls"] == 1
    assert result["body"] == "safe"
    headers = dict(result["headers"])
    assert headers["cache-control"] == "private, no-store"
    assert ("x-altimeter-trace-id" in headers) == (mode == "foreign_metrics")
    assert len(_spans(otlp_receiver)) == (1 if mode == "foreign_metrics" else 0)
    expected_metrics = 1 if mode == "foreign_tracing" else 0
    assert _metric_count(otlp_receiver, "altimeter.http.server.requests") == (
        expected_metrics
    )
    assert (
        _metric_count(otlp_receiver, "http.server.request.duration") == expected_metrics
    )


def test_exact_noise_paths_are_excluded_before_context_and_counting(
    otlp_receiver: OTLPReceiver,
) -> None:
    result = _run(
        otlp_receiver,
        """
        from django_altimeter.asgi import instrument
        from django_altimeter.http import get_request_state
        configure(excluded_routes=frozenset({'/health/'}))
        states = []
        async def app(scope, receive, send):
            state = get_request_state()
            states.append(None if state is None else state.concurrent_at_start)
            await send({'type': 'http.response.start', 'status': 200, 'headers': []})
            await send({'type': 'http.response.body', 'body': b'safe'})
        async def receive():
            return {'type': 'http.request', 'body': b''}
        async def send(message):
            return None
        observed = instrument(app)
        async def run():
            for path in ('/health/', '/health/more', '/safe/'):
                await observed({**SCOPE, 'path': path}, receive, send)
        asyncio.run(run())
        assert shutdown().completed
        print(json.dumps({'states': states}))
        """,
    )
    assert result == {"states": [None, 1, 1]}
    assert len(_spans(otlp_receiver)) == 2
    assert _metric_count(otlp_receiver, "altimeter.http.server.requests") == 2
    assert _metric_count(otlp_receiver, "http.server.request.duration") == 2


def test_unknown_and_oversized_methods_use_fixed_category(
    otlp_receiver: OTLPReceiver,
) -> None:
    result = _run(
        otlp_receiver,
        """
        from django_altimeter.asgi import instrument
        from django_altimeter.http import get_request_state
        configure()
        methods = []
        async def app(scope, receive, send):
            methods.append(get_request_state().method)
            await send({'type': 'http.response.start', 'status': 200, 'headers': []})
            await send({'type': 'http.response.body', 'body': b'safe'})
        async def receive():
            return {'type': 'http.request', 'body': b''}
        async def send(message):
            return None
        observed = instrument(app)
        async def run():
            for method in ('GET', 'CANARY', 'CANARY' * 100000):
                await observed({**SCOPE, 'method': method}, receive, send)
        asyncio.run(run())
        assert shutdown().completed
        print(json.dumps({'methods': methods}))
        """,
    )
    assert result == {"methods": ["GET", "_OTHER", "_OTHER"]}
    assert [span.name for span in _spans(otlp_receiver)] == ["GET", "HTTP", "HTTP"]
    assert _metric_count(otlp_receiver, "altimeter.http.server.requests") == 3
    assert _metric_count(otlp_receiver, "http.server.request.duration") == 3
    for request in otlp_receiver.requests:
        assert b"CANARY" not in request.message.SerializeToString()


def test_start_failure_cannot_disclose_parent_as_request_correlation(
    otlp_receiver: OTLPReceiver,
) -> None:
    result = _run(
        otlp_receiver,
        """
        from opentelemetry.sdk.trace import SpanProcessor
        from django_altimeter.asgi import instrument
        from django_altimeter.http import get_request_state
        runtime = configure()
        class FailFirstStart(SpanProcessor):
            failed = False
            def on_start(self, span, parent_context=None):
                if span.kind == trace.SpanKind.SERVER and not self.failed:
                    self.failed = True
                    raise RuntimeError('CANARY_START')
        runtime.tracer_provider.add_span_processor(FailFirstStart())
        valid = []
        entries = []
        headers = []
        async def app(scope, receive, send):
            valid.append(trace.get_current_span().get_span_context().is_valid)
            entries.append(get_request_state().concurrent_at_start)
            await send({'type': 'http.response.start', 'status': 200,
                        'headers': [(b'cache-control', b'private, no-store')]})
            await send({'type': 'http.response.body', 'body': b'safe'})
        async def receive():
            return {'type': 'http.request', 'body': b''}
        async def send(message):
            if message['type'] == 'http.response.start':
                headers.append(dict((k.decode(), v.decode())
                                    for k, v in message['headers']))
        observed = instrument(app)
        async def run(parent):
            for _ in range(2):
                await observed(dict(SCOPE), receive, send)
                assert trace.get_current_span() is parent
                assert get_request_state() is None
        tracer = runtime.tracer_provider.get_tracer('django_altimeter')
        with tracer.start_as_current_span('SPAN') as parent:
            asyncio.run(run(parent))
        assert shutdown().completed
        print(json.dumps({'valid': valid, 'entries': entries, 'headers': headers}))
        """,
    )
    assert result["valid"] == [False, True]
    assert result["entries"] == [1, 1]
    assert "x-altimeter-trace-id" not in result["headers"][0]
    server_spans = [span for span in _spans(otlp_receiver) if span.kind == 2]
    assert len(server_spans) == 1
    assert (
        result["headers"][1]["x-altimeter-trace-id"] == server_spans[0].trace_id.hex()
    )
    assert _metric_count(otlp_receiver, "altimeter.http.server.requests") == 2


@pytest.mark.parametrize(
    ("trusted", "configured", "sample_rate", "accepted_parent", "sampled"),
    [
        (True, True, 0.0, True, True),
        (False, True, 0.0, False, False),
        (True, False, 0.0, False, False),
        (False, False, 1.0, False, True),
    ],
)
def test_only_explicit_trusted_ingress_accepts_remote_parent(
    otlp_receiver: OTLPReceiver,
    trusted: bool,
    configured: bool,
    sample_rate: float,
    accepted_parent: bool,
    sampled: bool,
) -> None:
    result = _run(
        otlp_receiver,
        f"        TRUSTED={trusted!r}; CONFIGURED={configured!r}; "
        f"RATE={sample_rate!r}\n"
        + """
        from opentelemetry import baggage
        from django_altimeter.asgi import instrument
        configure(sample_rate=RATE, trust_remote_parent=CONFIGURED)
        seen = {}
        headers = {}
        async def app(scope, receive, send):
            current = trace.get_current_span().get_span_context()
            seen.update(trace_id=trace.format_trace_id(current.trace_id),
                        sampled=bool(current.trace_flags.sampled),
                        baggage=dict(baggage.get_all()),
                        tracestate=current.trace_state.to_header())
            await send({'type': 'http.response.start', 'status': 200,
                        'headers': [(b'cache-control', b'private, no-store')]})
            await send({'type': 'http.response.body', 'body': b'safe'})
        async def receive():
            return {'type': 'http.request', 'body': b''}
        async def send(message):
            if message['type'] == 'http.response.start':
                headers.update((k.decode(), v.decode()) for k,v in message['headers'])
        incoming = [
            (b'traceparent', b'00-' + b'1'*32 + b'-' + b'2'*16 + b'-01'),
            (b'tracestate', b'CANARY=PRIVATE'),
            (b'baggage', b'CANARY=PRIVATE'),
            (b'x-internal', b'true'),
        ]
        asyncio.run(instrument(app, trusted_ingress=TRUSTED)(
            {**SCOPE, 'headers': incoming}, receive, send))
        assert shutdown().completed
        print(json.dumps({'seen': seen, 'headers': headers}))
        """,
    )
    assert (result["seen"]["trace_id"] == "1" * 32) == accepted_parent
    assert result["seen"]["sampled"] == sampled
    assert result["seen"]["baggage"] == {}
    assert result["seen"]["tracestate"] == ""
    assert result["headers"]["x-altimeter-trace-id"] == result["seen"]["trace_id"]
    assert result["headers"]["x-altimeter-trace-sampled"] == ("1" if sampled else "0")
    spans = _spans(otlp_receiver)
    assert len(spans) == int(sampled)
    if accepted_parent:
        assert spans[0].parent_span_id.hex() == "2" * 16
    elif spans:
        assert not spans[0].parent_span_id
    assert _metric_count(otlp_receiver, "altimeter.http.server.requests") == 1
    assert _metric_count(otlp_receiver, "http.server.request.duration") == 1
    for request in otlp_receiver.requests:
        assert b"CANARY" not in request.message.SerializeToString()


@pytest.fixture(scope="module")
def asgi_wheel(tmp_path_factory):
    from support.uvicorn_server import install_wheel

    return install_wheel(tmp_path_factory.mktemp("asgi-wheel"))


def test_real_uvicorn_ninja_observes_resolved_and_unmatched_requests_once(
    otlp_receiver: OTLPReceiver, asgi_wheel, tmp_path
) -> None:
    from support.uvicorn_server import running_server

    with running_server(tmp_path, wheel=asgi_wheel, receiver=otlp_receiver) as server:
        responses = [
            server.request(path)
            for path in (
                "/api/items/7?CANARY_QUERY=secret",
                "/api/rejected",
                "/api/error",
                "/CANARY_RAW_PATH",
            )
        ]
        assert [response[0] for response in responses] == [200, 403, 500, 404]
        assert json.loads(responses[0][2])["item_id"] == 7
        assert json.loads(responses[1][2]) == {"rejected": True}
    spans = _spans(otlp_receiver)
    assert len(spans) == 4
    assert {span.kind for span in spans} == {2}
    by_status = {_attributes(span)["http.response.status_code"]: span for span in spans}
    assert _attributes(by_status[200])["http.route"] == "api/items/<item_id>"
    assert "http.route" not in _attributes(by_status[404])
    assert all(
        _attributes(span)["altimeter.http.outcome"] == "completed" for span in spans
    )
    assert _metric_count(otlp_receiver, "altimeter.http.server.requests") == 4
    assert _metric_count(otlp_receiver, "http.server.request.duration") == 4
    for request in otlp_receiver.requests:
        assert b"CANARY" not in request.message.SerializeToString()
        assert b"reference-error-canary" not in request.message.SerializeToString()


def test_real_uvicorn_stream_is_not_buffered_and_disconnect_finalizes(
    otlp_receiver: OTLPReceiver, asgi_wheel, tmp_path
) -> None:
    from support.uvicorn_server import running_server

    with running_server(tmp_path, wheel=asgi_wheel, receiver=otlp_receiver) as server:
        connection = HTTPConnection("127.0.0.1", server.port, timeout=5)
        try:
            connection.request("GET", "/api/stream")
            response = connection.getresponse()
            assert response.status == 200
            assert response.readline() == b"first\n"
            first_received = monotonic()
            assert response.readline() == b"last\n"
            assert monotonic() - first_received >= 0.14
            assert response.read() == b""
        finally:
            connection.close()

        connection = HTTPConnection("127.0.0.1", server.port, timeout=5)
        try:
            connection.request("GET", "/api/abort")
            response = connection.getresponse()
            assert response.status == 200
            assert response.readline() == b"first\n"
            response.close()
        finally:
            connection.close()
        deadline = monotonic() + 4
        while len(_spans(otlp_receiver)) < 2 and monotonic() < deadline:
            sleep(0.01)
        assert len(_spans(otlp_receiver)) == 2
        status, _, body = server.request("/api/items/8")
        assert status == 200
        assert json.loads(body)["item_id"] == 8
    by_route = {_attributes(span)["http.route"]: span for span in _spans(otlp_receiver)}
    assert set(by_route) == {"api/stream", "api/abort", "api/items/<item_id>"}
    stream = by_route["api/stream"]
    assert _attributes(stream)["altimeter.response_mode"] == "streaming"
    assert _attributes(stream)["altimeter.http.outcome"] == "completed"
    assert stream.end_time_unix_nano - stream.start_time_unix_nano >= 140_000_000
    assert _attributes(by_route["api/abort"])["altimeter.http.outcome"] == "aborted"
    assert _attributes(by_route["api/abort"])["http.response.status_code"] == 200
    next_request = by_route["api/items/<item_id>"]
    assert _attributes(next_request)["altimeter.http.server.concurrent_at_start"] == 1
    assert _attributes(next_request)["altimeter.response_mode"] == "buffered"
    assert _metric_count(otlp_receiver, "altimeter.http.server.requests") == 3
    assert _metric_count(otlp_receiver, "http.server.request.duration") == 3


def test_real_uvicorn_correlation_and_timing_origin_are_request_specific(
    otlp_receiver: OTLPReceiver, asgi_wheel, tmp_path
) -> None:
    from support.uvicorn_server import running_server

    with running_server(tmp_path, wheel=asgi_wheel, receiver=otlp_receiver) as server:
        responses = [
            server.request(
                "/api/items/9",
                headers={
                    "Origin": origin,
                    "traceparent": "00-" + "1" * 32 + "-" + "2" * 16 + "-01",
                    "tracestate": "vendor=CANARY_PRIVATE",
                    "baggage": "CANARY_PRIVATE=secret",
                    "Cookie": "CANARY_PRIVATE=secret",
                },
            )
            for origin in ("https://frontend.example", "https://CANARY.example")
        ]
        assert [response[0] for response in responses] == [200, 200]
        first, second = [
            {key.lower(): value for key, value in headers.items()}
            for _, headers, _ in responses
        ]
        assert first["timing-allow-origin"] == "https://frontend.example"
        assert "timing-allow-origin" not in second
        assert "origin" in first["vary"].lower().split(", ")
        assert first["cache-control"] == second["cache-control"] == "private, no-store"
        first_id, second_id = (
            first["x-altimeter-trace-id"],
            second["x-altimeter-trace-id"],
        )
        assert first_id != second_id
        assert first_id != "1" * 32 and second_id != "1" * 32
        assert first["x-altimeter-trace-sampled"] == "1"
        assert second["x-altimeter-trace-sampled"] == "1"
    spans = _spans(otlp_receiver)
    assert {span.trace_id.hex() for span in spans} == {first_id, second_id}
    assert all(not span.parent_span_id for span in spans)
    assert _metric_count(otlp_receiver, "altimeter.http.server.requests") == 2
    assert _metric_count(otlp_receiver, "http.server.request.duration") == 2
    for request in otlp_receiver.requests:
        assert b"CANARY" not in request.message.SerializeToString()


@pytest.mark.parametrize("sample_rate", [1.0, 0.1])
def test_real_uvicorn_sampling_changes_spans_only(
    otlp_receiver: OTLPReceiver, asgi_wheel, tmp_path, sample_rate: float
) -> None:
    from support.uvicorn_server import running_server

    with running_server(
        tmp_path,
        wheel=asgi_wheel,
        receiver=otlp_receiver,
        extra_env={"ALTIMETER_SAMPLE_RATE": str(sample_rate)},
    ) as server:
        flags = []
        for item_id in range(200):
            status, raw_headers, body = server.request(f"/api/items/{item_id}")
            assert status == 200
            assert json.loads(body)["item_id"] == item_id
            headers = {key.lower(): value for key, value in raw_headers.items()}
            flags.append(headers["x-altimeter-trace-sampled"])
    spans = _spans(otlp_receiver)
    assert len(spans) == flags.count("1")
    if sample_rate == 1.0:
        assert len(spans) == 200
    else:
        assert 0 < len(spans) < 100
    assert _metric_count(otlp_receiver, "altimeter.http.server.requests") == 200
    assert _metric_count(otlp_receiver, "http.server.request.duration") == 200


def test_real_uvicorn_mcp_branch_is_mounted_once_and_lifespan_survives(
    otlp_receiver: OTLPReceiver, asgi_wheel, tmp_path
) -> None:
    from support.uvicorn_server import ROOT, running_server

    factory = tmp_path / "mcp_reference.py"
    factory.write_text(
        textwrap.dedent(
            """
            import asyncio, json, os, sys
            from pathlib import Path
            sys.path.insert(0, os.environ['DJANGO_REFERENCE_PATH'])
            from altimeter_reference.asgi import create_application as django_factory
            from django_altimeter.asgi import instrument
            from django_altimeter.http import get_request_state

            def create_application():
                django = django_factory()
                calls = 0
                async def mcp(scope, receive, send):
                    nonlocal calls
                    if scope['type'] == 'lifespan':
                        assert get_request_state() is None
                        while True:
                            message = await receive()
                            event = message['type'].removeprefix('lifespan.')
                            with open(os.environ['MCP_EVENTS'], 'a') as stream:
                                stream.write(event + '\\n')
                            await send({'type': message['type'] + '.complete'})
                            if event == 'shutdown':
                                return
                    assert scope['type'] == 'http'
                    assert get_request_state().surface == 'mcp'
                    calls += 1
                    await send({'type': 'http.response.start', 'status': 200,
                                'headers': [(b'content-type', b'text/event-stream'),
                                            (b'cache-control', b'no-store')]})
                    body = ('data: ' + json.dumps({'calls': calls}) + '\\n\\n').encode()
                    await send({'type': 'http.response.body', 'body': body})

                mounted = instrument(mcp, surface='mcp')
                assert instrument(mounted, surface='mcp') is mounted
                async def router(scope, receive, send):
                    if scope['type'] == 'lifespan':
                        # The existing router owns lifespan composition. Neither
                        # instrumented HTTP branch replaces or duplicates it.
                        events, replies = asyncio.Queue(), asyncio.Queue()
                        async def composed_receive():
                            message = await receive()
                            await events.put(message)
                            reply = await replies.get()
                            assert reply['type'] == message['type'] + '.complete'
                            return message
                        async with asyncio.TaskGroup() as tasks:
                            tasks.create_task(mounted(scope, events.get, replies.put))
                            await django(scope, composed_receive, send)
                    elif scope['type'] == 'http' and scope['path'] == '/mcp/':
                        await mounted(scope, receive, send)
                    else:
                        await django(scope, receive, send)
                return router
            """
        )
    )
    event_path = tmp_path / "mcp-events.txt"
    with running_server(
        tmp_path / "server",
        wheel=asgi_wheel,
        receiver=otlp_receiver,
        extra_env={
            "REFERENCE_PATH": str(tmp_path),
            "REFERENCE_FACTORY": "mcp_reference:create_application",
            "DJANGO_REFERENCE_PATH": str(ROOT / "reference"),
            "MCP_EVENTS": str(event_path),
        },
    ) as server:
        status, _, body = server.request("/mcp/")
        assert status == 200
        assert body == b'data: {"calls": 1}\n\n'
        status, _, body = server.request("/api/items/19")
        assert status == 200
        assert json.loads(body)["item_id"] == 19
    assert event_path.read_text().splitlines() == ["startup", "shutdown"]
    spans = _spans(otlp_receiver)
    assert len(spans) == 2
    by_surface = {_attributes(span)["altimeter.surface"]: span for span in spans}
    assert set(by_surface) == {"django", "mcp"}
    assert _attributes(by_surface["mcp"])["altimeter.response_mode"] == "streaming"
    assert _attributes(by_surface["django"])["altimeter.response_mode"] == "buffered"
    assert all(
        _attributes(span)["altimeter.http.server.concurrent_at_start"] == 1
        for span in spans
    )
    assert _metric_count(otlp_receiver, "altimeter.http.server.requests") == 2
    assert _metric_count(otlp_receiver, "http.server.request.duration") == 2


@pytest.mark.parametrize("channel", ["receive", "send"])
def test_handled_channel_error_after_response_keeps_completion_and_records_error(
    otlp_receiver: OTLPReceiver, channel: str
) -> None:
    result = _run(
        otlp_receiver,
        f"        CHANNEL={channel!r}\n"
        + """
        from django_altimeter.asgi import instrument
        configure()
        failure = OSError('CANARY_AFTER_RESPONSE')
        sent = 0
        caught_same = False
        async def app(scope, receive, send):
            global caught_same
            await send({'type': 'http.response.start', 'status': 200, 'headers': []})
            await send({'type': 'http.response.body', 'body': b'safe'})
            try:
                if CHANNEL == 'receive':
                    await receive()
                else:
                    await send({'type': 'http.response.body', 'body': b'late'})
            except OSError as caught:
                caught_same = caught is failure
        async def receive():
            raise failure
        async def send(message):
            global sent
            sent += 1
            if sent == 3:
                raise failure
        asyncio.run(instrument(app)(dict(SCOPE), receive, send))
        assert shutdown().completed
        print(json.dumps({'caught_same': caught_same}))
        """,
    )
    assert result == {"caught_same": True}
    (span,) = _spans(otlp_receiver)
    assert _attributes(span)["altimeter.http.outcome"] == "completed"
    assert _attributes(span)["http.response.status_code"] == 200
    assert span.status.code == 2
    assert len(span.events) == 1
    assert _attributes(span.events[0]) == {"error.type": "after_response"}
    assert _metric_count(otlp_receiver, "altimeter.http.server.requests") == 1
    assert _metric_count(otlp_receiver, "http.server.request.duration") == 1
    for request in otlp_receiver.requests:
        assert b"CANARY" not in request.message.SerializeToString()


def test_observed_disconnect_cannot_later_count_as_completed(
    otlp_receiver: OTLPReceiver,
) -> None:
    result = _run(
        otlp_receiver,
        """
        from django_altimeter.asgi import instrument
        configure()
        async def app(scope, receive, send):
            await send({'type': 'http.response.start', 'status': 200, 'headers': []})
            assert await receive() == {'type': 'http.disconnect'}
            await send({'type': 'http.response.body', 'body': b'safe'})
        async def receive():
            return {'type': 'http.disconnect'}
        async def send(message):
            return None
        asyncio.run(instrument(app)(dict(SCOPE), receive, send))
        assert shutdown().completed
        print(json.dumps({'returned': True}))
        """,
    )
    assert result == {"returned": True}
    (span,) = _spans(otlp_receiver)
    assert _attributes(span)["altimeter.http.outcome"] == "aborted"
    assert _attributes(span)["http.response.status_code"] == 200
    assert span.status.code == 2
    assert _metric_count(otlp_receiver, "altimeter.http.server.requests") == 1
    assert _metric_count(otlp_receiver, "http.server.request.duration") == 1


def test_owned_runtime_resolves_request_tracer_once(otlp_receiver):
    result = _run(
        otlp_receiver,
        """
        from opentelemetry.sdk.trace import TracerProvider
        original = TracerProvider.get_tracer
        lookups = 0
        def counted(self, name, *args, **kwargs):
            global lookups
            if name == 'django_altimeter':
                lookups += 1
            return original(self, name, *args, **kwargs)
        TracerProvider.get_tracer = counted
        runtime = configure()
        after_setup = lookups
        async def app(scope, receive, send):
            await send({'type': 'http.response.start', 'status': 200, 'headers': []})
            await send({'type': 'http.response.body', 'body': b'safe'})
        async def receive():
            return {'type': 'http.request', 'body': b'', 'more_body': False}
        async def send(message):
            pass
        from django_altimeter.asgi import instrument
        observed = instrument(app)
        async def requests():
            for _ in range(3):
                await observed(dict(SCOPE), receive, send)
        asyncio.run(requests())
        after_requests = lookups
        stopped = shutdown()
        print(json.dumps({'after_setup': after_setup, 'after_requests': after_requests,
                         'completed': stopped.completed}))
        """,
    )
    assert len(_spans(otlp_receiver)) == 3
    assert _metric_count(otlp_receiver, "altimeter.http.server.requests") == 3
    assert result == {"after_setup": 1, "after_requests": 1, "completed": True}


@pytest.mark.parametrize("sample_rate", [0.0, 1.0])
def test_nonrecording_request_spans_do_not_build_final_attributes(
    otlp_receiver, sample_rate
):
    result = _run(
        otlp_receiver,
        """
        from opentelemetry.trace import NonRecordingSpan
        original = NonRecordingSpan.set_attributes
        enrichments = 0
        def counted(self, attributes):
            global enrichments
            enrichments += 1
            return original(self, attributes)
        NonRecordingSpan.set_attributes = counted
        configure(sample_rate=SAMPLE_RATE)
        async def app(scope, receive, send):
            await send({'type': 'http.response.start', 'status': 200, 'headers': []})
            await send({'type': 'http.response.body', 'body': b'safe'})
        async def receive():
            return {'type': 'http.request', 'body': b'', 'more_body': False}
        async def send(message):
            pass
        from django_altimeter.asgi import instrument
        observed = instrument(app)
        asyncio.run(observed(dict(SCOPE), receive, send))
        stopped = shutdown()
        print(json.dumps({'enrichments': enrichments, 'completed': stopped.completed}))
        """.replace("SAMPLE_RATE", str(sample_rate)),
    )
    assert result == {"enrichments": 0, "completed": True}
    assert len(_spans(otlp_receiver)) == int(sample_rate)
    assert _metric_count(otlp_receiver, "altimeter.http.server.requests") == 1


def test_owned_runtime_prepares_header_policy_once(otlp_receiver):
    result = _run(
        otlp_receiver,
        """
        import django_altimeter.headers as header_policy
        original = header_policy.prepare_response_header_policy
        preparations = 0
        def counted(origins):
            global preparations
            preparations += 1
            return original(origins)
        header_policy.prepare_response_header_policy = counted
        configure(frontend_origins=frozenset({'https://frontend.invalid'}))
        after_setup = preparations
        starts = []
        async def app(scope, receive, send):
            await send({'type': 'http.response.start', 'status': 200,
                        'headers': [(b'cache-control', b'private'),
                                    (b'vary', b'Accept')]})
            await send({'type': 'http.response.body', 'body': b'safe'})
        async def receive():
            return {'type': 'http.request', 'body': b'', 'more_body': False}
        async def send(message):
            if message['type'] == 'http.response.start':
                starts.append({k.decode(): v.decode() for k, v in message['headers']})
        from django_altimeter.asgi import instrument
        observed = instrument(app)
        async def requests():
            for origin in (b'https://frontend.invalid', b'https://untrusted.invalid',
                           b'https://frontend.invalid'):
                scope = dict(SCOPE, headers=[(b'origin', origin)])
                await observed(scope, receive, send)
        asyncio.run(requests())
        stopped = shutdown()
        print(json.dumps({'after_setup': after_setup, 'after_requests': preparations,
                         'completed': stopped.completed, 'headers': starts}))
        """,
    )
    assert result["completed"] is True
    assert len(_spans(otlp_receiver)) == 3
    assert _metric_count(otlp_receiver, "altimeter.http.server.requests") == 3
    headers = result["headers"]
    assert [h.get("timing-allow-origin") for h in headers] == [
        "https://frontend.invalid",
        None,
        "https://frontend.invalid",
    ]
    assert all(h["vary"] == "Accept, Origin" for h in headers)
    assert len({h["x-altimeter-trace-id"] for h in headers}) == 3
    assert all(h["x-altimeter-trace-sampled"] == "1" for h in headers)
    assert result["after_setup"] == result["after_requests"] == 1
