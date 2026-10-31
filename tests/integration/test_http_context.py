"""Native HTTP context contracts, isolated processes and decoded real OTLP."""

import json
import os
import subprocess
import sys
import textwrap

import pytest
from support.otlp_receiver import OTLPReceiver

_IMPORTS = """
import asyncio, json, os
from dataclasses import replace
from opentelemetry import baggage, context, trace
from django_altimeter.conf import Config, SignalConfig
from django_altimeter.context import get_correlation
from django_altimeter.privacy import DEFAULT_POLICY
from django_altimeter.runtime import setup, shutdown

def configure(**changes):
    def signal(name):
        return SignalConfig(True, os.environ['TEST_ENDPOINT'] + '/v1/' + name,
                            destination='collector')
    config = replace(Config(
        enabled=True, service_name='http-context', deployment_environment='test',
        release='v1', integrations=frozenset({'asgi', 'wsgi'}),
        traces=signal('traces'), metrics=signal('metrics'), export_timeout=0.5,
    ), **changes)
    assert setup(config).ok

SCOPE = {'type': 'http', 'method': 'GET', 'path': '/safe/', 'headers': []}
ENVIRON = {'REQUEST_METHOD': 'GET', 'PATH_INFO': '/safe/'}
"""


def _run(receiver: OTLPReceiver, source: str) -> dict:
    environment = {
        key: value
        for key, value in os.environ.items()
        if not key.startswith(("ALTIMETER_", "OTEL_"))
    }
    result = subprocess.run(
        [sys.executable, "-c", _IMPORTS + textwrap.dedent(source)],
        env={**environment, "TEST_ENDPOINT": receiver.endpoint},
        capture_output=True,
        text=True,
        timeout=15,
        check=False,
    )
    assert result.returncode == 0, result.stderr
    return json.loads(result.stdout)


def _spans(receiver: OTLPReceiver) -> list:
    return [
        span
        for request in receiver.requests
        if request.path == "/v1/traces"
        for resource in request.message.resource_spans
        for scope in resource.scope_spans
        for span in scope.spans
    ]


def _count(receiver: OTLPReceiver) -> int:
    return sum(
        point.as_int
        for request in receiver.requests
        if request.path == "/v1/metrics"
        for resource in request.message.resource_metrics
        for scope in resource.scope_metrics
        for metric in scope.metrics
        if metric.name == "altimeter.http.server.requests"
        for point in metric.sum.data_points
    )


@pytest.mark.parametrize("protocol", ["asgi", "wsgi"])
@pytest.mark.parametrize("tracing", ["foreign", "disabled"])
def test_metrics_only_preserves_foreign_parent_and_baggage(
    otlp_receiver: OTLPReceiver, protocol: str, tracing: str
) -> None:
    result = _run(
        otlp_receiver,
        f"        PROTOCOL, TRACING = {protocol!r}, {tracing!r}\n"
        + """
        from opentelemetry.sdk.trace import TracerProvider
        from opentelemetry.sdk.trace.export import SimpleSpanProcessor
        from opentelemetry.exporter.otlp.proto.http.trace_exporter import (
            OTLPSpanExporter,
        )
        from django_altimeter import asgi, wsgi
        from django_altimeter.http import request_state
        foreign = TracerProvider(shutdown_on_exit=False)
        foreign.add_span_processor(SimpleSpanProcessor(OTLPSpanExporter(
            endpoint=os.environ['TEST_ENDPOINT'] + '/v1/traces', timeout=0.5)))
        tracer = foreign.get_tracer('foreign')
        if TRACING == 'foreign':
            trace.set_tracer_provider(foreign)
            configure()
        else:
            configure(traces=SignalConfig())
        seen = []
        headers = []
        bodies = []
        def inspect(stage):
            current = trace.get_current_span().get_span_context()
            with tracer.start_as_current_span('child') as child:
                parent = child.parent
            seen.append({'stage': stage, 'same_parent': current == expected,
                         'baggage': baggage.get_baggage('business'),
                         'child_parent': parent == expected,
                         'correlation': get_correlation() is not None})
        async def async_app(scope, receive, send):
            inspect('app')
            await send({'type': 'http.response.start', 'status': 200, 'headers': []})
            await send({'type': 'http.response.body', 'body': b'safe'})
        async def receive():
            return {'type': 'http.request', 'body': b''}
        async def send(message):
            if message['type'] == 'http.response.start':
                headers.extend(name.decode().lower() for name, _ in message['headers'])
            elif message['type'] == 'http.response.body':
                bodies.append(message['body'].decode())
        class Body:
            def __iter__(self):
                inspect('iter')
                self.sent = False
                return self
            def __next__(self):
                inspect('next')
                if self.sent:
                    raise StopIteration
                self.sent = True
                return b'safe'
            def close(self):
                inspect('close')
        def sync_app(environ, start_response):
            inspect('app')
            write = start_response('200 OK', [])
            write(b'')
            return Body()
        def start_response(status, values, exc_info=None):
            inspect('start_response')
            headers.extend(name.lower() for name, _ in values)
            def write(body):
                inspect('write')
            return write
        baggage_token = context.attach(baggage.set_baggage('business', 'kept'))
        with tracer.start_as_current_span('parent') as parent:
            expected = parent.get_span_context()
            if PROTOCOL == 'asgi':
                asyncio.run(asgi.instrument(async_app)(dict(SCOPE), receive, send))
            else:
                body = wsgi.instrument(sync_app)(dict(ENVIRON), start_response)
                try:
                    bodies.extend(chunk.decode() for chunk in body)
                finally:
                    body.close()
            assert trace.get_current_span() is parent
            assert baggage.get_baggage('business') == 'kept'
            assert request_state.get() is None
        context.detach(baggage_token)
        assert not trace.get_current_span().get_span_context().is_valid
        assert baggage.get_baggage('business') is None
        assert shutdown().completed
        foreign.shutdown()
        print(json.dumps({'seen': seen, 'headers': headers, 'bodies': bodies,
                          'parent_span': format(expected.span_id, '016x')}))
        """,
    )
    assert result["bodies"] == ["safe"]
    assert result["seen"]
    for observation in result["seen"]:
        assert observation == {
            "stage": observation["stage"],
            "same_parent": True,
            "baggage": "kept",
            "child_parent": True,
            "correlation": False,
        }
    assert "x-altimeter-trace-id" not in result["headers"]
    spans = _spans(otlp_receiver)
    assert len(spans) == len(result["seen"]) + 1
    children = [span for span in spans if span.name == "child"]
    assert len(children) == len(result["seen"])
    assert all(span.parent_span_id.hex() == result["parent_span"] for span in children)
    assert _count(otlp_receiver) == 1


@pytest.mark.parametrize("protocol", ["asgi", "wsgi"])
@pytest.mark.parametrize("fail", [False, True])
def test_excluded_requests_do_not_export_orphan_children(
    otlp_receiver: OTLPReceiver, protocol: str, fail: bool
) -> None:
    result = _run(
        otlp_receiver,
        f"        PROTOCOL, FAIL = {protocol!r}, {fail!r}\n"
        + """
        from django_altimeter import asgi, wsgi
        from django_altimeter.context import client_span, span
        from django_altimeter.http import request_state
        configure(excluded_routes=frozenset({'/health/'}))
        seen, bodies, headers = [], [], []
        failure = ValueError('CANARY_FAILURE')
        def observe(path, stage):
            with client_span(
                'SELECT', attributes={'db.statement': 'CANARY_SQL'}
            ) as child:
                seen.append([path, stage, child.is_recording(),
                             request_state.get() is None])
        def outside():
            assert request_state.get() is None
            assert get_correlation() is None
            assert not trace.get_current_span().get_span_context().is_valid
        async def async_app(scope, receive, send):
            path = scope['path']
            observe(path, 'app')
            await send({'type': 'http.response.start', 'status': 200,
                        'headers': [(b'cache-control', b'private')]})
            with span('SPAN'):
                observe(path, 'first')
                await send({'type': 'http.response.body', 'body': b'first',
                            'more_body': True})
                await asyncio.sleep(0)
                observe(path, 'last')
                if FAIL:
                    raise failure
                await send({'type': 'http.response.body', 'body': b'last'})
        async def receive():
            return {'type': 'http.request', 'body': b''}
        async def send(message):
            if message['type'] == 'http.response.start':
                headers.append([name.decode().lower()
                                for name, _ in message['headers']])
            else:
                bodies.append(message['body'].decode())
        class Body:
            def __init__(self, path, write):
                self.path = path
                self.stream = self.chunks(write)
            def __iter__(self):
                observe(self.path, 'iter')
                return self
            def __next__(self):
                return next(self.stream)
            def chunks(self, write):
                with span('SPAN'):
                    observe(self.path, 'first')
                    yield b'first'
                    observe(self.path, 'last')
                    write(b'')
                    if FAIL:
                        raise failure
                    yield b'last'
            def close(self):
                observe(self.path, 'close')
                self.stream.close()
        def sync_app(environ, start_response):
            path = environ['PATH_INFO']
            observe(path, 'app')
            write = start_response('200 OK', [('Cache-Control', 'private')])
            return Body(path, write)
        def start_response(status, values, exc_info=None):
            headers.append([name.lower() for name, _ in values])
            return lambda data: None
        observed = (asgi.instrument(async_app) if PROTOCOL == 'asgi'
                    else wsgi.instrument(sync_app))
        if PROTOCOL == 'asgi':
            async def call(path):
                try:
                    await observed({**SCOPE, 'path': path}, receive, send)
                except ValueError as error:
                    assert FAIL and error is failure
                else:
                    assert not FAIL
                outside()
            async def run():
                await asyncio.gather(call('/health/'), call('/safe/'))
            asyncio.run(run())
        else:
            responses = [observed({**ENVIRON, 'PATH_INFO': path}, start_response)
                         for path in ('/health/', '/safe/')]
            outside()
            # Interleave operations, including a manual span held across yield.
            for body in responses:
                bodies.append(next(body).decode())
                outside()
            for body in responses:
                try:
                    while True:
                        bodies.append(next(body).decode())
                        outside()
                except StopIteration:
                    assert not FAIL
                except ValueError as error:
                    assert FAIL and error is failure
                finally:
                    body.close()
                    outside()
        outside()
        assert observed._active == 0
        assert shutdown().completed
        print(json.dumps({'seen': seen, 'bodies': bodies, 'headers': headers}))
        """,
    )
    assert result["bodies"].count("first") == 2
    assert result["bodies"].count("last") == (0 if fail else 2)
    assert "x-altimeter-trace-id" not in result["headers"][0]
    assert "x-altimeter-trace-id" in result["headers"][1]
    assert {entry[1] for entry in result["seen"]} == (
        {"app", "first", "last"}
        if protocol == "asgi"
        else {"app", "iter", "first", "last", "close"}
    )
    for path, stage, recording, no_state in result["seen"]:
        assert recording is (path == "/safe/"), (path, stage)
        assert no_state is (path == "/health/"), (path, stage)
    spans = _spans(otlp_receiver)
    (server,) = [record for record in spans if record.kind == 2]
    (manual,) = [record for record in spans if record.name == "SPAN"]
    children = [record for record in spans if record.kind == 3]
    assert len(children) == sum(entry[0] == "/safe/" for entry in result["seen"])
    assert len(spans) == len(children) + 2
    assert all(record.trace_id == server.trace_id for record in spans)
    assert manual.parent_span_id == server.span_id
    assert all(
        record.parent_span_id in {server.span_id, manual.span_id} for record in children
    )
    assert _count(otlp_receiver) == 1
    for request in otlp_receiver.requests:
        assert b"CANARY" not in request.message.SerializeToString()


@pytest.mark.parametrize("protocol", ["asgi", "wsgi"])
@pytest.mark.parametrize("foreign_global", [False, True])
def test_excluded_unowned_requests_leave_foreign_context_untouched(
    otlp_receiver: OTLPReceiver, protocol: str, foreign_global: bool
) -> None:
    result = _run(
        otlp_receiver,
        f"        PROTOCOL, FOREIGN_GLOBAL = {protocol!r}, {foreign_global!r}\n"
        + """
        from opentelemetry.sdk.trace import TracerProvider
        from opentelemetry.sdk.trace.export import SimpleSpanProcessor
        from opentelemetry.exporter.otlp.proto.http.trace_exporter import (
            OTLPSpanExporter,
        )
        from django_altimeter import asgi, wsgi
        from django_altimeter.http import request_state
        foreign = TracerProvider(shutdown_on_exit=False)
        foreign.add_span_processor(SimpleSpanProcessor(OTLPSpanExporter(
            endpoint=os.environ['TEST_ENDPOINT'] + '/v1/traces', timeout=0.5)))
        tracer = foreign.get_tracer('foreign')
        if FOREIGN_GLOBAL:
            trace.set_tracer_provider(foreign)
            configure(excluded_routes=frozenset({'/health/'}))
        else:
            configure(traces=SignalConfig(), excluded_routes=frozenset({'/health/'}))
        def inspect():
            assert trace.get_current_span() is parent
            assert baggage.get_baggage('business') == 'kept'
            assert get_correlation() is None
            assert request_state.get() is None
            with tracer.start_as_current_span('child') as child:
                assert child.parent == parent.get_span_context()
        async def async_app(scope, receive, send):
            inspect()
            await send({'type': 'http.response.start', 'status': 200, 'headers': []})
            await send({'type': 'http.response.body', 'body': b'safe'})
        async def receive():
            return {'type': 'http.request', 'body': b''}
        async def send(message):
            pass
        def sync_app(environ, start_response):
            inspect()
            start_response('200 OK', [])
            return iter([b'safe'])
        token = context.attach(baggage.set_baggage('business', 'kept'))
        with tracer.start_as_current_span('parent') as parent:
            if PROTOCOL == 'asgi':
                asyncio.run(asgi.instrument(async_app)(
                    {**SCOPE, 'path': '/health/'}, receive, send))
            else:
                result = wsgi.instrument(sync_app)(
                    {**ENVIRON, 'PATH_INFO': '/health/'}, lambda *args: None)
                assert list(result) == [b'safe']
            assert trace.get_current_span() is parent
            assert baggage.get_baggage('business') == 'kept'
        context.detach(token)
        assert shutdown().completed
        foreign.shutdown()
        print(json.dumps({'restored': not trace.get_current_span().is_recording()}))
        """,
    )
    assert result == {"restored": True}
    spans = _spans(otlp_receiver)
    assert sorted(record.name for record in spans) == ["child", "parent"]
    (child,) = [record for record in spans if record.name == "child"]
    (parent,) = [record for record in spans if record.name == "parent"]
    assert child.parent_span_id == parent.span_id
    assert _count(otlp_receiver) == 0


@pytest.mark.parametrize("close_fails", [False, True])
def test_excluded_native_file_keeps_identity_and_suppresses_close_children(
    otlp_receiver: OTLPReceiver, close_fails: bool
) -> None:
    result = _run(
        otlp_receiver,
        f"        CLOSE_FAILS = {close_fails!r}\n"
        + """
        from django_altimeter.wsgi import instrument
        from django_altimeter.context import client_span
        from django_altimeter.http import request_state
        configure(excluded_routes=frozenset({'/health/'}))
        seen = []
        failure = OSError('CANARY_CLOSE_FAILURE')
        def observe(path, stage):
            with client_span('SELECT') as child:
                seen.append([path, stage, child.is_recording()])
        class FileWrapper:
            def __init__(self, path):
                self.path = path
                self.closed = 0
            def __iter__(self):
                raise AssertionError('adapter must preserve native file dispatch')
            def close(self):
                self.closed += 1
                observe(self.path, 'close')
                if CLOSE_FAILS:
                    raise failure
        def app(environ, start_response):
            path = environ['PATH_INFO']
            observe(path, 'app')
            start_response('200 OK', [])
            return environ['body']
        observed = instrument(app)
        for path in ('/health/', '/safe/'):
            original = FileWrapper(path)
            result = observed({**ENVIRON, 'PATH_INFO': path, 'body': original,
                               'wsgi.file_wrapper': FileWrapper}, lambda *args: None)
            assert result is original
            assert request_state.get() is None
            assert not trace.get_current_span().get_span_context().is_valid
            try:
                result.close()
            except OSError as error:
                assert CLOSE_FAILS and error is failure
            else:
                assert not CLOSE_FAILS
            result.close()
            assert original.closed == 1
            assert request_state.get() is None
            assert not trace.get_current_span().get_span_context().is_valid
        assert observed._active == 0
        assert shutdown().completed
        print(json.dumps({'seen': seen}))
        """,
    )
    assert result == {
        "seen": [
            ["/health/", "app", False],
            ["/health/", "close", False],
            ["/safe/", "app", True],
            ["/safe/", "close", True],
        ]
    }
    spans = _spans(otlp_receiver)
    (server,) = [record for record in spans if record.kind == 2]
    children = [record for record in spans if record.kind == 3]
    assert len(spans) == 3
    assert len(children) == 2
    assert all(record.parent_span_id == server.span_id for record in children)
    assert _count(otlp_receiver) == 1
