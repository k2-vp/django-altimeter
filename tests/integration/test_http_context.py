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
