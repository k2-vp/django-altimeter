"""Fresh-process/native-provider/real-OTLP evidence for the public context API."""

import json
import os
import subprocess
import sys
import textwrap

import pytest

pytestmark = pytest.mark.integration
CANARY = "password=context-private-canary"

_IMPORTS = """
import asyncio
import json
import os
import sys
from collections.abc import Mapping, Sequence
from dataclasses import replace

from opentelemetry import trace
from opentelemetry.sdk.resources import Resource
from opentelemetry.sdk.trace import SpanProcessor, TracerProvider

from django_altimeter.conf import Config, SignalConfig
from django_altimeter.context import (
    Correlation, Identity, enrich, get_correlation, span,
)
from django_altimeter.privacy import PrivacyPolicy
from django_altimeter.runtime import get_owned_trace_runtime, setup, shutdown

CANARY = 'password=context-private-canary'
POLICY = PrivacyPolicy(
    span_names=frozenset({'work.execute', 'work.child', 'work.sibling'}),
    attribute_values=frozenset({
        ('service.name', 'context-test'),
        ('deployment.environment.name', 'test'),
        ('service.version', 'test-1'),
        ('altimeter.role', 'web'),
        ('http.route', '/work/{item_id}'),
        ('altimeter.operation', 'execute'),
    }),
)

def configure(resolver=None, *, enabled=True):
    config = Config(
        enabled=enabled,
        service_name='context-test',
        deployment_environment='test',
        release='test-1',
        traces=SignalConfig(
            enabled=enabled, endpoint=sys.argv[1], destination='collector',
        ),
        policy=POLICY,
        identity_resolver=resolver,
    )
    return setup(config)

class HostileSequence(Sequence):
    calls = []
    def __len__(self):
        self.calls.append('len')
        raise AssertionError('user accessor')
    def __getitem__(self, key):
        self.calls.append('getitem')
        raise AssertionError('user accessor')
    def __iter__(self):
        self.calls.append('iter')
        raise AssertionError('user accessor')
    def __str__(self):
        self.calls.append('str')
        raise AssertionError('user accessor')

class HostileMapping(Mapping):
    calls = []
    def __len__(self):
        self.calls.append('len')
        raise AssertionError('user accessor')
    def __getitem__(self, key):
        self.calls.append('getitem')
        raise AssertionError('user accessor')
    def __iter__(self):
        self.calls.append('iter')
        raise AssertionError('user accessor')

class HostileString(str):
    calls = []
    def __len__(self):
        self.calls.append('len')
        raise AssertionError('user accessor')
    def __str__(self):
        self.calls.append('str')
        raise AssertionError('user accessor')

class Subject:
    def __getattribute__(self, name):
        raise AssertionError('Subject properties must not be read by Altimeter')
"""


def _run(receiver, body):
    result = subprocess.run(
        [
            sys.executable,
            "-c",
            _IMPORTS + textwrap.dedent(body),
            receiver.url("traces"),
        ],
        env={
            key: value
            for key, value in os.environ.items()
            if not key.startswith(("ALTIMETER_", "OTEL_"))
        },
        capture_output=True,
        text=True,
        timeout=20,
        check=False,
    )
    assert result.returncode == 0, result.stdout + result.stderr
    assert CANARY not in result.stderr
    return json.loads(result.stdout)


def _spans(receiver):
    return [
        span
        for request in receiver.requests
        for resource in request.message.resource_spans
        for scope in resource.scope_spans
        for span in scope.spans
    ]


def _attributes(span):
    return {
        attribute.key: getattr(attribute.value, attribute.value.WhichOneof("value"))
        for attribute in span.attributes
    }


def test_public_span_validates_before_sdk_and_exports_safe_neighbors(otlp_receiver):
    result = _run(
        otlp_receiver,
        """
        assert 'traces' in configure().enabled_signals
        calls = []
        with span('work.execute', attributes={
            'http.route': '/work/{item_id}',
            'http.response.status_code': 200,
            'duration_ms': 1.25,
            'altimeter.operation': 'execute',
            'sequence': HostileSequence(),
            'mapping': HostileMapping(),
            'bytes': CANARY.encode(),
            'password': CANARY,
            'oversized': [1] * 1_000_000,
            'posthogDistinctId': 'forged-identity',
            'altimeter.tenant_id': 'forged-tenant',
        }) as current:
            calls.append('application')
            correlation = get_correlation()
            assert correlation is not None
            assert current.attributes['http.response.status_code'] == 200
            assert current.attributes['password'] == CANARY
            for key in (
                'sequence', 'mapping', 'bytes', 'oversized',
                'posthogDistinctId', 'altimeter.tenant_id',
            ):
                assert key not in current.attributes, key
            native = current.get_span_context()
            assert correlation.trace_id == trace.format_trace_id(native.trace_id)
            assert correlation.span_id == trace.format_span_id(native.span_id)
        assert get_correlation() is None
        assert calls == ['application']
        assert HostileSequence.calls == [] and HostileMapping.calls == []
        assert shutdown().completed
        print(json.dumps({
            'trace_id': correlation.trace_id, 'span_id': correlation.span_id,
        }))
        """,
    )
    spans = _spans(otlp_receiver)
    assert len(spans) == 1
    assert spans[0].name == "work.execute"
    assert spans[0].trace_id.hex() == result["trace_id"]
    assert spans[0].span_id.hex() == result["span_id"]
    assert spans[0].end_time_unix_nano >= spans[0].start_time_unix_nano > 0
    assert _attributes(spans[0]) == {
        "http.route": "/work/{item_id}",
        "http.response.status_code": 200,
        "duration_ms": 1.25,
        "altimeter.operation": "execute",
    }
    for request in otlp_receiver.requests:
        assert CANARY.encode() not in request.message.SerializeToString()
        assert request.headers["Accept-Encoding"] == "identity"


def test_async_contexts_are_isolated_and_unknown_names_preserve_parent(otlp_receiver):
    result = _run(
        otlp_receiver,
        """
        assert 'traces' in configure().enabled_signals
        async def run():
            ready = 0
            both_ready = asyncio.Event()
            async def work(name):
                nonlocal ready
                attributes = {'http.response.status_code': 200}
                with span(name, attributes=attributes) as parent:
                    before = get_correlation()
                    ready += 1
                    if ready == 2:
                        both_ready.set()
                    await both_ready.wait()
                    await asyncio.sleep(0)
                    assert get_correlation() == before
                    with span(
                        'SELECT token FROM private', attributes=HostileMapping(),
                    ) as denied:
                        assert denied is trace.INVALID_SPAN
                        assert trace.get_current_span() is parent
                        assert get_correlation() == before
                    with span(HostileString('work.child')) as denied:
                        assert denied is trace.INVALID_SPAN
                    with span('work.child'):
                        child = get_correlation()
                        assert child.trace_id == before.trace_id
                        assert child.span_id != before.span_id
                    assert get_correlation() == before
                    return before.trace_id, before.span_id
            roots = await asyncio.gather(work('work.execute'), work('work.sibling'))
            assert roots[0][0] != roots[1][0]
            assert get_correlation() is None
            return roots
        roots = asyncio.run(run())
        assert HostileMapping.calls == [] and HostileString.calls == []
        assert shutdown().completed
        print(json.dumps(roots))
        """,
    )
    spans = _spans(otlp_receiver)
    assert len(spans) == 4
    for trace_id, span_id in result:
        family = [span for span in spans if span.trace_id.hex() == trace_id]
        assert len(family) == 2
        child = next(span for span in family if span.name == "work.child")
        assert child.parent_span_id.hex() == span_id


@pytest.mark.parametrize(
    "exception_type", ["RuntimeError", "BaseException", "asyncio.CancelledError"]
)
def test_application_exception_identity_and_context_survive_cleanup(
    otlp_receiver, exception_type
):
    _run(
        otlp_receiver,
        f"""
        configure()
        class ApplicationFailure({exception_type}):
            def __str__(self):
                raise AssertionError('Exception prose must never be formatted')
        failure = ApplicationFailure(CANARY)
        async def work():
            try:
                with span('work.execute'):
                    raise failure
            except BaseException as caught:
                assert caught is failure
            else:
                raise AssertionError('Application exception was suppressed')
            assert get_correlation() is None
            with span('work.sibling', attributes={{'http.response.status_code': 200}}):
                assert get_correlation() is not None
        asyncio.run(work())
        assert shutdown().completed
        print(json.dumps(True))
        """,
    )
    spans = _spans(otlp_receiver)
    assert len(spans) == 2
    assert all(not span.events and not span.status.message for span in spans)
    assert all(CANARY.encode() not in span.SerializeToString() for span in spans)
    safe = next(span for span in spans if span.name == "work.sibling")
    assert _attributes(safe) == {"http.response.status_code": 200}


@pytest.mark.parametrize("enabled", [False, True], ids=["disabled", "foreign-provider"])
def test_unowned_helpers_leave_foreign_context_untouched(otlp_receiver, enabled):
    _run(
        otlp_receiver,
        f"""
        foreign = TracerProvider(resource=Resource({{}}), shutdown_on_exit=False)
        trace.set_tracer_provider(foreign)
        resolver_calls = []
        def resolver(subject):
            resolver_calls.append('called')
            raise AssertionError('Unowned resolver was called')
        configure(resolver, enabled={enabled!r})
        tracer = foreign.get_tracer('foreign')
        with tracer.start_as_current_span(
            'foreign', attributes={{'foreign': 'retained'}},
        ) as parent:
            expected = parent.get_span_context()
            assert get_correlation() is None
            enrich(Subject())
            with span('work.execute', attributes=HostileMapping()) as child:
                assert child is trace.INVALID_SPAN
                assert get_correlation() is None
                assert trace.get_current_span() is parent
                assert trace.get_current_span().get_span_context() == expected
            assert parent.attributes == {{'foreign': 'retained'}}
        assert trace.get_tracer_provider() is foreign
        assert resolver_calls == [] and HostileMapping.calls == []
        shutdown()
        foreign.shutdown()
        print(json.dumps(True))
        """,
    )
    assert otlp_receiver.requests == ()


def test_enrichment_uses_only_valid_trusted_identity_fields(otlp_receiver):
    expected = _run(
        otlp_receiver,
        """
        subject = Subject()
        resolver_calls = []
        identity = None
        def resolver(value):
            assert value is subject
            resolver_calls.append('called')
            return identity
        configure(resolver)
        enrich(subject)
        assert resolver_calls == []
        rows = []
        candidates = [
            (
                Identity('opaque-person', 'opaque-tenant'),
                {'posthogDistinctId': 'opaque-person',
                 'altimeter.tenant_id': 'opaque-tenant'},
            ),
            (
                Identity('x' * 200, '\U0001f600' * 50),
                {'posthogDistinctId': 'x' * 200,
                 'altimeter.tenant_id': '\U0001f600' * 50},
            ),
            (Identity('', 'safe'), {'altimeter.tenant_id': 'safe'}),
            (Identity(' \t', 'safe'), {'altimeter.tenant_id': 'safe'}),
            (Identity('x' * 201, 'safe'), {'altimeter.tenant_id': 'safe'}),
            (Identity('\U0001f600' * 51, 'safe'), {'altimeter.tenant_id': 'safe'}),
            (Identity(HostileString('p'), 'safe'), {'altimeter.tenant_id': 'safe'}),
            (Identity(HostileSequence(), 'safe'), {'altimeter.tenant_id': 'safe'}),
            (Identity('safe', 7), {'posthogDistinctId': 'safe'}),
            (HostileMapping(), {}),
            (None, {}),
        ]
        for identity, fields in candidates:
            attributes = {'http.response.status_code': 200}
            with span('work.execute', attributes=attributes) as current:
                enrich(subject)
                assert dict(current.attributes) == {**attributes, **fields}
                rows.append(fields)
        assert len(resolver_calls) == len(candidates)
        assert HostileString.calls == []
        assert HostileSequence.calls == [] and HostileMapping.calls == []
        assert shutdown().completed
        print(json.dumps(rows))
        """,
    )
    spans = _spans(otlp_receiver)
    assert len(spans) == len(expected)
    for span, identity in zip(spans, expected, strict=True):
        assert _attributes(span) == {"http.response.status_code": 200, **identity}
    for request in otlp_receiver.requests:
        for resource in request.message.resource_spans:
            assert not {a.key for a in resource.resource.attributes} & {
                "posthogDistinctId",
                "altimeter.tenant_id",
            }


def test_resolver_failure_is_bounded_and_a_later_enrichment_succeeds(otlp_receiver):
    _run(
        otlp_receiver,
        """
        failing = True
        def resolver(subject):
            if failing:
                raise RuntimeError(CANARY)
            return Identity('opaque-person', 'opaque-tenant')
        configure(resolver)
        for name in ('work.execute', 'work.sibling'):
            with span(name, attributes={'http.response.status_code': 200}):
                enrich(Subject())
            failing = False
        assert shutdown().completed
        print(json.dumps(True))
        """,
    )
    spans = _spans(otlp_receiver)
    assert len(spans) == 2
    assert _attributes(spans[0]) == {"http.response.status_code": 200}
    assert _attributes(spans[1]) == {
        "http.response.status_code": 200,
        "posthogDistinctId": "opaque-person",
        "altimeter.tenant_id": "opaque-tenant",
    }


def test_native_processor_failures_never_replace_application_behavior(otlp_receiver):
    _run(
        otlp_receiver,
        """
        import logging
        configure()
        class BrokenHandler(logging.Handler):
            def emit(self, record):
                raise RuntimeError(CANARY)
        logging.getLogger('django_altimeter.context').addHandler(BrokenHandler())
        class UnreliableProcessor(SpanProcessor):
            fail_start = True
            fail_end = False
            def on_start(self, span, parent_context=None):
                if self.fail_start:
                    raise RuntimeError(CANARY)
            def on_end(self, span):
                if self.fail_end:
                    raise RuntimeError(CANARY)
        processor = UnreliableProcessor()
        get_owned_trace_runtime().tracer_provider.add_span_processor(processor)
        calls = []
        with span('work.execute') as refused:
            assert refused is trace.INVALID_SPAN
            calls.append('start-failed-body')
        processor.fail_start = False
        processor.fail_end = True
        failure = ValueError(CANARY)
        try:
            with span('work.execute'):
                calls.append('end-failed-body')
                raise failure
        except ValueError as caught:
            assert caught is failure
        else:
            raise AssertionError('Application exception was suppressed')
        assert get_correlation() is None
        processor.fail_end = False
        with span('work.sibling', attributes={'http.response.status_code': 200}):
            calls.append('healthy-body')
        assert calls == ['start-failed-body', 'end-failed-body', 'healthy-body']
        assert shutdown().completed
        print(json.dumps(True))
        """,
    )
    spans = _spans(otlp_receiver)
    assert [span.name for span in spans] == ["work.execute", "work.sibling"]
    assert all(CANARY.encode() not in span.SerializeToString() for span in spans)


def test_inherited_context_is_not_disclosed_or_enriched_after_fork(otlp_receiver):
    _run(
        otlp_receiver,
        """
        def resolver(subject):
            raise AssertionError('Inherited child resolver was called')
        configure(resolver)
        with span('work.execute') as parent:
            assert get_correlation() is not None
            read_fd, write_fd = os.pipe()
            pid = os.fork()
            if pid == 0:
                os.close(read_fd)
                try:
                    assert get_correlation() is None
                    enrich(Subject())
                    with span('work.child', attributes=HostileMapping()) as child:
                        assert child is trace.INVALID_SPAN
                        assert trace.get_current_span() is parent
                    assert HostileMapping.calls == []
                    os.write(write_fd, b'ok')
                    os._exit(0)
                except BaseException:
                    os._exit(1)
            os.close(write_fd)
            assert os.read(read_fd, 10) == b'ok'
            os.close(read_fd)
            assert os.waitpid(pid, 0)[1] == 0
            assert get_correlation() is not None
        assert shutdown().completed
        print(json.dumps(True))
        """,
    )
    assert [span.name for span in _spans(otlp_receiver)] == ["work.execute"]


def test_rejected_inputs_are_released_before_the_application_body(otlp_receiver):
    _run(
        otlp_receiver,
        """
        import weakref
        configure()
        payload = HostileSequence()
        reference = weakref.ref(payload)
        attributes = {'bad': payload, 'http.response.status_code': 200}
        del payload
        with span('work.execute', attributes=attributes) as current:
            del attributes
            assert reference() is None, 'Rejected input retained across the body'
            assert current.attributes == {'http.response.status_code': 200}
        assert HostileSequence.calls == []
        assert shutdown().completed
        print(json.dumps(True))
        """,
    )
    spans = _spans(otlp_receiver)
    assert len(spans) == 1
    assert _attributes(spans[0]) == {"http.response.status_code": 200}
