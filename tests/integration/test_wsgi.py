"""Native WSGI protocol behavior through a wheel-installed gunicorn gthread."""

import json
import os
import socket
import struct
import subprocess
from concurrent.futures import ThreadPoolExecutor
from contextlib import contextmanager
from http.client import HTTPConnection, IncompleteRead
from time import monotonic, sleep

import pytest
from support.otlp_receiver import OTLPReceiver

_APP = r"""
import importlib.util
import json
import os
import sys
import time
from dataclasses import replace
from pathlib import Path
from threading import Barrier, Lock
from opentelemetry import trace
from django_altimeter.conf import Config, SignalConfig
from django_altimeter.context import get_correlation
from django_altimeter.http import get_request_state
from django_altimeter.privacy import DEFAULT_POLICY
from django_altimeter.runtime import setup

mode = os.environ['PROTOCOL_MODE']
paths = ('/normal', '/write', '/exc_info', '/late_exc_info', '/iter_error',
         '/close_error', '/iterator_start_error', '/app_error', '/empty', '/early')
config = Config(
    enabled=mode != 'disabled',
    service_name='' if mode == 'invalid' else 'wsgi-protocol',
    deployment_environment='test', release='v1',
    integrations=frozenset({'wsgi'}),
    traces=SignalConfig(True, os.environ['RECEIVER_ENDPOINT'] + '/v1/traces',
                        destination='collector'),
    metrics=SignalConfig(True, os.environ['RECEIVER_ENDPOINT'] + '/v1/metrics',
                         destination='collector'),
    export_timeout=0.5, excluded_routes=frozenset({'/health/'}),
    sample_rate=float(os.environ.get('PROTOCOL_SAMPLE_RATE', '1')),
    policy=replace(DEFAULT_POLICY,
        attribute_values=frozenset(('http.route', path) for path in paths)),
)
setup_result = setup(config)
assert setup_result.ok == (mode == 'enabled'), setup_result
lock = Lock()
barrier = Barrier(2)
failures = {path: RuntimeError('CANARY_WSGI_PRIVATE') for path in paths}


def record(stage, path, **extra):
    correlation = get_correlation()
    state = get_request_state()
    data = {'stage': stage, 'path': path, 'time_ns': time.time_ns(),
            'trace': None if correlation is None else correlation.trace_id,
            'span': None if correlation is None else correlation.span_id,
            'state': state is not None,
            'active': None if state is None else state.concurrent_at_start,
            **extra}
    with lock:
        with open(os.environ['PROTOCOL_EVENTS_FILE'], 'a') as output:
            output.write(json.dumps(data) + '\n')


class Body:
    def __init__(self, path, write):
        self.path, self.write, self.index = path, write, 0
    def __iter__(self):
        record('inside_iter', self.path)
        if self.path == '/iterator_start_error':
            raise failures[self.path]
        return self
    def __next__(self):
        record('inside_next', self.path)
        self.index += 1
        if self.path == '/empty':
            raise StopIteration
        if self.path == '/iter_error' and self.index == 2:
            raise failures[self.path]
        if self.path == '/early':
            if self.index > 10000:
                raise AssertionError('Expected an early client disconnect')
            time.sleep(0.005)
            return b'x' * 16384
        if self.path == '/write':
            if self.index == 1:
                assert self.write(b'middle') is None
                return b'tail'
            raise StopIteration
        if self.index > 2:
            raise StopIteration
        return b'first' if self.index == 1 else b'last'
    def close(self):
        record('inside_close', self.path)
        time.sleep(float(os.environ.get('PROTOCOL_CLOSE_DELAY', '0')))
        if self.path == '/close_error':
            raise failures[self.path]


def app(environ, start_response):
    path = environ['PATH_INFO']
    if path == '/health/':
        start_response('200 OK', [('Content-Length', '11')])
        return [b'{"ok":true}']
    record('inside_app', path)
    if environ.get('QUERY_STRING') == 'barrier=1':
        barrier.wait(timeout=3)
    state = get_request_state()
    if state is not None:
        state.route = path
        state.response_mode = 'streaming'
        state.response_ready_at = time.monotonic()
    if path == '/app_error':
        raise failures[path]
    write = start_response('200 OK', [('Cache-Control', 'private'),
                                     ('X-Business', 'kept')])
    if path == '/write':
        assert write(b'prefix') is None
    if path in {'/exc_info', '/late_exc_info'}:
        if path == '/late_exc_info':
            write(b'prefix')
        try:
            raise failures[path]
        except RuntimeError:
            original = sys.exc_info()
            environ['test.exc_info'] = original
            try:
                start_response('503 Safe', [('Cache-Control', 'private')], original)
            except BaseException as error:
                record('exc_info_error', path, same_exception=error is failures[path])
                raise
    return Body(path, write)


# The uninstrumented installed parent is an observable red baseline, not a skip.
# Every enabled test below still requires decoded spans and metrics.
if importlib.util.find_spec('django_altimeter.wsgi') is None:
    observed = other_branch = app
else:
    from django_altimeter import wsgi
    observed = wsgi.instrument(wsgi.instrument(app))
    other_branch = (wsgi.instrument(app, surface='mcp')
                    if os.environ.get('PROTOCOL_SEPARATE_BRANCHES') == '1'
                    else observed)
    if os.environ.get('PROTOCOL_FINALIZER_FAILURE') == '1':
        def failed_finalizer(span, state, **kwargs):
            record('telemetry_failure', state.route)
            raise RuntimeError('CANARY_FINALIZER')
        wsgi.finish_request = failed_finalizer


class Outside:
    def __init__(self, iterable, path):
        self.iterable, self.path, self.iterator = iterable, path, None
    def __iter__(self):
        try:
            self.iterator = iter(self.iterable)
        except BaseException as error:
            record('iteration_error', self.path,
                   same_exception=error is failures[self.path])
            raise
        return self
    def __next__(self):
        record('outside_before_next', self.path)
        try:
            return next(self.iterator)
        except StopIteration:
            raise
        except BaseException as error:
            record('iteration_error', self.path,
                   same_exception=error is failures[self.path])
            raise
        finally:
            record('outside_after_next', self.path)
    def close(self):
        try:
            close = getattr(self.iterable, 'close', None)
            if close is not None:
                close()
        except BaseException as error:
            record('close_error', self.path,
                   same_exception=error is failures[self.path])
            raise
        finally:
            record('outside_after_close', self.path)


def application(environ, start_response):
    path = environ['PATH_INFO']
    if path == '/health/':
        return observed(environ, start_response)
    record('outside_before_app', path)
    def forwarded(status, headers, *args):
        record('inside_start_response', path,
               exc_info_same=not args or args[0] is environ['test.exc_info'])
        write = start_response(status, headers, *args)
        def forwarded_write(data):
            record('inside_write', path)
            return write(data)
        return forwarded_write
    try:
        branch = other_branch if path == '/write' else observed
        result = branch(environ, forwarded)
    except BaseException as error:
        record('application_error', path, same_exception=error is failures[path])
        raise
    finally:
        record('outside_after_app', path)
    return Outside(result, path)
"""


def _spans(receiver: OTLPReceiver):
    return [
        span
        for request in receiver.requests
        if request.path == "/v1/traces"
        for resource in request.message.resource_spans
        for scope in resource.scope_spans
        for span in scope.spans
    ]


def _attributes(record):
    return {
        item.key: getattr(item.value, item.value.WhichOneof("value"))
        for item in record.attributes
    }


def _metric_count(receiver, name):
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


@pytest.fixture(scope="module")
def protocol_wheel(tmp_path_factory):
    from support.gunicorn_server import install_wheel

    return install_wheel(tmp_path_factory.mktemp("wsgi-protocol-wheel"))


@contextmanager
def _server(tmp_path, wheel, receiver, mode, **extra):
    from support.gunicorn_server import running_server

    module = tmp_path / "protocol_app.py"
    module.write_text(_APP)
    with running_server(
        tmp_path / "server",
        wheel=wheel,
        receiver=receiver,
        application="protocol_app:application",
        module_path=tmp_path,
        enabled=mode != "disabled",
        threads=2,
        extra_env={
            "PROTOCOL_MODE": mode,
            "PROTOCOL_EVENTS_FILE": str(tmp_path / "events.jsonl"),
            **extra,
        },
    ) as server:
        yield server


def _events(tmp_path):
    path = tmp_path / "events.jsonl"
    return [json.loads(line) for line in path.read_text().splitlines()]


def _request(server, path):
    connection = HTTPConnection("127.0.0.1", server.port, timeout=5)
    try:
        connection.request("GET", path)
        response = connection.getresponse()
        try:
            body = response.read()
            complete = True
        except IncompleteRead as error:
            body, complete = error.partial, False
        return response.status, body, complete
    finally:
        connection.close()


@pytest.mark.parametrize("mode", ["enabled", "disabled", "invalid"])
@pytest.mark.parametrize(
    ("path", "status", "body"),
    [
        ("/normal", 200, b"firstlast"),
        ("/write", 200, b"prefixmiddletail"),
        ("/exc_info", 503, b"firstlast"),
        ("/empty", 200, b""),
        ("/close_error", 200, b"firstlast"),
    ],
)
def test_real_gunicorn_preserves_response_and_scopes_each_operation(
    tmp_path, protocol_wheel, otlp_receiver, mode, path, status, body
):
    with _server(tmp_path, protocol_wheel, otlp_receiver, mode) as server:
        assert _request(server, path) == (status, body, True)
    events = _events(tmp_path)
    assert sum(event["stage"] == "inside_app" for event in events) == 1
    assert sum(event["stage"] == "inside_close" for event in events) == 1
    assert all(
        not event["state"] and event["trace"] is None
        for event in events
        if event["stage"].startswith("outside_")
    )
    assert all(event["exc_info_same"] for event in events if "exc_info_same" in event)
    if path == "/close_error":
        assert [e["same_exception"] for e in events if e["stage"] == "close_error"] == [
            True
        ]
    spans = _spans(otlp_receiver)
    if mode == "enabled":
        assert len(spans) == 1
        span = spans[0]
        assert span.kind == 2 and span.name == "GET"
        attrs = _attributes(span)
        assert attrs["http.route"] == path
        assert attrs["http.response.status_code"] == status
        assert attrs["altimeter.http.outcome"] == "completed"
        assert attrs["altimeter.http.server.concurrent_at_start"] == 1
        assert _metric_count(otlp_receiver, "altimeter.http.server.requests") == 1
        assert _metric_count(otlp_receiver, "http.server.request.duration") == 1
        for event in events:
            if event["stage"].startswith("inside_"):
                assert event["state"] is True
                assert event["trace"] == span.trace_id.hex()
                assert event["span"] == span.span_id.hex()
    else:
        assert not otlp_receiver.requests
    assert all(
        b"CANARY" not in request.message.SerializeToString()
        for request in otlp_receiver.requests
    )


@pytest.mark.parametrize("mode", ["enabled", "disabled", "invalid"])
@pytest.mark.parametrize(
    ("path", "status", "complete", "closes"),
    [
        ("/app_error", 500, True, 0),
        ("/iterator_start_error", 500, True, 1),
        ("/iter_error", 200, False, 1),
        ("/late_exc_info", 200, False, 0),
    ],
)
def test_real_gunicorn_preserves_original_failures_and_recovers(
    tmp_path, protocol_wheel, otlp_receiver, mode, path, status, complete, closes
):
    with _server(tmp_path, protocol_wheel, otlp_receiver, mode) as server:
        observed_status, _, observed_complete = _request(server, path)
        assert (observed_status, observed_complete) == (status, complete)
        assert _request(server, "/normal") == (200, b"firstlast", True)
    events = _events(tmp_path)
    failed = [event for event in events if event["path"] == path]
    assert sum(event["stage"] == "inside_app" for event in failed) == 1
    assert sum(event["stage"] == "inside_close" for event in failed) == closes
    assert all(event["same_exception"] for event in failed if "same_exception" in event)
    assert any("same_exception" in event for event in failed)
    assert all(
        event["trace"] is None and not event["state"]
        for event in events
        if event["stage"].startswith("outside_")
    )
    if mode == "enabled":
        spans = _spans(otlp_receiver)
        assert len(spans) == 2
        attrs = [_attributes(span) for span in spans]
        aborted = next(item for item in attrs if item["http.route"] == path)
        assert aborted["altimeter.http.outcome"] == "aborted"
        if path == "/app_error":
            assert "http.response.status_code" not in aborted
        else:
            assert aborted["http.response.status_code"] == 200
        recovered = next(item for item in attrs if item["http.route"] == "/normal")
        assert recovered["altimeter.http.server.concurrent_at_start"] == 1
        assert _metric_count(otlp_receiver, "altimeter.http.server.requests") == 2
        assert _metric_count(otlp_receiver, "http.server.request.duration") == 2
        aborted_span = next(
            span for span in spans if _attributes(span)["http.route"] == path
        )
        assert aborted_span.status.code == 2
    else:
        assert not otlp_receiver.requests
    assert all(
        b"CANARY" not in request.message.SerializeToString()
        for request in otlp_receiver.requests
    )


@pytest.mark.parametrize("mode", ["enabled", "disabled", "invalid"])
def test_real_client_disconnect_closes_once_and_releases_concurrency(
    tmp_path, protocol_wheel, otlp_receiver, mode
):
    with _server(tmp_path, protocol_wheel, otlp_receiver, mode) as server:
        with socket.create_connection(("127.0.0.1", server.port), timeout=5) as client:
            client.sendall(b"GET /early HTTP/1.1\r\nHost: localhost\r\n\r\n")
            response = b""
            while b"\r\n\r\n" not in response:
                chunk = client.recv(4096)
                assert chunk, "Server closed before the response headers"
                response += chunk
            assert b"200 OK" in response
            client.setsockopt(
                socket.SOL_SOCKET, socket.SO_LINGER, struct.pack("ii", 1, 0)
            )
        deadline = monotonic() + 5
        while monotonic() < deadline:
            if any(e["stage"] == "inside_close" for e in _events(tmp_path)):
                break
            sleep(0.01)
        assert _request(server, "/normal") == (200, b"firstlast", True)
    events = _events(tmp_path)
    assert (
        sum(e["stage"] == "inside_close" and e["path"] == "/early" for e in events) == 1
    )
    if mode == "enabled":
        spans = _spans(otlp_receiver)
        assert len(spans) == 2
        attrs = [_attributes(span) for span in spans]
        early = next(item for item in attrs if item["http.route"] == "/early")
        assert early["altimeter.http.outcome"] == "aborted"
        assert early["http.response.status_code"] == 200
        recovered = next(item for item in attrs if item["http.route"] == "/normal")
        assert recovered["altimeter.http.server.concurrent_at_start"] == 1
        assert _metric_count(otlp_receiver, "altimeter.http.server.requests") == 2
    else:
        assert not otlp_receiver.requests


@pytest.mark.parametrize(
    ("path", "status", "complete"),
    [
        ("/normal", 200, True),
        ("/iter_error", 200, False),
        ("/close_error", 200, True),
        ("/late_exc_info", 200, False),
    ],
)
def test_failing_telemetry_finalizer_never_masks_native_behavior(
    tmp_path, protocol_wheel, otlp_receiver, path, status, complete
):
    with _server(
        tmp_path,
        protocol_wheel,
        otlp_receiver,
        "enabled",
        PROTOCOL_FINALIZER_FAILURE="1",
    ) as server:
        actual_status, _, actual_complete = _request(server, path)
        assert (actual_status, actual_complete) == (status, complete)
        assert _request(server, "/normal") == (200, b"firstlast", True)
    events = _events(tmp_path)
    assert [
        event["path"] for event in events if event["stage"] == "telemetry_failure"
    ] == [
        path,
        "/normal",
    ]
    assert all(event["same_exception"] for event in events if "same_exception" in event)
    if path != "/normal":
        assert any("same_exception" in event for event in events)
    assert all(
        not event["state"] and event["trace"] is None
        for event in events
        if event["stage"].startswith("outside_")
    )
    assert [event["active"] for event in events if event["stage"] == "inside_app"] == [
        1,
        1,
    ]
    assert all(
        b"CANARY" not in request.message.SerializeToString()
        for request in otlp_receiver.requests
    )


def _direct(wheel, receiver, tmp_path, source):
    process = subprocess.run(
        [str(wheel.python), "-I", "-c", _APP + source],
        cwd=tmp_path,
        env={
            "PATH": os.environ.get("PATH", ""),
            "RECEIVER_ENDPOINT": receiver.endpoint,
            "PROTOCOL_MODE": "enabled",
            "PROTOCOL_EVENTS_FILE": str(tmp_path / "events.jsonl"),
        },
        capture_output=True,
        text=True,
        timeout=15,
        check=False,
    )
    assert process.returncode == 0, process.stderr
    return json.loads(process.stdout)


@pytest.mark.parametrize("exhausted", [True, False])
def test_missing_close_never_keeps_context_and_abandonment_releases_counter(
    tmp_path, protocol_wheel, otlp_receiver, exhausted
):
    result = _direct(
        protocol_wheel,
        otlp_receiver,
        tmp_path,
        f"\nEXHAUSTED = {exhausted!r}\n"
        + r"""
import gc
from django_altimeter.runtime import shutdown

def start(status, headers, exc_info=None):
    return lambda data: None

environ = {'REQUEST_METHOD': 'GET', 'PATH_INFO': '/normal'}
response = observed(environ, start)
assert get_request_state() is None and get_correlation() is None
if EXHAUSTED:
    assert b''.join(response) == b'firstlast'
else:
    assert next(iter(response)) == b'first'
assert get_request_state() is None and get_correlation() is None
# Deliberately omit close: this simulates an unsupported server. Collection must
# not run application close or export; it may release only Altimeter's counter.
del response
gc.collect()
following = observed(dict(environ), start)
assert b''.join(following) == b'firstlast'
following.close()
following.close()
assert get_request_state() is None and get_correlation() is None
assert shutdown().completed
print(json.dumps({'safe': True}))
""",
    )
    assert result == {"safe": True}
    events = _events(tmp_path)
    assert [event["active"] for event in events if event["stage"] == "inside_app"] == [
        1,
        1,
    ]
    assert sum(event["stage"] == "inside_close" for event in events) == 1
    assert len(_spans(otlp_receiver)) == 1
    assert _metric_count(otlp_receiver, "altimeter.http.server.requests") == 1


def test_exhaustion_timestamp_excludes_delayed_close_work(
    tmp_path, protocol_wheel, otlp_receiver
):
    with _server(
        tmp_path, protocol_wheel, otlp_receiver, "enabled", PROTOCOL_CLOSE_DELAY="0.25"
    ) as server:
        assert _request(server, "/normal") == (200, b"firstlast", True)
    (span,) = _spans(otlp_receiver)
    events = _events(tmp_path)
    close_start = next(event for event in events if event["stage"] == "inside_close")
    close_end = next(
        event for event in events if event["stage"] == "outside_after_close"
    )
    assert close_end["time_ns"] - close_start["time_ns"] >= 250_000_000
    assert span.end_time_unix_nano <= close_start["time_ns"] + 10_000_000
    assert span.end_time_unix_nano + 200_000_000 < close_end["time_ns"]
    assert _attributes(span)["altimeter.http.outcome"] == "completed"
    assert _metric_count(otlp_receiver, "altimeter.http.server.requests") == 1


@pytest.mark.parametrize("sample_rate", ["1", "0"])
@pytest.mark.parametrize("separate_branches", [False, True])
def test_real_gthread_concurrency_is_branch_local_and_counts_unsampled_requests(
    tmp_path, protocol_wheel, otlp_receiver, sample_rate, separate_branches
):
    with _server(
        tmp_path,
        protocol_wheel,
        otlp_receiver,
        "enabled",
        PROTOCOL_SAMPLE_RATE=sample_rate,
        PROTOCOL_SEPARATE_BRANCHES="1" if separate_branches else "0",
    ) as server:
        with ThreadPoolExecutor(max_workers=2) as clients:
            results = list(
                clients.map(
                    lambda path: _request(server, path + "?barrier=1"),
                    ["/normal", "/write"],
                )
            )
        assert results == [(200, b"firstlast", True), (200, b"prefixmiddletail", True)]
        assert _request(server, "/normal") == (200, b"firstlast", True)
    events = _events(tmp_path)
    entries = [event for event in events if event["stage"] == "inside_app"]
    assert len(entries) == 3
    expected = [1, 1] if separate_branches else [1, 2]
    assert sorted(event["active"] for event in entries[:2]) == expected
    assert entries[2]["active"] == 1
    assert len({event["trace"] for event in entries}) == 3
    assert all(
        event["trace"] is None and not event["state"]
        for event in events
        if event["stage"].startswith("outside_")
    )
    spans = _spans(otlp_receiver)
    if sample_rate == "1":
        assert len(spans) == 3
        assert sorted(
            _attributes(span)["altimeter.http.server.concurrent_at_start"]
            for span in spans
        ) == sorted([*expected, 1])
        write = next(
            span for span in spans if _attributes(span)["http.route"] == "/write"
        )
        assert _attributes(write)["altimeter.surface"] == (
            "mcp" if separate_branches else "django"
        )
        assert {span.trace_id.hex() for span in spans} == {
            event["trace"] for event in entries
        }
    else:
        assert spans == []
    assert _metric_count(otlp_receiver, "altimeter.http.server.requests") == 3
    assert _metric_count(otlp_receiver, "http.server.request.duration") == 3
    assert all(
        b"concurrent_at_start" not in request.message.SerializeToString()
        for request in otlp_receiver.requests
        if request.path == "/v1/metrics"
    )


@pytest.mark.parametrize("nested", [False, True])
@pytest.mark.parametrize("failure_point", [None, "iterator", "close"])
def test_nested_wsgi_adapters_keep_the_outer_request_owner(
    tmp_path, protocol_wheel, otlp_receiver, nested, failure_point
):
    result = _direct(
        protocol_wheel,
        otlp_receiver,
        tmp_path,
        f"\nNESTED, FAILURE_POINT = {nested!r}, {failure_point!r}\n"
        + r"""
from django_altimeter.runtime import shutdown
from django_altimeter.wsgi import instrument

failure = RuntimeError('CANARY_NESTED_WSGI')
calls = closes = iterations = 0
surfaces = []
written = []
write_result = object()
class Response:
    def __iter__(self):
        global iterations
        iterations += 1
        self.sent = False
        return self
    def __next__(self):
        surfaces.append(get_request_state().surface)
        if self.sent:
            if FAILURE_POINT == 'iterator':
                raise failure
            raise StopIteration
        self.sent = True
        return b'safe'
    def close(self):
        global closes
        closes += 1
        surfaces.append(get_request_state().surface)
        if FAILURE_POINT == 'close':
            raise failure
body = Response()
def app(environ, start_response):
    global calls
    calls += 1
    surfaces.append(get_request_state().surface)
    write = start_response('200 OK', [('Cache-Control', 'private')])
    assert write(b'prefix') is write_result
    return body
def start(status, headers, exc_info=None):
    assert status == '200 OK'
    def write(data):
        written.append(data.decode())
        return write_result
    return write
inner = instrument(app, surface='mcp')
def router(environ, start_response):
    return inner(environ, start_response)
outer = instrument(router if NESTED else app, surface='django')
environ = {'REQUEST_METHOD': 'GET', 'PATH_INFO': '/normal'}
response = outer(environ, start)
assert get_request_state() is None and get_correlation() is None
chunks = []
try:
    for chunk in response:
        chunks.append(chunk.decode())
        assert get_request_state() is None and get_correlation() is None
except RuntimeError as error:
    assert FAILURE_POINT == 'iterator' and error is failure
else:
    assert FAILURE_POINT != 'iterator'
try:
    response.close()
except RuntimeError as error:
    assert FAILURE_POINT == 'close' and error is failure
else:
    assert FAILURE_POINT != 'close'
response.close()
assert get_request_state() is None and get_correlation() is None
assert outer._active == inner._active == 0
assert shutdown().completed
print(json.dumps({'calls': calls, 'closes': closes, 'iterations': iterations,
                  'chunks': chunks, 'written': written, 'surfaces': surfaces}))
""",
    )
    assert result["calls"] == result["closes"] == result["iterations"] == 1
    assert result["chunks"] == ["safe"]
    assert result["written"] == ["prefix"]
    (span,) = _spans(otlp_receiver)
    assert span.kind == 2
    assert _attributes(span)["altimeter.surface"] == "django"
    assert _attributes(span)["altimeter.http.outcome"] == (
        "aborted" if failure_point == "iterator" else "completed"
    )
    assert span.status.code == (0 if failure_point is None else 2)
    assert result["surfaces"] == ["django"] * 4
    assert _metric_count(otlp_receiver, "altimeter.http.server.requests") == 1
    assert _metric_count(otlp_receiver, "http.server.request.duration") == 1
    points = [
        point
        for request in otlp_receiver.requests
        if request.path == "/v1/metrics"
        for resource in request.message.resource_metrics
        for scope in resource.scope_metrics
        for metric in scope.metrics
        if metric.name
        in {"altimeter.http.server.requests", "http.server.request.duration"}
        for point in (
            metric.sum.data_points
            if metric.HasField("sum")
            else metric.histogram.data_points
        )
    ]
    assert points and all(
        _attributes(point)["altimeter.surface"] == "django" for point in points
    )
    assert all(
        b"CANARY" not in request.message.SerializeToString()
        for request in otlp_receiver.requests
    )


_STREAMING_APP = r"""
import json
import logging
import os
import time
from dataclasses import replace
from threading import Barrier

import requests
from opentelemetry import trace
from django_altimeter.conf import Config, SignalConfig
from django_altimeter.context import get_correlation, span
from django_altimeter.http import get_request_state
from django_altimeter.logging import install_logging
from django_altimeter.privacy import DEFAULT_POLICY
from django_altimeter.runtime import setup
from django_altimeter.wsgi import instrument

logger = logging.getLogger('stream.review')
logger.setLevel(logging.INFO)
logger.addHandler(logging.StreamHandler())
logger.propagate = False

def signal(name):
    return SignalConfig(True, os.environ['RECEIVER_ENDPOINT'] + '/v1/' + name,
                        destination='collector')

assert setup(Config(
    enabled=True, service_name='streaming-context', deployment_environment='test',
    release='v1', integrations=frozenset({'wsgi', 'requests'}),
    traces=signal('traces'), logs=signal('logs'), metrics=signal('metrics'),
    sample_rate=float(os.environ['STREAM_SAMPLE_RATE']), export_timeout=0.5,
    excluded_routes=frozenset({'/health/'}),
    policy=replace(DEFAULT_POLICY,
        span_names=DEFAULT_POLICY.span_names | {'stream.work'},
        scope_names=DEFAULT_POLICY.scope_names | {'stream.review'},
        log_bodies=frozenset({'stream.chunk', 'stream.write', 'stream.close'}),
        attribute_values=frozenset({('http.route', '/stream/')})),
)).ok
assert install_logging(routes=('stream.review',)).ok
barrier = Barrier(4)

def emit(stage):
    current = get_correlation()
    logger.info('stream.' + stage)
    logger.warning('CANARY_STREAM_LOG')
    with requests.get(os.environ['STREAM_UPSTREAM'] + '/' + stage +
                      '?secret=CANARY_STREAM_QUERY', timeout=2) as response:
        assert response.status_code == 200
        assert response.content == b'safe-response'
    return {
        'trace': current.trace_id if current else None,
        'current': current.span_id if current else None,
        'native': format(trace.get_current_span().get_span_context().span_id, '016x'),
    }

def produce(manual, write):
    barrier.wait(timeout=4)
    try:
        for index in range(2):
            row = emit('chunk')
            row.update(index=index,
                       enclosing=format(manual.get_span_context().span_id, '016x'))
            # Native write is called while the manual span is already current.
            # Its outer observer emits a log and client call at that exact point.
            write(b'')
            yield (json.dumps(row) + '\n').encode()
            time.sleep(0.01)
    finally:
        emit('close')

def app(environ, start_response):
    if environ['PATH_INFO'] == '/health/':
        start_response('200 OK', [('Content-Type', 'application/json')])
        return [b'{"ok":true}']
    state = get_request_state()
    state.route = '/stream/'
    state.response_mode = 'streaming'
    state.response_ready_at = time.monotonic()
    if os.environ['STREAM_SPAN_START'] == 'app':
        manager = span('stream.work')
        manual = manager.__enter__()
    else:
        manager = manual = None
    write = start_response('200 OK', [('Content-Type', 'application/x-ndjson'),
                                     ('Cache-Control', 'private, no-store')])
    def chunks():
        if manager is None:
            with span('stream.work') as current:
                yield from produce(current, write)
        else:
            try:
                yield from produce(manual, write)
            finally:
                manager.__exit__(None, None, None)
    return chunks()

observed = instrument(app)

def outside():
    assert get_request_state() is None
    assert get_correlation() is None
    assert not trace.get_current_span().get_span_context().is_valid

class Outside:
    def __init__(self, iterable):
        self.iterable = iterable
        self.iterator = iter(iterable)
        outside()
    def __iter__(self):
        return self
    def __next__(self):
        outside()
        try:
            return next(self.iterator)
        finally:
            outside()
    def close(self):
        outside()
        try:
            close = getattr(self.iterable, 'close', None)
            if close is not None:
                close()
        finally:
            outside()

def application(environ, start_response):
    outside()
    def forwarded(status, headers, exc_info=None):
        write = start_response(status, headers, exc_info)
        def forwarded_write(data):
            emit('write')
            return write(data)
        return forwarded_write
    try:
        return Outside(observed(environ, forwarded))
    finally:
        outside()
"""


@pytest.fixture(scope="module")
def streaming_wheel(tmp_path_factory):
    from support.gunicorn_server import install_wheel

    return install_wheel(
        tmp_path_factory.mktemp("wsgi-streaming-wheel"), extras=("requests",)
    )


@pytest.mark.parametrize("sample_rate", ["1", "0"])
@pytest.mark.parametrize("span_start", ["app", "generator"])
def test_manual_span_survives_wsgi_yields_and_parents_logs_and_http_children(
    tmp_path, streaming_wheel, otlp_receiver, sample_rate, span_start
):
    from collections import Counter

    from support.gunicorn_server import running_server
    from support.http_receiver import HTTPReceiver

    (tmp_path / "streaming_app.py").write_text(_STREAMING_APP)
    with HTTPReceiver() as upstream:
        with running_server(
            tmp_path / "server",
            wheel=streaming_wheel,
            receiver=otlp_receiver,
            application="streaming_app:application",
            module_path=tmp_path,
            threads=4,
            extra_env={
                "STREAM_SPAN_START": span_start,
                "STREAM_SAMPLE_RATE": sample_rate,
                "STREAM_UPSTREAM": upstream.origin,
            },
        ) as server:
            with ThreadPoolExecutor(max_workers=4) as clients:
                responses = list(
                    clients.map(lambda _: server.request("/stream/"), range(4))
                )
            assert server.request("/health/")[0] == 200
        assert len(upstream.requests) == 20
    by_trace = {}
    for status, _, body in responses:
        assert status == 200, body
        rows = [json.loads(line) for line in body.splitlines()]
        assert [row["index"] for row in rows] == [0, 1]
        assert len({row["enclosing"] for row in rows}) == 1
        assert len({row["trace"] for row in rows}) == 1
        assert all(row["current"] == row["native"] == row["enclosing"] for row in rows)
        assert rows[0]["trace"] is not None
        by_trace[rows[0]["trace"]] = rows[0]["enclosing"]
    assert len(by_trace) == 4
    logs = [
        record
        for request in otlp_receiver.requests
        if request.path == "/v1/logs"
        for resource in request.message.resource_logs
        for scope in resource.scope_logs
        for record in scope.log_records
    ]
    assert Counter(
        (record.trace_id.hex(), record.span_id.hex(), record.body.string_value)
        for record in logs
    ) == Counter(
        {
            (trace_id, manual_id, body): count
            for trace_id, manual_id in by_trace.items()
            for body, count in (
                ("stream.chunk", 2),
                ("stream.write", 2),
                ("stream.close", 1),
            )
        }
    )
    assert all(record.flags & 1 == int(sample_rate) for record in logs)
    spans = _spans(otlp_receiver)
    if sample_rate == "1":
        assert len(spans) == 28
        for trace_id, manual_id in by_trace.items():
            related = [span for span in spans if span.trace_id.hex() == trace_id]
            (server_span,) = [span for span in related if span.kind == 2]
            (manual,) = [span for span in related if span.kind == 1]
            children = [span for span in related if span.kind == 3]
            assert manual.name == "stream.work" and manual.span_id.hex() == manual_id
            assert manual.parent_span_id == server_span.span_id
            assert len(children) == 5
            assert all(child.parent_span_id == manual.span_id for child in children)
            assert all(child.name == "GET" for child in children)
            assert _attributes(server_span)["altimeter.http.outcome"] == "completed"
    else:
        assert spans == []
    assert _metric_count(otlp_receiver, "altimeter.http.server.requests") == 4
    assert _metric_count(otlp_receiver, "http.server.request.duration") == 4
    assert "CANARY_STREAM_LOG" in server.output
    assert all(
        b"CANARY" not in request.message.SerializeToString()
        for request in otlp_receiver.requests
    )


_FILE_BODY = b"file-CANARY_FILE_BODY\n" * 4096
_FILE_APP = r"""
import json
import os
import tempfile
import time
from dataclasses import replace
from threading import Lock

from django.conf import settings
settings.configure(SECRET_KEY='test-only-file-response', ROOT_URLCONF=__name__,
                   ALLOWED_HOSTS=['*'], INSTALLED_APPS=[], DEBUG=False,
                   MIDDLEWARE=['django_altimeter.django.RequestMetadataMiddleware'])
from django.core.wsgi import get_wsgi_application
from django.http import FileResponse, JsonResponse
from django.urls import path
from django_altimeter.conf import Config, SignalConfig
from django_altimeter.context import get_correlation
from django_altimeter.http import get_request_state
from django_altimeter.privacy import DEFAULT_POLICY
from django_altimeter.runtime import setup
from django_altimeter.wsgi import instrument

BODY = b'file-CANARY_FILE_BODY\n' * 4096
lock = Lock()
def record(event, **extra):
    current = get_correlation()
    state = get_request_state()
    with lock, open(os.environ['FILE_EVENTS'], 'a') as output:
        output.write(json.dumps(dict(event=event, time_ns=time.time_ns(),
            trace=current.trace_id if current else None,
            finalized=state.finalized if state else None, **extra)) + '\n')

class AuditedFile:
    def __init__(self, failing, method):
        self.file = tempfile.TemporaryFile()
        self.file.write(BODY)
        self.file.seek(0)
        self.failing, self.method = failing, method
    def fileno(self):
        return self.file.fileno()
    def tell(self):
        return self.file.tell()
    def seek(self, *args):
        return self.file.seek(*args)
    def read(self, count=-1):
        record('read', method=self.method, failing=self.failing)
        if self.failing:
            raise RuntimeError('CANARY_FILE_READ')
        return self.file.read(count)
    def close(self):
        record('close', method=self.method, failing=self.failing)
        self.file.close()

class ReadOnlyWrapper:
    __slots__ = ('filelike', 'blksize')
    def __init__(self, filelike, blksize=8192):
        self.filelike, self.blksize = filelike, blksize
    def __getitem__(self, index):
        data = self.filelike.read(self.blksize)
        if data:
            return data
        raise IndexError
    def close(self):
        self.filelike.close()

def health(request):
    assert get_correlation() is None
    return JsonResponse({'ok': True})

def file_response(request):
    source = AuditedFile(request.GET.get('failing') == '1', request.method)
    result = FileResponse(source, content_type='application/octet-stream')
    result['Cache-Control'] = 'private, no-store'
    return result

urlpatterns = [path('health/', health), path('file/', file_response)]
django_app = get_wsgi_application()
mode = os.environ['FILE_MODE']
def signal(name):
    return SignalConfig(True, os.environ['RECEIVER_ENDPOINT'] + '/v1/' + name,
                        destination='collector')
assert setup(Config(
    enabled=mode != 'disabled',
    service_name='' if mode == 'invalid' else 'file-response',
    deployment_environment='test', release='v1',
    integrations=frozenset({'wsgi', 'django'}), traces=signal('traces'),
    metrics=signal('metrics'), export_timeout=0.5,
    excluded_routes=frozenset({'/health/'}),
    policy=replace(DEFAULT_POLICY,
        attribute_values=frozenset({('http.route', 'file/')})),
)).ok == (mode == 'enabled')

def captured(environ, start_response):
    result = django_app(environ, start_response)
    if environ['PATH_INFO'] == '/file/':
        environ['test.original_file_wrapper'] = result
    return result

observed = instrument(captured)
def application(environ, start_response):
    if os.environ['FILE_WRAPPER'] == 'readonly':
        environ['wsgi.file_wrapper'] = ReadOnlyWrapper
    result = observed(environ, start_response)
    if environ['PATH_INFO'] == '/file/':
        record('returned', same=result is environ.pop('test.original_file_wrapper'),
               native=isinstance(result, environ['wsgi.file_wrapper']))
        # Keep a visible gap between app return and server close. Native file
        # duration must include it; the unhookable fallback explicitly cannot.
        time.sleep(0.04)
    assert get_correlation() is None
    assert get_request_state() is None
    return result
"""


@pytest.mark.parametrize("sendfile", ["yes", "no"])
@pytest.mark.parametrize("wrapper", ["native", "readonly"])
def test_django_file_response_preserves_native_sendfile_get_and_head(
    tmp_path, protocol_wheel, sendfile, wrapper
):
    from support.gunicorn_server import running_server

    results = {}
    for mode in ("disabled", "invalid", "enabled"):
        workspace = tmp_path / mode
        workspace.mkdir()
        (workspace / "file_app.py").write_text(_FILE_APP)
        event_path = workspace / "events.jsonl"
        responses = {}
        with OTLPReceiver() as receiver:
            with running_server(
                workspace / "server",
                wheel=protocol_wheel,
                receiver=receiver,
                application="file_app:application",
                module_path=workspace,
                threads=1,
                extra_env={
                    "FILE_MODE": mode,
                    "FILE_WRAPPER": wrapper,
                    "FILE_EVENTS": str(event_path),
                    "SENDFILE": sendfile,
                },
            ) as server:
                for method in ("GET", "HEAD"):
                    for failing in (False, True):
                        # Gunicorn's native file path can send bytes for HEAD.
                        # Drain to EOF so this parity client does not abort an
                        # otherwise successful transfer after receiving headers.
                        with socket.create_connection(
                            ("127.0.0.1", server.port), timeout=5
                        ) as connection:
                            connection.sendall(
                                (
                                    f"{method} /file/?failing={int(failing)} "
                                    "HTTP/1.1\r\n"
                                    "Host: localhost\r\nConnection: close\r\n\r\n"
                                ).encode()
                            )
                            chunks = []
                            while chunk := connection.recv(65536):
                                chunks.append(chunk)
                        head, separator, wire_body = b"".join(chunks).partition(
                            b"\r\n\r\n"
                        )
                        assert separator
                        status_line, *header_lines = head.decode("latin-1").split(
                            "\r\n"
                        )
                        headers = dict(
                            (name.lower(), value.strip())
                            for name, value in (
                                line.split(":", 1) for line in header_lines
                            )
                        )
                        responses[method, failing] = (
                            int(status_line.split()[1]),
                            b"" if method == "HEAD" else wire_body,
                            headers.get("content-length"),
                            headers.get("cache-control"),
                            wire_body,
                        )
                assert server.request("/health/")[0] == 200
            events = [json.loads(line) for line in event_path.read_text().splitlines()]
            closes = [event for event in events if event["event"] == "close"]
            returned = [event for event in events if event["event"] == "returned"]
            assert len(closes) == len(returned) == 4
            assert all(event["same"] and event["native"] for event in returned)
            assert all(event["trace"] is None for event in returned)
            if sendfile == "yes":
                assert not any(event["event"] == "read" for event in events)
            else:
                assert sum(event["event"] == "read" for event in events) >= 4
            for (method, failing), (status, body, _, _, _) in responses.items():
                expected = 500 if failing and sendfile == "no" else 200
                assert status == expected, (mode, method, failing, responses)
                if method == "HEAD":
                    assert body == b""
                elif expected == 200:
                    assert body == _FILE_BODY
            spans = _spans(receiver)
            if mode == "enabled":
                assert len(spans) == 4
                assert all(span.kind == 2 for span in spans)
                assert _metric_count(receiver, "altimeter.http.server.requests") == 4
                assert _metric_count(receiver, "http.server.request.duration") == 4
                for span, app_return, closed in zip(
                    spans, returned, closes, strict=True
                ):
                    assert _attributes(span)["http.route"] == "file/"
                    if wrapper == "native":
                        assert (
                            span.end_time_unix_nano
                            >= app_return["time_ns"] + 30_000_000
                        )
                        assert span.end_time_unix_nano <= closed["time_ns"]
                        assert closed["finalized"] is True
                        assert _attributes(span)["altimeter.http.outcome"] == (
                            "aborted"
                            if closed["failing"] and sendfile == "no"
                            else "completed"
                        ), (wrapper, sendfile, closed, app_return, _attributes(span))
                    else:
                        assert span.end_time_unix_nano <= app_return["time_ns"]
                        assert closed["trace"] is None
                assert ("http.file_wrapper_unobserved" in server.output) == (
                    wrapper == "readonly"
                )
            else:
                assert receiver.requests == ()
            assert all(
                b"CANARY" not in request.message.SerializeToString()
                for request in receiver.requests
            )
        results[mode] = responses
    assert results["enabled"] == results["disabled"] == results["invalid"]


def test_native_file_close_finalizes_once_before_preserving_original_exception(
    tmp_path, protocol_wheel, otlp_receiver
):
    result = _direct(
        protocol_wheel,
        otlp_receiver,
        tmp_path,
        r"""
from gunicorn.http.wsgi import FileWrapper
from django_altimeter.wsgi import instrument
from django_altimeter.runtime import shutdown

failure = RuntimeError('CANARY_FILE_CLOSE')
closes = 0
class File:
    def read(self, count=-1):
        raise AssertionError('Native file path must not read during wrapping')
    def close(self):
        global closes
        closes += 1
        state = get_request_state()
        assert state is not None and state.finalized
        raise failure
body = FileWrapper(File())
def app(environ, start_response):
    start_response('200 OK', [])
    return body
def start(status, headers, exc_info=None):
    return lambda data: None
response = instrument(app)({'REQUEST_METHOD': 'GET', 'PATH_INFO': '/normal',
                            'wsgi.file_wrapper': FileWrapper}, start)
assert response is body
assert get_request_state() is None and get_correlation() is None
try:
    response.close()
except RuntimeError as error:
    assert error is failure
else:
    raise AssertionError('Native close exception was lost')
response.close()
assert closes == 1
assert get_request_state() is None and get_correlation() is None
assert shutdown().completed
print(json.dumps({'closes': closes}))
""",
    )
    assert result == {"closes": 1}
    assert len(_spans(otlp_receiver)) == 1
    assert _metric_count(otlp_receiver, "altimeter.http.server.requests") == 1
    assert _metric_count(otlp_receiver, "http.server.request.duration") == 1
    assert all(
        b"CANARY" not in request.message.SerializeToString()
        for request in otlp_receiver.requests
    )
