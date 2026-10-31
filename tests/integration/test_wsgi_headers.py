"""Real no-preload gunicorn proof of WSGI headers and ingress trust."""

import os
from http.client import HTTPConnection

import pytest

pytestmark = pytest.mark.integration
ORIGIN = "https://frontend.example"
FORGED = "00-" + "1" * 32 + "-" + "2" * 16 + "-01"
CANARY = "wsgi-header-identity-canary"

_HEADER_APP = """
import os
from django_altimeter.conf import Config, SignalConfig
from django_altimeter.runtime import setup

def native_application(environ, start_response):
    if environ['PATH_INFO'] == '/health/':
        start_response('200 OK', [('Content-Type', 'application/json')])
        return [b'{"ok":true}']
    headers = [
        ('Content-Type', 'text/plain'),
        ('Cache-Control', os.environ.get('HEADER_CACHE', 'private, no-store')),
        ('Vary', 'Cookie'),
        ('Vary', 'Accept-Encoding'),
        ('Set-Cookie', 'first=one'),
        ('Set-Cookie', 'second=two'),
    ]
    start_response('200 OK', headers)
    return [b'headers-safe-response']

application = native_application
if os.environ.get('H2_HEADER_NATIVE_BASELINE') != '1':
    from django_altimeter.wsgi import instrument
    config = Config(
        enabled=os.environ['ALTIMETER_ENABLED'] == 'true',
        service_name='wsgi-headers', deployment_environment='test', release='test',
        traces=SignalConfig(True, os.environ['RECEIVER_ENDPOINT'] + '/v1/traces',
            destination='collector'),
        frontend_origins=frozenset({'https://frontend.example'}),
        excluded_routes=frozenset({'/health/'}),
        sample_rate=float(os.environ.get('HEADER_SAMPLE_RATE', '1')),
        trust_remote_parent=os.environ.get('HEADER_TRUST_REMOTE') == '1',
        export_timeout=-1 if os.environ.get('HEADER_INVALID') == '1' else 0.2,
    )
    setup(config)
    application = instrument(native_application,
        trusted_ingress=os.environ.get('HEADER_TRUSTED_INGRESS') == '1')
"""


@pytest.fixture(scope="module")
def header_wheel(tmp_path_factory):
    from support.gunicorn_server import install_wheel

    return install_wheel(tmp_path_factory.mktemp("wsgi-header-wheel"))


@pytest.fixture
def header_application(tmp_path):
    directory = tmp_path / "application"
    directory.mkdir()
    (directory / "header_app.py").write_text(_HEADER_APP)
    return directory


def _server_environment(**values):
    return {
        "H2_HEADER_NATIVE_BASELINE": os.environ.get("H2_HEADER_NATIVE_BASELINE", "0"),
        **values,
    }


def _request(server, headers):
    connection = HTTPConnection("127.0.0.1", server.port, timeout=5)
    try:
        connection.request("GET", "/headers", headers=headers)
        response = connection.getresponse()
        return response.status, response.getheaders(), response.read()
    finally:
        connection.close()


def _spans(receiver):
    return [
        span
        for request in receiver.requests
        if request.path == "/v1/traces"
        for resource in request.message.resource_spans
        for scope in resource.scope_spans
        for span in scope.spans
    ]


def _assert_application_response(status, headers, body, cache="private, no-store"):
    assert status == 200
    assert body == b"headers-safe-response"
    assert [value for key, value in headers if key.lower() == "set-cookie"] == [
        "first=one",
        "second=two",
    ]
    lowered = {key.lower(): value for key, value in headers}
    assert lowered["cache-control"] == cache
    return lowered


@pytest.mark.parametrize("origin", [ORIGIN, ORIGIN + ":443", "https://other.example"])
def test_gthread_response_uses_exact_origin_and_owned_trace_headers(
    header_wheel, header_application, tmp_path, otlp_receiver, origin
):
    from support.gunicorn_server import running_server

    with running_server(
        tmp_path / "server",
        wheel=header_wheel,
        receiver=otlp_receiver,
        application="header_app:application",
        module_path=header_application,
        extra_env=_server_environment(),
    ) as server:
        status, pairs, body = _request(server, {"Origin": origin})
        headers = _assert_application_response(status, pairs, body)
        assert headers["vary"] == "Cookie, Accept-Encoding, Origin"
        assert headers.get("timing-allow-origin") == (
            ORIGIN if origin == ORIGIN else None
        )
        assert headers["x-altimeter-trace-sampled"] == "1"
        trace_id = headers["x-altimeter-trace-id"]
    spans = [item for item in _spans(otlp_receiver) if item.kind == 2]
    assert any(item.trace_id.hex() == trace_id for item in spans)
    assert all(
        CANARY.encode() not in item.message.SerializeToString()
        for item in otlp_receiver.requests
    )


@pytest.mark.parametrize(
    "cache", ["public, max-age=60", 'private="Set-Cookie"', "private, s-maxage=60"]
)
def test_gthread_cacheable_headers_never_disclose_per_request_ids(
    header_wheel, header_application, tmp_path, otlp_receiver, cache
):
    from support.gunicorn_server import running_server

    with running_server(
        tmp_path / "server",
        wheel=header_wheel,
        receiver=otlp_receiver,
        application="header_app:application",
        module_path=header_application,
        extra_env=_server_environment(HEADER_CACHE=cache),
    ) as server:
        status, pairs, body = _request(server, {"Origin": ORIGIN})
        headers = _assert_application_response(status, pairs, body, cache)
        assert headers["timing-allow-origin"] == ORIGIN
        assert "x-altimeter-trace-id" not in headers
        assert "x-altimeter-trace-sampled" not in headers


@pytest.mark.parametrize("mode", ["disabled", "invalid"])
def test_gthread_unowned_preserves_application_without_correlation(
    header_wheel, header_application, tmp_path, otlp_receiver, mode
):
    from support.gunicorn_server import running_server

    with running_server(
        tmp_path / "server",
        wheel=header_wheel,
        receiver=otlp_receiver,
        enabled=mode != "disabled",
        application="header_app:application",
        module_path=header_application,
        extra_env=_server_environment(HEADER_INVALID="1" if mode == "invalid" else "0"),
    ) as server:
        status, pairs, body = _request(server, {"Origin": ORIGIN})
        headers = _assert_application_response(status, pairs, body)
        assert "x-altimeter-trace-id" not in headers
        assert "x-altimeter-trace-sampled" not in headers
        assert [value for key, value in pairs if key.lower() == "vary"] == [
            "Cookie",
            "Accept-Encoding",
        ]
    assert not otlp_receiver.requests


@pytest.mark.parametrize(
    "trusted, configured, parent, expected_remote",
    [
        (False, True, FORGED, False),
        (True, False, FORGED, False),
        (True, True, FORGED, True),
        (True, True, FORGED + "," + FORGED, False),
    ],
)
def test_gthread_remote_sampling_requires_both_trust_gates(
    header_wheel,
    header_application,
    tmp_path,
    otlp_receiver,
    trusted,
    configured,
    parent,
    expected_remote,
):
    from support.gunicorn_server import running_server

    with running_server(
        tmp_path / "server",
        wheel=header_wheel,
        receiver=otlp_receiver,
        application="header_app:application",
        module_path=header_application,
        extra_env=_server_environment(
            HEADER_SAMPLE_RATE="0",
            HEADER_TRUSTED_INGRESS="1" if trusted else "0",
            HEADER_TRUST_REMOTE="1" if configured else "0",
        ),
    ) as server:
        status, pairs, body = _request(
            server,
            {
                "traceparent": parent,
                "tracestate": "vendor=" + CANARY,
                "baggage": "posthogDistinctId=" + CANARY,
                "X-Posthog-Distinct-Id": CANARY,
                "X-Altimeter-Trusted": "true",
            },
        )
        headers = _assert_application_response(status, pairs, body)
        trace_id = headers["x-altimeter-trace-id"]
        assert (trace_id == "1" * 32) is expected_remote
        assert headers["x-altimeter-trace-sampled"] == ("1" if expected_remote else "0")
    spans = [item for item in _spans(otlp_receiver) if item.kind == 2]
    if expected_remote:
        assert len(spans) == 1
        assert spans[0].trace_id.hex() == "1" * 32
        assert spans[0].parent_span_id.hex() == "2" * 16
    else:
        assert not spans
    assert all(
        CANARY.encode() not in item.message.SerializeToString()
        for item in otlp_receiver.requests
    )
