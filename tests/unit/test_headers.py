"""Metadata and pure HTTP policy; server behavior is proved by H1 integration."""

import pytest
from opentelemetry import baggage, context, trace
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.sampling import ParentBased, TraceIdRatioBased

from django_altimeter.context import Correlation
from django_altimeter.headers import incoming_context, response_headers

ORIGIN = "https://frontend.example"
CORRELATION = Correlation("1234567890abcdef" * 2, "1234567890abcdef", 1)


def test_approved_origin_adds_timing_without_losing_vary_or_cookies():
    original = [
        (b"content-type", b"text/plain"),
        (b"vary", b"Accept-Encoding, Cookie"),
        (b"set-cookie", b"a=one"),
        (b"set-cookie", b"b=two"),
        (b"cache-control", b"private, no-store"),
    ]
    before = list(original)
    result = response_headers(
        original,
        request_headers=[(b"origin", ORIGIN.encode())],
        frontend_origins=frozenset({ORIGIN}),
    )
    assert (b"timing-allow-origin", ORIGIN.encode()) in result
    vary = b", ".join(value for key, value in result if key.lower() == b"vary")
    assert vary == b"Accept-Encoding, Cookie, Origin"
    assert [value for key, value in result if key == b"set-cookie"] == [
        b"a=one",
        b"b=two",
    ]
    assert (b"cache-control", b"private, no-store") in result
    assert original == before


def test_owned_trace_correlation_is_current_and_private():
    result = response_headers(
        [(b"cache-control", b"private, no-store")],
        request_headers=[],
        correlation=CORRELATION,
        owned_tracing=True,
    )
    assert (b"x-altimeter-trace-id", CORRELATION.trace_id.encode()) in result
    assert (b"x-altimeter-trace-sampled", b"1") in result
    assert (b"cache-control", b"private, no-store") in result


TRACEPARENT = b"00-11111111111111111111111111111111-2222222222222222-01"


@pytest.mark.parametrize(
    "request_headers",
    [
        [],
        [(b"origin", b"null")],
        [(b"origin", b"*")],
        [(b"origin", b"https://frontend.example.attacker.invalid")],
        [(b"origin", b"https://FRONTEND.example")],
        [(b"origin", b"https://frontend.example/")],
        [(b"origin", b"https://frontend.example:443")],
        [(b"origin", b"https://frontend.example https://attacker.invalid")],
        [(b"origin", ORIGIN.encode()), (b"Origin", ORIGIN.encode())],
        [(b"origin", ORIGIN.encode()), (b"origin", b"https://attacker.invalid")],
        [(b"origin", b"https://frontend.example\r\nx-canary: secret")],
        [(b"origin", b"\xff")],
        [(b"origin", b"x" * 1_000_000)],
    ],
)
def test_unapproved_or_ambiguous_origins_never_open_timing(request_headers):
    result = response_headers(
        [(b"timing-allow-origin", b"*"), (b"vary", b"Cookie")],
        request_headers=request_headers,
        frontend_origins=frozenset({ORIGIN}),
    )
    assert not any(key == b"timing-allow-origin" for key, _ in result)
    assert (b"vary", b"Cookie, Origin") in result


@pytest.mark.parametrize(
    "vary, expected",
    [
        (
            [(b"Vary", b"Cookie"), (b"vary", b"Accept-Encoding")],
            b"Cookie, Accept-Encoding, Origin",
        ),
        ([(b"vary", b"Cookie, origin")], b"Cookie, origin"),
        ([(b"vary", b"*")], b"*"),
    ],
)
def test_vary_merge_preserves_existing_tokens(vary, expected):
    result = response_headers(
        vary,
        request_headers=[(b"Origin", ORIGIN.encode())],
        frontend_origins=frozenset({ORIGIN}),
    )
    assert (
        b", ".join(value for key, value in result if key.lower() == b"vary") == expected
    )
    assert (b"timing-allow-origin", ORIGIN.encode()) in result


@pytest.mark.parametrize(
    "flags, expected", [(0, b"0"), (1, b"1"), (2, b"0"), (3, b"1")]
)
@pytest.mark.parametrize(
    "cache", [b"private", b"no-store", b"Private, No-Store", b"private, max-age=60"]
)
def test_owned_headers_use_current_sampling_flags_and_keep_cache_policy(
    flags, expected, cache
):
    correlation = Correlation(CORRELATION.trace_id, CORRELATION.span_id, flags)
    result = response_headers(
        [(b"cache-control", cache), (b"X-Altimeter-Trace-Id", b"stale-id")],
        request_headers=[],
        correlation=correlation,
        owned_tracing=True,
    )
    assert (b"x-altimeter-trace-id", correlation.trace_id.encode()) in result
    assert (b"x-altimeter-trace-sampled", expected) in result
    assert b"stale-id" not in dict(result).values()
    assert (b"cache-control", cache) in result


@pytest.mark.parametrize(
    "cache",
    [
        b"",
        b"public",
        b"max-age=60",
        b"no-cache",
        b"private, public",
        b"private, s-maxage=60",
        b'private="Set-Cookie"',
        b'private="X-Other"',
        b'no-store="x"',
        b'extension="x, private, y"',
        b'extension="x,no-store,y"',
    ],
)
def test_cacheable_response_cannot_replay_per_request_correlation(cache):
    result = response_headers(
        [
            (b"cache-control", cache),
            (b"x-altimeter-trace-id", b"stale"),
            (b"x-altimeter-trace-sampled", b"1"),
        ],
        request_headers=[],
        correlation=CORRELATION,
        owned_tracing=True,
    )
    assert not any(key.lower().startswith(b"x-altimeter-trace-") for key, _ in result)
    assert (b"cache-control", cache) in result


@pytest.mark.parametrize("owned", [False, None, 1, "true"])
def test_unowned_provider_discloses_no_correlation(owned):
    result = response_headers(
        [(b"cache-control", b"private, no-store"), (b"x-altimeter-trace-id", b"stale")],
        request_headers=[],
        correlation=CORRELATION,
        owned_tracing=owned,
    )
    assert not any(key.startswith(b"x-altimeter") for key, _ in result)


@pytest.mark.parametrize(
    "correlation",
    [
        None,
        Correlation("0" * 32, CORRELATION.span_id, 1),
        Correlation("A" * 32, CORRELATION.span_id, 1),
        Correlation("x" * 1_000_000, CORRELATION.span_id, 1),
        Correlation(CORRELATION.trace_id, "0" * 16, 1),
        Correlation(CORRELATION.trace_id, CORRELATION.span_id, True),
        Correlation(CORRELATION.trace_id, CORRELATION.span_id, 256),
    ],
)
def test_invalid_owned_correlation_is_not_a_response_header(correlation):
    result = response_headers(
        [(b"cache-control", b"private")],
        request_headers=[],
        correlation=correlation,
        owned_tracing=True,
    )
    assert result == [(b"cache-control", b"private")]


@pytest.mark.parametrize(
    "trusted, configured",
    [(False, False), (False, True), (True, False), (1, True), (True, "true")],
)
def test_public_headers_cannot_inherit_parent_sampling_or_identity(trusted, configured):
    external = incoming_context(
        [(b"traceparent", TRACEPARENT)], trusted_ingress=True, trust_remote_parent=True
    )
    ambient = baggage.set_baggage(
        "posthogDistinctId", "identity-secret", context=external
    )
    token = context.attach(ambient)
    provider = TracerProvider(
        sampler=ParentBased(root=TraceIdRatioBased(0)), shutdown_on_exit=False
    )
    try:
        clean = incoming_context(
            [
                (b"traceparent", TRACEPARENT),
                (b"baggage", b"posthogDistinctId=identity-secret"),
                (b"x-altimeter-trusted", b"true"),
            ],
            trusted_ingress=trusted,
            trust_remote_parent=configured,
        )
        assert not trace.get_current_span(clean).get_span_context().is_valid
        assert not baggage.get_all(clean)
        with provider.get_tracer("unit").start_as_current_span(
            "public", context=clean
        ) as span:
            assert span.get_span_context().trace_id != int("1" * 32, 16)
            assert not span.get_span_context().trace_flags.sampled
    finally:
        context.detach(token)
        provider.shutdown()


def test_trusted_ingress_extracts_only_a_valid_w3c_parent():
    clean = incoming_context(
        [
            (b"Traceparent", TRACEPARENT),
            (b"tracestate", b"vendor=secret"),
            (b"baggage", b"tenant=secret"),
            (b"x-posthog-distinct-id", b"secret"),
        ],
        trusted_ingress=True,
        trust_remote_parent=True,
    )
    span_context = trace.get_current_span(clean).get_span_context()
    assert span_context.is_valid and span_context.is_remote
    assert span_context.trace_id == int("1" * 32, 16)
    assert span_context.span_id == int("2" * 16, 16)
    assert span_context.trace_flags.sampled
    assert not span_context.trace_state
    assert not baggage.get_all(clean)


@pytest.mark.parametrize(
    "traceparents",
    [
        [],
        [b""],
        [b"0" * 55],
        [TRACEPARENT.replace(b"00-", b"ff-", 1)],
        [TRACEPARENT.replace(b"1", b"0")],
        [TRACEPARENT.replace(b"2", b"0")],
        [TRACEPARENT.replace(b"1", b"A")],
        [TRACEPARENT + b"-extra"],
        [TRACEPARENT + b"\n"],
        [b"x" * 1_000_000],
        [TRACEPARENT, TRACEPARENT],
    ],
)
def test_invalid_or_duplicate_traceparent_cannot_control_a_trusted_request(
    traceparents,
):
    clean = incoming_context(
        [(b"traceparent", value) for value in traceparents],
        trusted_ingress=True,
        trust_remote_parent=True,
    )
    assert not trace.get_current_span(clean).get_span_context().is_valid
    assert not baggage.get_all(clean)


class HostileContainer:
    def __len__(self):
        raise AssertionError("user len called")

    def __iter__(self):
        raise AssertionError("user iter called")


class HostileBytes(bytes):
    def lower(self):
        raise AssertionError("user lower called")


def test_hostile_header_containers_are_not_traversed():
    hostile = HostileContainer()
    clean = incoming_context(hostile, trusted_ingress=True, trust_remote_parent=True)
    assert not trace.get_current_span(clean).get_span_context().is_valid
    for request_headers in (hostile, [(HostileBytes(b"origin"), ORIGIN.encode())]):
        result = response_headers(
            [], request_headers=request_headers, frontend_origins=frozenset({ORIGIN})
        )
        assert result == [(b"vary", b"Origin")]
    assert response_headers(hostile, request_headers=[]) is hostile


def test_oversized_header_counts_and_vary_values_do_not_expand_work():
    many = [(b"traceparent", TRACEPARENT)] * 129
    clean = incoming_context(many, trusted_ingress=True, trust_remote_parent=True)
    assert not trace.get_current_span(clean).get_span_context().is_valid
    original = [(b"vary", b"x" * 1_000_000)]
    result = response_headers(
        original,
        request_headers=[(b"origin", ORIGIN.encode())],
        frontend_origins=frozenset({ORIGIN}),
    )
    assert result == original


def test_pure_policy_import_starts_no_threads_or_network_and_imports_no_django():
    import os
    import subprocess
    import sys

    result = subprocess.run(
        [
            sys.executable,
            "-c",
            """
import sys
import threading
sockets = []
def audit(event, args):
    if event.startswith("socket."):
        sockets.append(event)
sys.addaudithook(audit)
before = threading.enumerate()
import django_altimeter.headers
assert "django" not in sys.modules
assert threading.enumerate() == before
assert not sockets
""",
        ],
        env={"PATH": os.environ.get("PATH", "")},
        capture_output=True,
        text=True,
        timeout=10,
        check=False,
    )
    assert result.returncode == 0, result.stdout + result.stderr


@pytest.mark.parametrize(
    "content_types, expected",
    [
        ([b"text/event-stream"], "streaming"),
        ([b"Text/Event-Stream; charset=utf-8"], "streaming"),
        ([b"application/json"], "buffered"),
        ([b"application/json; charset=utf-8"], "buffered"),
        ([], "unknown"),
        ([b"text/plain"], "unknown"),
        ([b"text/event-stream-malicious"], "unknown"),
        ([b"application/json", b"text/event-stream"], "unknown"),
        ([b"text/event-stream;\r\nsecret"], "unknown"),
        ([b"x" * 1_000_000], "unknown"),
    ],
)
def test_mcp_mode_uses_only_one_bounded_known_media_type(content_types, expected):
    from django_altimeter import headers

    classify = headers.response_mode
    assert classify([(b"content-type", value) for value in content_types]) == expected


def test_oversized_cache_policy_still_removes_stale_ids_and_wildcard_timing():
    cache = b"private, extension=" + b"x" * 1_000_000
    original = [
        (b"cache-control", cache),
        (b"x-altimeter-trace-id", b"stale-request"),
        (b"x-altimeter-trace-sampled", b"1"),
        (b"timing-allow-origin", b"*"),
    ]
    result = response_headers(
        original,
        request_headers=[(b"origin", ORIGIN.encode())],
        frontend_origins=frozenset({ORIGIN}),
        correlation=CORRELATION,
        owned_tracing=True,
    )
    assert not any(key.startswith(b"x-altimeter-trace-") for key, _ in result)
    assert (b"timing-allow-origin", ORIGIN.encode()) in result
    assert (b"timing-allow-origin", b"*") not in result
    assert next(value for key, value in result if key == b"cache-control") is cache
    assert (b"x-altimeter-trace-id", b"stale-request") in original
