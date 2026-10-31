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


# Frozen pre-optimization reference: keep its repeated scans independent of the
# implementation so the optimization cannot silently change wire bytes/order.
def _original_header_values(headers, name):
    if type(headers) not in {list, tuple} or len(headers) > 128:
        return None
    values = []
    for pair in headers:
        if type(pair) not in {list, tuple} or len(pair) != 2:
            return None
        key, value = pair
        if type(key) is not bytes or len(key) > 128 or type(value) is not bytes:
            return None
        if key.lower() == name:
            if len(value) > 8192:
                return None
            values.append(value)
    return tuple(values)


def _original_response_headers(
    headers, *, request_headers, frontend_origins, correlation, owned_tracing
):
    import re

    if _original_header_values(headers, None) is None:
        return headers
    result = [
        (key, value)
        for key, value in headers
        if key.lower() not in {b"x-altimeter-trace-id", b"x-altimeter-trace-sampled"}
    ]
    if (
        type(frontend_origins) is frozenset
        and 0 < len(frontend_origins) <= 128
        and all(type(item) is str and len(item) <= 2048 for item in frontend_origins)
    ):
        result = [
            (key, value)
            for key, value in result
            if key.lower() != b"timing-allow-origin"
        ]
        vary = _original_header_values(result, b"vary")
        if vary is not None and sum(map(len, vary)) <= 8192:
            tokens = {
                token.strip().lower() for value in vary for token in value.split(b",")
            }
            if b"origin" not in tokens and b"*" not in tokens:
                result = [
                    (key, value) for key, value in result if key.lower() != b"vary"
                ] + [(b"vary", b", ".join((*vary, b"Origin")))]
            origins = _original_header_values(request_headers, b"origin")
            if origins is not None and len(origins) == 1:
                origin = origins[0]
                if (
                    len(origin) <= 2048
                    and origin not in {b"*", b"null"}
                    and all(33 <= byte <= 126 for byte in origin)
                    and origin.decode("ascii") in frontend_origins
                ):
                    result.append((b"timing-allow-origin", origin))
    valid_correlation = (
        type(correlation) is Correlation
        and type(correlation.trace_id) is str
        and len(correlation.trace_id) == 32
        and re.fullmatch("[0-9a-f]{32}", correlation.trace_id) is not None
        and correlation.trace_id != "0" * 32
        and type(correlation.span_id) is str
        and len(correlation.span_id) == 16
        and re.fullmatch("[0-9a-f]{16}", correlation.span_id) is not None
        and correlation.span_id != "0" * 16
        and type(correlation.trace_flags) is int
        and 0 <= correlation.trace_flags <= 255
    )
    if owned_tracing is True and valid_correlation:
        values = _original_header_values(result, b"cache-control")
        private = False
        if (
            values is not None
            and sum(map(len, values)) <= 8192
            and not any(
                b'"' in value
                or b"\\" in value
                or any(byte < 32 or byte > 126 for byte in value)
                for value in values
            )
        ):
            directives = {
                part.strip().lower() for value in values for part in value.split(b",")
            }
            names = {part.partition(b"=")[0].strip() for part in directives}
            private = bool(directives & {b"private", b"no-store"}) and not bool(
                names & {b"public", b"s-maxage"}
            )
        if private:
            result.extend(
                [
                    (b"x-altimeter-trace-id", correlation.trace_id.encode("ascii")),
                    (
                        b"x-altimeter-trace-sampled",
                        b"1" if correlation.trace_flags & 1 else b"0",
                    ),
                ]
            )
    return result


_EQUIVALENCE_RESPONSES = [
    [],
    [(b"cache-control", b"private, no-store")],
    [(b"Cache-Control", b"private"), (b"cache-control", b"public, max-age=60")],
    [(b"cache-control", b"private, s-maxage =60")],
    [(b"cache-control", b'private="Set-Cookie"')],
    [(b"cache-control", b"private, extension=\\value")],
    [(b"cache-control", b"private\t")],
    [(b"cache-control", b"private\x7f")],
    [(b"cache-control", b"no-store"), (b"cache-control", b"\xff")],
    [(b"vary", b"Cookie"), (b"Vary", b"Accept-Encoding")],
    [(b"vary", b"Cookie, oRiGiN"), (b"Vary", b"Accept-Encoding")],
    [(b"Vary", b"*"), (b"vary", b"Cookie")],
    [(b"vary", b"x" * 8192)],
    [(b"vary", b"x" * 8193)],
    [(b"vary", b"x" * 4096), (b"Vary", b"y" * 4097)],
    [(b"cache-control", b"private," + b"x" * 8184)],
    [(b"cache-control", b"private," + b"x" * 8185)],
    [(b"cache-control", b"private"), (b"Cache-Control", b"x" * 8186)],
    [(b"other", b"x" * 1_000_000), (b"cache-control", b"private")],
    [(b"x" * 128, b"ok"), (b"cache-control", b"private")],
    [(b"x" * 129, b"ok"), (b"cache-control", b"private")],
    [(HostileBytes(b"cache-control"), b"private")],
    [(b"cache-control", HostileBytes(b"private"))],
    [(b"cache-control", b"private"), (b"broken",)],
    [[b"cache-control", b"private"], [b"set-cookie", b"a=1"]],
    ((b"cache-control", b"private"), (b"set-cookie", b"a=1")),
    [(b"other", b"value")] * 129,
    *[
        [(b"cache-control", b"private")] + [(b"other", b"value")] * count
        for count in (125, 126, 127)
    ],
    [
        (b"Vary", b"Cookie"),
        (b"cache-control", b"Private, No-Store"),
        (b"Timing-Allow-Origin", b"*"),
        (b"set-cookie", b"a=1"),
        (b"set-cookie", b"b=2"),
        (b"X-Altimeter-Trace-Id", b"stale-id"),
        (b"X-Altimeter-Trace-Sampled", b"1"),
    ],
]
_EQUIVALENCE_REQUESTS = [
    [],
    [(b"origin", ORIGIN.encode())],
    [(b"Origin", b"https://other.example")],
    [(b"origin", ORIGIN.encode()), (b"Origin", ORIGIN.encode())],
    [(b"origin", b"*")],
    [(b"origin", b"null")],
    [(b"origin", b"")],
    [(b"origin", b"https://frontend.example\n")],
    [(b"origin", b"\xff")],
    [(b"origin", b"x" * 2048)],
    [(b"origin", b"x" * 2049)],
    [(b"origin", b"x" * 8193)],
    [(HostileBytes(b"origin"), ORIGIN.encode())],
    [(b"origin", ORIGIN.encode()), (b"bad",)],
    [(b"origin", ORIGIN.encode())] * 129,
    HostileContainer(),
]
_EQUIVALENCE_CONFIGURATIONS = [
    frozenset(),
    frozenset({ORIGIN}),
    frozenset({ORIGIN, "https://other.example"}),
    frozenset({ORIGIN, "*", "null", "", "https://frontend.example\n", "\u00e9"}),
    frozenset({"x" * 2048}),
    frozenset({ORIGIN, "x" * 2049}),
    frozenset({ORIGIN, 1}),
    {ORIGIN},
    frozenset(f"https://frontend{i}.example" for i in range(129)),
    HostileContainer(),
]


@pytest.mark.parametrize("frontend_origins", _EQUIVALENCE_CONFIGURATIONS)
@pytest.mark.parametrize("original", _EQUIVALENCE_RESPONSES)
def test_response_policy_matches_original_bytes_across_case_matrix(
    original, frontend_origins
):
    from django_altimeter import headers

    # The legacy API remains covered as well as the once-prepared runtime API.
    policy = headers.prepare_response_header_policy(frontend_origins)
    for request_headers in _EQUIVALENCE_REQUESTS:
        for owned, correlation in (
            (False, None),
            (True, CORRELATION),
            (True, Correlation(CORRELATION.trace_id, CORRELATION.span_id, 0)),
            (True, Correlation("0" * 32, CORRELATION.span_id, 1)),
            (True, Correlation(CORRELATION.trace_id, CORRELATION.span_id, True)),
            (1, CORRELATION),
        ):
            keywords = {
                "request_headers": request_headers,
                "frontend_origins": frontend_origins,
                "correlation": correlation,
                "owned_tracing": owned,
            }
            expected = _original_response_headers(original, **keywords)
            assert headers.response_headers(original, **keywords) == expected
            keywords.pop("frontend_origins")
            assert (
                headers.response_headers(original, policy=policy, **keywords)
                == expected
            )


def test_prepared_policy_is_frozen_and_does_not_reuse_dynamic_header_facts():
    from dataclasses import FrozenInstanceError

    from django_altimeter.headers import prepare_response_header_policy

    policy = prepare_response_header_policy(frozenset({ORIGIN}))
    with pytest.raises(FrozenInstanceError):
        policy.timing_enabled = False
    with pytest.raises(FrozenInstanceError):
        policy.allowed_origins = frozenset({b"*"})
    original = [(b"cache-control", b"private")]
    request = [(b"origin", ORIGIN.encode())]
    keywords = dict(
        request_headers=request,
        policy=policy,
        correlation=CORRELATION,
        owned_tracing=True,
    )
    positive = response_headers(original, **keywords)
    assert (b"timing-allow-origin", ORIGIN.encode()) in positive
    assert (b"x-altimeter-trace-id", CORRELATION.trace_id.encode()) in positive
    original[0] = (b"cache-control", b"private, public")
    request[0] = (b"origin", ORIGIN.encode() + b"\r\nsecret")
    assert response_headers(original, **keywords) == [
        (b"cache-control", b"private, public"),
        (b"vary", b"Origin"),
    ]
    original[0] = (b"cache-control", b"private")
    request[0] = (b"origin", ORIGIN.encode())
    keywords["correlation"] = Correlation("A" * 32, CORRELATION.span_id, 1)
    assert response_headers(original, **keywords) == [
        (b"cache-control", b"private"),
        (b"vary", b"Origin"),
        (b"timing-allow-origin", ORIGIN.encode()),
    ]


def test_prepared_configuration_does_not_call_user_accessors():
    from django_altimeter.headers import prepare_response_header_policy

    class HostileOrigin(str):
        def __len__(self):
            raise AssertionError("user len called")

        def isascii(self):
            raise AssertionError("user isascii called")

        def encode(self, *args, **kwargs):
            raise AssertionError("user encode called")

    for origins in (HostileContainer(), frozenset({HostileOrigin(ORIGIN)})):
        policy = prepare_response_header_policy(origins)
        assert not policy.timing_enabled
        assert policy.allowed_origins == frozenset()


@pytest.mark.parametrize(
    "origin",
    ["http://internal.example", "https://internal.example:8443", "http://[::1]:8080"],
)
def test_internal_origins_accept_declared_http_and_https(origin):
    from django_altimeter.headers import normalize_internal_origin

    assert normalize_internal_origin(origin) is not None


@pytest.mark.parametrize(
    "origin",
    [
        "http://user@internal.example",
        "http://internal.example/",
        "https://internal.example/path",
        "https://internal.example?",
        "https://internal.example#",
        "http://*.example",
        "ftp://internal.example",
        "http://internal.example:0",
    ],
)
def test_internal_origins_reject_non_origin_components(origin):
    from django_altimeter.headers import normalize_internal_origin

    assert normalize_internal_origin(origin) is None


@pytest.mark.parametrize(
    "url,approved",
    [
        ("http://internal.example/safe?CANARY=1", True),
        ("http://internal.example:80/safe", True),
        ("https://internal.example/safe", False),
        ("http://internal.example:81/safe", False),
        ("http://internal.example.attacker/safe", False),
        ("http://user@internal.example/safe", False),
        ("http://internal.example\\@attacker/safe", False),
    ],
)
def test_outbound_headers_are_local_valid_context_for_exact_origin(url, approved):
    from django_altimeter.headers import outgoing_headers

    correlation = Correlation("1" * 32, "2" * 16, 1)
    result = outgoing_headers(
        url,
        internal_origins=frozenset({"http://internal.example"}),
        correlation=correlation,
        owned_tracing=True,
    )
    assert result == (
        {"traceparent": "00-" + "1" * 32 + "-" + "2" * 16 + "-01"} if approved else {}
    )
    assert (
        outgoing_headers(
            url,
            internal_origins=frozenset({"http://internal.example"}),
            correlation=correlation,
            owned_tracing=1,
        )
        == {}
    )


@pytest.mark.parametrize(
    "trusted, configured",
    [(True, True), (False, True), (True, False), (1, True), (True, 1), ("true", True)],
)
def test_wsgi_environ_delegates_literal_true_trust_to_shared_policy(
    trusted, configured
):
    from django_altimeter.headers import wsgi_request_headers

    pairs = wsgi_request_headers(
        {
            "HTTP_TRACEPARENT": TRACEPARENT.decode(),
            "HTTP_ORIGIN": ORIGIN,
            "HTTP_TRACESTATE": "private=identity-canary",
            "HTTP_BAGGAGE": "posthogDistinctId=identity-canary",
            "HTTP_X_POSTHOG_DISTINCT_ID": "identity-canary",
            "HTTP_X_ALTIMETER_TRUSTED": "true",
        }
    )
    assert pairs == [(b"traceparent", TRACEPARENT), (b"origin", ORIGIN.encode())]
    clean = incoming_context(
        pairs, trusted_ingress=trusted, trust_remote_parent=configured
    )
    current = trace.get_current_span(clean).get_span_context()
    assert current.is_valid is (trusted is True and configured is True)
    assert not current.trace_state
    assert not baggage.get_all(clean)


@pytest.mark.parametrize(
    "value", [None, 1, b"bytes", "x" * 1_000_000, "\ud800", "é", HostileContainer()]
)
def test_wsgi_request_headers_reject_values_before_encoding(value):
    from django_altimeter.headers import wsgi_request_headers

    assert wsgi_request_headers({"HTTP_ORIGIN": value, "HTTP_TRACEPARENT": value}) == []
    assert wsgi_request_headers(
        {"HTTP_ORIGIN": value, "HTTP_TRACEPARENT": TRACEPARENT.decode()}
    ) == [(b"traceparent", TRACEPARENT)]


def test_wsgi_request_header_selection_never_traverses_unrelated_environ_values():
    from django_altimeter.headers import wsgi_request_headers

    class HostileEnviron(dict):
        def get(self, key, default=None):
            raise AssertionError("Subclass accessor invoked")

    class HostileText(str):
        def encode(self, *args, **kwargs):
            raise AssertionError("Subclass encoder invoked")

        def __len__(self):
            raise AssertionError("Subclass length invoked")

    assert wsgi_request_headers(HostileEnviron(HTTP_ORIGIN=ORIGIN)) == []
    assert wsgi_request_headers({"HTTP_ORIGIN": HostileText(ORIGIN)}) == []
    environ = {f"unrelated_{index}": HostileContainer() for index in range(50000)}
    environ["HTTP_ORIGIN"] = ORIGIN
    assert wsgi_request_headers(environ) == [(b"origin", ORIGIN.encode())]


@pytest.mark.parametrize("owned", [False, True, 1])
@pytest.mark.parametrize(
    "origin", [None, ORIGIN, ORIGIN + ":443", ORIGIN + ", " + ORIGIN]
)
@pytest.mark.parametrize(
    "original",
    [
        [
            ("Cache-Control", "private, no-store"),
            ("Vary", "Cookie"),
            ("Vary", "Accept-Encoding"),
        ],
        [("Cache-Control", "public, max-age=60"), ("X-Altimeter-Trace-Id", "stale")],
        [("Cache-Control", 'private="Set-Cookie"'), ("Timing-Allow-Origin", "*")],
        [
            ("Cache-Control", "private"),
            ("Vary", "*"),
            ("Set-Cookie", "a=one"),
            ("Set-Cookie", "b=two"),
        ],
        [
            ("Cache-Control", "private"),
            ("Vary", "Cookie, oRiGiN"),
            ("X-Business", "café"),
        ],
    ],
)
def test_wsgi_response_policy_matches_asgi_without_changing_application_headers(
    original, origin, owned
):
    from django_altimeter.headers import (
        prepare_response_header_policy,
        wsgi_request_headers,
        wsgi_response_headers,
    )

    environ = {"HTTP_ORIGIN": origin} if origin is not None else {}
    policy = prepare_response_header_policy(frozenset({ORIGIN}))
    before = list(original)
    expected = response_headers(
        [(key.encode("latin-1"), value.encode("latin-1")) for key, value in original],
        request_headers=wsgi_request_headers(environ),
        policy=policy,
        correlation=CORRELATION,
        owned_tracing=owned,
    )
    result = wsgi_response_headers(
        original,
        environ=environ,
        policy=policy,
        correlation=CORRELATION,
        owned_tracing=owned,
    )
    assert result == [
        (key.decode("latin-1"), value.decode("latin-1")) for key, value in expected
    ]
    assert original == before
    assert all(type(key) is type(value) is str for key, value in result)


def test_wsgi_response_header_bounds_are_checked_before_allocation_or_accessors():
    import tracemalloc

    from django_altimeter.headers import (
        prepare_response_header_policy,
        wsgi_response_headers,
    )

    class HostileText(str):
        def encode(self, *args, **kwargs):
            raise AssertionError("Subclass encoder invoked")

        def __len__(self):
            raise AssertionError("Subclass length invoked")

    policy = prepare_response_header_policy(frozenset({ORIGIN}))
    for original in (
        [("X-Business", "x" * 1_000_000)],
        [("Vary", "x" * 8193)],
        [("x" * 129, "value")],
        [("X-Business", "value")] * 129,
        [("X-Business", HostileText("canary"))],
        [(HostileText("X-Business"), "canary")],
        [("X-Business", "\ud800")],
        [("broken",)],
        HostileContainer(),
    ):
        tracemalloc.start()
        result = wsgi_response_headers(
            original,
            environ={"HTTP_ORIGIN": ORIGIN},
            policy=policy,
            correlation=CORRELATION,
            owned_tracing=True,
        )
        _, peak = tracemalloc.get_traced_memory()
        tracemalloc.stop()
        assert result is original
        assert peak < 32768
