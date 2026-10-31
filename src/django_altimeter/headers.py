"""Bounded, framework-independent ingress and response header policy."""

import logging
import re
from dataclasses import InitVar, dataclass, field
from typing import TypeGuard, cast

from opentelemetry.context import Context
from opentelemetry.trace.propagation.tracecontext import TraceContextTextMapPropagator

from django_altimeter.context import Correlation

_HEADERS_LIMIT = 128
_VALUE_LIMIT = 8192
_CORRELATION_HEADERS = frozenset(
    {b"x-altimeter-trace-id", b"x-altimeter-trace-sampled"}
)
_TRACE_ID = re.compile("[0-9a-f]{32}")
_SPAN_ID = re.compile("[0-9a-f]{16}")
_ORIGIN_VALUE = re.compile(rb"[\x21-\x7e]*")
_UNSAFE_CACHE_VALUE = re.compile(rb'["\\\x00-\x1f\x7f-\xff]')
Headers = list[tuple[bytes, bytes]]


@dataclass(frozen=True)
class ResponseHeaderPolicy:
    """Once-validated immutable origin policy; never retains request data."""

    frontend_origins: InitVar[object]
    timing_enabled: bool = field(init=False, default=False)
    allowed_origins: frozenset[bytes] = field(
        init=False, default=frozenset(), repr=False
    )

    def __post_init__(self, frontend_origins: object) -> None:
        if (
            type(frontend_origins) is not frozenset
            or not 0 < len(frontend_origins) <= 128
            or not all(
                type(item) is str and len(item) <= 2048 for item in frontend_origins
            )
        ):
            return
        allowed = set()
        for origin in cast(frozenset[str], frontend_origins):
            if origin.isascii():
                encoded = origin.encode("ascii")
                if encoded not in {b"*", b"null"} and _ORIGIN_VALUE.fullmatch(encoded):
                    allowed.add(encoded)
        object.__setattr__(self, "timing_enabled", True)
        object.__setattr__(self, "allowed_origins", frozenset(allowed))


def prepare_response_header_policy(frontend_origins: object) -> ResponseHeaderPolicy:
    """Prepare frozen configuration once when constructing the owned runtime."""
    return ResponseHeaderPolicy(frontend_origins)


def _header_values(headers: object, name: bytes | None) -> tuple[bytes, ...] | None:
    if type(headers) not in {list, tuple}:
        return None
    pairs = cast(list[object] | tuple[object, ...], headers)
    if len(pairs) > _HEADERS_LIMIT:
        return None
    values: list[bytes] = []
    for pair in pairs:
        if type(pair) not in {list, tuple}:
            return None
        parts = cast(list[object] | tuple[object, ...], pair)
        if len(parts) != 2:
            return None
        key, value = parts
        if type(key) is not bytes or len(key) > 128 or type(value) is not bytes:
            return None
        if key.lower() == name:
            if len(value) > _VALUE_LIMIT:
                return None
            values.append(value)
    return tuple(values)


def _diagnostic(code: str) -> None:
    try:
        logging.getLogger("django_altimeter.headers").warning("%s", code)
    except Exception:
        pass


def incoming_context(
    headers: object,
    *,
    trusted_ingress: bool = False,
    trust_remote_parent: bool = False,
) -> Context:
    """Extract only W3C traceparent on an explicitly trusted deployment branch.

    Neither client headers nor ambient context can grant ingress trust. All
    baggage, tracestate and browser identity hints are deliberately absent.
    """
    empty = Context()
    try:
        if trusted_ingress is not True or trust_remote_parent is not True:
            return empty
        values = _header_values(headers, b"traceparent")
        if values is None or len(values) != 1:
            return empty
        value = values[0]
        if not 55 <= len(value) <= 512 or any(
            byte < 33 or byte > 126 for byte in value
        ):
            return empty
        return TraceContextTextMapPropagator().extract(
            {"traceparent": value.decode("ascii")}, context=empty
        )
    except Exception:
        _diagnostic("http.propagation_failed")
        return empty


def _response_header_parts(
    headers: object, timing_enabled: bool
) -> tuple[Headers, list[bytes], list[bytes]] | None:
    """Validate once, retaining wire order and only the policy's input fields."""
    if type(headers) is not list and type(headers) is not tuple:
        return None
    if len(headers) > _HEADERS_LIMIT:
        return None
    result: Headers = []
    vary: list[bytes] = []
    cache: list[bytes] = []
    for pair in headers:
        if type(pair) is not list and type(pair) is not tuple:
            return None
        if len(pair) != 2:
            return None
        key, value = pair
        if type(key) is not bytes or len(key) > 128 or type(value) is not bytes:
            return None
        name = key.lower()
        if name in _CORRELATION_HEADERS:
            continue
        if timing_enabled and name == b"timing-allow-origin":
            continue
        result.append((key, value))
        if name == b"vary":
            vary.append(value)
        elif name == b"cache-control":
            cache.append(value)
    return result, vary, cache


def _vary_origin(headers: Headers, values: list[bytes]) -> Headers | None:
    if sum(map(len, values)) > _VALUE_LIMIT:
        return None
    if any(
        token.strip().lower() in {b"origin", b"*"}
        for value in values
        for token in value.split(b",")
    ):
        return headers
    combined = b", ".join((*values, b"Origin"))
    return [(key, value) for key, value in headers if key.lower() != b"vary"] + [
        (b"vary", combined)
    ]


def _cache_is_private(values: list[bytes]) -> bool:
    if sum(map(len, values)) > _VALUE_LIMIT:
        return False
    private = False
    for value in values:
        # Qualified private directives can permit shared caching of other headers.
        # Decline quoted/escaped policies instead of interpreting a partial grammar.
        if _UNSAFE_CACHE_VALUE.search(value):
            return False
        for part in value.split(b","):
            directive = part.strip().lower()
            if directive.partition(b"=")[0].strip() in {b"public", b"s-maxage"}:
                return False
            if directive in {b"private", b"no-store"}:
                private = True
    return private


def _valid_correlation(value: object) -> TypeGuard[Correlation]:
    return (
        type(value) is Correlation
        and type(value.trace_id) is str
        and len(value.trace_id) == 32
        and _TRACE_ID.fullmatch(value.trace_id) is not None
        and value.trace_id != "0" * 32
        and type(value.span_id) is str
        and len(value.span_id) == 16
        and _SPAN_ID.fullmatch(value.span_id) is not None
        and value.span_id != "0" * 16
        and type(value.trace_flags) is int
        and 0 <= value.trace_flags <= 255
    )


def response_headers(
    headers: Headers,
    *,
    request_headers: object,
    frontend_origins: frozenset[str] = frozenset(),
    policy: ResponseHeaderPolicy | None = None,
    correlation: Correlation | None = None,
    owned_tracing: bool = False,
) -> Headers:
    """Copy headers and apply exact-origin timing and cache-safe owned IDs.

    Pass a prepared policy to avoid revalidating frozen runtime configuration.
    Existing cache directives are never relaxed. Per-request IDs are omitted on
    cacheable responses; the application must explicitly use private/no-store.
    """
    try:
        if type(policy) is not ResponseHeaderPolicy:
            policy = prepare_response_header_policy(frontend_origins)
        parts = _response_header_parts(headers, policy.timing_enabled)
        if parts is None:
            return headers
        result, vary, cache = parts
        if policy.timing_enabled:
            varied = _vary_origin(result, vary)
            if varied is not None:
                result = varied
                origins = _header_values(request_headers, b"origin")
                if origins is not None and len(origins) == 1:
                    origin = origins[0]
                    # Membership in these prepared bytes also validates ASCII,
                    # control characters and the forbidden wildcard/null values.
                    if len(origin) <= 2048 and origin in policy.allowed_origins:
                        result.append((b"timing-allow-origin", origin))
        if (
            owned_tracing is True
            and _valid_correlation(correlation)
            and len(result) <= _HEADERS_LIMIT
            and _cache_is_private(cache)
        ):
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
    except Exception:
        _diagnostic("http.headers_failed")
        return headers


def response_mode(headers: object) -> str:
    """Classify MCP response media without inferring streaming from body chunks."""
    try:
        values = _header_values(headers, b"content-type")
        if values is None or len(values) != 1:
            return "unknown"
        value = values[0]
        if len(value) > 256 or any(byte < 32 or byte > 126 for byte in value):
            return "unknown"
        media_type = value.partition(b";")[0].strip().lower()
        if media_type == b"text/event-stream":
            return "streaming"
        if media_type == b"application/json":
            return "buffered"
    except Exception:
        _diagnostic("http.response_mode_failed")
    return "unknown"


# Only propagation/identity headers are removed; application auth and body headers
# remain the client's responsibility. A fixed set also covers mixed-case carriers.
OUTBOUND_CONTEXT_HEADERS = frozenset(
    {
        "traceparent",
        "tracestate",
        "baggage",
        "x-altimeter-trace-id",
        "x-altimeter-trace-sampled",
        "posthogdistinctid",
        "altimeter.tenant_id",
        "sessionid",
        "x-posthog-distinct-id",
        "x-posthog-session-id",
        "x-tenant-id",
        "x-session-id",
    }
)


def _destination_origin(value: object, *, origin_only: bool = False) -> str | None:
    from urllib.parse import urlsplit

    if type(value) is not str or len(value) > 2048 or not value.isascii():
        return None
    if any(ord(char) <= 32 or ord(char) >= 127 for char in value) or "\\" in value:
        return None
    try:
        parsed = urlsplit(value)
        if (
            parsed.scheme not in {"http", "https"}
            or not parsed.hostname
            or parsed.username is not None
            or parsed.password is not None
            or "*" in parsed.netloc
            or "%" in parsed.netloc
        ):
            return None
        if origin_only and (parsed.path or "?" in value or "#" in value):
            return None
        port = parsed.port or (443 if parsed.scheme == "https" else 80)
        if parsed.port == 0 or not 1 <= port <= 65535:
            return None
        host = parsed.hostname
        if ":" in host:
            from ipaddress import IPv6Address

            host = f"[{IPv6Address(host).compressed}]"
        return f"{parsed.scheme}://{host}:{port}"
    except (ValueError, UnicodeError):
        return None


def normalize_internal_origin(value: object) -> str | None:
    """Validate a declared HTTP(S) origin independently of exporter security."""
    return _destination_origin(value, origin_only=True)


def outgoing_headers(
    url: object,
    *,
    internal_origins: frozenset[str],
    correlation: Correlation | None = None,
    owned_tracing: bool = False,
) -> dict[str, str]:
    """Generate only a bounded local W3C parent for an exact declared origin.

    Client boundaries remove OUTBOUND_CONTEXT_HEADERS from their native carrier
    before applying this result, including on every redirect hop. No global
    propagator is consulted, so baggage and tracestate cannot be injected.
    """
    if owned_tracing is not True or not _valid_correlation(correlation):
        return {}
    if type(internal_origins) is not frozenset or len(internal_origins) > 128:
        return {}
    destination = _destination_origin(url)
    if destination is None:
        return {}
    if not any(
        normalize_internal_origin(origin) == destination for origin in internal_origins
    ):
        return {}
    flags = "01" if correlation.trace_flags & 1 else "00"
    return {"traceparent": f"00-{correlation.trace_id}-{correlation.span_id}-{flags}"}


def wsgi_request_headers(environ: object) -> Headers:
    """Read only the bounded WSGI fields consumed by the shared header policy."""
    if type(environ) is not dict:
        return []
    result: Headers = []
    for key, name, maximum in (
        ("HTTP_TRACEPARENT", b"traceparent", 512),
        ("HTTP_ORIGIN", b"origin", 2048),
    ):
        value = environ.get(key)
        if type(value) is str and len(value) <= maximum and value.isascii():
            result.append((name, value.encode("ascii")))
    return result


def wsgi_response_headers(
    headers: list[tuple[str, str]],
    *,
    environ: object,
    policy: ResponseHeaderPolicy | None,
    correlation: Correlation | None = None,
    owned_tracing: bool = False,
) -> list[tuple[str, str]]:
    """Apply the shared frozen policy to bounded native WSGI string headers.

    Native WSGI uses Latin-1 strings. Check lengths before encoding, and leave
    malformed or oversized application headers untouched on telemetry failure.
    """
    try:
        if type(headers) not in {list, tuple} or len(headers) > _HEADERS_LIMIT:
            return headers
        encoded: Headers = []
        for pair in cast(list[object], headers):
            if type(pair) not in {list, tuple}:
                return headers
            parts = cast(list[object] | tuple[object, ...], pair)
            if len(parts) != 2:
                return headers
            key, value = parts
            if (
                type(key) is not str
                or len(key) > 128
                or type(value) is not str
                or len(value) > _VALUE_LIMIT
            ):
                return headers
            encoded.append((key.encode("latin-1"), value.encode("latin-1")))
        projected = response_headers(
            encoded,
            request_headers=wsgi_request_headers(environ),
            policy=policy,
            correlation=correlation,
            owned_tracing=owned_tracing,
        )
        return [
            (key.decode("latin-1"), value.decode("latin-1")) for key, value in projected
        ]
    except Exception:
        _diagnostic("http.headers_failed")
        return headers
