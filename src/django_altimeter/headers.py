"""Bounded, framework-independent ingress and response header policy."""

import logging
import re
from typing import TypeGuard, cast

from opentelemetry.context import Context
from opentelemetry.trace.propagation.tracecontext import TraceContextTextMapPropagator

from django_altimeter.context import Correlation

_HEADERS_LIMIT = 128
_VALUE_LIMIT = 8192
_CORRELATION_HEADERS = frozenset(
    {b"x-altimeter-trace-id", b"x-altimeter-trace-sampled"}
)
Headers = list[tuple[bytes, bytes]]


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


def _vary_origin(headers: Headers) -> Headers | None:
    values = _header_values(headers, b"vary")
    if values is None or sum(map(len, values)) > _VALUE_LIMIT:
        return None
    tokens = {token.strip().lower() for value in values for token in value.split(b",")}
    if b"origin" in tokens or b"*" in tokens:
        return headers
    combined = b", ".join((*values, b"Origin"))
    return [(key, value) for key, value in headers if key.lower() != b"vary"] + [
        (b"vary", combined)
    ]


def _cache_is_private(headers: Headers) -> bool:
    values = _header_values(headers, b"cache-control")
    if values is None or sum(map(len, values)) > _VALUE_LIMIT:
        return False
    # Qualified private directives can permit shared caching of other headers.
    # Decline quoted/escaped policies instead of interpreting a partial grammar.
    if any(
        b'"' in value
        or b"\\" in value
        or any(byte < 32 or byte > 126 for byte in value)
        for value in values
    ):
        return False
    directives = {
        part.strip().lower() for value in values for part in value.split(b",")
    }
    names = {part.partition(b"=")[0].strip() for part in directives}
    return bool(directives & {b"private", b"no-store"}) and not bool(
        names & {b"public", b"s-maxage"}
    )


def _valid_correlation(value: object) -> TypeGuard[Correlation]:
    return (
        type(value) is Correlation
        and type(value.trace_id) is str
        and len(value.trace_id) == 32
        and re.fullmatch("[0-9a-f]{32}", value.trace_id) is not None
        and value.trace_id != "0" * 32
        and type(value.span_id) is str
        and len(value.span_id) == 16
        and re.fullmatch("[0-9a-f]{16}", value.span_id) is not None
        and value.span_id != "0" * 16
        and type(value.trace_flags) is int
        and 0 <= value.trace_flags <= 255
    )


def response_headers(
    headers: Headers,
    *,
    request_headers: object,
    frontend_origins: frozenset[str] = frozenset(),
    correlation: Correlation | None = None,
    owned_tracing: bool = False,
) -> Headers:
    """Copy headers and apply exact-origin timing and cache-safe owned IDs.

    Existing cache directives are never relaxed. Per-request IDs are omitted on
    cacheable responses; the application must explicitly use private/no-store.
    """
    try:
        if _header_values(headers, None) is None:
            return headers
        result = [
            (key, value)
            for key, value in headers
            if key.lower() not in _CORRELATION_HEADERS
        ]
        if (
            type(frontend_origins) is frozenset
            and 0 < len(frontend_origins) <= 128
            and all(
                type(item) is str and len(item) <= 2048 for item in frontend_origins
            )
        ):
            result = [
                (key, value)
                for key, value in result
                if key.lower() != b"timing-allow-origin"
            ]
            varied = _vary_origin(result)
            if varied is not None:
                result = varied
                origins = _header_values(request_headers, b"origin")
                if origins is not None and len(origins) == 1:
                    origin = origins[0]
                    if (
                        len(origin) <= 2048
                        and origin not in {b"*", b"null"}
                        and all(33 <= byte <= 126 for byte in origin)
                        and origin.decode("ascii") in frontend_origins
                    ):
                        result.append((b"timing-allow-origin", origin))
        if (
            owned_tracing is True
            and _valid_correlation(correlation)
            and _cache_is_private(result)
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
