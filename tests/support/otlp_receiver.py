"""Small real OTLP/HTTP receiver; successful requests are decoded before capture.

Use ``with OTLPReceiver() as receiver`` or the integration ``otlp_receiver``
fixture. Each instance binds loopback on an OS-assigned port and owns its server
thread. Teardown interrupts active clients before waiting for that thread.
Only uncompressed protobuf POSTs are supported. Responses default to valid empty
protobuf success. Queue literal wire responses with ``queue_response`` for
transport tests; all waits end on peer disconnect or receiver teardown.
"""

from collections import deque
from collections.abc import Mapping
from dataclasses import dataclass, field
from http.client import HTTPMessage
from http.server import BaseHTTPRequestHandler, HTTPServer
from math import isfinite
from select import select
from socket import SHUT_RDWR, socket
from threading import Condition, Event, Thread
from time import monotonic
from types import TracebackType
from typing import Literal, Self, cast

from google.protobuf.message import DecodeError, Message
from opentelemetry.proto.collector.logs.v1.logs_service_pb2 import (
    ExportLogsServiceRequest,
    ExportLogsServiceResponse,
)
from opentelemetry.proto.collector.metrics.v1.metrics_service_pb2 import (
    ExportMetricsServiceRequest,
    ExportMetricsServiceResponse,
)
from opentelemetry.proto.collector.trace.v1.trace_service_pb2 import (
    ExportTraceServiceRequest,
    ExportTraceServiceResponse,
)

type Signal = Literal["traces", "logs", "metrics"]
type ExportRequest = (
    ExportTraceServiceRequest | ExportLogsServiceRequest | ExportMetricsServiceRequest
)

_MAX_REQUEST_BYTES = 1024 * 1024


def _interrupt_client(connection: socket) -> None:
    try:
        connection.shutdown(SHUT_RDWR)
    except OSError:
        pass  # The handler may have already closed this connection.


@dataclass(frozen=True)
class ResponseEvents:
    """Wire milestones; a body write is not evidence that a client read it.

    ``client_closed`` records observed peer EOF or connection failure, excluding
    local receiver shutdown. ``finished`` marks response handling completion.
    """

    headers_sent: Event = field(default_factory=Event)
    body_started: Event = field(default_factory=Event)
    client_closed: Event = field(default_factory=Event)
    finished: Event = field(default_factory=Event)


@dataclass(frozen=True)
class _Response:
    status: int = 200
    body: bytes = b""
    headers: tuple[tuple[str, str], ...] = ()
    reason: str | None = None
    header_delay: float = 0
    body_delay: float = 0
    chunk_size: int = 65536
    chunk_delay: float = 0
    hang: bool = False
    send_content_length: bool = True
    events: ResponseEvents = field(default_factory=ResponseEvents)


@dataclass(frozen=True)
class CapturedRequest:
    """A decoded request and its original case-insensitive HTTP headers."""

    path: str
    headers: HTTPMessage
    message: ExportRequest


class OTLPReceiver:
    """Capture OTLP requests over a real socket without exporting elsewhere."""

    def __init__(self) -> None:
        self._requests: list[CapturedRequest] = []
        self._responses: deque[_Response] = deque()
        self._received = Condition()
        self._active_connection: socket | None = None
        self._stopping = Event()
        receiver = self

        class Handler(BaseHTTPRequestHandler):
            def setup(self) -> None:
                super().setup()
                self.connection.settimeout(2)
                with receiver._received:
                    receiver._active_connection = self.connection
                    stopping = receiver._stopping.is_set()
                if stopping:
                    # Cover acceptance between teardown's snapshot and shutdown.
                    _interrupt_client(self.connection)

            def handle(self) -> None:
                try:
                    super().handle()
                except ConnectionError:
                    self.close_connection = True

            def finish(self) -> None:
                try:
                    super().finish()
                finally:
                    with receiver._received:
                        receiver._active_connection = None

            def do_POST(self) -> None:
                message: ExportRequest
                response: Message
                if self.path == "/v1/traces":
                    message = ExportTraceServiceRequest()
                    response = ExportTraceServiceResponse()
                elif self.path == "/v1/logs":
                    message = ExportLogsServiceRequest()
                    response = ExportLogsServiceResponse()
                elif self.path == "/v1/metrics":
                    message = ExportMetricsServiceRequest()
                    response = ExportMetricsServiceResponse()
                else:
                    self.send_error(404, "Unknown OTLP endpoint")
                    return

                if (
                    self.headers.get_content_type() != "application/x-protobuf"
                    or self.headers.get("Content-Encoding", "identity") != "identity"
                ):
                    self.send_error(415, "Expected uncompressed protobuf")
                    return
                try:
                    length = int(self.headers.get("Content-Length", "-1"))
                except ValueError:
                    self.send_error(400, "Invalid content length")
                    return
                if not 0 <= length <= _MAX_REQUEST_BYTES:
                    self.send_error(400, "Missing or oversized content length")
                    return
                body = self.rfile.read(length)
                if len(body) != length:
                    self.send_error(400, "Incomplete protobuf body")
                    return
                try:
                    message.ParseFromString(body)
                except DecodeError:
                    self.send_error(400, "Invalid protobuf")
                    return

                with receiver._received:
                    receiver._requests.append(
                        CapturedRequest(
                            self.path, cast(HTTPMessage, self.headers), message
                        )
                    )
                    scripted = (
                        receiver._responses.popleft()
                        if receiver._responses
                        else _Response(body=response.SerializeToString())
                    )
                    receiver._received.notify_all()

                self._send_script(scripted)

            def _send_script(self, scripted: _Response) -> None:
                events = scripted.events
                try:
                    if scripted.hang:
                        receiver._wait_for_response(self.connection, None, events)
                        return
                    if not receiver._wait_for_response(
                        self.connection, scripted.header_delay, events
                    ):
                        return
                    self.send_response(scripted.status, scripted.reason)
                    headers = {"content-type": "application/x-protobuf"}
                    if scripted.send_content_length:
                        headers["content-length"] = str(len(scripted.body))
                    headers.update(
                        (name.lower(), value) for name, value in scripted.headers
                    )
                    for name, value in headers.items():
                        self.send_header(name, value)
                    self.end_headers()
                    events.headers_sent.set()
                    if not receiver._wait_for_response(
                        self.connection, scripted.body_delay, events
                    ):
                        return
                    for offset in range(0, len(scripted.body), scripted.chunk_size):
                        if offset and not receiver._wait_for_response(
                            self.connection, scripted.chunk_delay, events
                        ):
                            return
                        if receiver._stopping.is_set():
                            return
                        events.body_started.set()
                        self.wfile.write(
                            scripted.body[offset : offset + scripted.chunk_size]
                        )
                except ConnectionError:
                    if not receiver._stopping.is_set():
                        events.client_closed.set()
                    self.close_connection = True
                finally:
                    events.finished.set()

            def log_message(self, format: str, *args: object) -> None:
                """Keep request data out of pytest's diagnostic output."""

        self._server = HTTPServer(("127.0.0.1", 0), Handler)
        self._thread = Thread(
            target=self._server.serve_forever,
            kwargs={"poll_interval": 0.01},
            name="otlp-test-receiver",
        )

    def _wait_for_response(
        self, connection: socket, delay: float | None, events: ResponseEvents
    ) -> bool:
        """Wait out a delay, stopping early on peer EOF or local teardown."""
        deadline = None if delay is None else monotonic() + delay
        while not self._stopping.is_set():
            remaining = None if deadline is None else deadline - monotonic()
            if remaining is not None and remaining <= 0:
                return True
            ready, _, _ = select(
                [connection],
                [],
                [],
                0.05 if remaining is None else min(0.05, remaining),
            )
            if not ready:
                continue
            if self._stopping.is_set():
                return False
            try:
                data = connection.recv(1)
            except ConnectionError:
                data = b""
            if not data:
                if not self._stopping.is_set():
                    events.client_closed.set()
                return False
        return False

    def queue_response(
        self,
        *,
        status: int = 200,
        body: bytes = b"",
        headers: Mapping[str, str] | None = None,
        reason: str | None = None,
        header_delay: float = 0,
        body_delay: float = 0,
        chunk_size: int = 65536,
        chunk_delay: float = 0,
        hang: bool = False,
        send_content_length: bool = True,
    ) -> ResponseEvents:
        """Queue one wire response for the next successfully decoded POST.

        Scripts are FIFO across signals; malformed requests consume none.
        Empty queues return normal empty protobuf success. Header names override
        defaults case-insensitively. Disable content length for close framing.
        Delays and hangs end on peer disconnect or receiver exit. A hang sends
        no headers; body_delay starts after headers, chunk_delay between writes.

        For example, queue_response(status=503, headers={"Retry-After": "0"})
        followed by queue_response() produces one retryable response then success.
        Supply generated per-signal response bytes for warning/partial replies.
        """
        if not 100 <= status <= 599 or type(body) is not bytes:
            raise ValueError("Expected HTTP status and bytes body")
        if type(chunk_size) is not int or chunk_size < 1:
            raise ValueError("Expected positive chunk size")
        if any(
            not isfinite(delay) or delay < 0
            for delay in (header_delay, body_delay, chunk_delay)
        ):
            raise ValueError("Expected finite nonnegative delays")
        response = _Response(
            status=status,
            body=body,
            headers=tuple(headers.items()) if headers is not None else (),
            reason=reason,
            header_delay=header_delay,
            body_delay=body_delay,
            chunk_size=chunk_size,
            chunk_delay=chunk_delay,
            hang=hang,
            send_content_length=send_content_length,
        )
        with self._received:
            if self._stopping.is_set():
                raise RuntimeError("Receiver is stopping")
            self._responses.append(response)
        return response.events

    @property
    def endpoint(self) -> str:
        """Base URL; append the signal path with ``url(signal)``."""
        return f"http://127.0.0.1:{self._server.server_port}"

    def url(self, signal: Signal) -> str:
        if signal not in ("traces", "logs", "metrics"):
            raise ValueError("Unknown OTLP signal")
        return f"{self.endpoint}/v1/{signal}"

    @property
    def requests(self) -> tuple[CapturedRequest, ...]:
        """Snapshot all successfully decoded requests in receipt order."""
        with self._received:
            return tuple(self._requests)

    def wait_for_requests(
        self, count: int = 1, timeout: float = 5.0
    ) -> tuple[CapturedRequest, ...]:
        """Wait for at least ``count`` captures; fail instead of waiting forever."""
        if count < 1 or timeout < 0:
            raise ValueError("Expected positive count and nonnegative timeout")
        with self._received:
            if not self._received.wait_for(
                lambda: len(self._requests) >= count, timeout
            ):
                raise TimeoutError(
                    f"Expected {count} OTLP requests; received {len(self._requests)}"
                )
            return tuple(self._requests)

    def __enter__(self) -> Self:
        self._thread.start()
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc_value: BaseException | None,
        traceback: TracebackType | None,
    ) -> None:
        with self._received:
            self._stopping.set()
            connection = self._active_connection
        if connection is not None:
            _interrupt_client(connection)
        self._server.shutdown()
        self._server.server_close()
        self._thread.join(timeout=5)
        if self._thread.is_alive():
            raise RuntimeError("OTLP receiver thread did not stop")
