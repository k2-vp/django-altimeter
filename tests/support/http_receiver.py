"""A real ephemeral HTTP destination for outbound header/redirect proofs."""

from collections import deque
from dataclasses import dataclass
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from socket import SHUT_RDWR, socket
from threading import Condition, Event, Thread
from types import TracebackType
from typing import Self


@dataclass(frozen=True)
class HTTPRequest:
    method: str
    path: str
    headers: tuple[tuple[str, str], ...]
    body: bytes


class HTTPReceiver:
    def __init__(self) -> None:
        self.requests: list[HTTPRequest] = []
        self._responses: deque[tuple[int, dict[str, str], bytes, float]] = deque()
        self._condition = Condition()
        self._connections: set[socket] = set()
        self._stopping = Event()
        receiver = self

        class Handler(BaseHTTPRequestHandler):
            def setup(self) -> None:
                super().setup()
                self.connection.settimeout(2)
                with receiver._condition:
                    receiver._connections.add(self.connection)

            def finish(self) -> None:
                try:
                    super().finish()
                finally:
                    with receiver._condition:
                        receiver._connections.discard(self.connection)

            def do_GET(self) -> None:
                size = int(self.headers.get("Content-Length", "0"))
                if not 0 <= size <= 65536:
                    self.send_error(413)
                    return
                body = self.rfile.read(size)
                with receiver._condition:
                    receiver.requests.append(
                        HTTPRequest(
                            self.command, self.path, tuple(self.headers.items()), body
                        )
                    )
                    status, headers, output, delay = (
                        receiver._responses.popleft()
                        if receiver._responses
                        else (200, {}, b"safe-response", 0)
                    )
                    receiver._condition.notify_all()
                if receiver._stopping.wait(delay):
                    return
                try:
                    self.send_response(status)
                    self.send_header("Content-Length", str(len(output)))
                    self.send_header("Connection", "close")
                    for name, value in headers.items():
                        self.send_header(name, value)
                    self.end_headers()
                    self.wfile.write(output)
                except (BrokenPipeError, ConnectionResetError):
                    pass
                self.close_connection = True

            do_POST = do_GET

            def log_message(self, format: str, *args: object) -> None:
                pass

        self._server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
        self._thread = Thread(
            target=self._server.serve_forever, kwargs={"poll_interval": 0.01}
        )

    @property
    def origin(self) -> str:
        return f"http://127.0.0.1:{self._server.server_port}"

    def respond(
        self,
        *,
        status: int = 200,
        headers: dict[str, str] | None = None,
        body: bytes = b"safe-response",
        delay: float = 0,
    ) -> None:
        with self._condition:
            self._responses.append((status, headers or {}, body, delay))

    def __enter__(self) -> Self:
        self._thread.start()
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc_value: BaseException | None,
        traceback: TracebackType | None,
    ) -> None:
        self._stopping.set()
        with self._condition:
            for connection in tuple(self._connections):
                try:
                    connection.shutdown(SHUT_RDWR)
                except OSError:
                    pass
        self._server.shutdown()
        self._server.server_close()
        self._thread.join(timeout=2)
        if self._thread.is_alive():
            raise RuntimeError("HTTP receiver did not stop")
