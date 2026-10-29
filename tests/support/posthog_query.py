"""Local PostHog query boundary, never evidence of real PostHog ingestion."""

from __future__ import annotations

import json
from collections import deque
from dataclasses import dataclass, field
from datetime import UTC, datetime
from http.client import HTTPMessage
from http.server import BaseHTTPRequestHandler, HTTPServer
from socket import SHUT_RDWR, socket
from threading import Event, Lock, Thread
from types import TracebackType
from typing import Self, cast

_COLUMNS = ["signal", "probe_timestamp", "instance_id", "probe", "probe_name", "scope"]
_TABLES = {"traces": "trace_spans", "logs": "logs", "metrics": "metrics"}


@dataclass(frozen=True)
class QueryRequest:
    path: str
    headers: HTTPMessage = field(repr=False)
    body: dict[str, object] = field(repr=False)


@dataclass(frozen=True)
class _Response:
    status: int = 200
    body: bytes | None = None
    headers: tuple[tuple[str, str], ...] = ()
    delay: float = 0
    hang: bool = False
    chunk_size: int = 8192
    chunk_delay: float = 0


class PostHogQueryServer:
    """Serve parameterized probe queries and scripted responses on loopback.

    ``add_record`` populates independent signal tables. ``queue_response`` can
    override the next response. ``requests`` exposes actual socket captures.
    This intentionally implements only this test's external query boundary.
    """

    def __init__(self) -> None:
        self._lock = Lock()
        self._stopping = Event()
        self._connection: socket | None = None
        self._requests: list[QueryRequest] = []
        self._records: list[tuple[str, datetime, str, str, str, str]] = []
        self._responses: deque[_Response] = deque()
        boundary = self

        class Handler(BaseHTTPRequestHandler):
            def setup(self) -> None:
                super().setup()
                self.connection.settimeout(1)
                with boundary._lock:
                    boundary._connection = self.connection
                    stopping = boundary._stopping.is_set()
                if stopping:
                    boundary._interrupt(self.connection)

            def finish(self) -> None:
                try:
                    super().finish()
                finally:
                    with boundary._lock:
                        boundary._connection = None

            def do_POST(self) -> None:
                try:
                    length = int(self.headers.get("Content-Length", "-1"))
                    if not 0 <= length <= 16384:
                        self.send_error(400)
                        return
                    payload = json.loads(self.rfile.read(length))
                    if type(payload) is not dict:
                        self.send_error(400)
                        return
                    with boundary._lock:
                        boundary._requests.append(
                            QueryRequest(
                                self.path, cast(HTTPMessage, self.headers), payload
                            )
                        )
                        script = (
                            boundary._responses.popleft()
                            if boundary._responses
                            else _Response()
                        )
                        body = script.body
                        if body is None:
                            body = boundary._answer(payload)
                    if script.hang:
                        boundary._stopping.wait()
                        return
                    if boundary._stopping.wait(script.delay):
                        return
                    self.send_response(script.status)
                    headers = {
                        "content-type": "application/json",
                        "content-length": str(len(body)),
                    }
                    headers.update(
                        (key.lower(), value) for key, value in script.headers
                    )
                    for key, value in headers.items():
                        self.send_header(key, value)
                    self.end_headers()
                    for offset in range(0, len(body), script.chunk_size):
                        if offset and boundary._stopping.wait(script.chunk_delay):
                            return
                        self.wfile.write(body[offset : offset + script.chunk_size])
                        self.wfile.flush()
                except (OSError, ValueError):
                    self.close_connection = True

            def log_message(self, format: str, *args: object) -> None:
                """Never print request credentials or scripted vendor text."""

        self._server = HTTPServer(("127.0.0.1", 0), Handler)
        self._thread = Thread(
            target=self._server.serve_forever,
            kwargs={"poll_interval": 0.01},
            name="posthog-query-test",
        )

    @property
    def endpoint(self) -> str:
        return f"http://127.0.0.1:{self._server.server_port}"

    @property
    def requests(self) -> tuple[QueryRequest, ...]:
        with self._lock:
            return tuple(self._requests)

    def add_record(
        self,
        signal: str,
        *,
        instance_id: str,
        timestamp: datetime,
        probe: bool = True,
        name: str = "altimeter.probe",
        scope: str = "django_altimeter",
    ) -> None:
        if signal not in _TABLES or timestamp.tzinfo is None:
            raise ValueError("invalid test record")
        with self._lock:
            self._records.append(
                (
                    signal,
                    timestamp.astimezone(UTC),
                    instance_id,
                    str(probe).lower(),
                    name,
                    scope,
                )
            )

    def queue_response(
        self,
        *,
        status: int = 200,
        body: bytes | None = None,
        headers: tuple[tuple[str, str], ...] = (),
        delay: float = 0,
        hang: bool = False,
        chunk_size: int = 8192,
        chunk_delay: float = 0,
    ) -> None:
        if chunk_size < 1 or delay < 0 or chunk_delay < 0:
            raise ValueError("invalid scripted response")
        with self._lock:
            self._responses.append(
                _Response(status, body, headers, delay, hang, chunk_size, chunk_delay)
            )

    def _answer(self, payload: dict[str, object]) -> bytes:
        query = payload.get("query")
        rows: list[list[str]] = []
        if type(query) is dict and query.get("kind") == "HogQLQuery":
            sql, values = query.get("query"), query.get("values")
            if type(sql) is str and type(values) is dict:
                for signal, table in _TABLES.items():
                    if f"FROM posthog.{table}\n" not in sql:
                        continue
                    start = datetime.fromisoformat(values["started_at"]).replace(
                        tzinfo=UTC
                    )
                    finish = datetime.fromisoformat(values["finished_at"]).replace(
                        tzinfo=UTC
                    )
                    for (
                        row_signal,
                        timestamp,
                        instance,
                        probe,
                        name,
                        scope,
                    ) in self._records:
                        if (
                            signal == row_signal
                            and instance == values.get("instance_id")
                            and probe == "true"
                            and name == "altimeter.probe"
                            and (signal != "metrics" or scope == "django_altimeter")
                            and start <= timestamp <= finish
                        ):
                            rows.append(
                                [
                                    signal,
                                    timestamp.isoformat(),
                                    instance,
                                    probe,
                                    name,
                                    scope,
                                ]
                            )
                            break
        return json.dumps({"columns": _COLUMNS, "results": rows}).encode()

    @staticmethod
    def _interrupt(connection: socket) -> None:
        try:
            connection.shutdown(SHUT_RDWR)
        except OSError:
            pass  # Already closed by the peer.

    def __enter__(self) -> Self:
        self._thread.start()
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        traceback: TracebackType | None,
    ) -> None:
        self._stopping.set()
        with self._lock:
            connection = self._connection
        if connection is not None:
            self._interrupt(connection)
        self._server.shutdown()
        self._server.server_close()
        self._thread.join(timeout=1)
        if self._thread.is_alive():
            raise RuntimeError("PostHog query stand-in did not stop")
