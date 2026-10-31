"""Run an installed-wheel uvicorn and expose its real decoded OTLP to Playwright."""

import json
import signal
import subprocess
import sys
from contextlib import ExitStack
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from tempfile import TemporaryDirectory
from threading import Event, Thread

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / "tests"))
from support.otlp_receiver import OTLPReceiver  # noqa: E402
from support.uvicorn_server import install_wheel, running_server  # noqa: E402


def main() -> None:
    origins = json.loads(sys.argv[1])
    stopped = Event()
    signal.signal(signal.SIGTERM, lambda *_: stopped.set())
    signal.signal(signal.SIGINT, lambda *_: stopped.set())
    with ExitStack() as stack:
        workspace = Path(
            stack.enter_context(TemporaryDirectory(prefix="altimeter-browser-"))
        )
        receiver = stack.enter_context(OTLPReceiver())
        wheel = install_wheel(workspace / "wheel")
        requests_path = workspace / "requests.jsonl"
        requests_path.touch()
        server = stack.enter_context(
            running_server(
                workspace / "server",
                wheel=wheel,
                receiver=receiver,
                extra_env={
                    "DJANGO_SETTINGS_MODULE": "altimeter_reference.browser_settings",
                    "REFERENCE_BROWSER_ORIGINS": json.dumps(origins),
                    "REFERENCE_BROWSER_REQUESTS": str(requests_path),
                },
            )
        )
        asset_root = subprocess.check_output(
            [
                str(wheel.python),
                "-I",
                "-c",
                "from pathlib import Path; import django_altimeter; "
                "print(Path(django_altimeter.__file__).parent / 'browser')",
            ],
            text=True,
            timeout=10,
        ).strip()

        class Handler(BaseHTTPRequestHandler):
            def do_GET(self) -> None:
                if self.path != "/snapshot":
                    self.send_error(404)
                    return
                spans = []
                for request in receiver.requests:
                    if request.path != "/v1/traces":
                        continue
                    for resource in request.message.resource_spans:
                        for scope in resource.scope_spans:
                            for span in scope.spans:
                                spans.append(
                                    {
                                        "traceId": span.trace_id.hex(),
                                        "spanId": span.span_id.hex(),
                                        "parentSpanId": span.parent_span_id.hex(),
                                        "kind": span.kind,
                                        "name": span.name,
                                        "durationMs": (
                                            span.end_time_unix_nano
                                            - span.start_time_unix_nano
                                        )
                                        / 1_000_000,
                                        "attributes": {
                                            value.key: getattr(
                                                value.value,
                                                value.value.WhichOneof("value"),
                                            )
                                            for value in span.attributes
                                        },
                                    }
                                )
                payload = json.dumps(
                    {
                        "spans": spans,
                        "requests": [
                            json.loads(line)
                            for line in requests_path.read_text().splitlines()
                        ],
                        "wire": [str(request.message) for request in receiver.requests],
                    }
                ).encode()
                self.send_response(200)
                self.send_header("Content-Type", "application/json")
                self.send_header("Content-Length", str(len(payload)))
                self.end_headers()
                self.wfile.write(payload)

            def log_message(self, format: str, *args: object) -> None:
                pass

        control = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
        thread = Thread(target=control.serve_forever, daemon=True)
        thread.start()
        print(
            "BROWSER_STACK "
            + json.dumps(
                {
                    "apiOrigin": f"http://127.0.0.1:{server.port}",
                    "controlOrigin": f"http://127.0.0.1:{control.server_port}",
                    "assetRoot": asset_root,
                    "wheelSha256": wheel.sha256,
                }
            ),
            flush=True,
        )
        try:
            stopped.wait()
        finally:
            control.shutdown()
            thread.join(timeout=2)
            control.server_close()


if __name__ == "__main__":
    main()
