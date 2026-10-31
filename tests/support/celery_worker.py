"""Real wheel-installed Celery processes on isolated Redis queues and prefixes."""

import json
import os
import signal
import subprocess
import time
from contextlib import contextmanager
from dataclasses import dataclass
from pathlib import Path
from urllib.parse import urlsplit, urlunsplit
from uuid import uuid4

from support.uvicorn_server import ROOT, WheelEnvironment

_BOOTSTRAP = r"""
import importlib.metadata
import json
import os
from pathlib import Path
import sys

sys.path.insert(0, os.environ["REFERENCE_PATH"])
import django_altimeter
origin = Path(django_altimeter.__file__).resolve()
assert origin.is_relative_to(Path(sys.prefix).resolve())
distribution = importlib.metadata.distribution("django-altimeter")
direct = json.loads(distribution.read_text("direct_url.json"))
assert direct["archive_info"]["hashes"]["sha256"] == os.environ["WHEEL_SHA256"]
assert not direct.get("dir_info", {}).get("editable", False)
from altimeter_reference.celery_app import app
"""

_WORKER = (
    _BOOTSTRAP
    + r"""
app.worker_main(json.loads(os.environ["CELERY_ARGUMENTS"]))
"""
)

_BEAT = (
    _BOOTSTRAP
    + r"""
app.start(["beat", "--loglevel=WARNING", "--schedule=" + os.environ["BEAT_SCHEDULE"]])
"""
)

_CLIENT = r"""
import time
from django_altimeter import setup, shutdown

request = json.load(sys.stdin)
if request["action"] != "result" and request.get("setup", True):
    setup(role="web")

def resolve(result):
    value = result.get(timeout=15, interval=0.02, propagate=False)
    failure = isinstance(value, BaseException)
    return {
        "id": result.id, "state": result.state,
        "result": None if failure else value,
        "error_type": type(value).__name__ if failure else None,
        "error": str(value) if failure else None,
    }

try:
    if request["action"] == "result":
        output = resolve(app.AsyncResult(request["task_id"]))
    elif request["action"] == "submit":
        result = app.send_task(request["task"], args=request["args"],
                               kwargs=request["kwargs"], **request["options"])
        output = resolve(result) if request["get"] else {"id": result.id}
    elif request["action"] == "workload":
        for _ in range(request["warmup"]):
            result = app.send_task("reference.echo", args=[request["payload"]])
            value = resolve(result)
            assert value["state"] == "SUCCESS", value
            assert value["result"]["payload"] == request["payload"], value
            result.forget()
        started = time.monotonic()
        completed = errors = timeouts = 0
        samples = []
        while (time.monotonic() - started < request["seconds"] if request["seconds"]
               else completed + errors + timeouts < request["count"]):
            begin = time.monotonic()
            try:
                result = app.send_task("reference.echo", args=[request["payload"]])
                value = resolve(result)
                if (value["state"] == "SUCCESS"
                    and value["result"]["payload"] == request["payload"]):
                    completed += 1
                    samples.append(time.monotonic() - begin)
                else:
                    errors += 1
                result.forget()
            except Exception as error:
                if type(error).__name__ == "TimeoutError":
                    timeouts += 1
                else:
                    errors += 1
        elapsed = time.monotonic() - started
        output = {"completed": completed, "errors": errors, "timeouts": timeouts,
                  "seconds": elapsed, "throughput": completed / elapsed,
                  "samples": samples}
    else:
        raise ValueError("unsupported test action")
finally:
    shutdown(timeout=2.0)
print(json.dumps(output))
"""


def redis_urls():
    base = urlsplit(
        os.environ.get("ALTIMETER_TEST_REDIS_URL", "redis://127.0.0.1:6379/0")
    )
    broker = os.environ.get("ALTIMETER_TEST_CELERY_BROKER") or urlunsplit(
        base._replace(path="/13")
    )
    backend = os.environ.get("ALTIMETER_TEST_CELERY_BACKEND") or urlunsplit(
        base._replace(path="/14")
    )
    return broker, backend


def worker_environment(workspace, wheel, receiver, *, mode="enabled", extra_env=None):
    namespace = "altimeter-" + uuid4().hex[:12]
    broker, backend = redis_urls()
    return {
        "PATH": os.environ.get("PATH", ""),
        "REFERENCE_PATH": str(ROOT / "reference"),
        "DJANGO_SETTINGS_MODULE": "altimeter_reference.celery_settings",
        "WHEEL_SHA256": wheel.sha256,
        "REFERENCE_CELERY_QUEUE": namespace,
        "REFERENCE_CELERY_PREFIX": namespace + ":",
        "REFERENCE_CELERY_EVENTS": str(workspace / "events.jsonl"),
        "ALTIMETER_TEST_CELERY_BROKER": broker,
        "ALTIMETER_TEST_CELERY_BACKEND": backend,
        "RECEIVER_ENDPOINT": receiver.endpoint,
        "ALTIMETER_ENABLED": "false" if mode == "disabled" else "true",
        "ALTIMETER_SERVICE_NAME": "" if mode == "invalid" else "reference-celery",
        "ALTIMETER_DEPLOYMENT_ENVIRONMENT": "test",
        "ALTIMETER_RELEASE": wheel.sha256[:16],
        "ALTIMETER_INTEGRATIONS": "celery,asgi,django",
        "ALTIMETER_TRACES_COLLECTOR_ENDPOINT": receiver.url("traces"),
        "ALTIMETER_LOGS_COLLECTOR_ENDPOINT": receiver.url("logs"),
        "ALTIMETER_METRICS_COLLECTOR_ENDPOINT": receiver.url("metrics"),
        "ALTIMETER_LOGS_ENABLED": "true",
        "ALTIMETER_BATCH_SCHEDULE_DELAY_MILLIS": "250",
        "ALTIMETER_METRIC_EXPORT_INTERVAL_MILLIS": "250",
        "ALTIMETER_EXPORT_TIMEOUT": "0.2",
        "OTEL_SEMCONV_STABILITY_OPT_IN": "http",
        "C_FORCE_ROOT": "1",
        **(extra_env or {}),
    }


@dataclass
class Worker:
    wheel: WheelEnvironment
    process: subprocess.Popen
    workspace: Path
    environment: dict[str, str]
    exit_seconds: float | None = None

    @property
    def events(self):
        path = Path(self.environment["REFERENCE_CELERY_EVENTS"])
        if not path.exists():
            return []
        return [json.loads(line) for line in path.read_text().splitlines() if line]

    @property
    def output(self):
        return (self.workspace / "worker.log").read_text()

    def wait_for(self, event, count=1, timeout=20):
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            matching = [row for row in self.events if row["event"] == event]
            if len(matching) >= count:
                return matching
            assert self.process.poll() is None, self.output
            time.sleep(0.025)
        raise AssertionError(f"Missing {event} x{count}: {self.events}\n{self.output}")

    def execute(self, source, *, input=None, timeout=30, extra_env=None):
        environment = {
            **self.environment,
            "ALTIMETER_SERVICE_NAME": self.environment["ALTIMETER_SERVICE_NAME"]
            + ("-publisher" if self.environment["ALTIMETER_SERVICE_NAME"] else ""),
            "REFERENCE_CELERY_EVENTS": str(self.workspace / "publisher.jsonl"),
            **(extra_env or {}),
        }
        result = subprocess.run(
            [str(self.wheel.python), "-I", "-c", _BOOTSTRAP + source],
            input=json.dumps(input),
            cwd=self.workspace,
            env=environment,
            text=True,
            capture_output=True,
            timeout=timeout,
        )
        print(result.stderr, end="")
        assert result.returncode == 0, result.stdout + result.stderr
        return json.loads(result.stdout.splitlines()[-1])

    def client(self, request, *, timeout=30):
        return self.execute(_CLIENT, input=request, timeout=timeout)

    def submit(
        self, task="reference.echo", *, args=(), kwargs=None, options=None, get=True
    ):
        return self.client(
            {
                "action": "submit",
                "task": task,
                "args": args,
                "kwargs": kwargs or {},
                "options": options or {},
                "get": get,
            }
        )

    def result(self, task_id):
        return self.client({"action": "result", "task_id": task_id})

    def stop(self):
        if self.process.poll() is not None:
            assert self.process.returncode == 0, self.output
            return
        started = time.monotonic()
        self.process.send_signal(signal.SIGTERM)
        try:
            self.process.wait(timeout=12)
        except subprocess.TimeoutExpired:
            os.killpg(self.process.pid, signal.SIGKILL)
            self.process.wait(timeout=3)
            raise AssertionError(
                f"Celery did not exit after SIGTERM\n{self.output}"
            ) from None
        self.exit_seconds = time.monotonic() - started
        assert self.process.returncode in (0, -signal.SIGTERM), self.output


@contextmanager
def running_worker(
    workspace,
    *,
    wheel,
    receiver,
    pool="prefork",
    recycle=None,
    mode="enabled",
    extra_env=None,
):
    workspace.mkdir(parents=True, exist_ok=True)
    environment = worker_environment(
        workspace, wheel, receiver, mode=mode, extra_env=extra_env
    )
    arguments = [
        "worker",
        "--pool=" + pool,
        "--concurrency=1",
        "--without-mingle",
        "--without-gossip",
        "--without-heartbeat",
        "--loglevel=WARNING",
        "--hostname=" + environment["REFERENCE_CELERY_QUEUE"] + "@%h",
    ]
    if recycle is not None:
        arguments.append("--max-tasks-per-child=" + str(recycle))
    environment.update(
        REFERENCE_CELERY_POOL=pool, CELERY_ARGUMENTS=json.dumps(arguments)
    )
    with (workspace / "worker.log").open("w") as output:
        process = subprocess.Popen(
            [str(wheel.python), "-I", "-c", _WORKER],
            cwd=workspace,
            env=environment,
            stdout=output,
            stderr=subprocess.STDOUT,
            start_new_session=True,
        )
        worker = Worker(wheel, process, workspace, environment)
        try:
            worker.wait_for("ready")
            if pool in ("prefork", "solo"):
                worker.wait_for("setup")
            yield worker
        finally:
            worker.stop()
            print(worker.output, end="")


@contextmanager
def running_beat(workspace, *, worker):
    workspace.mkdir(parents=True, exist_ok=True)
    environment = {
        **worker.environment,
        "REFERENCE_CELERY_BEAT": "1",
        "REFERENCE_CELERY_POOL": "beat",
        "REFERENCE_CELERY_EVENTS": str(workspace / "events.jsonl"),
        "BEAT_SCHEDULE": str(workspace / "schedule"),
    }
    with (workspace / "worker.log").open("w") as output:
        process = subprocess.Popen(
            [str(worker.wheel.python), "-I", "-c", _BEAT],
            cwd=workspace,
            env=environment,
            stdout=output,
            stderr=subprocess.STDOUT,
            start_new_session=True,
        )
        beat = Worker(worker.wheel, process, workspace, environment)
        try:
            beat.wait_for("beat_ready")
            yield beat
        finally:
            beat.stop()
            print(beat.output, end="")


def spans(receiver):
    return [
        (resource.resource, span)
        for request in receiver.requests
        if request.path == "/v1/traces"
        for resource in request.message.resource_spans
        for scope in resource.scope_spans
        for span in scope.spans
    ]


def attributes(values):
    return {
        value.key: getattr(value.value, value.value.WhichOneof("value"))
        for value in values
    }
