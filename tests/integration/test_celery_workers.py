"""Installed-wheel Celery process lifetimes, using real Redis and decoded OTLP."""

import json
import os
import signal
import subprocess
import time
from pathlib import Path

import pytest
from support.celery_worker import attributes, running_beat, running_worker, spans
from support.otlp_receiver import OTLPReceiver
from support.uvicorn_server import install_wheel, running_server

pytestmark = pytest.mark.integration


@pytest.fixture(scope="module")
def wheel(tmp_path_factory):
    return install_wheel(
        tmp_path_factory.mktemp("celery-wheel"), extras=("celery", "redis")
    )


def test_celery_extra_pins_supported_worker_dependencies():
    import tomllib

    project = tomllib.loads(
        (Path(__file__).resolve().parents[2] / "pyproject.toml").read_text()
    )
    extra = project["project"]["optional-dependencies"]["celery"]
    assert "celery==5.6.3" in extra
    assert "kombu==5.6.2" in extra
    assert "billiard==4.2.4" in extra
    assert any(pin.startswith("redis==7.4.0") for pin in extra)


def test_installed_worker_versions_match_reference_pins(wheel):
    result = subprocess.run(
        [
            str(wheel.python),
            "-I",
            "-c",
            "import importlib.metadata as m,json,sys; "
            "import celery,kombu,billiard,redis,django_altimeter.celery; "
            "print(json.dumps({'python':list(sys.version_info[:2]),"
            "'versions':{n:m.version(n) for n in "
            "('celery','kombu','billiard','redis')}}))",
        ],
        check=True,
        capture_output=True,
        text=True,
        timeout=30,
    )
    print(result.stderr, end="")
    reported = json.loads(result.stdout)
    assert reported["versions"] == {
        "celery": "5.6.3",
        "kombu": "5.6.2",
        "billiard": "4.2.4",
        "redis": "6.1.1" if reported["python"] < [3, 13] else "7.4.0",
    }


@pytest.mark.parametrize(
    "pool,recycle", [("prefork", None), ("prefork", 1), ("solo", None)]
)
def test_http_publish_attempt_lifetime(wheel, tmp_path, pool, recycle):
    with OTLPReceiver() as receiver:
        with running_worker(
            tmp_path / "worker",
            wheel=wheel,
            receiver=receiver,
            pool=pool,
            recycle=recycle,
        ) as worker:
            with running_server(
                tmp_path / "web",
                wheel=wheel,
                receiver=receiver,
                extra_env={
                    **worker.environment,
                    "ALTIMETER_SERVICE_NAME": "reference-publisher",
                    "REFERENCE_CELERY_EVENTS": str(tmp_path / "web-events.jsonl"),
                },
            ) as server:
                results = []
                for _ in range(2):
                    status, _, body = server.request("/tasks/echo/?payload=http-canary")
                    assert status == 200
                    result = worker.result(json.loads(body)["task_id"])
                    assert result["state"] == "SUCCESS"
                    assert result["result"]["payload"] == "http-canary"
                    results.append(result["result"])
            assert all(result["correlation"] is not None for result in results)
            assert len({result["pid"] for result in results}) == (2 if recycle else 1)
            assert len({result["instance_id"] for result in results}) == (
                2 if recycle else 1
            )
            if pool == "prefork":
                assert all(result["pid"] != worker.process.pid for result in results)
            else:
                assert all(result["pid"] == worker.process.pid for result in results)
        observed = spans(receiver)
        consumers = [(resource, span) for resource, span in observed if span.kind == 5]
        producers = [span for _, span in observed if span.kind == 4]
        servers = [span for _, span in observed if span.kind == 2]
        assert len(consumers) == len(producers) == len(servers) == 2
        for resource, attempt in consumers:
            producer = next(
                span for span in producers if span.span_id == attempt.parent_span_id
            )
            request = next(
                span for span in servers if span.span_id == producer.parent_span_id
            )
            assert attempt.trace_id == producer.trace_id == request.trace_id
            assert attributes(resource.attributes)["service.instance.id"] in {
                result["instance_id"] for result in results
            }
        assert "http-canary" not in str(receiver.requests)
        assert "header-canary" not in str(receiver.requests)
        assert all(
            not event["network"] for event in worker.events if event["event"] == "setup"
        )
        drains = [event for event in worker.events if event["event"] == "drain"]
        assert drains and all(event["elapsed"] <= 2.2 for event in drains)
        served_pids = {result["pid"] for result in results}
        assert served_pids <= {event["pid"] for event in drains}
        assert all(
            event["completed"] is True
            for event in drains
            if event["pid"] in served_pids
        )
        assert all(
            event["export_codes"]
            == {
                "traces": "export.accepted",
                "logs": "export.accepted",
                "metrics": "export.accepted",
            }
            for event in drains
            if event["pid"] in served_pids
        )


def test_beat_publishes_to_real_worker(wheel, tmp_path):
    with OTLPReceiver() as receiver:
        with running_worker(
            tmp_path / "worker", wheel=wheel, receiver=receiver
        ) as worker:
            with running_beat(tmp_path / "beat", worker=worker) as beat:
                worker.wait_for("tick")
            beat_start = beat.events[0]
            assert beat_start["instance_id"] is not None
            assert beat_start["network"] == []
        observed = spans(receiver)
        producers = [
            span
            for resource, span in observed
            if span.kind == 4
            and attributes(resource.attributes).get("altimeter.role") == "beat"
        ]
        attempts = [span for _, span in observed if span.kind == 5]
        assert producers and attempts
        assert any(
            attempt.parent_span_id == producer.span_id
            and attempt.trace_id == producer.trace_id
            for attempt in attempts
            for producer in producers
        )
        assert "beat-result-canary" not in str(receiver.requests)


def test_unsupported_thread_pool_still_executes_without_worker_telemetry(
    wheel, tmp_path
):
    with OTLPReceiver() as receiver:
        with running_worker(
            tmp_path, wheel=wheel, receiver=receiver, pool="threads"
        ) as worker:
            result = worker.submit(args=["unsupported-pool-canary"])
            assert result["state"] == "SUCCESS"
            assert result["result"]["payload"] == "unsupported-pool-canary"
            assert result["result"]["pid"] == worker.process.pid
            assert result["result"]["instance_id"] is None
            assert result["result"]["correlation"] is None
            assert "celery.unsupported_pool" in worker.output
        assert not [span for _, span in spans(receiver) if span.kind == 5]
        assert len([span for _, span in spans(receiver) if span.kind == 4]) == 1
        assert "unsupported-pool-canary" not in str(receiver.requests)


@pytest.mark.parametrize("mode", ["enabled", "disabled", "invalid"])
def test_failure_then_success_preserves_native_results_and_clears_context(
    wheel, tmp_path, mode
):
    with OTLPReceiver() as receiver:
        with running_worker(
            tmp_path, wheel=wheel, receiver=receiver, mode=mode
        ) as worker:
            failed = worker.submit("reference.fail")
            succeeded = worker.submit(args=["safe-result-canary"])
            assert failed["state"] == "FAILURE"
            assert failed["error_type"] == "ValueError"
            assert failed["error"] == "exception-message-canary"
            assert succeeded["state"] == "SUCCESS"
            assert succeeded["result"]["payload"] == "safe-result-canary"
            failure = worker.wait_for("failure")[0]
            postrun = worker.wait_for("postrun", count=2)
            assert failure["pid"] == succeeded["result"]["pid"]
            assert [event["state"] for event in postrun] == ["FAILURE", "SUCCESS"]
            assert all(event["correlation"] is None for event in postrun)
            assert (failure["correlation"] is not None) == (mode == "enabled")
        if mode == "enabled":
            attempts = [span for _, span in spans(receiver) if span.kind == 5]
            assert len(attempts) == 2
            assert len({span.trace_id for span in attempts}) == 2
            assert "reference.task.failed" in worker.output
        else:
            assert receiver.requests == ()
        assert "exception-message-canary" not in str(receiver.requests)
        assert "safe-result-canary" not in str(receiver.requests)


def test_killed_child_keeps_parent_unowned_and_replacement_exports(wheel, tmp_path):
    with OTLPReceiver() as receiver:
        with running_worker(tmp_path, wheel=wheel, receiver=receiver) as worker:
            first = worker.submit()["result"]
            deadline = time.monotonic() + 5
            while time.monotonic() < deadline:
                if any(span.kind == 5 for _, span in spans(receiver)):
                    break
                time.sleep(0.025)
            assert len([span for _, span in spans(receiver) if span.kind == 5]) == 1
            blocked = worker.submit("reference.block", get=False)
            event = worker.wait_for("blocked")[0]
            assert event["pid"] == first["pid"]
            os.kill(event["pid"], signal.SIGKILL)
            failed = worker.result(blocked["id"])
            assert failed["state"] == "FAILURE"
            assert failed["error_type"] == "WorkerLostError"
            parent_failure = worker.wait_for("failure")[0]
            assert parent_failure["pid"] == worker.process.pid
            assert parent_failure["instance_id"] is None
            assert parent_failure["correlation"] is None
            assert parent_failure["parent_calls"] == []
            assert parent_failure["parent_connections"] == []
            assert not any("Otel" in name for name in parent_failure["threads"])
            replacement = worker.submit()["result"]
            assert replacement["pid"] != first["pid"]
            assert replacement["instance_id"] not in (None, first["instance_id"])
        writers = {
            attributes(resource.attributes)["service.instance.id"]
            for resource, span in spans(receiver)
            if span.kind == 5
        }
        assert replacement["instance_id"] in writers
        # The killed task has no fabricated terminal span or parent outcome.
        assert len([span for _, span in spans(receiver) if span.kind == 5]) == 2


@pytest.mark.parametrize("mode", ["enabled", "disabled", "invalid"])
def test_cancellation_and_loader_error_preserve_worker_reuse(wheel, tmp_path, mode):
    with OTLPReceiver() as receiver:
        with running_worker(
            tmp_path, wheel=wheel, receiver=receiver, mode=mode
        ) as worker:
            cancelled = worker.submit("reference.cancel", get=False)
            cancellation = worker.wait_for("cancelled")[0]
            worker.wait_for("postrun")
            failed = worker.submit("reference.internal")
            assert failed["state"] == "FAILURE"
            assert failed["error_type"] == "ValueError"
            assert failed["error"] == "loader-error-canary"
            internal = worker.wait_for("internal")[0]
            loader_error = worker.wait_for("loader_error")[0]
            recovered = worker.submit("reference.internal")
            assert recovered["state"] == "SUCCESS"
            assert recovered["result"]["payload"] == "loader-recovered-canary"
            assert recovered["result"]["pid"] == cancellation["pid"] == internal["pid"]
            ignored_state = worker.execute(
                "\nprint(json.dumps(app.AsyncResult(json.load(sys.stdin)).state))\n",
                input=cancelled["id"],
            )
            assert ignored_state == "PENDING"
            postruns = worker.wait_for("postrun", count=3)
            assert [row["state"] for row in postruns] == ["IGNORED", None, "SUCCESS"]
            assert all(row["correlation"] is None for row in postruns)
            assert internal["correlation"] is None
            assert (loader_error["correlation"] is not None) == (mode == "enabled")
            assert (cancellation["correlation"] is not None) == (mode == "enabled")
        if mode == "enabled":
            attempts = [span for _, span in spans(receiver) if span.kind == 5]
            assert len(attempts) == 3
            assert len({span.span_id for span in attempts}) == 3
        else:
            assert receiver.requests == ()
        assert "loader-error-canary" not in str(receiver.requests)
        assert "loader-recovered-canary" not in str(receiver.requests)


def test_hanging_export_drain_uses_one_budget(wheel, tmp_path):
    with OTLPReceiver() as receiver:
        for _ in range(256):
            receiver.queue_response(hang=True)
        with running_worker(
            tmp_path,
            wheel=wheel,
            receiver=receiver,
            extra_env={
                "ALTIMETER_EXPORT_TIMEOUT": "2",
                "ALTIMETER_BATCH_SCHEDULE_DELAY_MILLIS": "60000",
                "ALTIMETER_METRIC_EXPORT_INTERVAL_MILLIS": "60000",
            },
        ) as worker:
            result = worker.submit(args=["drain-canary"])
            assert result["state"] == "SUCCESS"
            assert result["result"]["instance_id"] is not None
        child_drains = [
            event
            for event in worker.events
            if event["event"] == "drain" and event["pid"] != worker.process.pid
        ]
        assert len(child_drains) == 1
        assert child_drains[0]["elapsed"] <= 2.2
        assert child_drains[0]["completed"] is False
        assert child_drains[0]["codes"]
        assert worker.exit_seconds < 8
        assert receiver.requests
        assert "drain-canary" not in str(receiver.requests)
