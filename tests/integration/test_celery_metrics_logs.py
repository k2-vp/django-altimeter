"""Real installed-wheel Celery attempts, metric totals and safe correlated logs."""

from collections import Counter

import pytest
from support.celery_worker import attributes, running_worker
from support.uvicorn_server import install_wheel

pytestmark = pytest.mark.integration
_CANARY = "worker-argument-result-canary"


@pytest.fixture(scope="module")
def worker_wheel(tmp_path_factory):
    return install_wheel(
        tmp_path_factory.mktemp("celery-metrics-wheel"), extras=("celery", "redis")
    )


def _metrics(receiver, name):
    return [
        (attributes(resource.resource.attributes), point)
        for request in receiver.requests
        if request.path == "/v1/metrics"
        for resource in request.message.resource_metrics
        for scope in resource.scope_metrics
        for metric in scope.metrics
        if metric.name == name
        for point in getattr(metric, metric.WhichOneof("data")).data_points
    ]


@pytest.mark.parametrize("sample_rate", [1.0, 0.1, 0.0])
@pytest.mark.parametrize(
    ("pool", "recycle"),
    [("prefork", None), ("prefork", 1), ("solo", None)],
    ids=["reused-prefork", "recycled-prefork", "solo"],
)
def test_task_totals_are_exact_independent_of_sampling(
    tmp_path, worker_wheel, otlp_receiver, pool, recycle, sample_rate
):
    results = []
    with running_worker(
        tmp_path / "worker",
        wheel=worker_wheel,
        receiver=otlp_receiver,
        pool=pool,
        recycle=recycle,
        extra_env={"ALTIMETER_SAMPLE_RATE": str(sample_rate)},
    ) as worker:
        for _ in range(4):
            outcome = worker.submit(args=[_CANARY], kwargs={"delay": 0.025})
            assert outcome["state"] == "SUCCESS", outcome
            assert outcome["result"]["payload"] == _CANARY
            results.append(outcome["result"])
        worker.wait_for("postrun", count=4)
    attempts = _metrics(otlp_receiver, "altimeter.worker.attempts")
    durations = _metrics(otlp_receiver, "altimeter.worker.duration")
    assert sum(point.as_int for _, point in attempts) == 4
    assert sum(point.count for _, point in durations) == 4
    assert sum(point.sum for _, point in durations) >= 0.1
    assert {resource["service.instance.id"] for resource, _ in attempts} == {
        result["instance_id"] for result in results
    }
    assert len({result["pid"] for result in results}) == (4 if recycle else 1)
    for _, point in [*attempts, *durations]:
        assert attributes(point.attributes) == {
            "messaging.system": "celery",
            "task.name": "reference.echo",
            "queue.name": worker.environment["REFERENCE_CELERY_QUEUE"],
            "altimeter.outcome": "success",
        }
    assert all(
        _CANARY.encode() not in request.message.SerializeToString()
        for request in otlp_receiver.requests
    )
    assert Counter(result["payload"] for result in results) == {_CANARY: 4}


def _logs(receiver):
    return [
        (attributes(resource.resource.attributes), record)
        for request in receiver.requests
        if request.path == "/v1/logs"
        for resource in request.message.resource_logs
        for scope in resource.scope_logs
        for record in scope.log_records
    ]


def _correlation_tuple(correlation):
    assert correlation is not None
    return (
        bytes.fromhex(correlation["trace_id"]),
        bytes.fromhex(correlation["span_id"]),
        correlation["trace_flags"] & 1,
    )


def _record_tuple(record):
    return record.trace_id, record.span_id, record.flags & 1


@pytest.mark.parametrize("mode", ["enabled", "disabled", "invalid"])
@pytest.mark.parametrize("sample_rate", [1.0, 0.1, 0.0])
def test_task_and_error_logs_match_each_attempt_and_preserve_native_output(
    tmp_path, worker_wheel, otlp_receiver, mode, sample_rate
):
    with running_worker(
        tmp_path / "worker",
        wheel=worker_wheel,
        receiver=otlp_receiver,
        mode=mode,
        extra_env={"ALTIMETER_SAMPLE_RATE": str(sample_rate)},
    ) as worker:
        succeeded = [worker.submit(args=[_CANARY])]
        failed = worker.submit("reference.fail")
        succeeded.append(worker.submit(args=[_CANARY]))
        assert failed["state"] == "FAILURE"
        assert failed["error_type"] == "ValueError"
        assert failed["error"] == "exception-message-canary"
        assert all(result["state"] == "SUCCESS" for result in succeeded)
        assert all(result["result"]["payload"] == _CANARY for result in succeeded)
        failure = worker.wait_for("failure")[0]
        postrun = worker.wait_for("postrun", count=3)
        assert all(event["correlation"] is None for event in postrun)
        assert len({result["result"]["pid"] for result in succeeded}) == 1
    # The native logging output still contains application text and exceptions.
    assert worker.output.count("reference.task.completed") == 2
    assert worker.output.count("unapproved " + _CANARY) == 2
    assert worker.output.count("reference.task.failed") == 1
    assert "exception-message-canary" in worker.output
    records = _logs(otlp_receiver)
    if mode == "enabled":
        assert len(records) == 3
        completed = [
            record
            for _, record in records
            if record.body.string_value == "reference.task.completed"
        ]
        failures = [
            record
            for _, record in records
            if record.body.string_value == "reference.task.failed"
        ]
        assert len(completed) == 2
        assert len(failures) == 1
        expected = Counter(
            _correlation_tuple(result["result"]["log_correlation"])
            for result in succeeded
        )
        assert Counter(_record_tuple(record) for record in completed) == expected
        assert Counter(_record_tuple(record) for record in failures) == Counter(
            [_correlation_tuple(failure["correlation"])]
        )
        assert len({record.trace_id for _, record in records}) == 3
        assert {resource["service.instance.id"] for resource, _ in records} == {
            succeeded[0]["result"]["instance_id"]
        }
        assert all(not record.attributes for _, record in records)
        if sample_rate in (0.0, 1.0):
            assert {record.flags & 1 for _, record in records} == {int(sample_rate)}
    else:
        assert otlp_receiver.requests == ()
        assert failure["correlation"] is None
        assert all(result["result"]["log_correlation"] is None for result in succeeded)
    for request in otlp_receiver.requests:
        wire = request.message.SerializeToString()
        assert _CANARY.encode() not in wire
        assert b"exception-message-canary" not in wire


@pytest.mark.parametrize("sample_rate", [1.0, 0.1, 0.0])
def test_retry_counts_attempts_and_actual_override_queue_with_known_delay(
    tmp_path, worker_wheel, otlp_receiver, sample_rate
):
    with running_worker(
        tmp_path / "worker",
        wheel=worker_wheel,
        receiver=otlp_receiver,
        extra_env={"ALTIMETER_SAMPLE_RATE": str(sample_rate)},
    ) as worker:
        result = worker.submit(
            "reference.retry",
            args=[_CANARY],
            options={
                "queue": worker.environment["REFERENCE_CELERY_QUEUE"] + ".override",
                "countdown": 0.2,
            },
        )
        assert result["state"] == "SUCCESS", result
        assert result["result"]["payload"] == _CANARY
        assert [event["retries"] for event in worker.wait_for("attempt", count=2)] == [
            0,
            1,
        ]
        assert [event["state"] for event in worker.wait_for("postrun", count=2)] == [
            "RETRY",
            "SUCCESS",
        ]
    points = _metrics(otlp_receiver, "altimeter.worker.attempts")
    by_outcome = Counter()
    for _, point in points:
        attrs = attributes(point.attributes)
        assert attrs["task.name"] == "reference.retry"
        assert (
            attrs["queue.name"]
            == worker.environment["REFERENCE_CELERY_QUEUE"] + ".override"
        )
        by_outcome[attrs["altimeter.outcome"]] += point.as_int
    assert by_outcome == {"retry": 1, "success": 1}
    for suffix in ("duration", "enqueue_to_start", "intentional_delay"):
        values = _metrics(otlp_receiver, "altimeter.worker." + suffix)
        assert sum(point.count for _, point in values) == 2
        assert all(point.sum >= 0 for _, point in values)
        if suffix != "duration":
            assert sum(point.sum for _, point in values) >= 0.35
        assert all(point.bucket_counts[-1] == 0 for _, point in values)
    assert sum(
        point.count for _, point in _metrics(otlp_receiver, "altimeter.worker.duration")
    ) == sum(point.as_int for _, point in points)
    assert all(
        _CANARY.encode() not in request.message.SerializeToString()
        for request in otlp_receiver.requests
    )


def test_malformed_and_future_epochs_do_not_fabricate_elapsed_metrics(
    tmp_path, worker_wheel, otlp_receiver
):
    # An uninstrumented publisher sends carrier candidates over the real broker.
    with running_worker(
        tmp_path / "worker", wheel=worker_wheel, receiver=otlp_receiver
    ) as worker:
        output = worker.execute("""
import time
traceparent = '00-' + '1' * 32 + '-' + '2' * 16 + '-01'
results = []
for timestamp in (None, -1, 'CLOCK_CANARY', time.time() + 3600, time.time()):
    headers = {'django_altimeter': {'v': 1, 'origin': {'traceparent': traceparent},
       'attempt': {'traceparent': traceparent, 'published_at': timestamp}}}
    value = app.send_task('reference.echo', args=['CLOCK_RESULT_CANARY'],
                          headers=headers).get(timeout=15, interval=0.02)
    assert value['payload'] == 'CLOCK_RESULT_CANARY'
    results.append(value)
print(json.dumps(results))
""")
        assert len(output) == 5
        worker.wait_for("postrun", count=5)
    assert (
        sum(
            point.as_int
            for _, point in _metrics(otlp_receiver, "altimeter.worker.attempts")
        )
        == 5
    )
    assert (
        sum(
            point.count
            for _, point in _metrics(otlp_receiver, "altimeter.worker.duration")
        )
        == 5
    )
    known = _metrics(otlp_receiver, "altimeter.worker.enqueue_to_start")
    assert sum(point.count for _, point in known) == 1
    assert all(0 <= point.sum < 30 for _, point in known)
    assert _metrics(otlp_receiver, "altimeter.worker.intentional_delay") == []
    assert all(
        b"CLOCK_CANARY" not in request.message.SerializeToString()
        and b"CLOCK_RESULT_CANARY" not in request.message.SerializeToString()
        for request in otlp_receiver.requests
    )


def test_beat_publication_does_not_add_worker_attempts(
    tmp_path, worker_wheel, otlp_receiver
):
    from support.celery_worker import running_beat

    with running_worker(
        tmp_path / "worker", wheel=worker_wheel, receiver=otlp_receiver
    ) as worker:
        with running_beat(tmp_path / "beat", worker=worker):
            worker.wait_for("tick")
            worker.wait_for("postrun")
    completed = [event for event in worker.events if event["event"] == "postrun"]
    assert completed and all(event["state"] == "SUCCESS" for event in completed)
    attempts = _metrics(otlp_receiver, "altimeter.worker.attempts")
    assert sum(point.as_int for _, point in attempts) == len(completed)
    assert {resource["altimeter.role"] for resource, _ in attempts} == {
        "celery-prefork"
    }
    assert {resource["service.instance.id"] for resource, _ in attempts} == {
        event["instance_id"] for event in completed
    }
    assert all(
        attributes(point.attributes)["task.name"] == "reference.beat"
        for _, point in attempts
    )
    assert all(
        b"beat-result-canary" not in request.message.SerializeToString()
        for request in otlp_receiver.requests
    )
