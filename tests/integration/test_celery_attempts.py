"""Installed-wheel Redis worker lineage; malformed carriers reach the consumer."""

from time import time

import pytest
from support.celery_worker import attributes, running_worker, spans
from support.uvicorn_server import install_wheel

pytestmark = pytest.mark.integration
_CANARY = "attempt-payload-canary"
_ORIGIN = "00-" + "1" * 32 + "-" + "2" * 16 + "-01"
_PARENT = "00-" + "3" * 32 + "-" + "4" * 16 + "-01"
_PREVIOUS = "00-" + "5" * 32 + "-" + "6" * 16 + "-01"


@pytest.fixture(scope="module")
def wheel(tmp_path_factory):
    return install_wheel(
        tmp_path_factory.mktemp("celery-attempt-wheel"), extras=("celery", "redis")
    )


def _points(receiver, instrument):
    return [
        point
        for request in receiver.requests
        if request.path == "/v1/metrics"
        for resource in request.message.resource_metrics
        for scope in resource.scope_metrics
        for metric in scope.metrics
        if metric.name == instrument
        for point in getattr(metric, metric.WhichOneof("data")).data_points
    ]


@pytest.mark.parametrize(
    ("pool", "recycle"),
    [("prefork", None), ("prefork", 1), ("solo", None)],
    ids=["reused", "recycled", "solo"],
)
def test_retry_links_previous_attempt_and_keeps_origin(
    wheel, tmp_path, otlp_receiver, pool, recycle
):
    with running_worker(
        tmp_path, wheel=wheel, receiver=otlp_receiver, pool=pool, recycle=recycle
    ) as worker:
        result = worker.submit(
            "reference.retry",
            args=[_CANARY],
            options={
                "headers": {
                    "business": "header-canary",
                    "django_altimeter": {
                        "v": 1,
                        "origin": {"traceparent": _ORIGIN},
                        "attempt": {"traceparent": _ORIGIN, "published_at": 1},
                    },
                }
            },
        )
        assert result["state"] == "SUCCESS"
        assert result["result"]["payload"] == _CANARY
        attempts = worker.wait_for("attempt", count=2)
        postrun = worker.wait_for("postrun", count=2)
        assert [event["retries"] for event in attempts] == [0, 1]
        assert [event["state"] for event in postrun] == ["RETRY", "SUCCESS"]
        assert all(event["correlation"] is None for event in postrun)
    metadata = [event["headers"]["django_altimeter"] for event in attempts]
    first, second = [item["attempt"] for item in metadata]
    assert metadata[0]["origin"] == metadata[1]["origin"] == {"traceparent": _ORIGIN}
    assert second["published_at"] > first["published_at"] > 1
    assert "eligible_at" not in first
    assert second["eligible_at"] >= second["published_at"]
    assert "previous_traceparent" not in first
    assert second["traceparent"] != first["traceparent"]
    assert all(event["headers"]["business"] == "header-canary" for event in attempts)

    observed = spans(otlp_receiver)
    consumers = [(resource, span) for resource, span in observed if span.kind == 5]
    producers = [span for _, span in observed if span.kind == 4]
    assert len(consumers) == len(producers) == 2
    ordered = []
    for event, carrier in zip(attempts, (first, second), strict=True):
        correlation = event["correlation"]
        resource, consumer = next(
            (resource, span)
            for resource, span in consumers
            if span.span_id.hex() == correlation["span_id"]
        )
        assert consumer.trace_id.hex() == correlation["trace_id"]
        assert (
            attributes(resource.attributes)["service.instance.id"]
            == event["instance_id"]
        )
        parent = next(
            span
            for span in producers
            if span.span_id.hex() == carrier["traceparent"].split("-")[2]
        )
        assert consumer.parent_span_id == parent.span_id
        assert consumer.trace_id == parent.trace_id
        ordered.append(consumer)
    previous, current = ordered
    assert len(previous.links) == 0
    assert len(current.links) == 1
    assert current.links[0].trace_id == previous.trace_id
    assert current.links[0].span_id == previous.span_id
    previous_flags = attempts[0]["correlation"]["trace_flags"]
    assert second["previous_traceparent"] == (
        f"00-{previous.trace_id.hex()}-{previous.span_id.hex()}-{previous_flags:02x}"
    )
    assert current.trace_id == previous.trace_id
    assert attributes(previous.attributes)["altimeter.outcome"] == "retry"
    assert attributes(current.attributes)["altimeter.outcome"] == "success"
    assert len({event["pid"] for event in attempts}) == (2 if recycle else 1)
    assert len({event["instance_id"] for event in attempts}) == (2 if recycle else 1)
    assert (
        sum(
            point.as_int
            for point in _points(otlp_receiver, "altimeter.worker.attempts")
        )
        == 2
    )
    assert (
        sum(
            point.count for point in _points(otlp_receiver, "altimeter.worker.duration")
        )
        == 2
    )
    assert (
        sum(
            point.count
            for point in _points(otlp_receiver, "altimeter.worker.intentional_delay")
        )
        == 1
    )
    assert "canary" not in str(otlp_receiver.requests).lower()


@pytest.mark.parametrize(
    "metadata",
    [
        None,
        {"v": 0, "traceparent": _PARENT},
        {"v": 1, "origin": "metadata-canary" * 1000, "attempt": {}},
    ],
    ids=["missing", "legacy", "oversized"],
)
def test_raw_malformed_carrier_fails_open_beside_valid_parent(
    wheel, tmp_path, otlp_receiver, metadata
):
    published = time() - 1
    valid = {
        "v": 1,
        "origin": {"traceparent": _ORIGIN},
        "attempt": {
            "traceparent": _PARENT,
            "previous_traceparent": _PREVIOUS,
            "published_at": published,
            "eligible_at": published + 0.1,
        },
    }
    with running_worker(tmp_path, wheel=wheel, receiver=otlp_receiver) as worker:
        results = []
        for carrier in (valid, metadata):
            result = worker.client(
                {
                    "action": "submit",
                    "setup": False,
                    "task": "reference.echo",
                    "args": [_CANARY],
                    "kwargs": {},
                    "get": True,
                    "options": {
                        "headers": {
                            "django_altimeter": carrier,
                            "business": "header-canary",
                            "baggage": "identity-canary",
                        }
                    },
                }
            )
            assert result["state"] == "SUCCESS"
            assert result["result"]["payload"] == _CANARY
            results.append(result["result"])
        postrun = worker.wait_for("postrun", count=2)
        assert all(event["correlation"] is None for event in postrun)
    assert results[0]["pid"] == results[1]["pid"]
    observed = [span for _, span in spans(otlp_receiver)]
    assert not [span for span in observed if span.kind == 4]
    consumers = [span for span in observed if span.kind == 5]
    assert len(consumers) == 2
    positive, negative = [
        next(
            span
            for span in consumers
            if span.span_id.hex() == result["correlation"]["span_id"]
        )
        for result in results
    ]
    assert positive.trace_id.hex() == _PARENT.split("-")[1]
    assert positive.parent_span_id.hex() == _PARENT.split("-")[2]
    assert len(positive.links) == 1
    assert positive.links[0].trace_id.hex() == _PREVIOUS.split("-")[1]
    assert positive.links[0].span_id.hex() == _PREVIOUS.split("-")[2]
    assert not negative.parent_span_id and not negative.links
    assert negative.trace_id != positive.trace_id
    assert (
        sum(
            point.as_int
            for point in _points(otlp_receiver, "altimeter.worker.attempts")
        )
        == 2
    )
    for name in (
        "altimeter.worker.enqueue_to_start",
        "altimeter.worker.intentional_delay",
    ):
        assert sum(point.count for point in _points(otlp_receiver, name)) == 1
    assert "canary" not in str(otlp_receiver.requests).lower()
