"""Optional process-source faults preserve real web responses and all signals."""

import json

import pytest
from support.otlp_receiver import OTLPReceiver
from support.process_web import (
    CANARY,
    PROCESS_NAMES,
    PSUTIL_NAMES,
    SAFE_LOG,
    break_psutil_installation,
    http_counts,
    install_process_wheel,
    metric_points,
    running_process_web,
    server_spans,
)

pytestmark = pytest.mark.integration


@pytest.mark.parametrize("source", ["healthy", "disabled", "missing", "broken"])
def test_optional_process_source_preserves_web_diagnosis(tmp_path, source):
    wheel = install_process_wheel(tmp_path / "wheel")
    if source in ("missing", "broken"):
        break_psutil_installation(wheel, source)
    with OTLPReceiver() as receiver:
        with running_process_web(
            tmp_path / "server",
            wheel=wheel,
            receiver=receiver,
            process_enabled=source != "disabled",
            logs=True,
        ) as (server, health):
            for _ in range(8):
                status, _, body = server.request("/api/benchmark")
                assert status == 200 and body == b"x" * 1024
            status, _, body = server.request("/process-log")
            assert status == 200
            logged = json.loads(body)
            assert logged["ok"] is True
            assert logged["trace_id"] and logged["span_id"]
            status, _, _ = server.request("/api/error")
            assert status == 500
        assert len(server_spans(receiver)) == 10
        assert http_counts(receiver) == {
            "altimeter.http.server.requests": 10,
            "http.server.request.duration": 10,
        }
        logs = [
            record
            for request in receiver.requests
            if request.path == "/v1/logs"
            for resource in request.message.resource_logs
            for scope in resource.scope_logs
            for record in scope.log_records
        ]
        assert len(logs) == 1
        assert logs[0].body.string_value == SAFE_LOG
        assert logs[0].trace_id.hex() == logged["trace_id"]
        assert logs[0].span_id.hex() == logged["span_id"]
        points = metric_points(receiver)
        names = {metric.name for _, metric, _ in points}
        if source == "healthy":
            assert PSUTIL_NAMES <= names
            for resource, metric, _ in points:
                if metric.name in PROCESS_NAMES:
                    attributes = {
                        item.key: item.value.string_value
                        for item in resource.attributes
                    }
                    assert attributes["service.instance.id"] == health["instance_id"]
        elif source == "disabled":
            assert not PROCESS_NAMES & names
        else:
            assert not PSUTIL_NAMES & names
        assert CANARY in server.output
        assert CANARY not in str(receiver.requests)
        assert "reference-error-canary" not in str(receiver.requests)
