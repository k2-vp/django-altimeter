"""Read-back against a local HTTP boundary; no live ingestion claim."""

from __future__ import annotations

import json
import os
import subprocess
import sys
from datetime import UTC, datetime, timedelta
from uuid import uuid4

import pytest
from support.posthog_query import PostHogQueryServer

pytestmark = pytest.mark.integration


def test_readback_queries_every_signal_for_this_probe() -> None:
    instance = str(uuid4())
    timestamp = datetime.now(UTC)
    with PostHogQueryServer() as query:
        for signal in ("traces", "logs", "metrics"):
            query.add_record(signal, instance_id=instance, timestamp=timestamp)
        environment = {
            **os.environ,
            "ALTIMETER_READBACK_HOST": query.endpoint,
            "ALTIMETER_READBACK_PROJECT_ID": "42",
            "ALTIMETER_READBACK_API_KEY": "phx_TEST_OPERATOR_CANARY",
            "TEST_INSTANCE": instance,
            "TEST_START": (timestamp - timedelta(seconds=1)).isoformat(),
            "TEST_END": (timestamp + timedelta(seconds=1)).isoformat(),
        }
        source = """
import json, os
from dataclasses import asdict
from datetime import datetime
from django_altimeter.readback import read_back
result = read_back(instance_id=os.environ['TEST_INSTANCE'],
    signals=('traces', 'logs', 'metrics'),
    started_at=datetime.fromisoformat(os.environ['TEST_START']),
    finished_at=datetime.fromisoformat(os.environ['TEST_END']))
print(json.dumps(asdict(result)))
"""
        process = subprocess.run(
            [sys.executable, "-c", source],
            env=environment,
            capture_output=True,
            text=True,
            timeout=15,
            check=False,
        )
        assert process.returncode == 0, process.stderr
        assert len(query.requests) == 3, "read-back must query all three signal tables"
        result = json.loads(process.stdout)
        assert result["codes"] == []
        assert [row["signal"] for row in result["signals"]] == [
            "traces",
            "logs",
            "metrics",
        ]
        assert all(row["observed"] for row in result["signals"])
        assert all(row["code"] == "readback.observed" for row in result["signals"])
        assert "CANARY" not in process.stdout + process.stderr


def _environment(query: PostHogQueryServer) -> dict[str, str]:
    return {
        "ALTIMETER_READBACK_HOST": query.endpoint,
        "ALTIMETER_READBACK_PROJECT_ID": "42",
        "ALTIMETER_READBACK_API_KEY": "phx_TEST_OPERATOR_CANARY",
    }


def _read(query: PostHogQueryServer, instance: str, timestamp: datetime, **changes):
    from django_altimeter.readback import read_back

    arguments = {
        "instance_id": instance,
        "signals": ("traces", "logs", "metrics"),
        "started_at": timestamp - timedelta(seconds=1),
        "finished_at": timestamp + timedelta(seconds=1),
        "environ": _environment(query),
        "timeout": 0.15,
    }
    arguments.update(changes)
    return read_back(**arguments)


def _body(
    signal: str,
    instance: str,
    timestamp: datetime,
    *,
    probe: str = "true",
    name: str = "altimeter.probe",
    scope: str = "django_altimeter",
) -> bytes:
    return json.dumps(
        {
            "columns": [
                "signal",
                "probe_timestamp",
                "instance_id",
                "probe",
                "probe_name",
                "scope",
            ],
            "results": [[signal, timestamp.isoformat(), instance, probe, name, scope]],
        }
    ).encode()


def test_wire_queries_are_separate_parameterized_and_keep_key_only_in_auth(monkeypatch):
    instance, now = str(uuid4()), datetime.now(UTC)
    monkeypatch.setenv("HTTP_PROXY", "http://127.0.0.1:1")
    monkeypatch.setenv("HTTPS_PROXY", "http://127.0.0.1:1")
    with PostHogQueryServer() as query:
        for signal in ("traces", "logs", "metrics"):
            query.add_record(signal, instance_id=instance, timestamp=now)
        result = _read(query, instance, now, timeout=2)
        assert result.ok
        assert len(query.requests) == 3
        for captured, signal, table in zip(
            query.requests,
            ("traces", "logs", "metrics"),
            ("trace_spans", "logs", "metrics"),
            strict=True,
        ):
            assert captured.path == "/api/projects/42/query/"
            assert (
                captured.headers["Authorization"] == "Bearer phx_TEST_OPERATOR_CANARY"
            )
            assert captured.headers["Accept-Encoding"] == "identity"
            assert "Cookie" not in captured.headers
            assert "CANARY" not in json.dumps(captured.body)
            assert captured.body["refresh"] == "force_blocking"
            selection = captured.body["query"]
            assert selection["kind"] == "HogQLQuery"
            assert selection["values"]["instance_id"] == instance
            sql = selection["query"]
            assert f"SELECT '{signal}' AS signal" in sql
            assert f"FROM posthog.{table}\n" in sql
            assert "timestamp >= toDateTime({started_at})" in sql
            assert "timestamp <= toDateTime({finished_at})" in sql
            resource = (
                "series.resource_attributes"
                if signal == "metrics"
                else "resource_attributes"
            )
            assert f"{resource}['service.instance.id'] = {{instance_id}}" in sql
            assert f"{resource}['altimeter.probe'] = 'true'" in sql
            assert sql.endswith("LIMIT 1")
            assert "SELECT *" not in sql
            assert (
                "body AS probe_name" in sql if signal == "logs" else "body" not in sql
            )
        assert "CANARY" not in repr(result)


@pytest.mark.parametrize("wrong", ["stale", "other_instance", "untagged", "trace_only"])
def test_trace_control_cannot_prove_missing_logs_or_metrics(wrong: str) -> None:
    instance, now = str(uuid4()), datetime.now(UTC)
    with PostHogQueryServer() as query:
        query.add_record("traces", instance_id=instance, timestamp=now)
        if wrong != "trace_only":
            for signal in ("logs", "metrics"):
                query.add_record(
                    signal,
                    instance_id=str(uuid4()) if wrong == "other_instance" else instance,
                    timestamp=now - timedelta(minutes=10) if wrong == "stale" else now,
                    probe=wrong != "untagged",
                )
        result = _read(query, instance, now)
        assert not result.ok
        assert result.signals[0].observed
        assert not result.signals[1].observed
        assert not result.signals[2].observed
        assert all(row.attempts >= 1 for row in result.signals)


@pytest.mark.parametrize("wrong", ["signal", "timestamp", "identity", "flag"])
def test_response_rows_are_rechecked_even_if_server_ignores_predicates(
    wrong: str,
) -> None:
    instance, now = str(uuid4()), datetime.now(UTC)
    body = _body(
        "traces" if wrong == "signal" else "logs",
        str(uuid4()) if wrong == "identity" else instance,
        now - timedelta(minutes=10) if wrong == "timestamp" else now,
        probe="false" if wrong == "flag" else "true",
    )
    with PostHogQueryServer() as query:
        query.queue_response(body=body)
        result = _read(query, instance, now, signals=("logs",))
        assert not result.ok
        assert not result.signals[0].observed


@pytest.mark.parametrize(
    ("status", "code"),
    [
        (400, "readback.unsupported"),
        (404, "readback.unsupported"),
        (401, "readback.auth"),
        (403, "readback.auth"),
        (202, "readback.invalid_response"),
        (204, "readback.invalid_response"),
    ],
)
def test_failed_query_is_terminal_per_signal_and_never_echoes_body(
    status, code, capsys
):
    instance, now = str(uuid4()), datetime.now(UTC)
    with PostHogQueryServer() as query:
        query.queue_response(status=status, body=b"CANARY_VENDOR_BODY_AND_DSN")
        for signal in ("logs", "metrics"):
            query.add_record(signal, instance_id=instance, timestamp=now)
        result = _read(query, instance, now)
        assert not result.ok
        assert result.signals[0].code == code
        assert result.signals[0].attempts == 1
        assert result.signals[1].observed and result.signals[2].observed
        assert len(query.requests) == 3
        assert "CANARY" not in repr(result)
        output = capsys.readouterr()
        assert "CANARY" not in output.out + output.err


def test_redirect_never_sends_operator_credentials_to_another_origin() -> None:
    instance, now = str(uuid4()), datetime.now(UTC)
    with PostHogQueryServer() as query, PostHogQueryServer() as destination:
        query.queue_response(status=302, headers=(("Location", destination.endpoint),))
        result = _read(query, instance, now, signals=("logs",))
        assert result.signals[0].code == "readback.redirect"
        assert not result.ok
        assert len(query.requests) == 1
        assert destination.requests == ()


@pytest.mark.parametrize("status", [429, 503])
def test_retryable_query_then_success_is_observed_without_requerying_success(status):
    instance, now = str(uuid4()), datetime.now(UTC)
    with PostHogQueryServer() as query:
        query.queue_response(status=status, body=b"CANARY_VENDOR")
        for signal in ("traces", "logs", "metrics"):
            query.add_record(signal, instance_id=instance, timestamp=now)
        result = _read(query, instance, now, timeout=2)
        assert result.ok
        assert [row.attempts for row in result.signals] == [2, 1, 1]
        assert len(query.requests) == 4


@pytest.mark.parametrize(
    "body",
    [
        b"{",
        b"\xff",
        b"{}",
        b'{"columns":[],"results":[]}',
        b'{"columns":["signal","probe_timestamp","instance_id","probe"],"results":true}',
    ],
)
def test_malformed_json_or_result_schema_never_passes(body: bytes) -> None:
    instance, now = str(uuid4()), datetime.now(UTC)
    with PostHogQueryServer() as query:
        query.queue_response(body=body)
        result = _read(query, instance, now, signals=("metrics",))
        assert not result.ok
        assert result.signals[0].code == "readback.invalid_response"
        assert len(query.requests) == 1


@pytest.mark.parametrize(
    "header", [("Content-Type", "text/html"), ("Content-Encoding", "gzip")]
)
def test_unsupported_content_is_rejected_before_decoding(header) -> None:
    instance, now = str(uuid4()), datetime.now(UTC)
    with PostHogQueryServer() as query:
        query.queue_response(body=_body("logs", instance, now), headers=(header,))
        result = _read(query, instance, now, signals=("logs",))
        assert not result.ok
        assert result.signals[0].code == "readback.invalid_response"


@pytest.mark.parametrize("size", [65535, 65536, 65537])
def test_response_size_boundary_keeps_valid_positive_cases(size: int) -> None:
    instance, now = str(uuid4()), datetime.now(UTC)
    body = _body("logs", instance, now)
    body += b" " * (size - len(body))
    with PostHogQueryServer() as query:
        query.queue_response(body=body)
        result = _read(query, instance, now, signals=("logs",), timeout=1)
        assert result.ok is (size <= 65536)
        assert result.signals[0].code == (
            "readback.observed" if size <= 65536 else "readback.response_size"
        )


@pytest.mark.parametrize("mode", ["hang", "headers", "body"])
def test_hangs_and_trickled_body_respect_budget_and_server_teardown(mode: str) -> None:
    from time import monotonic

    instance, now = str(uuid4()), datetime.now(UTC)
    start = monotonic()
    with PostHogQueryServer() as query:
        query.queue_response(
            body=_body("logs", instance, now),
            hang=mode == "hang",
            delay=1 if mode == "headers" else 0,
            chunk_size=1 if mode == "body" else 8192,
            chunk_delay=0.01 if mode == "body" else 0,
        )
        result = _read(query, instance, now, signals=("logs",), timeout=0.15)
        assert not result.ok
        assert result.signals[0].code == "readback.deadline"
    assert monotonic() - start < 0.6


@pytest.mark.parametrize("trickle", [False, True])
def test_chunked_query_body_obeys_the_same_deadline(trickle: bool) -> None:
    from time import monotonic

    instance, now = str(uuid4()), datetime.now(UTC)
    start = monotonic()
    with PostHogQueryServer() as query:
        query.queue_response(
            body=_body("logs", instance, now),
            chunked=True,
            chunk_size=1 if trickle else 8192,
            chunk_delay=0.01 if trickle else 0,
        )
        result = _read(
            query, instance, now, signals=("logs",), timeout=0.15 if trickle else 2
        )
        assert result.ok is not trickle
        assert result.signals[0].code == (
            "readback.deadline" if trickle else "readback.observed"
        )
        assert result.signals[0].attempts == 1
        assert len(query.requests) == 1
        assert "CANARY" not in repr(result)
    if trickle:
        assert monotonic() - start < 0.6


def test_absence_stops_after_six_attempts_with_bounded_backoff() -> None:
    from time import monotonic

    instance, now = str(uuid4()), datetime.now(UTC)
    with PostHogQueryServer() as query:
        start = monotonic()
        result = _read(query, instance, now, signals=("logs",), timeout=10)
        elapsed = monotonic() - start
        assert not result.ok
        assert result.signals[0].code == "readback.absent"
        assert result.signals[0].attempts == 6
        assert len(query.requests) == 6
        assert 5.5 <= elapsed < 8


def test_repeated_failed_calls_release_connections_and_deadline_threads() -> None:
    import threading

    instance, now = str(uuid4()), datetime.now(UTC)
    original_threads = {thread.ident for thread in threading.enumerate()}
    original_fds = len(os.listdir("/dev/fd"))
    for _ in range(5):
        with PostHogQueryServer() as query:
            query.queue_response(body=b"CANARY_VENDOR")
            result = _read(query, instance, now, signals=("logs",))
            assert result.signals[0].code == "readback.invalid_response"
    assert {thread.ident for thread in threading.enumerate()} == original_threads
    assert len(os.listdir("/dev/fd")) <= original_fds


@pytest.mark.live
@pytest.mark.skipif(
    not (
        os.environ.get("ALTIMETER_READBACK_API_KEY")
        and os.environ.get("ALTIMETER_READBACK_PROJECT_ID")
    ),
    reason="live PostHog operator credentials absent; ingestion is unverified",
)
def test_live_posthog_observes_all_three_fresh_probe_signals() -> None:
    """Requires real three-signal ingest configuration plus a scoped read key."""
    source = """
import json
from django_altimeter.conf import load_config
from django_altimeter.runtime import probe
from django_altimeter.readback import read_back
loaded = load_config()
assert loaded.config is not None, loaded.codes
sent = probe(loaded.config)
assert sent.ok, repr(sent)
assert {row.signal for row in sent.signals} == {'traces', 'logs', 'metrics'}
seen = read_back(instance_id=sent.instance_id,
    signals=tuple(row.signal for row in sent.signals),
    started_at=sent.started_at, finished_at=sent.finished_at, timeout=30)
assert seen.ok, repr(seen)
print(json.dumps({'observed': [row.signal for row in seen.signals]}))
"""
    process = subprocess.run(
        [sys.executable, "-c", source],
        capture_output=True,
        text=True,
        timeout=40,
        check=False,
    )
    assert process.returncode == 0, process.stderr
    assert json.loads(process.stdout) == {"observed": ["traces", "logs", "metrics"]}


@pytest.mark.parametrize(
    ("signal", "column"),
    [("traces", "name"), ("logs", "body"), ("metrics", "metric_name")],
)
def test_wire_query_requires_the_fixed_payload_name(signal: str, column: str) -> None:
    instance, now = str(uuid4()), datetime.now(UTC)
    with PostHogQueryServer() as query:
        query.add_record(signal, instance_id=instance, timestamp=now)
        result = _read(query, instance, now, signals=(signal,))
        assert result.ok
        sql = query.requests[0].body["query"]["query"]
        assert f"AND {column} = 'altimeter.probe'" in sql
        if signal == "metrics":
            assert "AND instrumentation_scope = 'django_altimeter'" in sql


@pytest.mark.parametrize("signal", ["traces", "logs", "metrics"])
def test_fixed_name_is_required_even_with_correct_identity_flag_and_time(signal):
    instance, now = str(uuid4()), datetime.now(UTC)
    with PostHogQueryServer() as query:
        query.add_record(signal, instance_id=instance, timestamp=now, name="unrelated")
        result = _read(query, instance, now, signals=(signal,))
        assert not result.ok
        assert not result.signals[0].observed


@pytest.mark.parametrize("signal", ["traces", "logs", "metrics"])
def test_wrong_name_in_returned_row_cannot_pass(signal):
    instance, now = str(uuid4()), datetime.now(UTC)
    with PostHogQueryServer() as query:
        query.queue_response(body=_body(signal, instance, now, name="CANARY_VENDOR"))
        result = _read(query, instance, now, signals=(signal,))
        assert not result.ok
        assert not result.signals[0].observed
        assert "CANARY" not in repr(result)


def test_metric_name_from_another_scope_cannot_pass() -> None:
    instance, now = str(uuid4()), datetime.now(UTC)
    with PostHogQueryServer() as query:
        query.queue_response(body=_body("metrics", instance, now, scope="other_scope"))
        result = _read(query, instance, now, signals=("metrics",))
        assert not result.ok
        assert not result.signals[0].observed
