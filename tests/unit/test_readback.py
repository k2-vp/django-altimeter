"""Pure read-back validation and safe immutable result contracts."""

from dataclasses import FrozenInstanceError
from datetime import UTC, datetime, timedelta
from uuid import uuid4

import pytest

from django_altimeter.readback import ReadbackResult, SignalReadback, read_back


def _request(**changes: object) -> ReadbackResult:
    now = datetime.now(UTC)
    arguments = {
        "instance_id": str(uuid4()),
        "signals": ("traces", "logs", "metrics"),
        "started_at": now - timedelta(seconds=1),
        "finished_at": now,
        "environ": {},
    }
    arguments.update(changes)
    return read_back(**arguments)


def test_results_are_frozen_and_empty_or_partial_results_do_not_pass() -> None:
    accepted = SignalReadback("logs", True, "readback.observed", 1)
    absent = SignalReadback("metrics", False, "readback.absent", 6)
    assert ReadbackResult((accepted,)).ok
    assert not ReadbackResult().ok
    assert not ReadbackResult((accepted, absent)).ok
    assert not ReadbackResult((accepted,), ("readback.failed",)).ok
    with pytest.raises(FrozenInstanceError):
        accepted.observed = False


def test_credentials_absent_is_a_failure_for_each_requested_signal() -> None:
    result = _request()
    assert not result.ok
    assert result.codes == ("readback.credentials_missing",)
    assert tuple(row.signal for row in result.signals) == ("traces", "logs", "metrics")
    assert all(row.code == "readback.credentials_missing" for row in result.signals)
    assert all(not row.observed and row.attempts == 0 for row in result.signals)


@pytest.mark.parametrize("timeout", [0, -1, 31, float("nan"), float("inf"), True, "5"])
def test_invalid_budget_is_rejected_before_network(timeout: object) -> None:
    result = _request(timeout=timeout)
    assert not result.ok
    assert result.codes == ("readback.invalid_request",)


@pytest.mark.parametrize(
    "changes",
    [
        {"instance_id": "CANARY_BAD_ID"},
        {"instance_id": "00000000-0000-0000-0000-000000000000"},
        {"signals": ("traces", "traces")},
        {"signals": ("CANARY_UNKNOWN_SIGNAL",)},
        {"signals": ["traces"]},
        {"started_at": datetime.now()},
        {"started_at": datetime.now(UTC) + timedelta(seconds=10)},
        {"started_at": datetime.now(UTC) - timedelta(minutes=6)},
        {"started_at": datetime.now(UTC) - timedelta(seconds=61)},
        {"finished_at": datetime.now(UTC) + timedelta(minutes=2)},
    ],
)
def test_invalid_identity_signal_or_window_is_bounded_and_safe(changes: dict) -> None:
    result = _request(**changes)
    assert result.codes == ("readback.invalid_request",)
    assert not result.ok
    assert "CANARY" not in repr(result)


def test_no_requested_signal_cannot_pass() -> None:
    result = _request(signals=())
    assert not result.ok
    assert result.codes == ("readback.nothing_requested",)


@pytest.mark.parametrize(
    ("key", "value"),
    [
        ("HOST", "http://example.com"),
        ("HOST", "https://user:CANARY_SECRET@example.com"),
        ("HOST", "https://example.com?key=CANARY_SECRET"),
        ("HOST", "https://example.com/private"),
        ("HOST", "https://example.com/#CANARY_SECRET"),
        ("HOST", "https://example.com:99999"),
        ("HOST", "https://example.com\nCANARY_SECRET"),
        ("PROJECT_ID", "CANARY_SECRET"),
        ("PROJECT_ID", "0"),
        ("PROJECT_ID", "1/../../CANARY_SECRET"),
        ("API_KEY", "phx_CANARY\r\nAuthorization: value"),
        ("API_KEY", " phx_CANARY"),
        ("API_KEY", "é"),
        ("API_KEY", "x" * 513),
    ],
)
def test_invalid_operator_config_never_exposes_input(key: str, value: str) -> None:
    environment = {
        "ALTIMETER_READBACK_HOST": "https://us.posthog.com",
        "ALTIMETER_READBACK_PROJECT_ID": "42",
        "ALTIMETER_READBACK_API_KEY": "phx_TEST_CANARY",
        "ALTIMETER_READBACK_" + key: value,
    }
    result = _request(environ=environment)
    assert not result.ok
    assert result.codes == ("readback.invalid_config",)
    assert "CANARY" not in repr(result)
    assert "posthog.com" not in repr(result)
