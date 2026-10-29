"""Explicit, bounded PostHog probe read-back, independent for each signal.

Operator credentials never enter runtime configuration or result values. This
module starts no workers or transport imports until read_back is called. Query
bodies select probe identity, flag, timestamp and fixed name/scope fields.
See https://posthog.com/docs/api/queries and the documented HogQL table schemas.
"""

from __future__ import annotations

import json
import os
from collections.abc import Mapping
from dataclasses import dataclass, field
from datetime import UTC, datetime
from math import isfinite
from time import monotonic, sleep
from typing import TYPE_CHECKING
from urllib.parse import urlsplit
from uuid import UUID

if TYPE_CHECKING:
    import requests

_SIGNALS = ("traces", "logs", "metrics")
_COLUMNS = ["signal", "probe_timestamp", "instance_id", "probe", "probe_name", "scope"]
_RETRYABLE = {"readback.absent", "readback.unavailable", "readback.network"}


@dataclass(frozen=True)
class SignalReadback:
    signal: str
    observed: bool
    code: str
    attempts: int = 0


@dataclass(frozen=True)
class ReadbackResult:
    signals: tuple[SignalReadback, ...] = ()
    codes: tuple[str, ...] = ()

    @property
    def ok(self) -> bool:
        return (
            bool(self.signals)
            and not self.codes
            and all(signal.observed for signal in self.signals)
        )


@dataclass(frozen=True)
class _OperatorConfig:
    endpoint: str = field(repr=False)
    key: str = field(repr=False)


def _operator_config(environ: Mapping[str, str]) -> tuple[_OperatorConfig | None, str]:
    key = environ.get("ALTIMETER_READBACK_API_KEY", "")
    if not key:
        return None, "readback.credentials_missing"
    host = environ.get("ALTIMETER_READBACK_HOST", "https://us.posthog.com")
    project = environ.get("ALTIMETER_READBACK_PROJECT_ID", "")
    if (
        type(key) is not str
        or not 1 <= len(key) <= 512
        or any(
            not (character.isascii() and (character.isalnum() or character in "_-"))
            for character in key
        )
        or type(project) is not str
        or not 1 <= len(project) <= 20
        or not project.isascii()
        or not project.isdecimal()
        or int(project) < 1
        or type(host) is not str
        or not 1 <= len(host) <= 2048
        or any(ord(character) <= 32 or ord(character) >= 127 for character in host)
    ):
        return None, "readback.invalid_config"
    try:
        parsed = urlsplit(host)
        if (
            not parsed.hostname
            or parsed.username is not None
            or parsed.password is not None
            or parsed.query
            or parsed.fragment
            or parsed.path not in ("", "/")
            or (parsed.port is not None and not 1 <= parsed.port <= 65535)
            or (
                parsed.scheme != "https"
                and not (
                    parsed.scheme == "http" and parsed.hostname in {"127.0.0.1", "::1"}
                )
            )
        ):
            return None, "readback.invalid_config"
    except ValueError:
        return None, "readback.invalid_config"
    return _OperatorConfig(f"{host.rstrip('/')}/api/projects/{project}/query/", key), ""


def _valid_request(
    instance_id: str,
    signals: tuple[str, ...],
    started_at: datetime,
    finished_at: datetime,
    timeout: float,
) -> bool:
    if (
        type(signals) is not tuple
        or len(signals) > 3
        or any(type(signal) is not str or signal not in _SIGNALS for signal in signals)
        or len(set(signals)) != len(signals)
        or type(instance_id) is not str
        or len(instance_id) != 36
        or type(timeout) not in (int, float)
        or not isfinite(timeout)
        or not 0 < timeout <= 30
        or type(started_at) is not datetime
        or type(finished_at) is not datetime
        or started_at.tzinfo is None
        or finished_at.tzinfo is None
    ):
        return False
    try:
        identity = UUID(instance_id)
        now = datetime.now(UTC)
        return (
            identity.version == 4
            and str(identity) == instance_id
            and 0 <= (finished_at - started_at).total_seconds() <= 60
            and 0 <= (now - started_at).total_seconds() <= 300
            and (finished_at - now).total_seconds() <= 5
        )
    except (ValueError, TypeError, OverflowError):
        return False


def _query(
    signal: str, instance_id: str, started_at: datetime, finished_at: datetime
) -> dict[str, object]:
    table = "trace_spans" if signal == "traces" else signal
    name_column = {"traces": "name", "logs": "body", "metrics": "metric_name"}[signal]
    scope_filter = (
        "AND instrumentation_scope = 'django_altimeter'\n"
        if signal == "metrics"
        else ""
    )
    resource = (
        "series.resource_attributes" if signal == "metrics" else "resource_attributes"
    )
    sql = (
        f"SELECT '{signal}' AS signal,\n"
        "toString(toTimeZone(timestamp, 'UTC')) AS probe_timestamp,\n"
        f"{resource}['service.instance.id'] AS instance_id,\n"
        f"{resource}['altimeter.probe'] AS probe,\n"
        f"{name_column} AS probe_name, instrumentation_scope AS scope\n"
        f"FROM posthog.{table}\n"
        "WHERE timestamp >= toDateTime({started_at})\n"
        "AND timestamp <= toDateTime({finished_at})\n"
        f"AND {resource}['service.instance.id'] = {{instance_id}}\n"
        f"AND {resource}['altimeter.probe'] = 'true'\n"
        f"AND {name_column} = 'altimeter.probe'\n"
        f"{scope_filter}"
        "LIMIT 1"
    )
    return {
        "query": {
            "kind": "HogQLQuery",
            "query": sql,
            "values": {
                "instance_id": instance_id,
                "started_at": started_at.astimezone(UTC).isoformat(),
                "finished_at": finished_at.astimezone(UTC).isoformat(),
            },
        },
        "refresh": "force_blocking",
        "name": f"altimeter_probe_{signal}",
    }


def _matching_result(
    payload: object,
    signal: str,
    instance_id: str,
    started_at: datetime,
    finished_at: datetime,
) -> str:
    if type(payload) is not dict or payload.get("columns") != _COLUMNS:
        return "readback.invalid_response"
    rows = payload.get("results")
    if type(rows) is not list or len(rows) > 1:
        return "readback.invalid_response"
    if not rows:
        return "readback.absent"
    row = rows[0]
    if (
        type(row) is not list
        or len(row) != 6
        or any(type(value) is not str for value in row)
        or len(row[1]) > 64
    ):
        return "readback.invalid_response"
    try:
        timestamp = datetime.fromisoformat(row[1])
        if timestamp.tzinfo is None:
            timestamp = timestamp.replace(tzinfo=UTC)
    except ValueError:
        return "readback.invalid_response"
    if (
        row[0] == signal
        and row[2] == instance_id
        and row[3] == "true"
        and row[4] == "altimeter.probe"
        and (signal != "metrics" or row[5] == "django_altimeter")
        and started_at <= timestamp <= finished_at
    ):
        return "readback.observed"
    return "readback.absent"


def _fetch(
    session: requests.Session,
    operator: _OperatorConfig,
    query: dict[str, object],
    deadline: float,
) -> tuple[object, str]:
    import requests

    from django_altimeter.exporting import ExportProtocolError, _read_response

    try:
        remaining = deadline - monotonic()
        if remaining <= 0:
            return None, "readback.deadline"
        with session.post(
            operator.endpoint,
            data=json.dumps(query).encode("utf-8"),
            headers={
                "Authorization": f"Bearer {operator.key}",
                "Accept": "application/json",
                "Content-Type": "application/json",
                "Accept-Encoding": "identity",
            },
            timeout=min(2.0, remaining),
            allow_redirects=False,
            stream=True,
        ) as response:
            status = response.status_code
            if status in (401, 403):
                return None, "readback.auth"
            if status in (400, 404, 405, 422):
                return None, "readback.unsupported"
            if status in (429, 500, 502, 503, 504):
                return None, "readback.unavailable"
            if 300 <= status < 400:
                return None, "readback.redirect"
            if (
                status != 200
                or response.headers.get("Content-Type", "")
                .split(";", 1)[0]
                .strip()
                .lower()
                != "application/json"
                or response.headers.get("Content-Encoding", "identity").strip().lower()
                != "identity"
            ):
                return None, "readback.invalid_response"
            body = _read_response(response, 65536, deadline)
            return json.loads(body), ""
    except ExportProtocolError as error:
        return None, {
            "export.deadline": "readback.deadline",
            "export.response_size": "readback.response_size",
        }.get(str(error), "readback.invalid_response")
    except requests.RequestException:
        return (
            None,
            "readback.deadline" if monotonic() >= deadline else "readback.network",
        )
    except (ValueError, OSError):
        return (
            None,
            "readback.deadline"
            if monotonic() >= deadline
            else "readback.invalid_response",
        )


def read_back(
    *,
    instance_id: str,
    signals: tuple[str, ...],
    started_at: datetime,
    finished_at: datetime,
    environ: Mapping[str, str] | None = None,
    timeout: float = 10.0,
) -> ReadbackResult:
    """Read only this probe; all requested signals must independently be seen.

    At most six rounds share one monotonic budget. The existing transport body
    reader bounds bytes and interrupts slow bodies. As in OTLP transport, a
    continuously trickled HTTP header is outside the absolute deadline guarantee.
    No response content or input credentials enter returned diagnostics.
    """
    requested: tuple[str, ...] = ()

    def failure(code: str) -> ReadbackResult:
        return ReadbackResult(
            tuple(SignalReadback(signal, False, code) for signal in requested), (code,)
        )

    try:
        if not _valid_request(instance_id, signals, started_at, finished_at, timeout):
            return failure("readback.invalid_request")
        requested = signals
        if not requested:
            return failure("readback.nothing_requested")
        operator, code = _operator_config(os.environ if environ is None else environ)
        if operator is None:
            return failure(code)
        # Local command validation and imports never load Requests or start a
        # response watchdog. Only an explicitly requested read-back reaches here.
        import requests

        results = {
            signal: SignalReadback(signal, False, "readback.absent")
            for signal in requested
        }
        deadline = monotonic() + timeout
        with requests.Session() as session:
            session.trust_env = False
            for round_number in range(6):
                for signal in requested:
                    previous = results[signal]
                    if previous.observed or previous.code not in _RETRYABLE:
                        continue
                    if monotonic() >= deadline:
                        results[signal] = SignalReadback(
                            signal, False, "readback.deadline", previous.attempts
                        )
                        continue
                    payload, code = _fetch(
                        session,
                        operator,
                        _query(signal, instance_id, started_at, finished_at),
                        deadline,
                    )
                    if not code:
                        code = _matching_result(
                            payload, signal, instance_id, started_at, finished_at
                        )
                    results[signal] = SignalReadback(
                        signal, code == "readback.observed", code, previous.attempts + 1
                    )
                if not any(result.code in _RETRYABLE for result in results.values()):
                    break
                if round_number < 5:
                    sleep(
                        min(
                            0.25 * 2**round_number,
                            2.0,
                            max(0.0, deadline - monotonic()),
                        )
                    )
        return ReadbackResult(tuple(results[signal] for signal in requested))
    except Exception:
        return failure("readback.failed")
