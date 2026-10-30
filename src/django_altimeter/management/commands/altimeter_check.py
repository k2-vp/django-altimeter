"""Explicit local validation, isolated OTLP probe and operator read-back."""

from __future__ import annotations

import json
import os
import re
from argparse import ArgumentParser, ArgumentTypeError
from dataclasses import asdict, dataclass, replace
from math import isfinite
from typing import Any, NoReturn, cast

from django.core.management.base import (  # type: ignore[import-untyped]
    BaseCommand,
    CommandError,
)

from django_altimeter.checks import LocalDiagnostics, diagnose_local
from django_altimeter.conf import Config

_SIGNALS = ("traces", "logs", "metrics")
_DISPLAY_MAX_CHARS = 128


@dataclass(frozen=True)
class SignalStatus:
    signal: str
    requested: bool
    configured: bool
    accepted: bool | None = None
    observed: bool | None = None
    codes: tuple[str, ...] = ()


def _timeout(value: str) -> float:
    if len(value) <= 32:
        try:
            number = float(value)
            if isfinite(number) and 0 < number <= 30:
                return number
        except ValueError:
            pass
    raise ArgumentTypeError("check.invalid_arguments")


def _service(config: Config | None) -> dict[str, str]:
    names = ("name", "environment", "release", "role")
    if config is None:
        return dict.fromkeys(names, "unavailable")
    # Only bounded identifiers are useful display values. Never display an
    # endpoint, a credential-shaped identifier or configured header material.
    secrets: set[str] = set()
    # Direct secret inputs may stay outside Config, including read-back keys.
    # Longer secrets cannot occur inside a bounded display identifier.
    for name in ("ALTIMETER_READBACK_API_KEY", "ALTIMETER_POSTHOG_TOKEN"):
        secret = os.environ.get(name, "")
        if 0 < len(secret) <= _DISPLAY_MAX_CHARS:
            secrets.add(secret)
    for signal in (config.traces, config.logs, config.metrics):
        for _, value in signal.headers:
            secrets.add(value)
            secrets.update(value.split())
    values = (
        config.service_name,
        config.deployment_environment,
        config.release,
        config.role,
    )
    return {
        name: value
        if (
            not value
            or (
                len(value) <= _DISPLAY_MAX_CHARS
                and re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9_.-]*", value)
                and not any(secret and secret in value for secret in secrets)
                and not any(token in value.lower() for token in ("phc_", "phx_"))
            )
        )
        else "[redacted]"
        for name, value in zip(names, values, strict=True)
    }


def _report(
    mode: str,
    local: LocalDiagnostics,
    signals: tuple[SignalStatus, ...],
    *,
    ok: bool,
    codes: tuple[str, ...],
) -> dict[str, object]:
    return {
        "mode": mode,
        "ok": ok,
        "configured": local.config is not None,
        "codes": list(dict.fromkeys(codes)),
        "service": _service(local.config),
        "versions": dict(local.versions),
        "integrations": {
            "enabled": list(local.enabled_integrations),
            "disabled": list(local.disabled_integrations),
            "missing": list(local.missing_integrations),
        },
        "signals": [asdict(signal) for signal in signals],
    }


def _run(mode: str, selected: tuple[str, ...], timeout: float) -> dict[str, object]:
    local = diagnose_local(role="probe" if mode != "local" else None)
    config = local.config
    enabled = tuple(
        name
        for name in _SIGNALS
        if config is not None and getattr(config, name).enabled
    )
    requested = selected or enabled
    signals = tuple(
        SignalStatus(
            name,
            name in requested,
            name in enabled,
            False if name in requested and mode != "local" else None,
            False if name in requested and mode == "read-back" else None,
        )
        for name in _SIGNALS
    )
    if mode == "local" or not local.ok or config is None:
        return _report(mode, local, signals, ok=local.ok, codes=local.codes)
    return _probe_report(mode, local, signals, requested, timeout)


def _probe_report(
    mode: str,
    local: LocalDiagnostics,
    signals: tuple[SignalStatus, ...],
    requested: tuple[str, ...],
    timeout: float,
) -> dict[str, object]:
    from django_altimeter.runtime import probe

    config = local.config
    assert config is not None
    result = probe(config, signals=requested, timeout=2.0)
    outcomes = {item.signal: item for item in result.signals}
    signals = tuple(
        replace(
            item,
            accepted=outcomes[item.signal].accepted,
            codes=(outcomes[item.signal].code,),
        )
        if item.requested and item.signal in outcomes
        else item
        for item in signals
    )
    codes = local.codes + result.codes + tuple(item.code for item in result.signals)
    accepted = (
        bool(requested)
        and result.ok
        and all(item.accepted for item in signals if item.requested)
    )
    if mode == "probe" or not accepted:
        return _report(mode, local, signals, ok=accepted, codes=codes)
    if result.instance_id is None:
        return _report(
            mode, local, signals, ok=False, codes=codes + ("check.not_probed",)
        )

    from django_altimeter.readback import read_back

    observed = read_back(
        instance_id=result.instance_id,
        signals=requested,
        started_at=result.started_at,
        finished_at=result.finished_at,
        timeout=timeout,
    )
    observations = {item.signal: item for item in observed.signals}
    signals = tuple(
        replace(
            item,
            observed=observations[item.signal].observed,
            codes=item.codes + (observations[item.signal].code,),
        )
        if item.requested and item.signal in observations
        else item
        for item in signals
    )
    codes += observed.codes + tuple(item.code for item in observed.signals)
    return _report(
        mode,
        local,
        signals,
        ok=observed.ok and all(item.observed for item in signals if item.requested),
        codes=codes,
    )


def _text(report: dict[str, object]) -> str:
    lines = [f"mode={report['mode']} ok={str(report['ok']).lower()}"]
    for name, value in cast(dict[str, str], report["service"]).items():
        lines.append(f"service.{name}={value}")
    for name, value in cast(dict[str, str], report["versions"]).items():
        lines.append(f"version.{name}={value}")
    for name, values in cast(dict[str, list[str]], report["integrations"]).items():
        lines.append(f"integrations.{name}={','.join(values) or 'none'}")
    for signal in cast(list[dict[str, object]], report["signals"]):
        states = " ".join(
            f"{name}="
            + (str(signal[name]).lower() if signal[name] is not None else "not_checked")
            for name in ("requested", "configured", "accepted", "observed")
        )
        codes = ",".join(cast(tuple[str, ...], signal["codes"])) or "none"
        lines.append(f"{signal['signal']} {states} codes={codes}")
    lines.append("codes=" + (",".join(cast(list[str], report["codes"])) or "none"))
    return "\n".join(lines)


class Command(BaseCommand):  # type: ignore[misc]
    help = "Validate local telemetry, explicitly probe OTLP or check PostHog read-back."
    requires_system_checks: list[str] = []
    requires_migrations_checks = False

    def create_parser(
        self, prog_name: str, subcommand: str, **kwargs: Any
    ) -> ArgumentParser:
        parser = cast(
            ArgumentParser,
            super().create_parser("django", "altimeter_check", **kwargs),
        )

        def safe_error(message: str) -> NoReturn:
            # argparse's usual error includes invalid values and unknown argv.
            if self._called_from_command_line:
                parser.exit(2, "check.invalid_arguments\n")
            raise CommandError("check.invalid_arguments", returncode=2)

        parser.error = safe_error  # type: ignore[method-assign]
        return parser

    def add_arguments(self, parser: ArgumentParser) -> None:
        modes = parser.add_mutually_exclusive_group()
        modes.add_argument("--local", action="store_const", const="local", dest="mode")
        modes.add_argument("--probe", action="store_const", const="probe", dest="mode")
        modes.add_argument(
            "--read-back", action="store_const", const="read-back", dest="mode"
        )
        parser.set_defaults(mode="local")
        parser.add_argument("--signal", action="append", choices=_SIGNALS, default=[])
        parser.add_argument("--json", action="store_true", dest="json_output")
        parser.add_argument(
            "--timeout",
            type=_timeout,
            default=10.0,
            help="Read-back budget, 0<seconds<=30.",
        )

    def handle(self, *args: object, **options: Any) -> None:
        mode = options["mode"]
        selected = tuple(name for name in _SIGNALS if name in options["signal"])
        try:
            report = _run(mode, selected, options["timeout"])
        except Exception:
            report = _report(
                mode,
                LocalDiagnostics(None),
                tuple(SignalStatus(name, name in selected, False) for name in _SIGNALS),
                ok=False,
                codes=("check.failed",),
            )
        self.stdout.write(
            json.dumps(report, separators=(",", ":"))
            if options["json_output"]
            else _text(report)
        )
        if not report["ok"]:
            raise CommandError("check.failed", returncode=1)
