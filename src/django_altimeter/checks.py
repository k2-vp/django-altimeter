"""Report safe configuration diagnostics without initializing providers."""

import re
from dataclasses import dataclass, field
from importlib.metadata import version

from django.core.checks import Warning  # type: ignore[import-untyped]

from .conf import Config, validate_config
from .django import load_django_config


def check_configuration(app_configs: object = None, **kwargs: object) -> list[Warning]:
    """Warn about invalid telemetry settings without blocking Django startup."""
    try:
        result = load_django_config()
    except Exception:
        return [Warning("config.invalid", id="django_altimeter.W001")]
    if result.ok:
        return []
    return [Warning(code, id="django_altimeter.W001") for code in result.codes]


@dataclass(frozen=True)
class LocalDiagnostics:
    """Local facts, with configuration retained privately for explicit probes."""

    config: Config | None = field(repr=False)
    codes: tuple[str, ...] = ()
    versions: tuple[tuple[str, str], ...] = ()
    enabled_integrations: tuple[str, ...] = ()
    disabled_integrations: tuple[str, ...] = ()
    missing_integrations: tuple[str, ...] = ()

    @property
    def ok(self) -> bool:
        return self.config is not None and all(
            code == "config.disabled" for code in self.codes
        )


_INTEGRATION_DISTRIBUTIONS = (
    ("django", ("Django",)),
    ("asgi", ("asgiref",)),
    ("wsgi", ("Django",)),
    ("requests", ("requests",)),
    ("httpx", ("httpx",)),
    ("psycopg", ("psycopg",)),
    ("psycopg2", ("psycopg2", "psycopg2-binary")),
    ("redis", ("redis",)),
    ("celery", ("celery",)),
    ("rq", ("rq",)),
)
_VERSION_DISTRIBUTIONS = (
    "django-altimeter",
    "Django",
    "opentelemetry-api",
    "opentelemetry-sdk",
    "opentelemetry-exporter-otlp-proto-http",
    "requests",
    "urllib3",
)


def _version(distribution: str) -> str:
    try:
        value = version(distribution)
        if len(value) <= 64 and re.fullmatch(r"[0-9][0-9A-Za-z.+!-]*", value):
            return value
    except Exception:
        # Package metadata is optional diagnostic evidence, never exception prose.
        pass
    return "unavailable"


def diagnose_local(*, role: str | None = None) -> LocalDiagnostics:
    """Validate locally without starting providers or network activity."""
    versions = tuple((name, _version(name)) for name in _VERSION_DISTRIBUTIONS)
    try:
        loaded = load_django_config(role=role)
        if loaded.config is None:
            return LocalDiagnostics(None, loaded.codes, versions)
        validated = validate_config(loaded.config)
        config = validated.config
        if config is None:
            return LocalDiagnostics(None, validated.codes, versions)
        enabled = tuple(sorted(config.integrations)) if config.enabled else ()
        disabled = tuple(
            name for name, _ in _INTEGRATION_DISTRIBUTIONS if name not in enabled
        )
        missing = tuple(
            name
            for name, distributions in _INTEGRATION_DISTRIBUTIONS
            if name in enabled
            and all(_version(item) == "unavailable" for item in distributions)
        )
        runtime_codes: tuple[str, ...] = ()
        if config.enabled:
            from .runtime import validate_runtime

            runtime_codes = validate_runtime(config)
        codes = (
            validated.codes
            + runtime_codes
            + tuple(f"check.{name}_missing" for name in missing)
        )
        return LocalDiagnostics(config, codes, versions, enabled, disabled, missing)
    except Exception:
        return LocalDiagnostics(None, ("check.failed",), versions)
