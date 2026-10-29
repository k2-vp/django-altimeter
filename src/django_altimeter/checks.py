"""Report safe configuration diagnostics without initializing providers."""

from django.core.checks import Warning  # type: ignore[import-untyped]

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
