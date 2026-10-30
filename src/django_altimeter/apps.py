"""Register local Django checks without initializing telemetry."""

from django.apps import AppConfig  # type: ignore[import-untyped]
from django.core.checks import Tags, register  # type: ignore[import-untyped]


class AltimeterConfig(AppConfig):  # type: ignore[misc]
    name = "django_altimeter"
    verbose_name = "Django Altimeter"

    def ready(self) -> None:
        from .checks import check_configuration

        register(Tags.compatibility, "django_altimeter")(check_configuration)
