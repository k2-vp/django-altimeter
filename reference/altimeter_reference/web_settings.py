"""Settings for the real uvicorn/Ninja reference application."""

from altimeter_reference.settings import (
    ALLOWED_HOSTS,
    DATABASES,
    DEBUG,
    DEFAULT_AUTO_FIELD,
    INSTALLED_APPS,
    SECRET_KEY,
)

__all__ = [
    "ALLOWED_HOSTS",
    "DATABASES",
    "DEBUG",
    "DEFAULT_AUTO_FIELD",
    "INSTALLED_APPS",
    "SECRET_KEY",
]
ROOT_URLCONF = "altimeter_reference.api"
MIDDLEWARE = [
    "django_altimeter.django.RequestMetadataMiddleware",
    "altimeter_reference.api.RejectMiddleware",
]
ALTIMETER = {
    "EXCLUDED_ROUTES": ["/health/"],
    "ATTRIBUTE_VALUES": [
        ("http.route", route)
        for route in (
            "api/items/<item_id>",
            "api/rejected",
            "api/error",
            "api/stream",
            "api/abort",
            "api/benchmark",
            *(f"api/series/{index}" for index in range(100)),
        )
    ],
    "FRONTEND_ORIGINS": ["https://frontend.example"],
}
