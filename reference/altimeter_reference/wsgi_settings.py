"""Settings for the real gunicorn gthread/DRF reference app."""

import os

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
ROOT_URLCONF = "altimeter_reference.wsgi_api"
MIDDLEWARE = [
    "django_altimeter.django.RequestMetadataMiddleware",
    "altimeter_reference.wsgi_api.RejectMiddleware",
]
REST_FRAMEWORK = {
    "DEFAULT_AUTHENTICATION_CLASSES": [],
    "UNAUTHENTICATED_USER": None,
}
ALTIMETER = {
    "EXCLUDED_ROUTES": ["/health/"],
    "SPAN_NAMES": ["reference.query", "reference.outgoing", "reference.work"],
    "SCOPE_NAMES": ["reference.work"],
    "LOG_BODIES": ["reference.work.completed"],
    "EVENT_NAMES": ["reference.work.completed"],
    "IDENTITY_RESOLVER": "altimeter_reference.wsgi_api.resolve_identity",
    "ATTRIBUTE_VALUES": [
        *[
            ("http.route", route)
            for route in (
                "api/items/<int:item_id>",
                "api/file",
                "api/stream",
                "api/abort",
                "api/error",
                "api/benchmark",
                "api/delay",
                "api/private",
                "api/cacheable",
                "work/direct",
                "work/outgoing",
                "work/redis",
                "work/redirect",
                "work/logs",
                "work/log-stream",
                "work/combined",
            )
        ],
        ("posthogDistinctId", "person-alpha"),
        ("posthogDistinctId", "person-beta"),
        ("altimeter.tenant_id", "tenant-alpha"),
        ("altimeter.tenant_id", "tenant-beta"),
    ],
    "FRONTEND_ORIGINS": ["https://frontend.example"],
    "INTERNAL_ORIGINS": [os.environ["REFERENCE_INTERNAL_ORIGIN"]]
    if os.environ.get("REFERENCE_INTERNAL_ORIGIN")
    else [],
}

LOGGING = {
    "version": 1,
    "disable_existing_loggers": False,
    "formatters": {"reference": {"format": "%(levelname)s|%(name)s|%(message)s"}},
    "handlers": {
        "stdout": {
            "class": "logging.StreamHandler",
            "stream": "ext://sys.stdout",
            "formatter": "reference",
        }
    },
    "loggers": {
        "reference.work": {
            "handlers": ["stdout"],
            "level": "INFO",
            "propagate": False,
        }
    },
}
