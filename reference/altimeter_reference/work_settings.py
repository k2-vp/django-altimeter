"""Real Postgres/Redis workloads, selected explicitly by the test launcher."""

import os

from psycopg.conninfo import conninfo_to_dict

from altimeter_reference.web_settings import (
    ALLOWED_HOSTS,
    DEBUG,
    DEFAULT_AUTO_FIELD,
    INSTALLED_APPS,
    MIDDLEWARE,
    ROOT_URLCONF,
    SECRET_KEY,
)

__all__ = [
    "ALLOWED_HOSTS",
    "DEBUG",
    "DEFAULT_AUTO_FIELD",
    "INSTALLED_APPS",
    "MIDDLEWARE",
    "ROOT_URLCONF",
    "SECRET_KEY",
]

_database = conninfo_to_dict(os.environ["REFERENCE_POSTGRES_DSN"])
DATABASES = {
    "default": {
        "ENGINE": "django.db.backends.postgresql",
        "NAME": _database["dbname"],
        "USER": _database.get("user", ""),
        "PASSWORD": _database.get("password", ""),
        "HOST": _database.get("host", ""),
        "PORT": _database.get("port", ""),
        "OPTIONS": {"connect_timeout": 5},
    }
}

ALTIMETER = {
    "EXCLUDED_ROUTES": ["/health/"],
    "SPAN_NAMES": [
        "reference.query",
        "reference.materialize",
        "reference.serialize",
        "reference.outgoing",
        "reference.work",
    ],
    "SCOPE_NAMES": ["reference.work"],
    "LOG_BODIES": ["reference.work.completed"],
    "EVENT_NAMES": ["reference.work.completed"],
    "ATTRIBUTE_VALUES": [
        ("http.route", f"work/{route}")
        for route in (
            "orm",
            "async-orm",
            "direct",
            "rows",
            "outgoing",
            "redirect",
            "logs",
            "combined",
        )
    ],
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
