"""Isolated Django settings shared by the reference publisher and workers."""

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
ROOT_URLCONF = "altimeter_reference.celery_urls"
MIDDLEWARE = ["django_altimeter.django.RequestMetadataMiddleware"]
QUEUE = os.environ.get("REFERENCE_CELERY_QUEUE", "reference")
ALTIMETER = {
    "EXCLUDED_ROUTES": ["/health/"],
    "QUEUE_NAMES": [QUEUE, QUEUE + ".override"],
    "SPAN_NAMES": ["reference.task.work"],
    "LOG_BODIES": ["reference.task.completed", "reference.task.failed"],
    "EVENT_NAMES": ["reference.task.completed", "reference.task.failed"],
    "ATTRIBUTE_VALUES": [
        ("http.route", "tasks/<str:task>/"),
        *[
            ("task.name", "reference." + name)
            for name in (
                "echo",
                "fail",
                "retry",
                "block",
                "beat",
                "override",
                "cancel",
                "internal",
            )
        ],
    ],
}
