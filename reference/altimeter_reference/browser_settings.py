"""Real cross-origin browser checks; telemetry headers stay native-owned."""

import json
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
ROOT_URLCONF = "altimeter_reference.browser_api"
MIDDLEWARE = ["django_altimeter.django.RequestMetadataMiddleware"]
BROWSER_ORIGINS = tuple(json.loads(os.environ["REFERENCE_BROWSER_ORIGINS"]))
ALTIMETER = {
    "EXCLUDED_ROUTES": ["/health/"],
    "FRONTEND_ORIGINS": [BROWSER_ORIGINS[0]],
    "ATTRIBUTE_VALUES": [("http.route", "api/data")],
}
