"""Isolated settings for local bootstrap and management-command tests."""

from secrets import token_hex

SECRET_KEY = token_hex(32)
DEBUG = False
ALLOWED_HOSTS = ["127.0.0.1", "localhost"]
INSTALLED_APPS = ["django.contrib.contenttypes", "django_altimeter"]
DATABASES = {"default": {"ENGINE": "django.db.backends.sqlite3", "NAME": ":memory:"}}
ROOT_URLCONF = "altimeter_reference.urls"
MIDDLEWARE = []
DEFAULT_AUTO_FIELD = "django.db.models.BigAutoField"
ALTIMETER = {}
