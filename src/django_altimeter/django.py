"""Lazy Django settings boundary; no provider or instrumentation construction."""

import re
from collections.abc import Mapping

from django_altimeter.conf import ConfigResult, load_config

_DOTTED_PATH = re.compile(r"[A-Za-z_]\w*(?:\.[A-Za-z_]\w*)+")


def load_django_config(
    *, role: str | None = None, environ: Mapping[str, str] | None = None
) -> ConfigResult:
    """Resolve trusted callbacks only when Django's application registry is ready."""
    try:
        from django.apps import apps  # type: ignore[import-untyped]
        from django.conf import settings  # type: ignore[import-untyped]
        from django.utils.module_loading import import_string  # type: ignore[import-untyped]

        if not settings.configured:
            result = load_config(environ=environ, role=role)
            if result.config is not None and result.config.enabled:
                return ConfigResult(None, ("config.django_not_ready",))
            return result
        raw = getattr(settings, "ALTIMETER", {})
        if type(raw) is not dict or len(raw) > 32:
            return ConfigResult(None, ("config.invalid_settings",))
        values = raw.copy()
        callbacks = {
            key: values.pop(key, None) for key in ("IDENTITY_RESOLVER", "LOG_SCRUBBER")
        }
        result = load_config(environ=environ, values=values, role=role)
        if result.config is None or not result.config.enabled:
            return result
        if not apps.ready:
            return ConfigResult(None, ("config.django_not_ready",))
        for key, callback in callbacks.items():
            if type(callback) is str:
                if len(callback) > 256 or not _DOTTED_PATH.fullmatch(callback):
                    return ConfigResult(None, ("config.invalid_callback",))
                try:
                    callback = import_string(callback)
                except Exception:
                    return ConfigResult(None, ("config.callback_import",))
            if callback is not None and not callable(callback):
                return ConfigResult(None, ("config.invalid_callback",))
            values[key] = callback
        return load_config(environ=environ, values=values, role=role)
    except Exception:
        return ConfigResult(None, ("config.django_settings",))
