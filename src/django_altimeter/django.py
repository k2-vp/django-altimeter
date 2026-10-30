"""Lazy Django settings and response metadata; no provider construction."""

import logging
import re
from collections.abc import Callable, Mapping
from time import monotonic
from typing import Any

from asgiref.sync import iscoroutinefunction, markcoroutinefunction

from django_altimeter.conf import Config, ConfigResult, load_config
from django_altimeter.privacy import is_allowed_attribute

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


def _metadata_diagnostic() -> None:
    try:
        logging.getLogger("django_altimeter.django").warning("http.metadata_failed")
    except Exception:
        pass  # A failing application log handler must not change the response.


def _record_response_metadata(request: Any, response: Any) -> None:
    try:
        from django_altimeter.http import RequestState, get_request_state

        state = get_request_state()
        if state is None:
            scope = getattr(request, "scope", None)
            if type(scope) is dict:
                state = scope.get("django_altimeter.request_state")
        if type(state) is not RequestState or state.finalized:
            return
        state.response_ready_at = monotonic()
        streaming = response.streaming
        state.response_mode = (
            "streaming"
            if streaming is True
            else "buffered"
            if streaming is False
            else "unknown"
        )
        match = request.resolver_match
        state.resolved = match is not None
        state.route = None
        route = getattr(match, "route", None)
        config = state.config
        if type(config) is not Config or type(route) is not str:
            return
        # A resolver template is trusted structure, but custom resolvers can still
        # supply unbounded strings. Never fall back to path, kwargs or view names.
        maximum = config.limits.max_string_bytes
        if len(route) > maximum or len(route.encode("utf-8")) > maximum:
            return
        if not state.excluded and is_allowed_attribute(
            "http.route", route, policy=config.policy, limits=config.limits
        ):
            state.route = route
    except Exception:
        _metadata_diagnostic()


class RequestMetadataMiddleware:
    """Add response facts to an existing native request lifetime, without IO."""

    sync_capable = True
    async_capable = True

    def __init__(self, get_response: Callable[[Any], Any]) -> None:
        self.get_response = get_response
        self._async = iscoroutinefunction(get_response)
        if self._async:
            markcoroutinefunction(self)

    def __call__(self, request: Any) -> Any:
        if self._async:
            return self._async_response(request)
        response = self.get_response(request)
        _record_response_metadata(request, response)
        return response

    async def _async_response(self, request: Any) -> Any:
        response = await self.get_response(request)
        _record_response_metadata(request, response)
        return response
