"""A callable that remains usable when telemetry is disabled or invalid."""

from django.http import HttpRequest, JsonResponse
from django.urls import path


def health(request: HttpRequest) -> JsonResponse:
    return JsonResponse({"ok": True})


urlpatterns = [path("health/", health, name="health")]
