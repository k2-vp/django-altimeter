"""A real delayed JSON response behind the existing native ASGI boundary."""

import asyncio
import json
import os

from django.conf import settings
from django.http import HttpRequest, HttpResponse, JsonResponse, StreamingHttpResponse
from django.urls import path


def _delay(value: str | None) -> float:
    try:
        return min(2000, max(0, int(value or "0"))) / 1000
    except ValueError:
        return 0


def _cors(request: HttpRequest, response: HttpResponse) -> HttpResponse:
    origin = request.headers.get("Origin")
    if origin in settings.BROWSER_ORIGINS:
        response["Access-Control-Allow-Origin"] = origin
        response["Access-Control-Allow-Credentials"] = "true"
        response["Access-Control-Allow-Methods"] = "GET, OPTIONS"
        response["Access-Control-Allow-Headers"] = (
            "Content-Type, Authorization, X-Posthog-Session-Id, X-Posthog-Distinct-Id"
        )
        response["Access-Control-Expose-Headers"] = (
            "X-Altimeter-Trace-Id, X-Altimeter-Trace-Sampled, Timing-Allow-Origin, "
            "Vary, Cache-Control"
        )
    response["Cache-Control"] = "private, no-store"
    response["Vary"] = "Cookie"
    return response


async def data(request: HttpRequest) -> HttpResponse:
    observation = {
        "method": request.method,
        "path": request.path,
        "query": request.META.get("QUERY_STRING", ""),
        "headers": dict(request.headers),
    }
    with open(
        os.environ["REFERENCE_BROWSER_REQUESTS"], "a", encoding="utf-8"
    ) as stream:
        stream.write(json.dumps(observation) + "\n")
    if request.method == "OPTIONS":
        return _cors(request, HttpResponse(status=204))
    await asyncio.sleep(_delay(request.GET.get("header_ms")))
    scenario = request.GET.get("scenario", "success")
    items = (
        []
        if scenario == "empty"
        else [{"id": "CANARY_RECORD_ID", "name": "First item"}]
    )

    async def body():
        yield b'{"items":'
        await asyncio.sleep(_delay(request.GET.get("body_ms")))
        yield json.dumps(items).encode() + b"}"

    response = StreamingHttpResponse(
        body(),
        status=500 if scenario == "failure" else 200,
        content_type="application/json",
    )
    return _cors(request, response)


def health(request: HttpRequest) -> JsonResponse:
    return JsonResponse({"ok": True})


urlpatterns = [path("health/", health), path("api/data", data)]
