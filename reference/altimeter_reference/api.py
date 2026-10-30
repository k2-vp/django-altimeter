"""Small HTTP workloads with explicit response and cancellation behavior."""

import asyncio
import os

from asgiref.sync import iscoroutinefunction, markcoroutinefunction
from django.http import HttpResponse, JsonResponse, StreamingHttpResponse
from django.urls import path
from ninja import NinjaAPI

from django_altimeter.runtime import get_process_runtime

api = NinjaAPI(docs_url=None, openapi_url=None)


def _private(response):
    response["Cache-Control"] = "private, no-store"
    return response


def health(request):
    runtime = get_process_runtime()
    return _private(
        JsonResponse(
            {
                "ok": True,
                "pid": os.getpid(),
                "instance_id": runtime.instance_id if runtime else None,
            }
        )
    )


@api.get("/items/{item_id}")
def item(request, item_id: int):
    runtime = get_process_runtime()
    return _private(
        JsonResponse(
            {
                "item_id": item_id,
                "pid": os.getpid(),
                "instance_id": runtime.instance_id if runtime else None,
            }
        )
    )


@api.get("/rejected")
def rejected(request):
    raise AssertionError("The rejection middleware must prevent this view")


@api.get("/error")
def error(request):
    raise ValueError("reference-error-canary")


@api.get("/benchmark")
def benchmark(request):
    return _private(HttpResponse(b"x" * 1024, content_type="application/octet-stream"))


@api.get("/stream")
async def stream(request):
    async def chunks():
        yield b"first\n"
        await asyncio.sleep(0.2)
        yield b"last\n"

    return _private(StreamingHttpResponse(chunks(), content_type="text/plain"))


@api.get("/abort")
async def abort(request):
    async def chunks():
        yield b"first\n"
        await asyncio.sleep(30)
        yield b"unreachable\n"

    return _private(StreamingHttpResponse(chunks(), content_type="text/plain"))


class RejectMiddleware:
    """Reject one static path without executing its endpoint."""

    sync_capable = True
    async_capable = True

    def __init__(self, get_response):
        self.get_response = get_response
        self.is_async = iscoroutinefunction(get_response)
        if self.is_async:
            markcoroutinefunction(self)

    def __call__(self, request):
        if self.is_async:
            return self._async(request)
        if request.path == "/api/rejected":
            return _private(JsonResponse({"rejected": True}, status=403))
        return self.get_response(request)

    async def _async(self, request):
        if request.path == "/api/rejected":
            return _private(JsonResponse({"rejected": True}, status=403))
        return await self.get_response(request)


async def series(request, status: int = 200):
    if status not in {200, 404, 503}:
        return _private(HttpResponse(status=400))
    return _private(HttpResponse(b"series", status=status))


# Static resolver templates exercise cardinality admission through real HTTP.
# The count is bounded and every template is explicitly approved in settings.
for series_index in range(100):
    api.api_operation(
        ["GET", "POST", "PUT", "PATCH", "DELETE", "HEAD", "OPTIONS", "TRACE"],
        f"/series/{series_index}",
        url_name=f"series_{series_index}",
        include_in_schema=False,
    )(series)


urlpatterns = [path("health/", health), path("api/", api.urls)]

if os.environ.get("REFERENCE_REQUEST_WORK") == "1":
    from altimeter_reference.work_api import api as work_api

    urlpatterns.append(path("work/", work_api.urls))
