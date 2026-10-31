"""Small synchronous DRF reference routes."""

import json
import os
import tempfile
import threading
from time import sleep

from django.http import FileResponse, HttpResponse, JsonResponse, StreamingHttpResponse
from django.urls import path
from rest_framework.decorators import api_view
from rest_framework.response import Response

from django_altimeter import enrich, get_correlation
from django_altimeter.context import Identity
from django_altimeter.runtime import get_process_runtime

FILE_BODY = b"reference-file\n" * 4096


class RejectMiddleware:
    def __init__(self, get_response):
        self.get_response = get_response

    def __call__(self, request):
        if request.path == "/api/rejected":
            return JsonResponse({"rejected": True}, status=403)
        return self.get_response(request)


def health(request):
    correlation = get_correlation()
    return JsonResponse(
        {
            "ok": True,
            "pid": os.getpid(),
            "thread_id": threading.get_ident(),
            "trace_id": correlation.trace_id if correlation else None,
            "span_id": correlation.span_id if correlation else None,
        }
    )


@api_view(["GET"])
def item(request, item_id):
    runtime = get_process_runtime()
    return _private(
        Response(
            {
                "item_id": item_id,
                "pid": os.getpid(),
                "instance_id": runtime.instance_id if runtime else None,
            }
        )
    )


@api_view(["GET"])
def file_response(request):
    stream = tempfile.TemporaryFile()
    stream.write(FILE_BODY)
    stream.seek(0)
    return FileResponse(stream, content_type="application/octet-stream")


def _stream(*, abort):
    try:
        yield b"first\n"
        sleep(0.5)
        if abort:
            for _ in range(256):
                yield b"x" * 65536
        else:
            yield b"last\n"
    finally:
        destination = os.environ.get("REFERENCE_CLOSE_FILE")
        if destination:
            with open(destination, "a", encoding="utf-8") as stream:
                stream.write(json.dumps({"abort": abort, "pid": os.getpid()}) + "\n")


@api_view(["GET"])
def streamed(request):
    return StreamingHttpResponse(_stream(abort=False), content_type="text/plain")


@api_view(["GET"])
def aborted(request):
    return StreamingHttpResponse(_stream(abort=True), content_type="text/plain")


@api_view(["GET"])
def error(request):
    raise ValueError("reference-error-canary")


@api_view(["GET"])
def benchmark(request):
    return HttpResponse(b"x" * 1024, content_type="application/octet-stream")


@api_view(["GET"])
def delayed(request):
    seconds = request.query_params.get("seconds", "20")
    if seconds not in {"20", "36", "60"}:
        return Response({"error": "unsupported_delay"}, status=400)
    sleep(int(seconds))
    return Response({"seconds": int(seconds)})


def _private(response):
    response["Cache-Control"] = "private, no-store"
    response["Vary"] = "Cookie"
    return response


@api_view(["GET"])
def private(request):
    return _private(Response({"ok": True}))


@api_view(["GET"])
def cacheable(request):
    response = Response({"ok": True})
    response["Cache-Control"] = "public, max-age=60"
    return response


@api_view(["GET"])
def direct(request):
    from altimeter_reference.workloads import direct_query

    return _private(Response(direct_query()))


@api_view(["GET"])
def outgoing(request):
    from altimeter_reference.workloads import outgoing_calls

    return _private(
        Response(outgoing_calls(request.query_params.get("client", "requests")))
    )


@api_view(["GET"])
def redis_work(request):
    from altimeter_reference.workloads import redis_roundtrip

    return _private(Response({"redis": redis_roundtrip()}))


@api_view(["GET"])
def redirect(request):
    from altimeter_reference.workloads import outgoing_calls

    return _private(
        Response(
            outgoing_calls(
                request.query_params.get("client", "requests"),
                redirect=True,
            )
        )
    )


def resolve_identity(subject):
    # Fixed test identities, resolved from an explicit application subject.
    # Browser identity and session headers never authorize enrichment.
    return {
        "alpha": Identity("person-alpha", "tenant-alpha"),
        "beta": Identity("person-beta", "tenant-beta"),
    }.get(subject, Identity())


@api_view(["GET"])
def logs(request):
    from altimeter_reference.workloads import emit_logs

    enrich(request.query_params.get("identity", "none"))
    delay = request.query_params.get("delay", "0")
    if delay not in {"0", "0.05", "0.1", "0.25"}:
        return Response({"error": "unsupported_delay"}, status=400)
    sleep(float(delay))
    result = emit_logs()
    correlation = get_correlation()
    result.update(
        pid=os.getpid(),
        thread_id=threading.get_ident(),
        trace_flags=correlation.trace_flags if correlation else None,
    )
    return _private(Response(result))


@api_view(["GET"])
def combined(request):
    from altimeter_reference.workloads import combined_work

    return _private(Response(combined_work()))


@api_view(["GET"])
def log_stream(request):
    from altimeter_reference.workloads import emit_logs

    def content():
        yield json.dumps(emit_logs()).encode() + b"\n"
        sleep(0.05)
        yield json.dumps(emit_logs()).encode() + b"\n"

    return _private(
        StreamingHttpResponse(content(), content_type="application/x-ndjson")
    )


urlpatterns = [
    path("health/", health),
    path("api/items/<int:item_id>", item),
    path("api/file", file_response),
    path("api/stream", streamed),
    path("api/abort", aborted),
    path("api/error", error),
    path("api/benchmark", benchmark),
    path("api/delay", delayed),
    path("api/private", private),
    path("api/cacheable", cacheable),
    path("work/direct", direct),
    path("work/outgoing", outgoing),
    path("work/redis", redis_work),
    path("work/redirect", redirect),
    path("work/logs", logs),
    path("work/log-stream", log_stream),
    path("work/combined", combined),
]
