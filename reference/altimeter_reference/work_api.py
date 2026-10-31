"""Thin Ninja routes over the real request-work functions."""

from asgiref.sync import sync_to_async
from django.http import HttpResponse, JsonResponse
from ninja import NinjaAPI

from altimeter_reference import workloads

api = NinjaAPI(docs_url=None, openapi_url=None, urls_namespace="reference-work")


def _private(response):
    response["Cache-Control"] = "private, no-store"
    return response


@api.get("/orm")
def orm(request):
    return _private(JsonResponse(workloads.orm_count()))


@api.get("/async-orm")
async def async_orm(request):
    return _private(JsonResponse(await workloads.async_orm_count()))


@api.get("/direct")
def direct(request):
    return _private(JsonResponse(workloads.direct_query()))


@api.get("/rows")
def rows(request):
    return _private(
        HttpResponse(workloads.serialized_rows(), content_type="application/json")
    )


@api.get("/outgoing")
async def outgoing(request, client: str = "requests"):
    if client == "async-httpx":
        result = await workloads.async_outgoing_calls()
    else:
        result = await sync_to_async(workloads.outgoing_calls)(client)
    return _private(JsonResponse(result))


@api.get("/redirect")
async def redirect(request, client: str = "requests"):
    if client == "async-httpx":
        result = await workloads.async_outgoing_calls(redirect=True)
    else:
        result = await sync_to_async(workloads.outgoing_calls)(client, redirect=True)
    return _private(JsonResponse(result))


@api.get("/logs")
def logs(request):
    return _private(JsonResponse(workloads.emit_logs()))


@api.get("/combined")
def combined(request):
    return _private(JsonResponse(workloads.combined_work()))
