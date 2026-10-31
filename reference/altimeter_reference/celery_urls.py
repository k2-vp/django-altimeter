"""Actual HTTP publication, with no eager task execution."""

from django.http import JsonResponse
from django.urls import path


def health(request):
    return JsonResponse({"ok": True})


def publish(request, task):
    from altimeter_reference.celery_app import app

    result = app.send_task(
        "reference." + task,
        args=[request.GET.get("payload", "http-argument-canary")]
        if task in ("echo", "retry")
        else [],
        headers={"business": "header-canary"},
    )
    return JsonResponse({"task_id": result.id})


urlpatterns = [path("health/", health), path("tasks/<str:task>/", publish)]
