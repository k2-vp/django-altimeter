"""Explicit serving-process lifespan for uvicorn's application factory mode."""

import json
import os
from time import monotonic

from django.core.asgi import get_asgi_application

from django_altimeter import setup, shutdown
from django_altimeter.asgi import instrument
from django_altimeter.runtime import get_process_runtime


def _lifecycle(event, started, result):
    # Optional local test observation; never a telemetry destination.
    destination = os.environ.get("REFERENCE_LIFECYCLE_FILE")
    if destination:
        runtime = get_process_runtime()
        record = {
            "event": event,
            "pid": os.getpid(),
            "elapsed": monotonic() - started,
            "instance_id": runtime.instance_id if runtime else None,
            "codes": result.codes,
        }
        with open(destination, "a", encoding="utf-8") as stream:
            stream.write(json.dumps(record) + "\n")


def create_application():
    os.environ.setdefault("DJANGO_SETTINGS_MODULE", "altimeter_reference.web_settings")
    django_application = get_asgi_application()
    http_application = instrument(django_application)

    async def application(scope, receive, send):
        if scope["type"] != "lifespan":
            await http_application(scope, receive, send)
            return
        while True:
            message = await receive()
            if message["type"] == "lifespan.startup":
                started = monotonic()
                result = setup(role="web")
                if os.environ.get("REFERENCE_REQUEST_WORK") == "1":
                    from django_altimeter.logging import install_logging

                    install_logging(routes=("reference.work",))
                _lifecycle("startup", started, result)
                await send({"type": "lifespan.startup.complete"})
            elif message["type"] == "lifespan.shutdown":
                started = monotonic()
                result = shutdown(timeout=2.0)
                _lifecycle("shutdown", started, result)
                await send({"type": "lifespan.shutdown.complete"})
                return

    return application
