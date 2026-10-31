"""Gunicorn's public child hooks; the master never initializes telemetry."""

import json
import os
from time import monotonic


def post_fork(server, worker):
    os.environ["REFERENCE_GUNICORN_WORKER_PID"] = str(worker.pid)


def record_lifecycle(event, started, result, instance_id):
    destination = os.environ.get("REFERENCE_LIFECYCLE_FILE")
    if destination:
        with open(destination, "a", encoding="utf-8") as stream:
            stream.write(
                json.dumps(
                    {
                        "event": event,
                        "pid": os.getpid(),
                        "elapsed": monotonic() - started,
                        "instance_id": instance_id,
                        "codes": result.codes,
                        "completed": getattr(result, "completed", None),
                    }
                )
                + "\n"
            )


def worker_exit(server, worker):
    from django_altimeter import shutdown
    from django_altimeter.runtime import get_process_runtime

    runtime = get_process_runtime()
    started = monotonic()
    result = shutdown(timeout=2.0)
    record_lifecycle(
        "shutdown", started, result, runtime.instance_id if runtime else None
    )
