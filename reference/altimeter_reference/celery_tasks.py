"""Small real tasks; their canary payloads must never appear in OTLP."""

import logging
import os
import time
from dataclasses import asdict

from celery.exceptions import Ignore

from altimeter_reference.celery_app import app, observe, queue
from django_altimeter import get_correlation, span
from django_altimeter.runtime import get_process_runtime


@app.task(name="reference.echo")
def echo(payload="argument-canary", delay=0):
    runtime = get_process_runtime()
    correlation = get_correlation()
    with span("reference.task.work"):
        log_correlation = get_correlation()
        if delay:
            time.sleep(delay)
        logging.getLogger("reference.tasks").warning("reference.task.completed")
        logging.getLogger("reference.tasks").warning("unapproved %s", payload)
    return {
        "payload": payload,
        "pid": os.getpid(),
        "instance_id": runtime.instance_id if runtime else None,
        "correlation": asdict(correlation) if correlation else None,
        "log_correlation": asdict(log_correlation) if log_correlation else None,
    }


@app.task(name="reference.fail")
def fail():
    raise ValueError("exception-message-canary")


@app.task(name="reference.override", queue=queue + ".override")
def override(payload="override-argument-canary"):
    return echo.run(payload)


@app.task(bind=True, name="reference.retry", max_retries=1)
def retry(self, payload="retry-argument-canary"):
    observe("attempt", retries=self.request.retries, headers=self.request.headers)
    if self.request.retries == 0:
        raise self.retry(countdown=0.2)
    return echo.run(payload)


@app.task(name="reference.block")
def block():
    observe("blocked")
    time.sleep(60)
    return "unexpected"


@app.task(name="reference.beat")
def tick():
    observe("tick")
    return echo.run("beat-result-canary")


@app.task(name="reference.cancel")
def cancel():
    observe("cancelled")
    raise Ignore()


@app.task(name="reference.internal")
def internal():
    return echo.run("loader-recovered-canary")
