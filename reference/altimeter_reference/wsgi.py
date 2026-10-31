"""No-preload Django bootstrap: initialize only in the serving child."""

import logging
import os
from time import monotonic

from django.core.wsgi import get_wsgi_application

from altimeter_reference.gunicorn_conf import record_lifecycle
from django_altimeter import setup
from django_altimeter.runtime import get_process_runtime
from django_altimeter.wsgi import instrument

os.environ.setdefault("DJANGO_SETTINGS_MODULE", "altimeter_reference.wsgi_settings")
django_application = get_wsgi_application()
if os.environ.get("REFERENCE_GUNICORN_WORKER_PID") == str(os.getpid()):
    started = monotonic()
    result = setup(role="web")
    if os.environ.get("REFERENCE_REQUEST_WORK") == "1":
        from django_altimeter.logging import install_logging

        install_logging(routes=("reference.work",))
    runtime = get_process_runtime()
    record_lifecycle(
        "startup", started, result, runtime.instance_id if runtime else None
    )
else:
    logging.getLogger("django_altimeter.reference").warning("setup.preload_unsupported")

application = instrument(
    django_application,
    trusted_ingress=os.environ.get("REFERENCE_TRUSTED_INGRESS") == "1",
)
