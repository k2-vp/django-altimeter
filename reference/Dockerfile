# Build from the repository root after `uv build`; never copy src into this image.
ARG PYTHON_VERSION=3.13
# The 0.164.0 distribution contains contrib components v0.164.1.
FROM otel/opentelemetry-collector-contrib:0.164.0@sha256:340c9ca6a36617084c6661bdc4f0fe92b7cda5a805d61673ec8db375fddc522b AS collector
FROM python:${PYTHON_VERSION}-slim
COPY --from=collector /otelcol-contrib /usr/local/bin/otelcol-contrib

ARG PYTHON_VERSION
ARG DJANGO_VERSION=6.0.9
ARG REQUESTS_VERSION=2.33.1
ARG URLLIB3_VERSION=2.6.3
ARG ASGI_DEPENDENCIES=0
ARG WSGI_DEPENDENCIES=0
ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1 \
    ALTIMETER_REFERENCE_PYTHON=${PYTHON_VERSION} \
    ALTIMETER_REFERENCE_DJANGO=${DJANGO_VERSION} \
    ALTIMETER_REFERENCE_REQUESTS=${REQUESTS_VERSION} \
    ALTIMETER_REFERENCE_URLLIB3=${URLLIB3_VERSION} \
    ALTIMETER_REFERENCE_ASGI=${ASGI_DEPENDENCIES} \
    ALTIMETER_REFERENCE_WSGI=${WSGI_DEPENDENCIES}

WORKDIR /verify
COPY dist/*.whl dist/*.tar.gz /verify/dist/
RUN python - <<'PY'
import os
from pathlib import Path
import subprocess
import sys

wheels = list(Path('/verify/dist').glob('*.whl'))
if len(wheels) != 1:
    raise SystemExit('Expected exactly one newly built wheel in dist/')
subprocess.run(
    [
        sys.executable, '-m', 'pip', 'install', '--no-cache-dir',
        f"Django=={os.environ['ALTIMETER_REFERENCE_DJANGO']}",
        f"requests=={os.environ['ALTIMETER_REFERENCE_REQUESTS']}",
        f"urllib3=={os.environ['ALTIMETER_REFERENCE_URLLIB3']}",
        'opentelemetry-api==1.45.1',
        'opentelemetry-sdk==1.45.1',
        'opentelemetry-exporter-otlp-proto-http==1.45.1',
        'pytest==9.2.0',
        'pytest-django==4.14.0',
        *(['django-ninja==1.6.2', 'uvicorn==0.46.0']
          if os.environ['ALTIMETER_REFERENCE_ASGI'] == '1' else []),
        *(['gunicorn==23.0.0', 'djangorestframework==3.16.1']
          if os.environ['ALTIMETER_REFERENCE_WSGI'] == '1' else []),
        f"{wheels[0]}[celery,psycopg,httpx,requests,redis,structlog,process]",
    ],
    check=True,
)
PY

# Only test configuration and tests accompany the installed wheel.
COPY pyproject.toml /verify/
COPY tests /verify/tests/
COPY deploy/collector.yaml /verify/deploy/collector.yaml
COPY reference/altimeter_reference /verify/reference/altimeter_reference
CMD ["python", "-m", "pytest", "tests/integration/test_wheel.py"]
