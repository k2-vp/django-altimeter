"""Isolated real services and installed-wheel execution for request-work tests."""

import json
import os
import subprocess
from contextlib import contextmanager
from dataclasses import dataclass, field
from urllib.parse import urlsplit, urlunsplit
from uuid import uuid4

from support.uvicorn_server import ROOT, WheelEnvironment

WORK_EXTRAS = ("psycopg", "httpx", "requests", "redis", "structlog")
ROW_COUNT = 4096
ROW_CANARY = "private-row-canary"


@dataclass(frozen=True)
class WorkServices:
    postgres_dsn: str = field(repr=False)
    redis_url: str = field(repr=False)
    redis_key_prefix: str

    def environment(self) -> dict[str, str]:
        return {
            "REFERENCE_POSTGRES_DSN": self.postgres_dsn,
            "REFERENCE_REDIS_URL": self.redis_url,
            "REFERENCE_REDIS_KEY_PREFIX": self.redis_key_prefix,
            "REFERENCE_REQUEST_WORK": "1",
            "DJANGO_SETTINGS_MODULE": "altimeter_reference.work_settings",
        }


@contextmanager
def isolated_services():
    """Own one database and key prefix; never flush a shared Redis database."""
    import psycopg
    import redis
    from psycopg import sql
    from psycopg.conninfo import make_conninfo

    base_dsn = os.environ.get("ALTIMETER_TEST_POSTGRES_DSN")
    base_redis = os.environ.get("ALTIMETER_TEST_REDIS_URL")
    if not base_dsn or not base_redis:
        raise RuntimeError(
            "Set ALTIMETER_TEST_POSTGRES_DSN and ALTIMETER_TEST_REDIS_URL "
            "to isolated test services"
        )
    identity = uuid4()
    database = f"altimeter_{identity.hex}"
    redis_parts = urlsplit(base_redis)
    redis_url = urlunsplit(redis_parts._replace(path=f"/{identity.int % 15 + 1}"))
    client = redis.Redis.from_url(redis_url, socket_timeout=3, socket_connect_timeout=3)
    prefix = f"altimeter:{identity.hex}:"
    with psycopg.connect(base_dsn, autocommit=True, connect_timeout=5) as admin:
        admin.execute(sql.SQL("CREATE DATABASE {}").format(sql.Identifier(database)))
        try:
            assert client.ping(), "The isolated Redis service must be reachable"
            yield WorkServices(
                make_conninfo(base_dsn, dbname=database), redis_url, prefix
            )
        finally:
            try:
                for key in client.scan_iter(match=f"{prefix}*", count=100):
                    client.delete(key)
            finally:
                client.close()
                admin.execute(
                    sql.SQL("DROP DATABASE {} WITH (FORCE)").format(
                        sql.Identifier(database)
                    )
                )


_BOOTSTRAP = r"""
import importlib.metadata
import json
import os
from pathlib import Path
import sys

sys.path.insert(0, os.environ["REFERENCE_PATH"])
import django_altimeter
origin = Path(django_altimeter.__file__).resolve()
assert origin.is_relative_to(Path(sys.prefix).resolve())
distribution = importlib.metadata.distribution("django-altimeter")
direct = json.loads(distribution.read_text("direct_url.json"))
assert direct["archive_info"]["hashes"]["sha256"] == os.environ["WHEEL_SHA256"]
assert not direct.get("dir_info", {}).get("editable", False)
"""


def run_work_script(
    wheel: WheelEnvironment,
    script: str,
    *,
    environment: dict[str, str],
    timeout: float = 30,
) -> dict:
    """Run real library/application code without adding src to the import path."""
    result = subprocess.run(
        [str(wheel.python), "-I", "-c", _BOOTSTRAP + "\n" + script],
        env={
            "PATH": os.environ.get("PATH", ""),
            "REFERENCE_PATH": str(ROOT / "reference"),
            "WHEEL_SHA256": wheel.sha256,
            "OTEL_SEMCONV_STABILITY_OPT_IN": "http",
            **environment,
        },
        stdout=subprocess.PIPE,
        text=True,
        check=True,
        timeout=timeout,
    )
    print(result.stdout, end="")
    reports = [
        line.removeprefix("WORK_RESULT ")
        for line in result.stdout.splitlines()
        if line.startswith("WORK_RESULT ")
    ]
    assert len(reports) == 1, result.stdout
    return json.loads(reports[0])


def prepare_contenttypes(wheel: WheelEnvironment, services: WorkServices) -> None:
    """Migrate the existing Django model and seed bounded rows before setup."""
    result = run_work_script(
        wheel,
        f"""
import django
django.setup()
from django.core.management import call_command
from django.contrib.contenttypes.models import ContentType
call_command("migrate", "contenttypes", verbosity=0, interactive=False)
ContentType.objects.bulk_create([
    ContentType(app_label={ROW_CANARY!r}, model=f"item-{{index:04d}}")
    for index in range({ROW_COUNT})
])
count = ContentType.objects.filter(app_label={ROW_CANARY!r}).count()
print("WORK_RESULT " + json.dumps({{"rows": count}}))
""",
        environment={**services.environment(), "ALTIMETER_ENABLED": "false"},
    )
    assert result == {"rows": ROW_COUNT}
