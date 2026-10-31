"""Concrete application work, callable from HTTP views or standalone scripts."""

import json
import logging
import os
from time import sleep

import httpx
import psycopg
import redis
import requests
from asgiref.sync import sync_to_async
from django.contrib.contenttypes.models import ContentType
from django.db import connection

from django_altimeter import get_correlation, span

ROW_CANARY = "private-row-canary"
SQL_VALUE = "sql-value-canary"
SQL_QUERY = "SELECT current_query(), %s::text /* sql-comment-canary */"
SAFE_LOG = "reference.work.completed"
LOG_CANARIES = (
    "log-free-text-canary",
    "log-json-canary",
    "log-nested-canary",
    "log-error-canary",
    "log-exception-canary",
)


def orm_count():
    connection.ensure_connection()
    with span("reference.query"):
        count = ContentType.objects.filter(app_label=ROW_CANARY).count()
    return {"count": count, "canary": ROW_CANARY}


async def async_orm_count():
    await sync_to_async(lambda: connection.ensure_connection())()
    with span("reference.query"):
        count = await ContentType.objects.filter(app_label=ROW_CANARY).acount()
    return {"count": count, "canary": ROW_CANARY}


def direct_query():
    with psycopg.connect(
        os.environ["REFERENCE_POSTGRES_DSN"], connect_timeout=5
    ) as database:
        with span("reference.query"):
            statement, value = database.execute(SQL_QUERY, (SQL_VALUE,)).fetchone()
    return {"statement": statement, "value": value}


def serialized_rows():
    # Each boundary executes once. Fetch/construction and serialization are
    # separate from execute; driver time still includes client/transfer work.
    with connection.cursor() as cursor:
        with span("reference.query"):
            cursor.execute(
                "SELECT app_label, model FROM django_content_type, pg_sleep(0.02) "
                "WHERE app_label = %s ORDER BY model",
                (ROW_CANARY,),
            )
        with span("reference.materialize"):
            rows = [
                {"app_label": label, "model": model}
                for label, model in cursor.fetchall()
            ]
            sleep(0.03)
        with span("reference.serialize"):
            body = json.dumps({"rows": rows, "count": len(rows)}).encode()
            sleep(0.04)
    return body


def _outgoing_headers():
    return {
        "traceparent": "00-" + "1" * 32 + "-" + "2" * 16 + "-01",
        "tracestate": "private=state-canary",
        "baggage": "private=baggage-canary",
        "X-Altimeter-Trace-Id": "identity-header-canary",
        "X-Altimeter-Trace-Sampled": "1",
        "X-Application": "header-canary",
    }


def outgoing_calls(client="requests", *, redirect=False):
    internal = os.environ["REFERENCE_INTERNAL_ORIGIN"] + "/internal?secret=query-canary"
    external = os.environ["REFERENCE_EXTERNAL_ORIGIN"] + "/external?secret=query-canary"
    targets = (
        {"body": internal} if redirect else {"internal": internal, "external": external}
    )
    with span("reference.outgoing"):
        if client == "requests":
            with requests.Session() as session:
                session.trust_env = False
                result = {}
                for name, url in targets.items():
                    with session.get(
                        url, headers=_outgoing_headers(), timeout=3
                    ) as response:
                        response.raise_for_status()
                        result[name] = response.text
                return result
        if client == "httpx":
            with httpx.Client(
                trust_env=False, follow_redirects=True, timeout=3
            ) as session:
                result = {}
                for name, url in targets.items():
                    response = session.get(url, headers=_outgoing_headers())
                    response.raise_for_status()
                    result[name] = response.text
                return result
    raise ValueError("Unknown reference client")


async def async_outgoing_calls(*, redirect=False):
    internal = os.environ["REFERENCE_INTERNAL_ORIGIN"] + "/internal?secret=query-canary"
    external = os.environ["REFERENCE_EXTERNAL_ORIGIN"] + "/external?secret=query-canary"
    targets = (
        {"body": internal} if redirect else {"internal": internal, "external": external}
    )
    with span("reference.outgoing"):
        async with httpx.AsyncClient(
            trust_env=False, follow_redirects=True, timeout=3
        ) as session:
            result = {}
            for name, url in targets.items():
                response = await session.get(url, headers=_outgoing_headers())
                response.raise_for_status()
                result[name] = response.text
            return result


def redis_roundtrip():
    key = os.environ["REFERENCE_REDIS_KEY_PREFIX"] + "key-canary"
    with redis.Redis.from_url(
        os.environ["REFERENCE_REDIS_URL"],
        socket_timeout=3,
        socket_connect_timeout=3,
    ) as client:
        assert client.set(key, "redis-value-canary", ex=30)
        value = client.get(key)
        assert value is not None
        return value.decode()


def emit_logs():
    correlation = get_correlation()
    logger = logging.getLogger("reference.work")
    logger.info(SAFE_LOG)
    logger.warning("log-free-text-canary")
    logger.info({"event": "log-json-canary", "nested": {"secret": "log-nested-canary"}})
    try:
        raise ValueError("log-exception-canary")
    except ValueError:
        logger.error("log-error-canary", exc_info=True)
    return {
        "logged": True,
        "log_count": 4,
        "trace_id": correlation.trace_id if correlation else None,
        "span_id": correlation.span_id if correlation else None,
    }


def combined_work():
    return {
        "database": direct_query()["value"],
        "redis": redis_roundtrip(),
        "http": outgoing_calls(),
        "logs": emit_logs(),
    }
