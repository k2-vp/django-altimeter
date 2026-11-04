"""Real PostgreSQL operations and decoded OTLP; no live ingestion claim."""

import json
import os
import subprocess
import sys
import textwrap
from dataclasses import dataclass

import pytest
from support.otlp_receiver import OTLPReceiver

pytestmark = pytest.mark.integration
CANARY = "SQL_CANARY_private_value"

_IMPORTS = """
import asyncio, json, os
from dataclasses import replace
import psycopg
from psycopg_pool import ConnectionPool, AsyncConnectionPool
from django_altimeter import span
from django_altimeter.conf import Config, SignalConfig
from django_altimeter.privacy import DEFAULT_POLICY
from django_altimeter.runtime import setup, shutdown

DSN = os.environ["REFERENCE_DATABASE_URL"]
CANARY = "SQL_CANARY_private_value"
QUERY = "SELECT current_query(), 'SQL_CANARY_literal'::text, %s::text"
config = Config(
    enabled=True, service_name="driver-test", deployment_environment="test",
    release="v1", integrations=frozenset({"psycopg"}),
    traces=SignalConfig(True, os.environ["TEST_RECEIVER"] + "/v1/traces",
                        destination="collector"),
    policy=replace(DEFAULT_POLICY,
                   span_names=DEFAULT_POLICY.span_names | {"driver.parent"}),
)
"""


@dataclass(frozen=True)
class DriverRun:
    result: dict
    spans: tuple


def _run(receiver: OTLPReceiver, database_url: str, source: str) -> DriverRun:
    environment = {
        key: value
        for key, value in os.environ.items()
        if not key.startswith(("OTEL_", "ALTIMETER_"))
    }
    process = subprocess.run(
        [sys.executable, "-c", _IMPORTS + textwrap.dedent(source)],
        env={
            **environment,
            "REFERENCE_DATABASE_URL": database_url,
            "TEST_RECEIVER": receiver.endpoint,
        },
        text=True,
        capture_output=True,
        check=False,
        timeout=15,
    )
    assert process.returncode == 0, process.stderr
    assert CANARY not in process.stderr
    spans = tuple(
        span
        for request in receiver.requests
        for resource in request.message.resource_spans
        for scope in resource.scope_spans
        for span in scope.spans
    )
    assert b"SQL_CANARY" not in b"".join(
        request.message.SerializeToString() for request in receiver.requests
    )
    return DriverRun(json.loads(process.stdout), spans)


def _attributes(span):
    return {
        item.key: getattr(item.value, item.value.WhichOneof("value"))
        for item in span.attributes
    }


@pytest.fixture
def database_url():
    from support.reference_work import isolated_services

    with isolated_services() as services:
        yield services.postgres_dsn


class TestReadableQueries:
    def test_expression_from_and_null_keep_target_and_query_shape(
        self, otlp_receiver, database_url
    ):
        run = _run(
            otlp_receiver,
            database_url,
            """
            with psycopg.connect(DSN) as admin:
                admin.execute("CREATE TABLE cart_item (id integer, value text)")
                admin.execute("INSERT INTO cart_item VALUES (1, %s)", (CANARY,))
            query = (
                "SELECT EXTRACT(YEAR FROM DATE '2026-11-04'), "
                "substring(value FROM 1 FOR 3) FROM cart_item "
                "WHERE(value IS NOT NULL AND id=1+%s)"
            )
            assert setup(config).ok
            with span("driver.parent"):
                with psycopg.connect(DSN) as connection:
                    row = connection.execute(query, (0,)).fetchone()
            assert shutdown().completed
            print(json.dumps({"values_reached": row == (2026, "SQL")}))
            """,
        )
        assert run.result == {"values_reached": True}
        (child,) = [item for item in run.spans if item.kind == 3]
        assert child.name == "SELECT cart_item"
        assert _attributes(child)["db.query.text"] == (
            "SELECT EXTRACT(YEAR FROM DATE ?), substring(value FROM ? FOR ?) "
            "FROM cart_item WHERE (value IS NOT NULL AND id = ? + %s)"
        )

    @pytest.mark.parametrize(
        "path", ["sync", "async", "pool", "async_pool", "orm", "async_orm"]
    )
    def test_query_text_names_target_and_removes_values(
        self, path, otlp_receiver, database_url
    ):
        run = _run(
            otlp_receiver,
            database_url,
            f"""
            with psycopg.connect(DSN) as admin:
                admin.execute("CREATE TABLE cart_item (id integer, value text)")
                admin.execute("INSERT INTO cart_item VALUES (1, %s)", (CANARY,))
            query = (
                "SELECT current_query(), value, 'SQL_CANARY_literal', "
                "$$SQL_CANARY_dollar$$, E'SQL_CANARY_escape', 9284712635 "
                "FROM cart_item /* SQL_CANARY_block */ "
                "WHERE value = %s -- SQL_CANARY_line\\n"
            )
            if {path!r} in ("orm", "async_orm"):
                import django
                from django.conf import settings
                from psycopg.conninfo import conninfo_to_dict
                options = conninfo_to_dict(DSN)
                settings.configure(DATABASES={{"default": {{
                    "ENGINE": "django.db.backends.postgresql",
                    "NAME": options["dbname"], "USER": options["user"],
                    "HOST": options["host"], "PORT": options["port"],
                }}}}, INSTALLED_APPS=[])
                django.setup()
                from django.db import models, connections
                class CartItem(models.Model):
                    value = models.TextField()
                    class Meta:
                        app_label = "driver"
                        db_table = "cart_item"
            assert setup(config).ok
            async def asynchronous():
                if {path!r} == "async_orm":
                    item = await CartItem.objects.aget(value=CANARY)
                    return [(item.value,)]
                if {path!r} == "async":
                    async with await psycopg.AsyncConnection.connect(DSN) as connection:
                        cursor = await connection.execute(query, (CANARY,))
                        return await cursor.fetchone()
                async with AsyncConnectionPool(
                    DSN, min_size=1, max_size=1, open=False,
                ) as pool:
                    await pool.wait()
                    async with pool.connection() as connection:
                        cursor = await connection.execute(query, (CANARY,))
                        return await cursor.fetchone()
            with span("driver.parent"):
                if {path!r} in ("orm", "async_orm"):
                    if {path!r} == "async_orm":
                        row = asyncio.run(asynchronous())
                    else:
                        row = list(
                            CartItem.objects.filter(value=CANARY).values_list("value")
                        )
                    reached = row == [(CANARY,)]
                    connections.close_all()
                else:
                    if {path!r} in ("async", "async_pool"):
                        row = asyncio.run(asynchronous())
                    elif {path!r} == "pool":
                        with ConnectionPool(DSN, min_size=1, max_size=1) as pool:
                            pool.wait()
                            with pool.connection() as connection:
                                row = connection.execute(query, (CANARY,)).fetchone()
                    else:
                        with psycopg.connect(DSN) as connection:
                            row = connection.execute(query, (CANARY,)).fetchone()
                    reached = (
                        row[0] == query.replace("%s", "$1")
                        and row[1:] == (
                            CANARY, "SQL_CANARY_literal", "SQL_CANARY_dollar",
                            "SQL_CANARY_escape", 9284712635,
                        )
                    )
            assert shutdown().completed
            print(json.dumps({{"reached_postgres": reached}}))
            """,
        )
        assert run.result == {"reached_postgres": True}
        children = [item for item in run.spans if item.kind == 3]
        (child,) = [item for item in children if item.name == "SELECT cart_item"]
        text = _attributes(child)["db.query.text"]
        assert "cart_item" in text and "%s" in text
        assert _attributes(child)["db.operation.name"] == "SELECT"
        if path not in {"orm", "async_orm"}:
            assert text == (
                "SELECT current_query(), value, ?, ?, ?, ? "
                "FROM cart_item WHERE value = %s"
            )
        assert b"9284712635" not in b"".join(
            request.message.SerializeToString() for request in otlp_receiver.requests
        )

    @pytest.mark.parametrize("maximum", [80, 2048])
    def test_query_text_budget_survives_sdk_and_wire(
        self, maximum, otlp_receiver, database_url
    ):
        run = _run(
            otlp_receiver,
            database_url,
            f"""
            from django_altimeter.sql import sanitize_query
            with psycopg.connect(DSN) as admin:
                admin.execute("CREATE TABLE cart_item (id integer)")
                admin.execute("INSERT INTO cart_item VALUES (1)")
            query = "SELECT " + ", ".join(
                "'SQL_CANARY_long' AS column_" + str(index) for index in range(180)
            ) + " FROM cart_item"
            config = replace(config, limits=replace(
                config.limits, max_query_text_bytes={maximum},
            ))
            assert setup(config).ok
            with span("driver.parent"):
                with psycopg.connect(DSN) as connection:
                    row = connection.execute(query).fetchone()
            assert shutdown().completed
            expected = sanitize_query(query, max_text_bytes={maximum})
            print(json.dumps({{"values_reached": row == ("SQL_CANARY_long",) * 180,
                              "expected": expected.text}}))
            """,
        )
        assert run.result["values_reached"] is True
        (child,) = [item for item in run.spans if item.kind == 3]
        assert child.name == "SELECT cart_item"
        text = _attributes(child)["db.query.text"]
        assert text == run.result["expected"]
        assert maximum - 24 <= len(text.encode("utf-8")) <= maximum

    @pytest.mark.parametrize("shape", ["bytes", "nested_comment", "backslash", "zero"])
    def test_declined_description_preserves_operation_and_execution(
        self, shape, otlp_receiver, database_url
    ):
        run = _run(
            otlp_receiver,
            database_url,
            f"""
            query = "SELECT current_query(), 'SQL_CANARY_literal', %s::text"
            literal = "SQL_CANARY_literal"
            if {shape!r} == "bytes":
                query = query.encode()
            elif {shape!r} == "nested_comment":
                query += " /* SQL_CANARY_outer /* SQL_CANARY_inner */ */"
            elif {shape!r} == "backslash":
                literal = "SQL_CANARY_literal\\\\suffix"
                query = "SELECT current_query(), '" + literal + "', %s::text"
            else:
                config = replace(config, limits=replace(
                    config.limits, max_query_text_bytes=0,
                ))
            expected = query.decode() if isinstance(query, bytes) else query
            assert setup(config).ok
            with span("driver.parent"):
                with psycopg.connect(DSN) as connection:
                    row = connection.execute(query, (CANARY,)).fetchone()
            assert shutdown().completed
            print(json.dumps({{
                "query_unchanged": row[0] == expected.replace("%s", "$1"),
                "values_unchanged": row[1:] == (literal, CANARY),
            }}))
            """,
        )
        assert run.result == {"query_unchanged": True, "values_unchanged": True}
        (child,) = [item for item in run.spans if item.kind == 3]
        assert child.name == "SELECT"
        assert _attributes(child) == {
            "db.system.name": "postgresql",
            "db.operation.name": "SELECT",
        }


class TestStandaloneDrivers:
    @pytest.mark.parametrize("path", ["module", "class", "async", "pool", "async_pool"])
    def test_safe_child_preserves_sql_and_result(
        self, path, otlp_receiver, database_url
    ):
        run = _run(
            otlp_receiver,
            database_url,
            f"""
            result = setup(config)
            assert result.ok, result.codes
            async def asynchronous():
                if {path!r} == "async":
                    async with await psycopg.AsyncConnection.connect(DSN) as connection:
                        cursor = await connection.execute(QUERY, (CANARY,))
                        return await cursor.fetchone()
                pool = AsyncConnectionPool(DSN, min_size=1, max_size=1, open=False)
                async with pool:
                    await pool.wait()
                    async with pool.connection() as connection:
                        cursor = await connection.execute(QUERY, (CANARY,))
                        return await cursor.fetchone()
            with span("driver.parent"):
                if {path!r} in ("async", "async_pool"):
                    row = asyncio.run(asynchronous())
                elif {path!r} == "pool":
                    with ConnectionPool(DSN, min_size=1, max_size=1) as pool:
                        pool.wait()
                        with pool.connection() as connection:
                            row = connection.execute(QUERY, (CANARY,)).fetchone()
                else:
                    connect = (psycopg.connect if {path!r} == "module"
                               else psycopg.Connection.connect)
                    with connect(DSN) as connection:
                        row = connection.execute(QUERY, (CANARY,)).fetchone()
            stopped = shutdown()
            print(json.dumps({{"sql_unchanged": row[0] == QUERY.replace("%s", "$1"),
                              "literal_unchanged": row[1] == "SQL_CANARY_literal",
                              "parameter_unchanged": row[2] == CANARY,
                              "shutdown": stopped.completed}}))
            """,
        )
        assert run.result == {
            "sql_unchanged": True,
            "literal_unchanged": True,
            "parameter_unchanged": True,
            "shutdown": True,
        }
        assert len(run.spans) == 2
        parent = next(span for span in run.spans if span.name == "driver.parent")
        (child,) = [span for span in run.spans if span.kind == 3]
        assert child.name == "SELECT"
        assert child.trace_id == parent.trace_id
        assert child.parent_span_id == parent.span_id
        assert _attributes(child)["db.system.name"] == "postgresql"
        assert _attributes(child)["db.operation.name"] == "SELECT"
        assert _attributes(child)["db.query.text"] == (
            "SELECT current_query(), ?::text, %s::text"
        )
        assert "db.statement" not in _attributes(child)

    def test_pre_setup_connection_is_not_adopted(self, otlp_receiver, database_url):
        run = _run(
            otlp_receiver,
            database_url,
            """
            with psycopg.connect(DSN) as old:
                assert setup(config).ok
                with span("driver.parent"):
                    old_row = old.execute(QUERY, (CANARY,)).fetchone()
                    with psycopg.connect(DSN) as new:
                        new_row = new.execute(QUERY, (CANARY,)).fetchone()
                assert shutdown().completed
            print(json.dumps({"same_results": old_row == new_row}))
            """,
        )
        assert run.result == {"same_results": True}
        assert len(run.spans) == 2
        assert sum(span.kind == 3 for span in run.spans) == 1

    @pytest.mark.parametrize(
        "query_kind,operation",
        [
            ("bytes", "SELECT"),
            ("comment", "SELECT"),
            ("large", "DB"),
            ("composed", "DB"),
        ],
    )
    def test_bounded_names_never_format_or_change_queries(
        self, query_kind, operation, otlp_receiver, database_url
    ):
        run = _run(
            otlp_receiver,
            database_url,
            f"""
            adaptations = []
            class Query(psycopg.sql.Composable):
                def as_bytes(self, context=None):
                    adaptations.append("adapt")
                    return QUERY.encode()
            query = QUERY
            if {query_kind!r} == "bytes":
                query = QUERY.encode()
            elif {query_kind!r} == "comment":
                query = "/* SQL_CANARY_comment */ " + QUERY
            elif {query_kind!r} == "large":
                query = QUERY + " /*" + CANARY * 50000 + "*/"
            elif {query_kind!r} == "composed":
                query = Query(None)
            expected = (query.decode() if isinstance(query, bytes)
                        else query if isinstance(query, str) else QUERY)
            assert setup(config).ok
            with span("driver.parent"):
                with psycopg.connect(DSN) as connection:
                    row = connection.execute(query, (CANARY,)).fetchone()
            assert shutdown().completed
            print(json.dumps({{"unchanged": row[0] == expected.replace("%s", "$1"),
                              "parameter": row[2] == CANARY,
                              "adaptations": adaptations}}))
            """,
        )
        assert run.result == {
            "unchanged": True,
            "parameter": True,
            "adaptations": ["adapt"] if query_kind == "composed" else [],
        }
        assert len(run.spans) == 2
        (child,) = [item for item in run.spans if item.kind == 3]
        assert child.name == operation
        assert _attributes(child) == {
            "db.system.name": "postgresql",
            "db.operation.name": operation,
            **(
                {"db.query.text": "SELECT current_query(), ?::text, %s::text"}
                if query_kind == "comment"
                else {}
            ),
        }

    def test_repeated_concurrent_setup_and_install_export_once(
        self, otlp_receiver, database_url
    ):
        run = _run(
            otlp_receiver,
            database_url,
            """
            from concurrent.futures import ThreadPoolExecutor
            from django_altimeter.psycopg import install
            from django_altimeter.runtime import get_process_runtime
            with ThreadPoolExecutor(max_workers=8) as pool:
                results = list(pool.map(lambda _: setup(config), range(8)))
            assert all(result.ok and not result.codes for result in results)
            runtime = get_process_runtime()
            with ThreadPoolExecutor(max_workers=8) as pool:
                installed = list(pool.map(lambda _: install(runtime), range(8)))
            with span("driver.parent"):
                with psycopg.connect(DSN) as connection:
                    row = connection.execute(QUERY, (CANARY,)).fetchone()
            assert shutdown().completed
            print(json.dumps({"installed": all(item.installed for item in installed),
                              "parameter": row[2] == CANARY}))
            """,
        )
        assert run.result == {"installed": True, "parameter": True}
        assert len(run.spans) == 2
        assert sum(item.kind == 3 for item in run.spans) == 1

    @pytest.mark.parametrize("stage", ["start", "end"])
    def test_native_telemetry_failure_never_repeats_the_query(
        self, stage, otlp_receiver, database_url
    ):
        run = _run(
            otlp_receiver,
            database_url,
            f"""
            import logging
            from opentelemetry import trace
            from opentelemetry.sdk.trace import SpanProcessor
            from django_altimeter.runtime import get_owned_trace_runtime
            with psycopg.connect(DSN) as admin:
                admin.execute("CREATE SEQUENCE invocation_count")
            assert setup(config).ok
            class BrokenHandler(logging.Handler):
                def emit(self, record):
                    raise RuntimeError(CANARY)
            logging.getLogger("django_altimeter.context").addHandler(BrokenHandler())
            class Processor(SpanProcessor):
                broken = True
                failures = 0
                def on_start(self, current, parent_context=None):
                    if (self.broken and {stage!r} == "start"
                            and current.kind is trace.SpanKind.CLIENT):
                        self.failures += 1
                        raise RuntimeError(CANARY)
                def on_end(self, current):
                    if (self.broken and {stage!r} == "end"
                            and current.kind is trace.SpanKind.CLIENT):
                        self.failures += 1
                        raise RuntimeError(CANARY)
            processor = Processor()
            runtime = get_owned_trace_runtime()
            runtime.tracer_provider.add_span_processor(processor)
            values = []
            with span("driver.parent") as parent:
                with psycopg.connect(DSN) as connection:
                    query = "SELECT nextval('invocation_count')"
                    values.append(connection.execute(query).fetchone()[0])
                    assert trace.get_current_span() is parent
                    processor.broken = False
                    values.append(connection.execute(query).fetchone()[0])
                    assert trace.get_current_span() is parent
            assert trace.get_current_span() is trace.INVALID_SPAN
            assert shutdown().completed
            assert processor.failures == 1
            print(json.dumps({{"values": values}}))
            """,
        )
        assert run.result == {"values": [1, 2]}
        children = [item for item in run.spans if item.kind == 3]
        assert len(children) == (1 if stage == "start" else 2)
        assert all(item.name == "SELECT" for item in children)

    def test_native_error_identity_and_custom_cursor_behavior_survive(
        self, otlp_receiver, database_url
    ):
        run = _run(
            otlp_receiver,
            database_url,
            """
            from opentelemetry import trace
            from psycopg.rows import dict_row
            failures, calls = [], []
            class ApplicationCursor(psycopg.Cursor):
                def execute(self, *args, **kwargs):
                    calls.append("execute")
                    try:
                        return super().execute(*args, **kwargs)
                    except psycopg.Error as error:
                        failures.append(error)
                        raise
            assert setup(config).ok
            with span("driver.parent") as parent:
                with psycopg.connect(DSN, cursor_factory=ApplicationCursor,
                                     row_factory=dict_row) as connection:
                    try:
                        connection.execute("SELECT SQL_CANARY_missing_column")
                    except psycopg.errors.UndefinedColumn as caught:
                        same = caught is failures[0]
                    assert trace.get_current_span() is parent
                    connection.rollback()
                    row = connection.execute("SELECT 7 AS value").fetchone()
            assert trace.get_current_span() is trace.INVALID_SPAN
            assert shutdown().completed
            print(json.dumps({"same_error": same, "calls": calls, "row": row}))
            """,
        )
        assert run.result == {
            "same_error": True,
            "calls": ["execute", "execute"],
            "row": {"value": 7},
        }
        children = [item for item in run.spans if item.kind == 3]
        assert len(children) == 2
        assert sorted(item.status.code for item in children) == [0, 2]
        assert all(not item.events and not item.status.message for item in children)

    def test_pre_setup_pool_is_not_adopted(self, otlp_receiver, database_url):
        run = _run(
            otlp_receiver,
            database_url,
            """
            with ConnectionPool(DSN, min_size=1, max_size=1) as old_pool:
                old_pool.wait()
                assert setup(config).ok
                with span("driver.parent"):
                    with old_pool.connection() as old:
                        old_row = old.execute(QUERY, (CANARY,)).fetchone()
                    with psycopg.connect(DSN) as new:
                        new_row = new.execute(QUERY, (CANARY,)).fetchone()
                assert shutdown().completed
            print(json.dumps({"same_results": old_row == new_row}))
            """,
        )
        assert run.result == {"same_results": True}
        assert len(run.spans) == 2
        assert sum(item.kind == 3 for item in run.spans) == 1

    @pytest.mark.parametrize("asynchronous", [False, True])
    def test_django_orm_query_exports_once(
        self, asynchronous, work_wheel, work_services, otlp_receiver
    ):
        from support.reference_work import ROW_CANARY, ROW_COUNT, run_work_script

        result = run_work_script(
            work_wheel,
            _IMPORTS
            + f"""
import django
django.setup()
from django.db import connections
from altimeter_reference.workloads import orm_count, async_orm_count
config = replace(config, policy=replace(
    config.policy, span_names=config.policy.span_names | {{"reference.query"}},
))
assert setup(config).ok
with span("driver.parent"):
    result = (asyncio.run(async_orm_count()) if {asynchronous!r} else orm_count())
connections.close_all()
assert shutdown().completed
print("WORK_RESULT " + json.dumps(result))
""",
            environment={
                **work_services.environment(),
                "REFERENCE_DATABASE_URL": work_services.postgres_dsn,
                "TEST_RECEIVER": otlp_receiver.endpoint,
            },
        )
        assert result == {"count": ROW_COUNT, "canary": ROW_CANARY}
        spans = _spans(otlp_receiver)
        (parent,) = [item for item in spans if item.name == "driver.parent"]
        (query,) = [item for item in spans if item.name == "reference.query"]
        (driver,) = [item for item in spans if item.parent_span_id == query.span_id]
        assert query.parent_span_id == parent.span_id
        assert driver.kind == 3 and driver.name == "SELECT django_content_type"
        assert driver.trace_id == query.trace_id == parent.trace_id
        assert _attributes(driver) == {
            "db.system.name": "postgresql",
            "db.operation.name": "SELECT",
            "db.query.text": 'SELECT COUNT(*) AS "__count" FROM "django_content_type" '
            'WHERE "django_content_type"."app_label" = %s',
        }
        _assert_safe_wire(otlp_receiver)

    @pytest.mark.parametrize(
        "mode", ["disabled", "foreign", "not_requested", "psycopg2"]
    )
    def test_unowned_or_unrequested_driver_is_unchanged(
        self, mode, otlp_receiver, database_url
    ):
        run = _run(
            otlp_receiver,
            database_url,
            f"""
            from opentelemetry import trace
            from opentelemetry.sdk.trace import TracerProvider
            original = psycopg.connect
            foreign = None
            if {mode!r} == "disabled":
                config = replace(config, enabled=False)
            elif {mode!r} == "foreign":
                foreign = TracerProvider(shutdown_on_exit=False)
                trace.set_tracer_provider(foreign)
            elif {mode!r} == "not_requested":
                config = replace(config, integrations=frozenset())
            else:
                config = replace(config, integrations=frozenset({{"psycopg2"}}))
            result = setup(config)
            with span("driver.parent"):
                with psycopg.connect(DSN) as connection:
                    row = connection.execute(QUERY, (CANARY,)).fetchone()
            assert shutdown().completed
            if foreign is not None:
                foreign.shutdown()
            print(json.dumps({{"connect_unchanged": psycopg.connect == original,
                              "value": row[2] == CANARY, "codes": result.codes}}))
            """,
        )
        assert run.result == {
            "connect_unchanged": True,
            "value": True,
            "codes": (
                ["setup.traces_foreign"]
                if mode == "foreign"
                else ["setup.psycopg2_unsupported"]
                if mode == "psycopg2"
                else ["setup.disabled"]
                if mode == "disabled"
                else []
            ),
        }
        assert not any(item.kind == 3 for item in run.spans)
        assert len(run.spans) == int(mode in {"not_requested", "psycopg2"})

    @pytest.mark.parametrize("asynchronous", [False, True])
    def test_executemany_consumes_parameters_once_and_named_cursor_stays_native(
        self, asynchronous, otlp_receiver, database_url
    ):
        run = _run(
            otlp_receiver,
            database_url,
            f"""
            with psycopg.connect(DSN) as admin:
                admin.execute("CREATE TABLE invocation_values (value text)")
            consumed = []
            def parameters():
                for index in range(3):
                    consumed.append(index)
                    yield (CANARY + str(index),)
            assert setup(config).ok
            async def asynchronous():
                async with await psycopg.AsyncConnection.connect(DSN) as connection:
                    async with connection.cursor() as cursor:
                        await cursor.executemany(
                            "INSERT INTO invocation_values VALUES (%s)", parameters())
                    async with connection.cursor(name="SQL_CANARY_cursor") as cursor:
                        await cursor.execute(
                            "SELECT value FROM invocation_values ORDER BY value")
                        return await cursor.fetchall()
            with span("driver.parent"):
                if {asynchronous!r}:
                    rows = asyncio.run(asynchronous())
                else:
                    with psycopg.connect(DSN) as connection:
                        with connection.cursor() as cursor:
                            cursor.executemany(
                                "INSERT INTO invocation_values VALUES (%s)",
                                parameters())
                        with connection.cursor(name="SQL_CANARY_cursor") as cursor:
                            cursor.execute(
                                "SELECT value FROM invocation_values ORDER BY value")
                            rows = cursor.fetchall()
            assert shutdown().completed
            print(json.dumps({{"consumed": consumed,
                              "rows": rows == [(CANARY + str(i),) for i in range(3)]}}))
            """,
        )
        assert run.result == {"consumed": [0, 1, 2], "rows": True}
        (parent,) = [item for item in run.spans if item.name == "driver.parent"]
        children = [item for item in run.spans if item.kind == 3]
        assert sorted(item.name for item in children) == [
            "INSERT invocation_values",
            "SELECT invocation_values",
        ]
        assert all(item.parent_span_id == parent.span_id for item in children)
        assert all(item.trace_id == parent.trace_id for item in children)

    def test_async_cancellation_restores_parent_and_connection_recovers(
        self, otlp_receiver, database_url
    ):
        run = _run(
            otlp_receiver,
            database_url,
            """
            from opentelemetry import trace
            assert setup(config).ok
            async def run():
                async with await psycopg.AsyncConnection.connect(DSN) as connection:
                    started = asyncio.Event()
                    async def query():
                        try:
                            started.set()
                            await connection.execute("SELECT pg_sleep(30)")
                        finally:
                            assert trace.get_current_span() is parent
                    with span("driver.parent") as parent:
                        task = asyncio.create_task(query())
                        await started.wait()
                        await asyncio.sleep(0.05)
                        task.cancel()
                        try:
                            await asyncio.wait_for(task, 3)
                        except asyncio.CancelledError:
                            cancelled = True
                        assert trace.get_current_span() is parent
                        await connection.rollback()
                        cursor = await connection.execute("SELECT 7")
                        row = await cursor.fetchone()
                    assert trace.get_current_span() is trace.INVALID_SPAN
                    return {"cancelled": cancelled, "row": row}
            result = asyncio.run(run())
            assert shutdown().completed
            print(json.dumps(result))
            """,
        )
        assert run.result == {"cancelled": True, "row": [7]}
        (parent,) = [item for item in run.spans if item.name == "driver.parent"]
        children = [item for item in run.spans if item.kind == 3]
        assert len(children) == 2
        assert all(item.name == "SELECT" for item in children)
        assert all(item.parent_span_id == parent.span_id for item in children)
        assert [item.status.code for item in children] == [2, 0]
        assert all(not item.events and not item.status.message for item in children)

    def test_absent_optional_driver_reports_gap_and_keeps_native_span(
        self, tmp_path, otlp_receiver
    ):
        from support.reference_work import run_work_script
        from support.uvicorn_server import install_wheel

        wheel = install_wheel(tmp_path)
        result = run_work_script(
            wheel,
            """
import importlib.util
from django_altimeter import span
from django_altimeter.conf import Config, SignalConfig
from django_altimeter.runtime import setup, shutdown
assert importlib.util.find_spec("psycopg") is None
result = setup(Config(
    enabled=True, service_name="driver-test", deployment_environment="test",
    release="v1", integrations=frozenset({"psycopg"}),
    traces=SignalConfig(True, os.environ["TEST_ENDPOINT"], destination="collector"),
))
with span("SPAN"):
    answer = 6 * 7
assert shutdown().completed
print("WORK_RESULT " + json.dumps({"codes": result.codes, "answer": answer}))
""",
            environment={"TEST_ENDPOINT": otlp_receiver.url("traces")},
        )
        assert result == {"codes": ["setup.psycopg_missing"], "answer": 42}
        (span,) = _spans(otlp_receiver)
        assert span.kind == 1 and span.name == "SPAN"

    @pytest.mark.parametrize("asynchronous", [False, True])
    @pytest.mark.parametrize("inherited", [False, True])
    @pytest.mark.parametrize("method", ["execute", "executemany"])
    def test_reused_cursor_factory_executes_and_exports_once(
        self, asynchronous, inherited, method, otlp_receiver, database_url
    ):
        run = _run(
            otlp_receiver,
            database_url,
            f"""
            with psycopg.connect(DSN) as admin:
                admin.execute("CREATE SEQUENCE factory_calls")
            query = ("SELECT nextval('factory_calls'), "
                     "'SQL_CANARY_literal'::text, %s::text")
            def reuse(factory):
                if {inherited!r}:
                    class ApplicationCursor(factory):
                        application_marker = True
                    return ApplicationCursor
                return factory
            assert setup(config).ok
            async def execute_async(connection):
                async with connection.cursor() as cursor:
                    if {method!r} == "execute":
                        await cursor.execute(query, (CANARY,))
                    else:
                        await cursor.executemany(query, [(CANARY,)], returning=True)
                    return await cursor.fetchone()
            async def asynchronous():
                async with await psycopg.AsyncConnection.connect(DSN) as first:
                    rows = [await execute_async(first)]
                    factory = reuse(first.cursor_factory)
                    async with await psycopg.AsyncConnection.connect(
                        DSN, cursor_factory=factory,
                    ) as second:
                        rows.append(await execute_async(second))
                    return rows
            def execute_sync(connection):
                with connection.cursor() as cursor:
                    if {method!r} == "execute":
                        cursor.execute(query, (CANARY,))
                    else:
                        cursor.executemany(query, [(CANARY,)], returning=True)
                    return cursor.fetchone()
            with span("driver.parent"):
                if {asynchronous!r}:
                    rows = asyncio.run(asynchronous())
                else:
                    with psycopg.connect(DSN) as first:
                        rows = [execute_sync(first)]
                        factory = reuse(first.cursor_factory)
                        with psycopg.connect(DSN, cursor_factory=factory) as second:
                            rows.append(execute_sync(second))
            assert shutdown().completed
            print(json.dumps({{"rows": rows}}))
            """,
        )
        assert run.result == {
            "rows": [
                [1, "SQL_CANARY_literal", CANARY],
                [2, "SQL_CANARY_literal", CANARY],
            ],
        }
        (parent,) = [item for item in run.spans if item.name == "driver.parent"]
        children = [item for item in run.spans if item.kind == 3]
        assert len(run.spans) == 3
        assert len(children) == 2
        assert all(item.name == "SELECT" for item in children)
        assert all(item.parent_span_id == parent.span_id for item in children)
        assert all(item.trace_id == parent.trace_id for item in children)
        assert all(
            _attributes(item)
            == {
                "db.system.name": "postgresql",
                "db.operation.name": "SELECT",
                "db.query.text": "SELECT nextval(?), ?::text, %s::text",
            }
            for item in children
        )


@pytest.fixture(scope="module")
def work_wheel(tmp_path_factory):
    from support.reference_work import WORK_EXTRAS
    from support.uvicorn_server import install_wheel

    return install_wheel(tmp_path_factory.mktemp("driver-wheel"), extras=WORK_EXTRAS)


@pytest.fixture
def work_services(work_wheel):
    from support.reference_work import isolated_services, prepare_contenttypes

    with isolated_services() as services:
        prepare_contenttypes(work_wheel, services)
        yield services


def _spans(receiver):
    return [
        span
        for request in receiver.requests
        if request.path == "/v1/traces"
        for resource in request.message.resource_spans
        for scope in resource.scope_spans
        for span in scope.spans
    ]


def _assert_safe_wire(receiver):
    wire = b"".join(
        request.message.SerializeToString() for request in receiver.requests
    )
    assert not any(
        value in wire
        for value in (
            b"SQL_CANARY",
            b"private-row-canary",
            b"sql-value-canary",
            b"sql-comment-canary",
        )
    )


@pytest.fixture(scope="module")
def wsgi_driver_wheel(tmp_path_factory):
    from support.gunicorn_server import install_wheel
    from support.reference_work import WORK_EXTRAS

    return install_wheel(
        tmp_path_factory.mktemp("wsgi-driver-wheel"), extras=WORK_EXTRAS
    )


class TestWSGIDriverFlow:
    @pytest.mark.parametrize("mode", ["enabled", "disabled", "invalid"])
    def test_one_gthread_request_joins_database_clients_and_safe_log(
        self, mode, wsgi_driver_wheel, tmp_path
    ):
        from support.gunicorn_server import running_server
        from support.http_receiver import HTTPReceiver
        from support.reference_work import isolated_services

        with (
            isolated_services() as services,
            HTTPReceiver() as internal,
            HTTPReceiver() as external,
            OTLPReceiver() as receiver,
        ):
            internal.respond(body=b"internal-body-canary")
            external.respond(body=b"external-body-canary")
            environment = {
                **services.environment(),
                "DJANGO_SETTINGS_MODULE": "altimeter_reference.wsgi_settings",
                "REFERENCE_INTERNAL_ORIGIN": internal.origin,
                "REFERENCE_EXTERNAL_ORIGIN": external.origin,
                "ALTIMETER_INTEGRATIONS": "wsgi,django,psycopg,requests,redis",
                "ALTIMETER_LOGS_ENABLED": "true",
                "ALTIMETER_LOGS_COLLECTOR_ENDPOINT": receiver.url("logs"),
                "ALTIMETER_METRICS_ENABLED": "false",
            }
            if mode == "invalid":
                environment["ALTIMETER_EXPORT_TIMEOUT"] = "-1"
            with running_server(
                tmp_path,
                wheel=wsgi_driver_wheel,
                receiver=receiver,
                enabled=mode != "disabled",
                extra_env=environment,
                application=os.environ.get(
                    "H2_WSGI_APPLICATION",
                    "altimeter_reference.wsgi:application",
                ),
            ) as server:
                status, _, body = server.request("/work/combined")
                assert status == 200, body
                result = json.loads(body)
                assert result["database"] == "sql-value-canary"
                assert result["redis"] == "redis-value-canary"
                assert result["http"] == {
                    "internal": "internal-body-canary",
                    "external": "external-body-canary",
                }
                assert result["logs"]["logged"] and result["logs"]["log_count"] == 4
            if mode == "enabled":
                spans = _spans(receiver)
                assert len(spans) == 8
                (parent,) = [item for item in spans if item.kind == 2]
                (query,) = [item for item in spans if item.name == "reference.query"]
                (outgoing,) = [
                    item for item in spans if item.name == "reference.outgoing"
                ]
                assert query.parent_span_id == outgoing.parent_span_id == parent.span_id
                assert all(item.trace_id == parent.trace_id for item in spans)
                clients = [item for item in spans if item.kind == 3]
                assert len(clients) == 5
                assert sorted(item.name for item in clients) == [
                    "DB",
                    "DB",
                    "GET 127.0.0.1",
                    "GET 127.0.0.1",
                    "SELECT",
                ]
                assert sorted(
                    _attributes(item)["db.operation.name"]
                    for item in clients
                    if _attributes(item).get("db.system.name") == "redis"
                ) == ["GET", "SET"]
                for child in clients:
                    attributes = _attributes(child)
                    expected_parent = (
                        query.span_id
                        if attributes.get("db.system.name") == "postgresql"
                        else parent.span_id
                        if attributes.get("db.system.name") == "redis"
                        else outgoing.span_id
                    )
                    assert child.parent_span_id == expected_parent
                records = [
                    record
                    for request in receiver.requests
                    if request.path == "/v1/logs"
                    for resource in request.message.resource_logs
                    for scope in resource.scope_logs
                    for record in scope.log_records
                ]
                assert len(records) == 1
                assert records[0].body.string_value == "reference.work.completed"
                assert records[0].trace_id == parent.trace_id
                assert records[0].span_id == parent.span_id
                assert result["logs"]["trace_id"] == parent.trace_id.hex()
                assert result["logs"]["span_id"] == parent.span_id.hex()
            else:
                assert receiver.requests == ()
                assert result["logs"]["trace_id"] is None
                assert result["logs"]["span_id"] is None
            assert b"canary" not in b"".join(
                request.message.SerializeToString() for request in receiver.requests
            )

    @pytest.mark.parametrize("mode", ["enabled", "disabled", "invalid"])
    def test_real_gthread_query_keeps_results_and_one_parent_chain(
        self, mode, wsgi_driver_wheel, database_url, tmp_path
    ):
        from support.gunicorn_server import running_server

        with OTLPReceiver() as receiver:
            environment = {
                "REFERENCE_POSTGRES_DSN": database_url,
                "ALTIMETER_INTEGRATIONS": "wsgi,django,psycopg",
                "ALTIMETER_METRICS_ENABLED": "false",
            }
            if mode == "invalid":
                environment["ALTIMETER_EXPORT_TIMEOUT"] = "-1"
            with running_server(
                tmp_path,
                wheel=wsgi_driver_wheel,
                receiver=receiver,
                enabled=mode != "disabled",
                extra_env=environment,
            ) as server:
                status, _, body = server.request("/work/direct?private=query-canary")
                assert status == 200, body
                assert json.loads(body) == {
                    "statement": "SELECT current_query(), $1::text "
                    "/* sql-comment-canary */",
                    "value": "sql-value-canary",
                }
            spans = _spans(receiver)
            if mode == "enabled":
                assert len(spans) == 3
                (parent,) = [item for item in spans if item.kind == 2]
                (query,) = [item for item in spans if item.name == "reference.query"]
                (driver,) = [item for item in spans if item.kind == 3]
                assert _attributes(parent)["http.route"] == "work/direct"
                assert driver.name == "SELECT"
                assert driver.trace_id == query.trace_id == parent.trace_id
                assert query.parent_span_id == parent.span_id
                assert driver.parent_span_id == query.span_id
                assert _attributes(driver) == {
                    "db.system.name": "postgresql",
                    "db.operation.name": "SELECT",
                    "db.query.text": "SELECT current_query(), %s::text",
                }
                assert (
                    parent.start_time_unix_nano
                    <= query.start_time_unix_nano
                    <= driver.start_time_unix_nano
                )
                assert (
                    driver.end_time_unix_nano
                    <= query.end_time_unix_nano
                    <= parent.end_time_unix_nano
                )
            else:
                assert receiver.requests == ()
            _assert_safe_wire(receiver)


class TestASGIDriverFlow:
    @pytest.mark.parametrize("route", ["orm", "async-orm", "direct", "rows"])
    def test_real_request_separates_query_and_materialization_once(
        self, route, work_wheel, work_services, tmp_path
    ):
        from support.reference_work import ROW_CANARY, ROW_COUNT
        from support.uvicorn_server import running_server

        with OTLPReceiver() as receiver:
            with running_server(
                tmp_path,
                wheel=work_wheel,
                receiver=receiver,
                extra_env={
                    **work_services.environment(),
                    "ALTIMETER_INTEGRATIONS": "asgi,django,psycopg",
                    "ALTIMETER_LOGS_ENABLED": "false",
                    "ALTIMETER_METRICS_ENABLED": "false",
                },
            ) as server:
                status, _, body = server.request(f"/work/{route}")
                assert status == 200, body
                result = json.loads(body)
                if route in {"orm", "async-orm"}:
                    assert result == {"count": ROW_COUNT, "canary": ROW_CANARY}
                elif route == "direct":
                    assert result == {
                        "statement": "SELECT current_query(), $1::text "
                        "/* sql-comment-canary */",
                        "value": "sql-value-canary",
                    }
                else:
                    assert result == {
                        "count": ROW_COUNT,
                        "rows": [
                            {"app_label": ROW_CANARY, "model": f"item-{index:04d}"}
                            for index in range(ROW_COUNT)
                        ],
                    }
            spans = _spans(receiver)
            (server_span,) = [item for item in spans if item.kind == 2]
            assert _attributes(server_span)["http.route"] == f"work/{route}"
            (query,) = [item for item in spans if item.name == "reference.query"]
            (driver,) = [item for item in spans if item.parent_span_id == query.span_id]
            expected_name = (
                "SELECT" if route == "direct" else "SELECT django_content_type"
            )
            assert driver.kind == 3 and driver.name == expected_name
            assert driver.trace_id == query.trace_id == server_span.trace_id
            assert query.parent_span_id == server_span.span_id
            assert _attributes(driver) == {
                "db.system.name": "postgresql",
                "db.operation.name": "SELECT",
                "db.query.text": (
                    "SELECT current_query(), %s::text"
                    if route == "direct"
                    else "SELECT app_label, model FROM django_content_type, "
                    "pg_sleep(?) "
                    "WHERE app_label = %s ORDER BY model"
                    if route == "rows"
                    else 'SELECT COUNT(*) AS "__count" FROM "django_content_type" '
                    'WHERE "django_content_type"."app_label" = %s'
                ),
            }
            assert query.start_time_unix_nano <= driver.start_time_unix_nano
            assert driver.end_time_unix_nano <= query.end_time_unix_nano
            if route == "rows":
                previous_end = 0
                for name, minimum in (
                    ("reference.query", 0.015),
                    ("reference.materialize", 0.025),
                    ("reference.serialize", 0.035),
                ):
                    (work,) = [item for item in spans if item.name == name]
                    assert work.parent_span_id == server_span.span_id
                    assert work.trace_id == server_span.trace_id
                    assert work.start_time_unix_nano >= previous_end
                    assert (
                        work.end_time_unix_nano - work.start_time_unix_nano
                    ) / 1e9 >= minimum
                    previous_end = work.end_time_unix_nano
            _assert_safe_wire(receiver)
