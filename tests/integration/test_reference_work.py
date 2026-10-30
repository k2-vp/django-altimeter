"""Installed-wheel request workloads against real services and decoded OTLP."""

import json
import subprocess
import sys

import pytest
from support.http_receiver import HTTPReceiver
from support.otlp_receiver import OTLPReceiver
from support.reference_work import (
    ROW_CANARY,
    ROW_COUNT,
    WORK_EXTRAS,
    isolated_services,
    prepare_contenttypes,
    run_work_script,
)
from support.uvicorn_server import install_wheel, running_server

pytestmark = pytest.mark.integration


@pytest.fixture(scope="module")
def work_wheel(tmp_path_factory):
    return install_wheel(
        tmp_path_factory.mktemp("request-work-wheel"),
        extras=WORK_EXTRAS,
    )


@pytest.fixture
def services(work_wheel):
    with isolated_services() as isolated:
        prepare_contenttypes(work_wheel, isolated)
        yield isolated


def _spans(receiver):
    return [
        span
        for request in receiver.requests
        if request.path == "/v1/traces"
        for resource in request.message.resource_spans
        for scope in resource.scope_spans
        for span in scope.spans
    ]


def _logs(receiver):
    return [
        (scope.scope.name, record)
        for request in receiver.requests
        if request.path == "/v1/logs"
        for resource in request.message.resource_logs
        for scope in resource.scope_logs
        for record in scope.log_records
    ]


def _work_environment(services):
    return {
        **services.environment(),
        "ALTIMETER_INTEGRATIONS": "asgi,django,psycopg,requests,httpx,redis",
        "ALTIMETER_METRICS_ENABLED": "false",
        "ALTIMETER_EXPORT_TIMEOUT": "1",
    }


class TestASGIReferenceWork:
    @pytest.mark.parametrize("route", ["orm", "async-orm", "direct"])
    def test_database_results_keep_canaries_out_of_spans(
        self, work_wheel, services, tmp_path, route
    ):
        with OTLPReceiver() as receiver:
            with running_server(
                tmp_path,
                wheel=work_wheel,
                receiver=receiver,
                extra_env=_work_environment(services),
            ) as server:
                status, _, body = server.request(f"/work/{route}")
                assert status == 200, body
                result = json.loads(body)
                if route == "direct":
                    assert result["value"] == "sql-value-canary"
                    assert "sql-comment-canary" in result["statement"]
                    assert "traceparent" not in result["statement"]
                else:
                    assert result == {"count": ROW_COUNT, "canary": ROW_CANARY}
            spans = _spans(receiver)
            roots = [span for span in spans if span.kind == 2]
            assert len(roots) == 1
            queries = [span for span in spans if span.name == "reference.query"]
            assert len(queries) == 1
            children = [
                span for span in spans if span.parent_span_id == queries[0].span_id
            ]
            assert len(children) == 1
            assert children[0].kind == 3 and children[0].name == "SELECT"
            assert children[0].trace_id == roots[0].trace_id
            assert not any(
                value in str(receiver.requests)
                for value in (
                    ROW_CANARY,
                    "sql-value-canary",
                    "sql-comment-canary",
                )
            )

    def test_rows_preserve_results_and_separate_work_once(
        self, work_wheel, services, tmp_path
    ):
        with OTLPReceiver() as receiver:
            with running_server(
                tmp_path,
                wheel=work_wheel,
                receiver=receiver,
                extra_env=_work_environment(services),
            ) as server:
                status, _, body = server.request("/work/rows")
                assert status == 200, body
                result = json.loads(body)
                assert result["rows"] == [
                    {"app_label": ROW_CANARY, "model": f"item-{index:04d}"}
                    for index in range(ROW_COUNT)
                ]
                assert result["count"] == ROW_COUNT
            spans = _spans(receiver)
            roots = [span for span in spans if span.kind == 2]
            assert len(roots) == 1
            for name, minimum in (
                ("reference.query", 0.015),
                ("reference.materialize", 0.025),
                ("reference.serialize", 0.035),
            ):
                work = [span for span in spans if span.name == name]
                assert len(work) == 1, [span.name for span in spans]
                assert work[0].trace_id == roots[0].trace_id
                assert work[0].parent_span_id == roots[0].span_id
                assert (
                    work[0].end_time_unix_nano - work[0].start_time_unix_nano
                ) / 1e9 >= minimum
            query = next(span for span in spans if span.name == "reference.query")
            driver = [
                span
                for span in spans
                if span.kind == 3 and span.parent_span_id == query.span_id
            ]
            assert len(driver) == 1
            assert driver[0].name == "SELECT"
            assert driver[0].trace_id == roots[0].trace_id
            assert ROW_CANARY not in str(receiver.requests)

    @pytest.mark.parametrize("client", ["requests", "httpx", "async-httpx"])
    def test_outgoing_calls_keep_internal_context_and_external_payloads(
        self, work_wheel, services, tmp_path, client
    ):
        forged = "00-" + "1" * 32 + "-" + "2" * 16 + "-01"
        with (
            HTTPReceiver() as internal,
            HTTPReceiver() as external,
            OTLPReceiver() as receiver,
        ):
            internal.respond(body=b"internal-body-canary")
            external.respond(body=b"external-body-canary")
            with running_server(
                tmp_path,
                wheel=work_wheel,
                receiver=receiver,
                extra_env={
                    **_work_environment(services),
                    "REFERENCE_INTERNAL_ORIGIN": internal.origin,
                    "REFERENCE_EXTERNAL_ORIGIN": external.origin,
                },
            ) as server:
                status, _, body = server.request(
                    f"/work/outgoing?client={client}",
                    headers={"traceparent": forged, "X-Altimeter-Trace-Id": "1" * 32},
                )
                assert status == 200, body
                assert json.loads(body) == {
                    "internal": "internal-body-canary",
                    "external": "external-body-canary",
                }
            assert len(internal.requests) == len(external.requests) == 1
            sent = dict(
                (key.lower(), value) for key, value in internal.requests[0].headers
            )
            outside = dict(
                (key.lower(), value) for key, value in external.requests[0].headers
            )
            assert sent["x-application"] == outside["x-application"] == "header-canary"
            forbidden = {
                "baggage",
                "tracestate",
                "x-altimeter-trace-id",
                "x-altimeter-trace-sampled",
            }
            assert not forbidden.intersection(sent)
            assert not (forbidden | {"traceparent"}).intersection(outside)
            spans = _spans(receiver)
            roots = [span for span in spans if span.kind == 2]
            clients = [span for span in spans if span.kind == 3]
            assert len(roots) == 1 and len(clients) == 2
            assert roots[0].trace_id != bytes.fromhex("1" * 32)
            assert {span.name for span in clients} == {"GET"}
            assert all(span.trace_id == roots[0].trace_id for span in clients)
            assert sent["traceparent"].split("-")[1] == roots[0].trace_id.hex()
            assert bytes.fromhex(sent["traceparent"].split("-")[2]) in {
                span.span_id for span in clients
            }
            assert not any(
                value in str(receiver.requests)
                for value in (
                    "internal-body-canary",
                    "external-body-canary",
                    "header-canary",
                    "query-canary",
                )
            )

    @pytest.mark.parametrize("client", ["requests", "httpx", "async-httpx"])
    def test_redirect_strips_context_before_the_external_origin(
        self, work_wheel, services, tmp_path, client
    ):
        with (
            HTTPReceiver() as internal,
            HTTPReceiver() as external,
            OTLPReceiver() as receiver,
        ):
            internal.respond(
                status=302, headers={"Location": external.origin + "/redirect-canary"}
            )
            external.respond(body=b"redirect-body-canary")
            with running_server(
                tmp_path,
                wheel=work_wheel,
                receiver=receiver,
                extra_env={
                    **_work_environment(services),
                    "REFERENCE_INTERNAL_ORIGIN": internal.origin,
                    "REFERENCE_EXTERNAL_ORIGIN": external.origin,
                },
            ) as server:
                status, _, body = server.request(f"/work/redirect?client={client}")
                assert status == 200, body
                assert json.loads(body) == {"body": "redirect-body-canary"}
            assert len(internal.requests) == len(external.requests) == 1
            assert "traceparent" in dict(
                (key.lower(), value) for key, value in internal.requests[0].headers
            )
            external_headers = {key.lower() for key, _ in external.requests[0].headers}
            assert not {
                "traceparent",
                "tracestate",
                "baggage",
                "x-altimeter-trace-id",
            }.intersection(external_headers)
            clients = [span for span in _spans(receiver) if span.kind == 3]
            assert len(clients) == 2
            assert "redirect-canary" not in str(receiver.requests)
            assert "redirect-body-canary" not in str(receiver.requests)

    def test_safe_log_correlates_and_stdout_keeps_all_records(
        self, work_wheel, services, tmp_path
    ):
        outputs = []
        for enabled in (False, True):
            with OTLPReceiver() as receiver:
                with running_server(
                    tmp_path / str(enabled),
                    wheel=work_wheel,
                    receiver=receiver,
                    enabled=enabled,
                    extra_env={
                        **_work_environment(services),
                        "ALTIMETER_LOGS_ENABLED": "true",
                        "ALTIMETER_LOGS_COLLECTOR_ENDPOINT": receiver.url("logs"),
                    },
                ) as server:
                    status, _, body = server.request("/work/logs")
                    assert status == 200, body
                    result = json.loads(body)
                outputs.append(
                    [
                        line
                        for line in server.output.splitlines()
                        if "|reference.work|" in line
                    ]
                )
                if not enabled:
                    assert result == {
                        "logged": True,
                        "log_count": 4,
                        "trace_id": None,
                        "span_id": None,
                    }
                    assert receiver.requests == ()
                    continue
                logs = _logs(receiver)
                assert len(logs) == 1
                scope, record = logs[0]
                assert scope == "reference.work"
                assert record.body.string_value == "reference.work.completed"
                assert record.severity_number == 9
                root = next(span for span in _spans(receiver) if span.kind == 2)
                assert (
                    record.trace_id == root.trace_id and record.span_id == root.span_id
                )
                assert result == {
                    "logged": True,
                    "log_count": 4,
                    "trace_id": root.trace_id.hex(),
                    "span_id": root.span_id.hex(),
                }
                assert "log-exception-canary" in server.output
                assert not any(
                    value in str(receiver.requests)
                    for value in (
                        "log-free-text-canary",
                        "log-json-canary",
                        "log-nested-canary",
                        "log-error-canary",
                        "log-exception-canary",
                    )
                )
        assert (
            outputs[0]
            == outputs[1]
            == [
                "INFO|reference.work|reference.work.completed",
                "WARNING|reference.work|log-free-text-canary",
                "INFO|reference.work|{'event': 'log-json-canary', "
                "'nested': {'secret': 'log-nested-canary'}}",
                "ERROR|reference.work|log-error-canary",
            ]
        )

    @pytest.mark.parametrize("mode", ["enabled", "disabled", "invalid"])
    def test_combined_request_preserves_work_and_correlates_owned_signals(
        self, work_wheel, services, tmp_path, mode
    ):
        with (
            HTTPReceiver() as internal,
            HTTPReceiver() as external,
            OTLPReceiver() as receiver,
        ):
            internal.respond(body=b"internal-body-canary")
            external.respond(body=b"external-body-canary")
            environment = {
                **_work_environment(services),
                "REFERENCE_INTERNAL_ORIGIN": internal.origin,
                "REFERENCE_EXTERNAL_ORIGIN": external.origin,
                "ALTIMETER_LOGS_ENABLED": "true",
                "ALTIMETER_LOGS_COLLECTOR_ENDPOINT": receiver.url("logs"),
            }
            if mode == "invalid":
                environment["ALTIMETER_EXPORT_TIMEOUT"] = "-1"
            with running_server(
                tmp_path,
                wheel=work_wheel,
                receiver=receiver,
                enabled=mode != "disabled",
                extra_env=environment,
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
                assert result["logs"]["logged"] is True
            assert len(internal.requests) == len(external.requests) == 1
            if mode != "enabled":
                assert result["logs"]["trace_id"] is None
                assert receiver.requests == ()
                return
            spans = _spans(receiver)
            roots = [span for span in spans if span.kind == 2]
            assert len(roots) == 1
            clients = [span for span in spans if span.kind == 3]
            assert len(clients) == 5
            assert sum(span.name == "SELECT" for span in clients) == 1
            assert sum(span.name == "GET" for span in clients) == 2
            assert all(span.trace_id == roots[0].trace_id for span in clients)
            logs = _logs(receiver)
            assert len(logs) == 1
            assert logs[0][1].trace_id == roots[0].trace_id
            assert logs[0][1].span_id == roots[0].span_id
            assert "canary" not in str(receiver.requests)


class TestStandaloneReferenceWork:
    def test_real_work_and_safe_log_share_an_owned_trace(self, work_wheel, services):
        with (
            HTTPReceiver() as internal,
            HTTPReceiver() as external,
            OTLPReceiver() as receiver,
        ):
            internal.respond(body=b"internal-body-canary")
            external.respond(body=b"external-body-canary")
            result = run_work_script(
                work_wheel,
                """
import django
django.setup()
from altimeter_reference.workloads import combined_work, serialized_rows
from django_altimeter import setup, shutdown, span
from django_altimeter.logging import install_logging
setup_result = setup(role="web")
logging_result = install_logging(routes=("reference.work",))
with span("reference.work"):
    rows = json.loads(serialized_rows())
    result = combined_work()
drain = shutdown(timeout=2.0)
print("WORK_RESULT " + json.dumps({
    "rows": len(rows["rows"]), "first": rows["rows"][0], "last": rows["rows"][-1],
    "result": result, "setup_codes": setup_result.codes,
    "logging_ok": logging_result.ok, "completed": drain.completed,
}))
""",
                environment={
                    **_work_environment(services),
                    "REFERENCE_INTERNAL_ORIGIN": internal.origin,
                    "REFERENCE_EXTERNAL_ORIGIN": external.origin,
                    "ALTIMETER_ENABLED": "true",
                    "ALTIMETER_SERVICE_NAME": "reference-work",
                    "ALTIMETER_DEPLOYMENT_ENVIRONMENT": "test",
                    "ALTIMETER_RELEASE": "reference-tests",
                    "ALTIMETER_TRACES_COLLECTOR_ENDPOINT": receiver.url("traces"),
                    "ALTIMETER_LOGS_ENABLED": "true",
                    "ALTIMETER_LOGS_COLLECTOR_ENDPOINT": receiver.url("logs"),
                },
            )
            assert result["rows"] == ROW_COUNT
            assert result["first"] == {"app_label": ROW_CANARY, "model": "item-0000"}
            assert result["last"] == {
                "app_label": ROW_CANARY,
                "model": f"item-{ROW_COUNT - 1:04d}",
            }
            assert result["result"]["database"] == "sql-value-canary"
            assert result["result"]["redis"] == "redis-value-canary"
            assert result["result"]["http"] == {
                "internal": "internal-body-canary",
                "external": "external-body-canary",
            }
            assert result["setup_codes"] == []
            assert result["logging_ok"] and result["completed"]
            spans = _spans(receiver)
            roots = [span for span in spans if span.name == "reference.work"]
            assert len(roots) == 1
            assert roots[0].parent_span_id == b""
            assert all(span.trace_id == roots[0].trace_id for span in spans)
            clients = [span for span in spans if span.kind == 3]
            assert len(clients) >= 6
            assert sum(span.name == "SELECT" for span in clients) >= 2
            queries = [span for span in spans if span.name == "reference.query"]
            assert len(queries) == 2
            for query in queries:
                children = [
                    span for span in clients if span.parent_span_id == query.span_id
                ]
                assert len(children) == 1 and children[0].name == "SELECT"
            http = [span for span in clients if span.name == "GET"]
            assert len(http) == 2
            outgoing = [span for span in spans if span.name == "reference.outgoing"]
            assert len(outgoing) == 1
            assert {span.parent_span_id for span in http} == {outgoing[0].span_id}
            redis = [
                span
                for span in clients
                if any(
                    attribute.key == "db.system.name"
                    and attribute.value.string_value == "redis"
                    for attribute in span.attributes
                )
            ]
            assert len(redis) == 2
            assert {
                attribute.value.string_value
                for span in redis
                for attribute in span.attributes
                if attribute.key == "db.operation.name"
            } == {"SET", "GET"}
            logs = _logs(receiver)
            assert len(logs) == 1
            assert logs[0][0] == "reference.work"
            assert logs[0][1].body.string_value == "reference.work.completed"
            assert logs[0][1].trace_id == roots[0].trace_id
            assert logs[0][1].span_id == roots[0].span_id
            assert "canary" not in str(receiver.requests)


def test_request_work_extras_install_pinned_clients(tmp_path):
    wheel = install_wheel(tmp_path)
    artifact = next(tmp_path.glob("django_altimeter-*.whl"))
    subprocess.run(
        [
            str(wheel.python),
            "-m",
            "pip",
            "install",
            "--disable-pip-version-check",
            f"{artifact}[{','.join(WORK_EXTRAS)}]",
        ],
        check=True,
        timeout=180,
    )
    result = subprocess.run(
        [
            str(wheel.python),
            "-I",
            "-c",
            "import importlib.metadata, json; "
            "print(json.dumps({d.metadata['Name'].lower().replace('_', '-'): "
            "d.version for d in importlib.metadata.distributions()}))",
        ],
        check=True,
        capture_output=True,
        text=True,
        timeout=30,
    )
    installed = json.loads(result.stdout)
    expected = {
        "psycopg": "3.3.4",
        "psycopg-pool": "3.3.1",
        "httpx": "0.28.1",
        "redis": "7.4.0" if sys.version_info >= (3, 13) else "6.1.1",
        "structlog": "25.5.0",
        "opentelemetry-instrumentation-psycopg": "0.66b1",
        "opentelemetry-instrumentation-httpx": "0.66b1",
        "opentelemetry-instrumentation-requests": "0.66b1",
        "opentelemetry-instrumentation-redis": "0.66b1",
    }
    assert {name: installed.get(name) for name in expected} == expected
