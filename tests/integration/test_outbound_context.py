"""Real client/server and decoded OTLP proof; no live-service claim."""

import json
import os
import subprocess
import sys
import textwrap

import pytest
from support.http_receiver import HTTPReceiver

pytestmark = pytest.mark.integration
CANARY = "OUTBOUND_CANARY_secret"
_IMPORTS = """
import asyncio, json, os
from dataclasses import replace
from opentelemetry import trace
from django_altimeter.conf import Config, SignalConfig
from django_altimeter.privacy import DEFAULT_POLICY
from django_altimeter.runtime import setup, shutdown, get_process_runtime
from django_altimeter.context import span
config = Config(enabled=True, service_name='client-test', deployment_environment='test',
    release='v1', traces=SignalConfig(
        True, os.environ['OTLP_ENDPOINT'], destination='collector'),
    integrations=frozenset(os.environ['CLIENTS'].split(',')),
    internal_origins=frozenset([os.environ['INTERNAL_ORIGIN']]),
    policy=replace(DEFAULT_POLICY,
        span_names=DEFAULT_POLICY.span_names | {'outbound.parent'}),
    export_timeout=0.5)
assert setup(config).ok
"""


def _run(
    receiver,
    internal,
    source,
    *,
    clients="requests",
    bootstrap=_IMPORTS,
    **environment,
):
    env = {
        key: value
        for key, value in os.environ.items()
        if not key.startswith(("ALTIMETER_", "OTEL_"))
    }
    result = subprocess.run(
        [sys.executable, "-c", bootstrap + textwrap.dedent(source)],
        env={
            **env,
            "OTLP_ENDPOINT": receiver.url("traces"),
            "INTERNAL_ORIGIN": internal.origin,
            "CLIENTS": clients,
            **environment,
        },
        capture_output=True,
        text=True,
        timeout=15,
        check=False,
    )
    assert result.returncode == 0, result.stderr
    return json.loads(result.stdout)


def _spans(receiver):
    return [
        span
        for request in receiver.requests
        if request.path == "/v1/traces"
        for resource in request.message.resource_spans
        for scope in resource.scope_spans
        for span in scope.spans
    ]


def _attributes(span):
    return {
        entry.key: getattr(entry.value, entry.value.WhichOneof("value"))
        for entry in span.attributes
    }


class TestStandalonePropagation:
    @pytest.mark.parametrize("client", ["requests", "httpx", "httpx-async"])
    def test_http_safe_child_and_exact_origin(self, otlp_receiver, client):
        with HTTPReceiver() as internal, HTTPReceiver() as external:
            result = _run(
                otlp_receiver,
                internal,
                """
                import requests
                import httpx
                def fetch(url, headers):
                    if os.environ['CLIENT_API'] == 'requests':
                        return requests.get(url, headers=headers, timeout=2)
                    if os.environ['CLIENT_API'] == 'httpx':
                        with httpx.Client() as client:
                            return client.get(url, headers=headers, timeout=2)
                    async def async_fetch():
                        async with httpx.AsyncClient() as client:
                            return await client.get(url, headers=headers, timeout=2)
                    return asyncio.run(async_fetch())
                forged = {'traceparent': '00-' + '1'*32 + '-' + '2'*16 + '-01',
                          'tracestate': 'vendor=OUTBOUND_CANARY_secret',
                          'baggage': 'tenant=OUTBOUND_CANARY_secret',
                          'x-altimeter-trace-id': 'OUTBOUND_CANARY_secret',
                          'posthogDistinctId': 'OUTBOUND_CANARY_secret',
                          'Authorization': 'Bearer OUTBOUND_CANARY_secret'}
                with span('outbound.parent'):
                    first = fetch(os.environ['INTERNAL_ORIGIN']
                        + '/safe?q=OUTBOUND_CANARY_secret', forged)
                    second = fetch(os.environ['EXTERNAL_ORIGIN'] + '/safe', forged)
                assert shutdown().completed
                print(json.dumps({'bodies': [first.text, second.text]}))
            """,
                clients="httpx" if client.startswith("httpx") else "requests",
                CLIENT_API=client,
                EXTERNAL_ORIGIN=external.origin,
            )
            assert result["bodies"] == ["safe-response", "safe-response"]
            assert len(internal.requests) == len(external.requests) == 1
            inside = dict(
                (key.lower(), value) for key, value in internal.requests[0].headers
            )
            outside = dict(
                (key.lower(), value) for key, value in external.requests[0].headers
            )
            assert (
                inside["authorization"]
                == outside["authorization"]
                == f"Bearer {CANARY}"
            )
            assert CANARY in internal.requests[0].path
            spans = _spans(otlp_receiver)
            parents = [item for item in spans if item.name == "outbound.parent"]
            children = [item for item in spans if item.kind == 3]
            assert len(parents) == 1 and len(children) == 2
            assert all(item.parent_span_id == parents[0].span_id for item in children)
            assert all(item.trace_id == parents[0].trace_id for item in children)
            assert inside["traceparent"] in {
                f"00-{item.trace_id.hex()}-{item.span_id.hex()}-01" for item in children
            }
            assert "traceparent" not in outside
            for headers in (inside, outside):
                assert (
                    not {
                        "baggage",
                        "tracestate",
                        "posthogdistinctid",
                        "x-altimeter-trace-id",
                    }
                    & headers.keys()
                )
            assert all(
                item.name == "GET"
                and _attributes(item)["http.response.status_code"] == 200
                for item in children
            )
            for request in otlp_receiver.requests:
                assert CANARY.encode() not in request.message.SerializeToString()
                assert not {"traceparent", "baggage", "tracestate"} & {
                    key.lower() for key in request.headers
                }

    @pytest.mark.parametrize("client", ["requests", "httpx", "httpx-async"])
    def test_redirect_rechecks_each_destination(self, otlp_receiver, client):
        with HTTPReceiver() as internal, HTTPReceiver() as external:
            internal.respond(
                status=302, headers={"Location": internal.origin + "/second"}
            )
            internal.respond(
                status=302, headers={"Location": external.origin + "/third"}
            )
            external.respond(
                status=302, headers={"Location": internal.origin + "/fourth"}
            )
            result = _run(
                otlp_receiver,
                internal,
                """
                import requests, httpx
                with span('outbound.parent'):
                    url = os.environ['INTERNAL_ORIGIN'] + '/first'
                    if os.environ['CLIENT_API'] == 'requests':
                        response = requests.get(url, timeout=2)
                    elif os.environ['CLIENT_API'] == 'httpx':
                        with httpx.Client(follow_redirects=True) as client:
                            response = client.get(url, timeout=2)
                    else:
                        async def fetch():
                            async with httpx.AsyncClient(
                                    follow_redirects=True) as client:
                                return await client.get(url, timeout=2)
                        response = asyncio.run(fetch())
                assert shutdown().completed
                print(json.dumps({'body': response.text,
                                  'redirects':len(response.history)}))
            """,
                clients="httpx" if client.startswith("httpx") else "requests",
                CLIENT_API=client,
            )
            assert result == {"body": "safe-response", "redirects": 3}
            assert len(internal.requests) == 3 and len(external.requests) == 1
            assert all(
                "traceparent" in dict(request.headers) for request in internal.requests
            )
            assert "traceparent" not in dict(external.requests[0].headers)
            children = [item for item in _spans(otlp_receiver) if item.kind == 3]
            assert len(children) == 4
            assert sorted(
                _attributes(item)["http.response.status_code"] for item in children
            ) == [200, 302, 302, 302]
            parent = next(
                item for item in _spans(otlp_receiver) if item.name == "outbound.parent"
            )
            assert all(item.parent_span_id == parent.span_id for item in children)
            assert len({item.span_id for item in children}) == 4

    def test_repeated_install_and_export_session_do_not_duplicate_or_inject(
        self, otlp_receiver
    ):
        with HTTPReceiver() as internal:
            result = _run(
                otlp_receiver,
                internal,
                """
                import requests
                from django_altimeter.clients import install_clients
                from django_altimeter.exporting import SanitizingSession
                runtime = get_process_runtime()
                assert install_clients(runtime) == install_clients(runtime) == ()
                with span('outbound.parent'):
                    response = requests.get(os.environ['INTERNAL_ORIGIN'], timeout=2)
                    assert runtime.tracer_provider.force_flush(1000)
                assert shutdown().completed
                print(json.dumps({'body':response.text}))
            """,
            )
            assert result == {"body": "safe-response"}
            assert len(internal.requests) == 1
            assert len([item for item in _spans(otlp_receiver) if item.kind == 3]) == 1
            assert len(_spans(otlp_receiver)) == 2
            assert all(
                not {"traceparent", "tracestate", "baggage"}
                & {key.lower() for key in request.headers}
                for request in otlp_receiver.requests
            )

    def test_real_processor_fault_does_not_change_http_call(self, otlp_receiver):
        with HTTPReceiver() as internal:
            result = _run(
                otlp_receiver,
                internal,
                """
                import requests
                from opentelemetry.sdk.trace import SpanProcessor
                class BrokenProcessor(SpanProcessor):
                    def on_start(self, span, parent_context=None):
                        if span.kind == trace.SpanKind.CLIENT:
                            raise RuntimeError('OUTBOUND_CANARY_secret')
                get_process_runtime().tracer_provider.add_span_processor(BrokenProcessor())
                with span('outbound.parent'):
                    before = trace.get_current_span().get_span_context()
                    response = requests.get(os.environ['INTERNAL_ORIGIN'], timeout=2)
                    assert trace.get_current_span().get_span_context() == before
                assert shutdown().completed
                print(json.dumps({'body':response.text}))
            """,
            )
            assert result == {"body": "safe-response"}
            assert len(internal.requests) == 1
            assert [item.name for item in _spans(otlp_receiver)] == ["outbound.parent"]
            assert all(
                CANARY.encode() not in request.message.SerializeToString()
                for request in otlp_receiver.requests
            )

    def test_async_cancellation_preserves_parent_and_cleans_client_context(
        self, otlp_receiver
    ):
        with HTTPReceiver() as internal:
            internal.respond(delay=2)
            result = _run(
                otlp_receiver,
                internal,
                """
                import httpx
                async def exercise():
                    async with httpx.AsyncClient() as client:
                        with span('outbound.parent'):
                            before = trace.get_current_span().get_span_context()
                            pending = asyncio.create_task(
                                client.get(os.environ['INTERNAL_ORIGIN'])
                            )
                            await asyncio.sleep(0.2)
                            pending.cancel()
                            try:
                                await pending
                            except asyncio.CancelledError:
                                cancelled = True
                            else:
                                cancelled = False
                            assert trace.get_current_span().get_span_context() == before
                            good = await client.get(os.environ['INTERNAL_ORIGIN'])
                    return cancelled, good.text
                cancelled, body = asyncio.run(exercise())
                assert shutdown().completed
                print(json.dumps({'cancelled':cancelled, 'body':body}))
            """,
                clients="httpx",
            )
            assert result == {"cancelled": True, "body": "safe-response"}
            assert len(internal.requests) == 2
            children = [item for item in _spans(otlp_receiver) if item.kind == 3]
            assert len(children) == 2
            parent = next(
                item for item in _spans(otlp_receiver) if item.name == "outbound.parent"
            )
            assert all(item.parent_span_id == parent.span_id for item in children)

    def test_direct_adapter_keeps_positional_transport_arguments(self, otlp_receiver):
        with HTTPReceiver() as internal:
            result = _run(
                otlp_receiver,
                internal,
                """
                import requests
                request = requests.Request(
                    'GET', os.environ['INTERNAL_ORIGIN']).prepare()
                from contextlib import closing
                with closing(requests.adapters.HTTPAdapter()) as adapter:
                    with span('outbound.parent'):
                        response = adapter.send(request, False, 2, True, None, None)
                        body = response.text
                        response.close()
                assert shutdown().completed
                print(json.dumps({'body': body}))
                """,
            )
            assert result == {"body": "safe-response"}
            assert len(internal.requests) == 1
            assert len([item for item in _spans(otlp_receiver) if item.kind == 3]) == 1

    @pytest.mark.parametrize("client", ["requests", "httpx", "httpx-async"])
    def test_concurrent_calls_keep_distinct_owned_parents(self, otlp_receiver, client):
        with HTTPReceiver() as internal:
            result = _run(
                otlp_receiver,
                internal,
                """
                import requests, httpx
                from concurrent.futures import ThreadPoolExecutor
                from django_altimeter.clients import install_clients
                assert install_clients(get_process_runtime()) == ()
                assert install_clients(get_process_runtime()) == ()
                def sync_work(_):
                    with span('outbound.parent'):
                        parent = trace.get_current_span().get_span_context()
                        if os.environ['CLIENT_API'] == 'requests':
                            response = requests.get(os.environ['INTERNAL_ORIGIN'])
                        else:
                            with httpx.Client() as client:
                                response = client.get(os.environ['INTERNAL_ORIGIN'])
                        assert trace.get_current_span().get_span_context() == parent
                        return response.text
                async def async_work(client):
                    with span('outbound.parent'):
                        parent = trace.get_current_span().get_span_context()
                        response = await client.get(os.environ['INTERNAL_ORIGIN'])
                        assert trace.get_current_span().get_span_context() == parent
                        return response.text
                async def concurrent_async():
                    async with httpx.AsyncClient() as client:
                        return await asyncio.gather(
                            async_work(client), async_work(client))
                if os.environ['CLIENT_API'] == 'httpx-async':
                    values = asyncio.run(concurrent_async())
                else:
                    with ThreadPoolExecutor(max_workers=2) as pool:
                        values = list(pool.map(sync_work, range(2)))
                assert not trace.get_current_span().get_span_context().is_valid
                assert shutdown().completed
                print(json.dumps({'values': values}))
                """,
                clients="requests" if client == "requests" else "httpx",
                CLIENT_API=client,
            )
            assert result == {"values": ["safe-response", "safe-response"]}
            spans = _spans(otlp_receiver)
            parents = [item for item in spans if item.name == "outbound.parent"]
            children = [item for item in spans if item.kind == 3]
            assert len(parents) == len(children) == len(internal.requests) == 2
            assert len({item.trace_id for item in parents}) == 2
            assert {item.parent_span_id for item in children} == {
                item.span_id for item in parents
            }
            transmitted = {
                dict((key.lower(), value) for key, value in item.headers)["traceparent"]
                for item in internal.requests
            }
            assert transmitted == {
                f"00-{item.trace_id.hex()}-{item.span_id.hex()}-01" for item in children
            }

    @pytest.mark.parametrize("client", ["requests", "httpx", "httpx-async"])
    def test_timeout_keeps_native_exception_and_parent(self, otlp_receiver, client):
        with HTTPReceiver() as internal:
            internal.respond(delay=1)
            result = _run(
                otlp_receiver,
                internal,
                """
                import requests, httpx
                def sync_work():
                    with span('outbound.parent'):
                        parent = trace.get_current_span().get_span_context()
                        if os.environ['CLIENT_API'] == 'requests':
                            fetch, expected = requests.get, requests.ReadTimeout
                        else:
                            fetch, expected = httpx.get, httpx.ReadTimeout
                        try:
                            fetch(os.environ['INTERNAL_ORIGIN'], timeout=0.05)
                        except expected:
                            failed = True
                        else:
                            failed = False
                        assert trace.get_current_span().get_span_context() == parent
                        good = fetch(os.environ['INTERNAL_ORIGIN'], timeout=2)
                        return failed, good.text
                async def async_work():
                    async with httpx.AsyncClient() as client:
                        with span('outbound.parent'):
                            parent = trace.get_current_span().get_span_context()
                            try:
                                await client.get(os.environ['INTERNAL_ORIGIN'],
                                                 timeout=0.05)
                            except httpx.ReadTimeout:
                                failed = True
                            else:
                                failed = False
                            current = trace.get_current_span().get_span_context()
                            assert current == parent
                            good = await client.get(os.environ['INTERNAL_ORIGIN'])
                            return failed, good.text
                values = (asyncio.run(async_work())
                          if os.environ['CLIENT_API'] == 'httpx-async'
                          else sync_work())
                assert shutdown().completed
                print(json.dumps({'values':values}))
                """,
                clients="requests" if client == "requests" else "httpx",
                CLIENT_API=client,
            )
            assert result == {"values": [True, "safe-response"]}
            children = [item for item in _spans(otlp_receiver) if item.kind == 3]
            assert len(children) == len(internal.requests) == 2
            assert sorted(item.status.code for item in children) == [0, 2]
            assert all(not item.events and not item.status.message for item in children)

    @pytest.mark.parametrize("mode", ["disabled", "foreign"])
    def test_unowned_clients_preserve_native_behavior(self, otlp_receiver, mode):
        bootstrap = _IMPORTS.replace(
            "assert setup(config).ok",
            "assert not setup(config).ok",
        )
        if mode == "disabled":
            bootstrap = bootstrap.replace("Config(enabled=True", "Config(enabled=False")
        else:
            bootstrap = (
                "from opentelemetry import trace\n"
                "from opentelemetry.sdk.trace import TracerProvider\n"
                "provider = TracerProvider(shutdown_on_exit=False)\n"
                "trace.set_tracer_provider(provider)\n" + bootstrap
            )
        bootstrap = (
            "import requests, httpx\n"
            "native_requests = requests.adapters.HTTPAdapter.send\n"
            "native_httpx = httpx.HTTPTransport.handle_request\n" + bootstrap
        )
        with HTTPReceiver() as internal:
            result = _run(
                otlp_receiver,
                internal,
                """
                assert requests.adapters.HTTPAdapter.send is native_requests
                assert httpx.HTTPTransport.handle_request is native_httpx
                headers = {'baggage': 'application-owned'}
                responses = [requests.get(os.environ['INTERNAL_ORIGIN'],
                                          headers=headers).text]
                with httpx.Client() as client:
                    responses.append(client.get(os.environ['INTERNAL_ORIGIN'],
                                                headers=headers).text)
                async def fetch():
                    async with httpx.AsyncClient() as client:
                        return (await client.get(os.environ['INTERNAL_ORIGIN'],
                                                 headers=headers)).text
                responses.append(asyncio.run(fetch()))
                assert shutdown().completed
                print(json.dumps({'responses':responses}))
                """,
                clients="requests,httpx,redis",
                bootstrap=bootstrap,
            )
            assert result == {"responses": ["safe-response"] * 3}
            assert len(internal.requests) == 3
            assert all(
                dict((key.lower(), value) for key, value in item.headers)["baggage"]
                == "application-owned"
                for item in internal.requests
            )
            assert not otlp_receiver.requests

    def test_missing_optional_clients_report_gaps_from_base_wheel(
        self, tmp_path, otlp_receiver
    ):
        from support.reference_work import run_work_script
        from support.uvicorn_server import install_wheel

        wheel = install_wheel(tmp_path / "base-wheel")
        result = run_work_script(
            wheel,
            """
import importlib.util
assert importlib.util.find_spec('httpx') is None
assert importlib.util.find_spec('redis') is None
from django_altimeter.conf import Config, SignalConfig
from django_altimeter.runtime import setup, shutdown
from django_altimeter.context import span
result = setup(Config(enabled=True, service_name='client-base',
    deployment_environment='test', release='v1',
    traces=SignalConfig(True, os.environ['OTLP_ENDPOINT'], destination='collector'),
    integrations=frozenset({'httpx','redis'})))
assert result.ok
with span('GET'):
    pass
assert shutdown().completed
print('WORK_RESULT ' + json.dumps({'codes': sorted(result.codes)}))
""",
            environment={"OTLP_ENDPOINT": otlp_receiver.url("traces")},
        )
        assert result == {"codes": ["setup.httpx_missing", "setup.redis_missing"]}
        assert [item.name for item in _spans(otlp_receiver)] == ["GET"]

    @pytest.mark.parametrize("asynchronous", [False, True])
    def test_redis_commands_and_pipeline_keep_canaries_out_of_spans(
        self, otlp_receiver, asynchronous
    ):
        from support.reference_work import isolated_services

        with isolated_services() as services, HTTPReceiver() as internal:
            result = _run(
                otlp_receiver,
                internal,
                """
                import redis
                import redis.asyncio
                key = os.environ['KEY_PREFIX'] + 'OUTBOUND_CANARY_secret'
                async def asynchronous():
                    client = redis.asyncio.Redis.from_url(os.environ['REDIS_URL'])
                    try:
                        with span('outbound.parent'):
                            assert await client.ping()
                            assert await client.set(key, 'OUTBOUND_CANARY_secret')
                            async with client.pipeline(transaction=True) as pipeline:
                                pipeline.get(key).delete(key)
                                values = await pipeline.execute()
                            return values[0].decode(), values[1]
                    finally:
                        await client.aclose()
                def synchronous():
                    with redis.Redis.from_url(os.environ['REDIS_URL']) as client:
                        with span('outbound.parent'):
                            assert client.ping()
                            assert client.set(key, 'OUTBOUND_CANARY_secret')
                            with client.pipeline(transaction=True) as pipeline:
                                pipeline.get(key).delete(key)
                                values = pipeline.execute()
                            return values[0].decode(), values[1]
                values = (asyncio.run(asynchronous())
                          if os.environ['ASYNC'] == '1' else synchronous())
                assert shutdown().completed
                print(json.dumps({'values':values}))
            """,
                clients="redis",
                REDIS_URL=services.redis_url,
                KEY_PREFIX=services.redis_key_prefix,
                ASYNC="1" if asynchronous else "0",
            )
            assert result == {"values": [CANARY, 1]}
            spans = _spans(otlp_receiver)
            parent = next(item for item in spans if item.name == "outbound.parent")
            children = [item for item in spans if item.kind == 3]
            assert len(children) == 3
            assert all(
                item.name == "DB" and item.parent_span_id == parent.span_id
                for item in children
            )
            assert sorted(
                _attributes(item)["db.operation.name"] for item in children
            ) == ["PING", "PIPELINE", "SET"]
            assert all(
                _attributes(item)["db.system.name"] == "redis" for item in children
            )
            assert all(
                CANARY.encode() not in request.message.SerializeToString()
                for request in otlp_receiver.requests
            )

    @pytest.mark.parametrize("asynchronous", [False, True])
    def test_redis_native_errors_leave_parent_and_next_command_working(
        self, otlp_receiver, asynchronous
    ):
        from support.reference_work import isolated_services

        with isolated_services() as services, HTTPReceiver() as internal:
            result = _run(
                otlp_receiver,
                internal,
                """
                import redis, redis.asyncio
                command = 'UNKNOWN_OUTBOUND_CANARY_secret'
                def synchronous():
                    with redis.Redis.from_url(os.environ['REDIS_URL']) as client:
                        with span('outbound.parent'):
                            parent = trace.get_current_span().get_span_context()
                            for pipeline in (False, True):
                                try:
                                    if pipeline:
                                        with client.pipeline() as pending:
                                            pending.execute_command(command)
                                            pending.execute()
                                    else:
                                        client.execute_command(command)
                                except redis.ResponseError as error:
                                    assert command in str(error)
                                else:
                                    raise AssertionError('Native failure was lost')
                                current = trace.get_current_span().get_span_context()
                                assert current == parent
                            return client.ping()
                async def asynchronous():
                    client = redis.asyncio.Redis.from_url(os.environ['REDIS_URL'])
                    try:
                        with span('outbound.parent'):
                            parent = trace.get_current_span().get_span_context()
                            for pipeline in (False, True):
                                try:
                                    if pipeline:
                                        async with client.pipeline() as pending:
                                            pending.execute_command(command)
                                            await pending.execute()
                                    else:
                                        await client.execute_command(command)
                                except redis.ResponseError as error:
                                    assert command in str(error)
                                else:
                                    raise AssertionError('Native failure was lost')
                                current = trace.get_current_span().get_span_context()
                                assert current == parent
                            return await client.ping()
                    finally:
                        await client.aclose()
                value = (asyncio.run(asynchronous())
                         if os.environ['ASYNC'] == '1' else synchronous())
                assert shutdown().completed
                print(json.dumps({'ping':value}))
                """,
                clients="redis",
                REDIS_URL=services.redis_url,
                ASYNC="1" if asynchronous else "0",
            )
            assert result == {"ping": True}
            spans = _spans(otlp_receiver)
            parent = next(item for item in spans if item.name == "outbound.parent")
            children = [item for item in spans if item.kind == 3]
            assert len(children) == 3
            assert all(item.parent_span_id == parent.span_id for item in children)
            assert sorted(item.status.code for item in children) == [0, 2, 2]
            assert sorted(
                _attributes(item)["db.operation.name"] for item in children
            ) == ["DB", "PING", "PIPELINE"]
            assert all(not item.events and not item.status.message for item in children)
            assert all(
                CANARY.encode() not in request.message.SerializeToString()
                for request in otlp_receiver.requests
            )

    @pytest.mark.parametrize("plain", [False, True])
    @pytest.mark.parametrize("approved", [False, True])
    @pytest.mark.parametrize("business_count", [0, 1024])
    def test_mixed_case_prepared_headers_cannot_escape_origin_policy(
        self, otlp_receiver, plain, approved, business_count, monkeypatch
    ):
        import http.client

        from django_altimeter.headers import OUTBOUND_CONTEXT_HEADERS

        if business_count:
            # Allow the real test server to accept this deliberately large map.
            monkeypatch.setattr(http.client, "_MAXHEADERS", business_count + 64)

        with HTTPReceiver() as internal, HTTPReceiver() as external:
            result = _run(
                otlp_receiver,
                internal,
                """
                import requests
                headers = {
                    'TraceParent': '00-' + '1'*32 + '-' + '2'*16 + '-01',
                    'tRaCePaReNt': '00-' + '3'*32 + '-' + '4'*16 + '-01',
                    'TraceState': 'private=OUTBOUND_CANARY_secret',
                    'Baggage': 'private=OUTBOUND_CANARY_secret',
                    'X-Altimeter-Trace-Id': 'OUTBOUND_CANARY_secret',
                    'X-Altimeter-Trace-Sampled': '1',
                    'PosthogDistinctId': 'OUTBOUND_CANARY_secret',
                    'Altimeter.Tenant_Id': 'OUTBOUND_CANARY_secret',
                    'SessionId': 'OUTBOUND_CANARY_secret',
                    'X-Posthog-Distinct-Id': 'OUTBOUND_CANARY_secret',
                    'X-Tenant-Id': 'OUTBOUND_CANARY_secret',
                    'X-Session-Id': 'OUTBOUND_CANARY_secret',
                    'X-Application': 'kept',
                    'Authorization': 'Bearer OUTBOUND_CANARY_secret',
                }
                headers.update({f'X-Business-{index}': f'value-{index}'
                    for index in range(int(os.environ['BUSINESS_COUNT']))})
                request = requests.Request(
                    'GET', os.environ['TARGET_ORIGIN'], headers=headers).prepare()
                if os.environ['PLAIN'] == '1':
                    request.headers = dict(headers)
                with span('outbound.parent'):
                    with requests.Session() as client:
                        client.trust_env = False
                        with client.send(request, timeout=2) as response:
                            body = response.text
                assert shutdown().completed
                print(json.dumps({'body':body}))
                """,
                PLAIN="1" if plain else "0",
                BUSINESS_COUNT=str(business_count),
                TARGET_ORIGIN=internal.origin if approved else external.origin,
            )
            assert result == {"body": "safe-response"}
            captured = internal.requests if approved else external.requests
            assert len(captured) == 1
            sent = {key.lower(): value for key, value in captured[0].headers}
            assert sent["x-application"] == "kept"
            assert sent["authorization"] == f"Bearer {CANARY}"
            assert all(
                sent[f"x-business-{index}"] == f"value-{index}"
                for index in range(business_count)
            )
            assert OUTBOUND_CONTEXT_HEADERS.intersection(sent) == (
                {"traceparent"} if approved else set()
            )
            children = [item for item in _spans(otlp_receiver) if item.kind == 3]
            assert len(children) == 1
            if approved:
                assert sent["traceparent"] == (
                    f"00-{children[0].trace_id.hex()}-{children[0].span_id.hex()}-01"
                )
            assert all(
                CANARY.encode() not in request.message.SerializeToString()
                for request in otlp_receiver.requests
            )

    @pytest.mark.parametrize("cross_origin", [False, True])
    def test_header_projection_keeps_native_redirect_authorization_rules(
        self, otlp_receiver, cross_origin
    ):
        with HTTPReceiver() as internal, HTTPReceiver() as external:
            destination = external if cross_origin else internal
            internal.respond(
                status=302, headers={"Location": destination.origin + "/next"}
            )
            result = _run(
                otlp_receiver,
                internal,
                """
                import requests
                with span('outbound.parent'):
                    response = requests.get(os.environ['INTERNAL_ORIGIN'], headers={
                        'authorization': 'Bearer OUTBOUND_CANARY_secret',
                        'x-application': 'kept',
                    }, timeout=2)
                assert shutdown().completed
                print(json.dumps({'body':response.text}))
                """,
            )
            assert result == {"body": "safe-response"}
            first = {key.lower(): value for key, value in internal.requests[0].headers}
            last = {
                key.lower(): value for key, value in destination.requests[-1].headers
            }
            assert first["authorization"] == f"Bearer {CANARY}"
            assert first["x-application"] == last["x-application"] == "kept"
            if cross_origin:
                assert "authorization" not in last
                assert "traceparent" not in last
            else:
                assert last["authorization"] == first["authorization"]
                assert "traceparent" in last
            assert len([item for item in _spans(otlp_receiver) if item.kind == 3]) == 2
            assert all(
                CANARY.encode() not in request.message.SerializeToString()
                for request in otlp_receiver.requests
            )

    @pytest.mark.parametrize("copy_mode", ["task", "copy_context"])
    def test_redis_deferred_context_exports_after_originating_operation_finishes(
        self, otlp_receiver, copy_mode
    ):
        from support.reference_work import isolated_services

        with isolated_services() as services, HTTPReceiver() as internal:
            result = _run(
                otlp_receiver,
                internal,
                """
                from contextvars import copy_context
                import redis.asyncio
                async def exercise():
                    client = redis.asyncio.Redis.from_url(os.environ['REDIS_URL'])
                    key = os.environ['KEY_PREFIX'] + 'deferred'
                    value = b'OUTBOUND_CANARY_secret'
                    gate = asyncio.Event()
                    pending = []
                    contexts = []
                    try:
                        await client.set(key, value, ex=30)
                        assert setup(config).ok
                        async def deferred_get():
                            await gate.wait()
                            return await client.get(key)
                        def ping_response(response, **options):
                            if os.environ['COPY_MODE'] == 'task':
                                pending.append(asyncio.create_task(deferred_get()))
                            else:
                                contexts.append(copy_context())
                            return response == b'PONG'
                        client.set_response_callback('PING', ping_response)
                        with span('outbound.parent'):
                            assert await client.ping()
                            if contexts:
                                pending.append(contexts[0].run(
                                    asyncio.create_task, deferred_get()))
                            gate.set()
                            deferred = await pending[0]
                            ordinary = await client.get(key)
                        assert shutdown().completed
                        return deferred == ordinary == value
                    finally:
                        await client.delete(key)
                        await client.aclose()
                print(json.dumps({'same_values':asyncio.run(exercise())}))
                """,
                clients="redis",
                bootstrap=_IMPORTS.replace("assert setup(config).ok", ""),
                REDIS_URL=services.redis_url,
                KEY_PREFIX=services.redis_key_prefix,
                COPY_MODE=copy_mode,
            )
            assert result == {"same_values": True}
            spans = _spans(otlp_receiver)
            children = [item for item in spans if item.kind == 3]
            assert len(children) == 3
            assert sorted(
                _attributes(item)["db.operation.name"] for item in children
            ) == ["GET", "GET", "PING"]
            parent = next(item for item in spans if item.name == "outbound.parent")
            assert all(item.trace_id == parent.trace_id for item in children)
            assert all(
                CANARY.encode() not in request.message.SerializeToString()
                for request in otlp_receiver.requests
            )


@pytest.fixture(scope="module")
def client_wheel(tmp_path_factory):
    from support.reference_work import WORK_EXTRAS
    from support.uvicorn_server import install_wheel

    return install_wheel(tmp_path_factory.mktemp("client-wheel"), extras=WORK_EXTRAS)


class TestASGIPropagation:
    @pytest.mark.parametrize("client", ["requests", "httpx", "async-httpx"])
    @pytest.mark.parametrize("sample_rate", ["0", "1"])
    def test_uvicorn_public_parent_cannot_override_local_outbound_sampling(
        self, client_wheel, tmp_path, otlp_receiver, client, sample_rate
    ):
        from support.reference_work import isolated_services
        from support.uvicorn_server import running_server

        with (
            isolated_services() as services,
            HTTPReceiver() as internal,
            HTTPReceiver() as external,
        ):
            with running_server(
                tmp_path,
                wheel=client_wheel,
                receiver=otlp_receiver,
                extra_env={
                    **services.environment(),
                    "ALTIMETER_INTEGRATIONS": "asgi,django,requests,httpx",
                    "ALTIMETER_SAMPLE_RATE": sample_rate,
                    "ALTIMETER_TRUST_REMOTE_PARENT": "true",
                    "REFERENCE_INTERNAL_ORIGIN": internal.origin,
                    "REFERENCE_EXTERNAL_ORIGIN": external.origin,
                },
            ) as server:
                for _ in range(2):
                    status, _, body = server.request(
                        f"/work/outgoing?client={client}",
                        headers={
                            "traceparent": "00-" + "1" * 32 + "-" + "2" * 16 + "-01",
                            "baggage": "tenant=OUTBOUND_CANARY_secret",
                        },
                    )
                    assert status == 200, body
                    assert json.loads(body) == {
                        "internal": "safe-response",
                        "external": "safe-response",
                    }
            assert len(internal.requests) == len(external.requests) == 2
            parents = [
                dict((key.lower(), value) for key, value in request.headers)[
                    "traceparent"
                ]
                for request in internal.requests
            ]
            assert len(set(parents)) == 2
            assert all(value.split("-")[1] != "1" * 32 for value in parents)
            assert all(
                value.endswith("-00" if sample_rate == "0" else "-01")
                for value in parents
            )
            for request in external.requests:
                assert not {
                    "traceparent",
                    "tracestate",
                    "baggage",
                    "x-altimeter-trace-id",
                } & {key.lower() for key, _ in request.headers}
            spans = _spans(otlp_receiver)
            roots = [item for item in spans if item.kind == 2]
            children = [item for item in spans if item.kind == 3]
            assert len(roots) == 2 * int(sample_rate)
            assert len(children) == 4 * int(sample_rate)
            if sample_rate == "1":
                assert {item.trace_id.hex() for item in roots} == {
                    value.split("-")[1] for value in parents
                }
                assert all(
                    item.name == "GET"
                    and _attributes(item)["http.response.status_code"] == 200
                    for item in children
                )
            metric_count = sum(
                point.as_int
                for request in otlp_receiver.requests
                if request.path == "/v1/metrics"
                for resource in request.message.resource_metrics
                for scope in resource.scope_metrics
                for metric in scope.metrics
                if metric.name == "altimeter.http.server.requests"
                for point in metric.sum.data_points
            )
            assert metric_count == 2
            assert all(
                CANARY.encode() not in request.message.SerializeToString()
                for request in otlp_receiver.requests
            )
