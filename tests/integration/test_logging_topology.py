"""Real-process logging and decoded OTLP evidence, never live ingestion proof."""

import json
import os
import subprocess
import sys
import textwrap

import pytest

pytestmark = pytest.mark.integration
CANARY = "private-log-canary-3ca9"

_IMPORTS = """
import importlib.util
import io
import json
import logging
import os
import sys
from dataclasses import replace
from pathlib import Path

from django_altimeter.conf import Config, SignalConfig
from django_altimeter.context import get_correlation, span
from django_altimeter.privacy import PrivacyPolicy
from django_altimeter.runtime import setup, shutdown

if os.environ.get('H4_REQUIRE_WHEEL') == '1':
    import django_altimeter
    import importlib.metadata
    origin = Path(django_altimeter.__file__).resolve()
    assert origin.is_relative_to(Path(sys.prefix).resolve()), origin
    direct = json.loads(importlib.metadata.distribution('django-altimeter')
                        .read_text('direct_url.json'))
    assert not direct.get('dir_info', {}).get('editable', False)
    assert 'sha256' in direct['archive_info']['hashes']

CANARY = 'private-log-canary-3ca9'
POLICY = PrivacyPolicy(
    span_names=frozenset({'work.execute'}),
    event_names=frozenset({'work.done'}),
    log_bodies=frozenset({'work.done'}),
    scope_names=frozenset({'django_altimeter', 'app', 'app.solo', 'app.structured'}),
)

def configure(**changes):
    config = Config(
        enabled=True, service_name='logging-test', deployment_environment='test',
        release='test-1', policy=POLICY,
        traces=SignalConfig(
            enabled=True, endpoint=sys.argv[1], destination='collector'),
        logs=SignalConfig(enabled=True, endpoint=sys.argv[2], destination='collector'),
    )
    result = setup(replace(config, **changes))
    assert 'logs' in result.enabled_signals, result

def install(routes):
    # A missing adapter must fail on missing receiver records, not on an import.
    if importlib.util.find_spec('django_altimeter.logging') is not None:
        from django_altimeter.logging import install_logging
        result = install_logging(routes=routes)
        assert result.ok, result


def output_handler(stream):
    handler = logging.StreamHandler(stream)
    handler.setFormatter(logging.Formatter('%(name)s|%(levelname)s|%(message)s'))
    return handler
"""


def _run(receiver, body):
    result = subprocess.run(
        [
            sys.executable,
            "-c",
            _IMPORTS + textwrap.dedent(body),
            receiver.url("traces"),
            receiver.url("logs"),
        ],
        env={
            **{
                key: value
                for key, value in os.environ.items()
                if not key.startswith(("ALTIMETER_", "OTEL_"))
            },
            "H4_REQUIRE_WHEEL": "1"
            if "ALTIMETER_REFERENCE_PYTHON" in os.environ
            else "0",
        },
        capture_output=True,
        text=True,
        timeout=25,
        check=False,
    )
    print(result.stderr, end="")
    assert result.returncode == 0, result.stdout + result.stderr
    assert CANARY not in result.stderr
    return json.loads(result.stdout)


def _logs(receiver):
    return [
        (group.resource, scope.scope, record)
        for request in receiver.requests
        if request.path == "/v1/logs"
        for group in request.message.resource_logs
        for scope in group.scope_logs
        for record in scope.log_records
    ]


def _assert_private(receiver):
    for request in receiver.requests:
        assert CANARY.encode() not in request.message.SerializeToString()


class TestStandaloneLogging:
    def test_stdlib_routes_copy_records_and_export_once(self, otlp_receiver):
        result = _run(
            otlp_receiver,
            """
            configure()
            stream = io.StringIO()
            app = logging.getLogger('app')
            solo = logging.getLogger('app.solo')
            for logger in (app, solo):
                logger.setLevel(logging.DEBUG)
                logger.propagate = False
                logger.addHandler(output_handler(stream))
            install(('app', 'app.solo'))
            install(('app', 'app.solo'))
            snapshots = []
            with span('work.execute'):
                correlation = get_correlation()
                for logger in (app, solo):
                    record = logging.LogRecord(
                        logger.name, logging.INFO, '/private/path',
                        12, 'work.done', (), None)
                    record.secret = CANARY
                    # Run stdout first so formatter-owned message is in the baseline.
                    logger.handlers[0].handle(record)
                    baseline = dict(record.__dict__)
                    for handler in logger.handlers[1:]:
                        handler.handle(record)
                    assert record.__dict__ == baseline
                    snapshots.append(record.secret)
            assert shutdown().completed
            print(json.dumps({'stdout': stream.getvalue(), 'snapshots': snapshots,
                              'trace_id': correlation.trace_id,
                              'span_id': correlation.span_id}))
        """,
        )
        assert result["stdout"] == "app|INFO|work.done\napp.solo|INFO|work.done\n"
        assert result["snapshots"] == [CANARY, CANARY]
        records = _logs(otlp_receiver)
        assert len(records) == 2
        assert {scope.name for _, scope, _ in records} == {"app", "app.solo"}
        for resource, _, record in records:
            assert record.body.string_value == "work.done"
            assert record.severity_number == 9
            assert record.trace_id.hex() == result["trace_id"]
            assert record.span_id.hex() == result["span_id"]
            assert {a.key: a.value.string_value for a in resource.attributes}[
                "service.name"
            ] == "logging-test"
        _assert_private(otlp_receiver)

    def test_canaries_bounds_and_internal_logs_keep_safe_neighbors(self, otlp_receiver):
        result = _run(
            otlp_receiver,
            """
            configure()
            stream = io.StringIO()
            root = logging.getLogger()
            root.setLevel(logging.DEBUG)
            root.addHandler(output_handler(stream))
            install(('',))
            app = logging.getLogger('app')
            for level in (logging.DEBUG, logging.INFO, logging.WARNING, logging.ERROR):
                app.log(level, 'work.done', extra={'password': CANARY})
                app.log(level, 'private: %s', CANARY)
                app.log(level, json.dumps({'password': CANARY}))
                app.log(level, {'password': CANARY})
            try:
                raise ValueError(CANARY)
            except ValueError:
                app.error('work.done', exc_info=True)
            app.info({'event': 'work.done', 'http.response.status_code': 200,
                      'password': CANARY, 'nested': {'secret': CANARY}})
            app.info('x' * 1_000_000)
            app.info({'event': 'work.done', **{f'k{i}': CANARY for i in range(32)}})
            logging.getLogger('unapproved').info('work.done')
            for name in ('django_altimeter.export', 'opentelemetry.exporter',
                         'urllib3.connectionpool', 'requests'):
                logging.getLogger(name).error('work.done')
            assert shutdown().completed
            print(json.dumps({'stdout': stream.getvalue()}))
        """,
        )
        assert result["stdout"].count(CANARY) >= 17
        records = _logs(otlp_receiver)
        assert len(records) == 7
        assert [r.severity_number for _, _, r in records[:4]] == [5, 9, 13, 17]
        assert all(r.body.string_value == "work.done" for _, _, r in records)
        assert records[-1][1].name == "django_altimeter"
        assert {a.key: a.value.int_value for a in records[-2][2].attributes} == {
            "http.response.status_code": 200
        }
        assert all(not r.trace_id and not r.span_id for _, _, r in records)
        _assert_private(otlp_receiver)

    def test_structlog_processor_formatter_stdout_is_unchanged(self, otlp_receiver):
        result = _run(
            otlp_receiver,
            """
            import structlog
            configure()
            stream = io.StringIO()
            logger = logging.getLogger('app.structured')
            logger.setLevel(logging.DEBUG)
            logger.propagate = False
            output = logging.StreamHandler(stream)
            output.setFormatter(structlog.stdlib.ProcessorFormatter(processors=[
                structlog.stdlib.ProcessorFormatter.remove_processors_meta,
                structlog.processors.JSONRenderer(sort_keys=True),
            ]))
            logger.addHandler(output)
            structlog.configure(
                processors=[structlog.stdlib.ProcessorFormatter.wrap_for_formatter],
                logger_factory=structlog.stdlib.LoggerFactory(),
                wrapper_class=structlog.stdlib.BoundLogger,
            )
            bound = structlog.get_logger('app.structured')
            def log():
                bound.debug('work.done', password=CANARY)
                bound.error(CANARY, nested={'secret': CANARY})
            log()
            before = stream.getvalue()
            stream.truncate(0)
            stream.seek(0)
            install(('app.structured',))
            log()
            after = stream.getvalue()
            assert shutdown().completed
            print(json.dumps({'before': before, 'after': after}))
        """,
        )
        assert result["before"] == result["after"]
        assert CANARY in result["after"]
        records = _logs(otlp_receiver)
        assert len(records) == 1
        assert records[0][1].name == "app.structured"
        assert records[0][2].body.string_value == "work.done"
        assert records[0][2].severity_number == 5
        _assert_private(otlp_receiver)

    def test_foreign_tracing_and_shutdown_cannot_disclose_context(self, otlp_receiver):
        result = _run(
            otlp_receiver,
            """
            from opentelemetry import trace
            from opentelemetry.sdk.trace import TracerProvider
            foreign = TracerProvider(shutdown_on_exit=False)
            trace.set_tracer_provider(foreign)
            configure()
            app = logging.getLogger('app')
            app.setLevel(logging.INFO)
            app.propagate = False
            stream = io.StringIO()
            app.addHandler(output_handler(stream))
            install(('app',))
            with foreign.get_tracer('foreign').start_as_current_span('secret'):
                assert get_correlation() is None
                app.info('work.done')
            assert shutdown().completed
            app.info('work.done')
            foreign.shutdown()
            print(json.dumps({'stdout': stream.getvalue()}))
        """,
        )
        assert result["stdout"].count("work.done") == 2
        records = _logs(otlp_receiver)
        assert len(records) == 1
        assert not records[0][2].trace_id and not records[0][2].span_id
        _assert_private(otlp_receiver)

    def test_scrubber_uses_copy_revalidates_and_cannot_recurse(self, otlp_receiver):
        result = _run(
            otlp_receiver,
            """
            app = logging.getLogger('app')
            app.setLevel(logging.DEBUG)
            app.propagate = False
            stream = io.StringIO()
            app.addHandler(output_handler(stream))
            seen = []
            def scrubber(record):
                assert record.args == () and record.exc_info is None
                seen.append(record.msg)
                app.info('work.done')
                if record.msg == 'bad':
                    raise ValueError(CANARY)
                record.msg = 'work.done' if record.msg == 'known legacy' else CANARY
                record.attributes = {'http.response.status_code': 200, 'secret': CANARY}
                return record
            configure(log_scrubber=scrubber)
            install(('app',))
            for message in ('known legacy', 'unapproved', 'bad'):
                record = logging.LogRecord('app', logging.INFO, '/secret', 2,
                                           message, (), None)
                baseline = dict(record.__dict__)
                app.handlers[-1].handle(record)
                assert record.__dict__ == baseline
            assert shutdown().completed
            print(json.dumps({'seen': seen, 'stdout': stream.getvalue()}))
        """,
        )
        assert result["seen"] == ["known legacy", "unapproved", "bad"]
        assert result["stdout"] == "app|INFO|work.done\n" * 3
        records = _logs(otlp_receiver)
        assert len(records) == 1
        assert records[0][2].body.string_value == "work.done"
        assert {a.key: a.value.int_value for a in records[0][2].attributes} == {
            "http.response.status_code": 200
        }
        _assert_private(otlp_receiver)

    def test_concurrent_contexts_are_captured_before_native_batching(
        self, otlp_receiver
    ):
        result = _run(
            otlp_receiver,
            """
            from concurrent.futures import ThreadPoolExecutor
            from dataclasses import asdict
            from threading import Barrier
            configure()
            app = logging.getLogger('app')
            app.setLevel(logging.INFO)
            app.propagate = False
            app.addHandler(logging.NullHandler())
            install(('app',))
            barrier = Barrier(8)
            def work(_):
                with span('work.execute'):
                    expected = get_correlation()
                    barrier.wait(timeout=5)
                    app.info('work.done')
                assert get_correlation() is None
                return asdict(expected)
            with ThreadPoolExecutor(max_workers=8) as pool:
                expected = list(pool.map(work, range(8)))
            assert shutdown().completed
            print(json.dumps(expected))
        """,
        )
        records = _logs(otlp_receiver)
        assert len(records) == 8
        assert {(r.trace_id.hex(), r.span_id.hex()) for _, _, r in records} == {
            (c["trace_id"], c["span_id"]) for c in result
        }
        _assert_private(otlp_receiver)

    def test_route_validation_is_atomic_and_concurrent_install_is_idempotent(
        self, otlp_receiver
    ):
        result = _run(
            otlp_receiver,
            """
            from concurrent.futures import ThreadPoolExecutor
            from django_altimeter.logging import install_logging
            configure()
            parent = logging.getLogger('app')
            child = logging.getLogger('app.child')
            parent.setLevel(logging.INFO)
            parent.propagate = True
            logging.getLogger().addHandler(logging.NullHandler())
            before = (tuple(parent.handlers), tuple(child.handlers))
            invalid = install_logging(routes=('app', 'app.child'))
            assert not invalid.ok and invalid.codes == ('logging.invalid_routes',)
            assert before == (tuple(parent.handlers), tuple(child.handlers))
            with ThreadPoolExecutor(max_workers=8) as pool:
                results = list(pool.map(lambda _: install_logging(routes=('app',)),
                                        range(24)))
            assert all(result.ok for result in results)
            assert len(parent.handlers) == 1
            assert not install_logging(routes=('app.child',)).ok
            child.info('work.done')
            assert shutdown().completed
            print(json.dumps({'codes': invalid.codes}))
        """,
        )
        assert result["codes"] == ["logging.invalid_routes"]
        assert len(_logs(otlp_receiver)) == 1
        assert _logs(otlp_receiver)[0][1].name == "django_altimeter"

    def test_unowned_logs_do_not_attach_or_change_application_logging(
        self, otlp_receiver
    ):
        result = _run(
            otlp_receiver,
            """
            from opentelemetry._logs import set_logger_provider
            from opentelemetry.sdk._logs import LoggerProvider
            from django_altimeter.logging import install_logging
            foreign = LoggerProvider(shutdown_on_exit=False)
            set_logger_provider(foreign)
            config = Config(enabled=True, service_name='logging-test',
                deployment_environment='test', release='test-1', policy=POLICY,
                logs=SignalConfig(enabled=True, endpoint=sys.argv[2],
                                  destination='collector'))
            setup(config)
            app = logging.getLogger('app')
            app.setLevel(logging.INFO)
            app.propagate = False
            stream = io.StringIO()
            app.addHandler(output_handler(stream))
            result = install_logging(routes=('app',))
            assert not result.ok and result.codes == ('logging.unowned',)
            app.info('work.done')
            app.info(CANARY)
            assert len(app.handlers) == 1
            assert shutdown().completed
            foreign.shutdown()
            print(json.dumps({'stdout': stream.getvalue()}))
        """,
        )
        assert result["stdout"] == f"app|INFO|work.done\napp|INFO|{CANARY}\n"
        assert _logs(otlp_receiver) == []

    def test_hostile_values_are_not_formatted_or_traversed(self, otlp_receiver):
        result = _run(
            otlp_receiver,
            """
            configure()
            app = logging.getLogger('app')
            app.setLevel(logging.INFO)
            app.propagate = False
            app.addHandler(logging.NullHandler())
            install(('app',))
            calls = []
            class Hostile:
                def __str__(self):
                    calls.append('str')
                    raise AssertionError(CANARY)
                def __len__(self):
                    calls.append('len')
                    raise AssertionError(CANARY)
                def __iter__(self):
                    calls.append('iter')
                    raise AssertionError(CANARY)
            class HostileString(str):
                def encode(self, *args):
                    calls.append('encode')
                    raise AssertionError(CANARY)
            app.info(Hostile())
            app.info('work.done', Hostile())
            app.info({'event': 'work.done', 'http.route': Hostile(),
                      'password': HostileString(CANARY)})
            app.info(HostileString('work.done'))
            app.info('work.done',
                     extra={'attributes': {'http.response.status_code': 200}})
            assert calls == []
            assert shutdown().completed
            print(json.dumps({'calls': calls}))
        """,
        )
        assert result["calls"] == []
        records = _logs(otlp_receiver)
        assert len(records) == 3
        assert all(r.body.string_value == "work.done" for _, _, r in records)
        assert {a.key: a.value.int_value for a in records[-1][2].attributes} == {
            "http.response.status_code": 200
        }
        _assert_private(otlp_receiver)

    def test_async_reporter_receives_immutable_snapshot_after_detach(
        self, otlp_receiver
    ):
        result = _run(
            otlp_receiver,
            """
            from concurrent.futures import ThreadPoolExecutor
            from dataclasses import asdict
            configure()
            def report(captured):
                assert get_correlation() is None
                return asdict(captured)
            with span('work.execute') as current:
                snapshot = get_correlation()
                native_flags = int(current.get_span_context().trace_flags)
                assert snapshot.trace_flags == native_flags
            assert get_correlation() is None
            with ThreadPoolExecutor(max_workers=1) as reporter:
                reported = reporter.submit(report, snapshot).result(timeout=5)
            assert shutdown().completed
            print(json.dumps({'reported': reported, 'captured': asdict(snapshot)}))
        """,
        )
        spans = [
            span
            for request in otlp_receiver.requests
            if request.path == "/v1/traces"
            for group in request.message.resource_spans
            for scope in group.scope_spans
            for span in scope.spans
        ]
        assert len(spans) == 1
        assert result["reported"] == result["captured"]
        assert result["reported"]["trace_id"] == spans[0].trace_id.hex()
        assert result["reported"]["span_id"] == spans[0].span_id.hex()
        assert result["reported"]["trace_flags"] & 1 == 1

    def test_foreign_current_span_under_owned_provider_is_not_disclosed(
        self, otlp_receiver
    ):
        result = _run(
            otlp_receiver,
            """
            from opentelemetry.sdk.trace import TracerProvider
            configure()
            app = logging.getLogger('app')
            app.setLevel(logging.INFO)
            app.propagate = False
            app.addHandler(logging.NullHandler())
            install(('app',))
            foreign = TracerProvider(shutdown_on_exit=False)
            with foreign.get_tracer('foreign').start_as_current_span('secret'):
                app.info('work.done')
                leaked = get_correlation()
            with span('work.execute'):
                expected = get_correlation()
                app.info('work.done')
            assert shutdown().completed
            foreign.shutdown()
            print(json.dumps({'leaked': leaked is not None,
                              'trace_id': expected.trace_id,
                              'span_id': expected.span_id}))
        """,
        )
        assert not result["leaked"]
        records = _logs(otlp_receiver)
        assert len(records) == 2
        assert not records[0][2].trace_id and not records[0][2].span_id
        assert records[1][2].trace_id.hex() == result["trace_id"]
        assert records[1][2].span_id.hex() == result["span_id"]

    def test_record_correlation_fields_cannot_forge_owned_context(self, otlp_receiver):
        _run(
            otlp_receiver,
            """
            configure()
            app = logging.getLogger('app')
            app.setLevel(logging.INFO)
            app.propagate = False
            app.addHandler(logging.NullHandler())
            install(('app',))
            app.info('work.done', extra={'attributes': {
                'trace_id': 'cafe' * 8, 'span_id': 'babe' * 4,
                'http.response.status_code': 200,
            }})
            assert shutdown().completed
            print('{}')
        """,
        )
        records = _logs(otlp_receiver)
        assert len(records) == 1
        assert not records[0][2].trace_id and not records[0][2].span_id
        assert {a.key: a.value.int_value for a in records[0][2].attributes} == {
            "http.response.status_code": 200
        }

    @pytest.mark.parametrize("mode", ["ordinary", "propagating", "root", "module"])
    def test_handler_free_routes_preserve_fallback_output(self, otlp_receiver, mode):
        result = _run(
            otlp_receiver,
            f"mode = {mode!r}\n"
            + textwrap.dedent("""
            from contextlib import redirect_stderr
            from django_altimeter.logging import install_logging
            from django_altimeter.runtime import get_process_runtime
            configure()
            root = logging.getLogger()
            root.handlers.clear()
            root.setLevel(logging.WARNING)
            app = logging.getLogger('app')
            app.handlers.clear()
            app.setLevel(logging.WARNING)
            app.propagate = mode == 'propagating'
            route = app if mode in ('ordinary', 'propagating') else root
            routes = ('app',) if route is app else ('',)
            emit = logging.warning if mode == 'module' else route.warning
            stream = io.StringIO()
            with redirect_stderr(stream):
                emit(CANARY)
                emit('work.done')
            before = stream.getvalue()
            # Module-level warning() installs basicConfig's handler on first use.
            # Restore the original empty topology before the Altimeter attempt.
            for handler in tuple(root.handlers):
                root.removeHandler(handler)
                handler.close()
            stream.seek(0)
            stream.truncate(0)
            with redirect_stderr(stream):
                result = install_logging(routes=routes)
                attached = tuple(route.handlers)
                emit(CANARY)
                emit('work.done')
            after = stream.getvalue()
            assert after == before, (before, after)
            assert not result.ok
            assert result.codes == ('logging.no_effective_handler',)
            assert attached == ()
            assert get_process_runtime().log_handler is None
            positive = logging.getLogger('app.solo')
            positive.setLevel(logging.WARNING)
            positive.propagate = False
            output = io.StringIO()
            positive.addHandler(output_handler(output))
            install(('app.solo',))
            positive.warning('work.done')
            assert shutdown().completed
            print(json.dumps({'before': before, 'after': after,
                              'positive': output.getvalue(), 'codes': result.codes}))
        """),
        )
        prefix = "WARNING:root:" if mode == "module" else ""
        assert result["before"] == f"{prefix}{CANARY}\n{prefix}work.done\n"
        assert result["after"] == result["before"]
        assert result["positive"] == "app.solo|WARNING|work.done\n"
        assert result["codes"] == ["logging.no_effective_handler"]
        records = _logs(otlp_receiver)
        assert len(records) == 1
        assert records[0][1].name == "app.solo"
        assert records[0][2].body.string_value == "work.done"
        _assert_private(otlp_receiver)

    @pytest.mark.parametrize("key_kind", ["object", "str_subclass"])
    def test_structured_event_never_hashes_or_compares_unknown_keys(
        self, otlp_receiver, key_kind
    ):
        result = _run(
            otlp_receiver,
            f"key_kind = {key_kind!r}\n"
            + textwrap.dedent("""
            configure()
            app = logging.getLogger('app')
            app.setLevel(logging.INFO)
            app.propagate = False
            app.addHandler(logging.NullHandler())
            install(('app',))
            calls = []
            armed = False
            event_hash = hash('event')
            class HostileKey:
                def __hash__(self):
                    if armed:
                        calls.append('hash')
                        raise AssertionError(CANARY)
                    return event_hash
                def __eq__(self, other):
                    if armed:
                        calls.append('eq')
                        raise AssertionError(CANARY)
                    return False
            class HostileString(str):
                __hash__ = HostileKey.__hash__
                __eq__ = HostileKey.__eq__
            key = HostileKey() if key_kind == 'object' else HostileString('event')
            unknown = {key: CANARY}
            mixed = {key: CANARY, 'event': 'work.done',
                     'http.response.status_code': 201}
            armed = True
            app.info(unknown)
            app.info(mixed)
            app.info({'event': 'work.done', 'http.response.status_code': 200})
            assert shutdown().completed
            print(json.dumps({'calls': calls}))
        """),
        )
        assert result["calls"] == []
        records = _logs(otlp_receiver)
        assert len(records) == 2
        assert all(record.body.string_value == "work.done" for _, _, record in records)
        assert [
            {a.key: a.value.int_value for a in record.attributes}
            for _, _, record in records
        ] == [
            {"http.response.status_code": 201},
            {"http.response.status_code": 200},
        ]
        _assert_private(otlp_receiver)


@pytest.fixture(scope="module")
def logging_wheel(tmp_path_factory):
    from support.reference_work import WORK_EXTRAS
    from support.uvicorn_server import install_wheel

    return install_wheel(tmp_path_factory.mktemp("logging-wheel"), extras=WORK_EXTRAS)


class TestASGILogFlow:
    @pytest.mark.parametrize("sample_rate", ["1", "0"])
    def test_request_log_matches_owned_ingress(
        self, logging_wheel, tmp_path, sample_rate
    ):
        from support.otlp_receiver import OTLPReceiver
        from support.reference_work import isolated_services
        from support.uvicorn_server import running_server

        with isolated_services() as services, OTLPReceiver() as receiver:
            with running_server(
                tmp_path,
                wheel=logging_wheel,
                receiver=receiver,
                extra_env={
                    **services.environment(),
                    "ALTIMETER_LOGS_ENABLED": "true",
                    "ALTIMETER_LOGS_COLLECTOR_ENDPOINT": receiver.url("logs"),
                    "ALTIMETER_SAMPLE_RATE": sample_rate,
                },
            ) as server:
                status, _, body = server.request("/work/logs")
                assert status == 200, body
                response = json.loads(body)
                assert response["logged"] is True
                assert response["log_count"] == 4
            for canary in (
                "log-free-text-canary",
                "log-json-canary",
                "log-nested-canary",
                "log-error-canary",
                "log-exception-canary",
            ):
                assert canary in server.output
                assert all(
                    canary.encode() not in request.message.SerializeToString()
                    for request in receiver.requests
                )
            records = _logs(receiver)
            assert len(records) == 1
            resource, scope, record = records[0]
            assert scope.name == "reference.work"
            assert record.body.string_value == "reference.work.completed"
            assert record.trace_id.hex() == response["trace_id"]
            assert record.span_id.hex() == response["span_id"]
            assert record.flags & 1 == int(sample_rate)
            spans = [
                span
                for request in receiver.requests
                if request.path == "/v1/traces"
                for group in request.message.resource_spans
                for scope in group.scope_spans
                for span in scope.spans
            ]
            if sample_rate == "1":
                assert len(spans) == 1
                assert spans[0].kind == 2
                assert spans[0].trace_id == record.trace_id
                assert spans[0].span_id == record.span_id
            else:
                assert spans == []
            assert {a.key: a.value.string_value for a in resource.attributes}[
                "service.name"
            ] == "reference-web"


@pytest.fixture(scope="module")
def wsgi_logging_wheel(tmp_path_factory):
    from support.gunicorn_server import install_wheel
    from support.reference_work import WORK_EXTRAS

    return install_wheel(
        tmp_path_factory.mktemp("wsgi-logging-wheel"), extras=WORK_EXTRAS
    )


def _wsgi_log_environment(receiver, **changes):
    return {
        "REFERENCE_REQUEST_WORK": "1",
        "ALTIMETER_LOGS_ENABLED": "true",
        "ALTIMETER_LOGS_COLLECTOR_ENDPOINT": receiver.url("logs"),
        **changes,
    }


def _wire_attributes(values):
    return {
        value.key: getattr(value.value, value.value.WhichOneof("value"))
        for value in values
    }


class TestWSGILogFlow:
    @pytest.mark.parametrize("sample_rate", ["1", "0.1", "0"])
    @pytest.mark.parametrize("threads", [1, 4])
    def test_request_logs_and_identity_stay_in_their_gthread_context(
        self, wsgi_logging_wheel, tmp_path, sample_rate, threads
    ):
        from collections import Counter
        from concurrent.futures import ThreadPoolExecutor

        from support.gunicorn_server import running_server
        from support.otlp_receiver import OTLPReceiver

        identities = ["alpha", "none", "beta", "none"] * 6
        canaries = (
            "forged-identity-canary",
            "forged-tenant-canary",
            "forged-session-canary",
            "cookie-session-canary",
            "log-free-text-canary",
            "log-json-canary",
            "log-nested-canary",
            "log-error-canary",
            "log-exception-canary",
        )
        with OTLPReceiver() as receiver:
            with running_server(
                tmp_path,
                wheel=wsgi_logging_wheel,
                receiver=receiver,
                workers=1,
                threads=threads,
                extra_env=_wsgi_log_environment(
                    receiver, ALTIMETER_SAMPLE_RATE=sample_rate
                ),
            ) as server:

                def request(identity):
                    status, _, body = server.request(
                        f"/work/logs?identity={identity}&delay=0.05",
                        headers={
                            "posthogDistinctId": canaries[0],
                            "altimeter.tenant_id": canaries[1],
                            "sessionId": canaries[2],
                            "Cookie": f"sessionid={canaries[3]}",
                        },
                    )
                    assert status == 200, body
                    result = json.loads(body)
                    assert result["logged"] and result["log_count"] == 4
                    if threads == 1:
                        status, _, health_body = server.request("/health/")
                        assert status == 200
                        health = json.loads(health_body)
                        assert health["pid"] == result["pid"]
                        assert health["thread_id"] == result["thread_id"]
                        assert health["trace_id"] is None
                        assert health["span_id"] is None
                    return identity, result

                with ThreadPoolExecutor(max_workers=threads) as clients:
                    results = list(clients.map(request, identities))
            records = _logs(receiver)
            assert len(records) == len(identities)
            by_trace = {
                result["trace_id"]: (identity, result) for identity, result in results
            }
            assert len(by_trace) == len(identities)
            assert None not in by_trace
            expected_correlations = Counter(
                (result["trace_id"], result["span_id"], result["trace_flags"] & 1)
                for _, result in results
            )
            observed_correlations = Counter(
                (record.trace_id.hex(), record.span_id.hex(), record.flags & 1)
                for _, _, record in records
            )
            assert observed_correlations == expected_correlations
            assert len({result["pid"] for _, result in results}) == 1
            thread_ids = {result["thread_id"] for _, result in results}
            if threads == 1:
                assert len(thread_ids) == 1
            else:
                assert 2 <= len(thread_ids) <= threads
            assert server.output.count("reference.work.completed") == len(identities)
            for resource, scope, record in records:
                identity, expected = by_trace[record.trace_id.hex()]
                assert record.span_id.hex() == expected["span_id"]
                assert record.flags & 1 == expected["trace_flags"] & 1
                assert record.body.string_value == "reference.work.completed"
                assert scope.name == "reference.work"
                assert record.severity_number == 9
                resource_attributes = _wire_attributes(resource.attributes)
                assert resource_attributes["service.name"] == "reference-web"
                assert "posthogDistinctId" not in resource_attributes
                assert "altimeter.tenant_id" not in resource_attributes
                attributes = _wire_attributes(record.attributes)
                for key, prefix in (
                    ("posthogDistinctId", "person"),
                    ("altimeter.tenant_id", "tenant"),
                ):
                    expected_identity = (
                        f"{prefix}-{identity}" if identity != "none" else None
                    )
                    assert attributes.get(key) in (None, expected_identity)
            spans = [
                span
                for request in receiver.requests
                if request.path == "/v1/traces"
                for group in request.message.resource_spans
                for scope in group.scope_spans
                for span in scope.spans
            ]
            assert len(spans) == sum(
                bool(result["trace_flags"] & 1) for _, result in results
            )
            assert all(span.kind == 2 for span in spans)
            assert len({span.trace_id for span in spans}) == len(spans)
            for span in spans:
                identity, result = by_trace[span.trace_id.hex()]
                assert span.span_id.hex() == result["span_id"]
                attributes = _wire_attributes(span.attributes)
                assert attributes["http.route"] == "work/logs"
                assert attributes.get("posthogDistinctId") == (
                    f"person-{identity}" if identity != "none" else None
                )
                assert attributes.get("altimeter.tenant_id") == (
                    f"tenant-{identity}" if identity != "none" else None
                )
            if sample_rate == "1":
                assert len(spans) == len(identities)
            elif sample_rate == "0":
                assert spans == []
            else:
                assert len(spans) < len(identities)
            for request in receiver.requests:
                payload = request.message.SerializeToString()
                assert all(canary.encode() not in payload for canary in canaries)
                if request.path == "/v1/metrics":
                    for forbidden in (
                        b"person-alpha",
                        b"person-beta",
                        b"tenant-alpha",
                        b"tenant-beta",
                    ):
                        assert forbidden not in payload

    def test_disabled_and_invalid_keep_the_same_application_log_output(
        self, wsgi_logging_wheel, tmp_path
    ):
        from support.gunicorn_server import running_server
        from support.otlp_receiver import OTLPReceiver

        outputs = {}
        for mode in ("disabled", "invalid", "enabled"):
            with OTLPReceiver() as receiver:
                with running_server(
                    tmp_path / mode,
                    wheel=wsgi_logging_wheel,
                    receiver=receiver,
                    enabled=mode != "disabled",
                    threads=1,
                    extra_env=_wsgi_log_environment(
                        receiver,
                        ALTIMETER_SAMPLE_RATE="invalid" if mode == "invalid" else "1",
                    ),
                ) as server:
                    status, _, body = server.request("/work/logs?identity=none")
                    assert status == 200, body
                    result = json.loads(body)
                    assert result["logged"] and result["log_count"] == 4
                start = server.output.index(
                    "INFO|reference.work|reference.work.completed"
                )
                end = server.output.index("ValueError: log-exception-canary", start)
                outputs[mode] = server.output[
                    start : end + len("ValueError: log-exception-canary")
                ]
                if mode == "enabled":
                    records = _logs(receiver)
                    assert len(records) == 1
                    assert records[0][2].trace_id.hex() == result["trace_id"]
                    assert records[0][2].span_id.hex() == result["span_id"]
                else:
                    assert result["trace_id"] is None and result["span_id"] is None
                    assert receiver.requests == ()
        assert outputs["enabled"] == outputs["disabled"] == outputs["invalid"]

    @pytest.mark.parametrize("sample_rate", ["1", "0.1", "0"])
    def test_stream_iteration_logs_keep_correlation_then_release_context(
        self, wsgi_logging_wheel, tmp_path, sample_rate
    ):
        from support.gunicorn_server import running_server
        from support.otlp_receiver import OTLPReceiver

        with OTLPReceiver() as receiver:
            with running_server(
                tmp_path,
                wheel=wsgi_logging_wheel,
                receiver=receiver,
                threads=1,
                extra_env=_wsgi_log_environment(
                    receiver, ALTIMETER_SAMPLE_RATE=sample_rate
                ),
            ) as server:
                status, headers, body = server.request("/work/log-stream")
                assert status == 200
                headers = {key.lower(): value for key, value in headers.items()}
                responses = [json.loads(line) for line in body.splitlines()]
                assert len(responses) == 2
                for response in responses:
                    assert response["logged"] and response["log_count"] == 4
                    assert response["trace_id"] == headers["x-altimeter-trace-id"]
                    assert response["span_id"]
                assert responses[0]["span_id"] == responses[1]["span_id"]
                status, _, health_body = server.request("/health/")
                assert status == 200
                health = json.loads(health_body)
                assert health["trace_id"] is None and health["span_id"] is None
            records = _logs(receiver)
            assert len(records) == 2
            assert server.output.count("reference.work.completed") == 2
            for _, scope, record in records:
                assert scope.name == "reference.work"
                assert record.body.string_value == "reference.work.completed"
                assert record.trace_id.hex() == responses[0]["trace_id"]
                assert record.span_id.hex() == responses[0]["span_id"]
                assert record.flags & 1 == int(headers["x-altimeter-trace-sampled"])
            spans = [
                span
                for request in receiver.requests
                if request.path == "/v1/traces"
                for group in request.message.resource_spans
                for scope in group.scope_spans
                for span in scope.spans
            ]
            assert len(spans) == int(headers["x-altimeter-trace-sampled"])
            if sample_rate == "1":
                assert len(spans) == 1
            elif sample_rate == "0":
                assert spans == []
            for span in spans:
                assert span.kind == 2
                assert span.trace_id == records[0][2].trace_id
                assert span.span_id == records[0][2].span_id
                attributes = _wire_attributes(span.attributes)
                assert attributes["http.route"] == "work/log-stream"
                assert attributes["altimeter.response_mode"] == "streaming"
            for request in receiver.requests:
                for canary in (
                    b"log-free-text-canary",
                    b"log-json-canary",
                    b"log-nested-canary",
                    b"log-error-canary",
                    b"log-exception-canary",
                ):
                    assert canary not in request.message.SerializeToString()
