"""Installed-wheel process observations and real Linux fork attribution.

CPU delta / elapsed wall seconds is cores used; utilization needs an explicit
core denominator. RSS snapshots remain per writer: summing them can double-count
shared pages and is not physical host/container memory. A cleanly recycled child
has a new writer UUID; that is not evidence of a crash or OOM.
"""

import json
import os
import subprocess

import pytest
from support.uvicorn_server import install_wheel

pytestmark = pytest.mark.integration

_CPU = "process.cpu.time"
_RSS = "process.memory.usage"
_THREADS = "process.thread.count"
_FDS = "process.unix.file_descriptor.count"
_GC = {
    "altimeter.process.gc.collections": "{collection}",
    "altimeter.process.gc.collected": "{object}",
    "altimeter.process.gc.uncollectable": "{object}",
}
_SCHEMA = {
    _CPU: "s",
    _RSS: "By",
    _THREADS: "{thread}",
    _FDS: "{file_descriptor}",
    **_GC,
}
_CONTROL = "control.process.calls"
_CANARY = "PROCESS_PRIVATE_CANARY"


@pytest.fixture(scope="module")
def process_wheel(tmp_path_factory):
    return install_wheel(
        tmp_path_factory.mktemp("process-wheel"),
        extras=("process",),
        server_dependencies=(),
    )


_BOOTSTRAP = r"""
import gc
import importlib.metadata
import json
import os
from pathlib import Path
import resource
import sys
import threading
import time
from dataclasses import replace

import django_altimeter
from django_altimeter.conf import Config, SignalConfig
from django_altimeter.privacy import DEFAULT_POLICY
from django_altimeter.runtime import (
    ProcessRuntime, setup, shutdown, get_process_runtime,
)

origin = Path(django_altimeter.__file__).resolve()
assert origin.is_relative_to(Path(sys.prefix).resolve()), origin
installed = importlib.metadata.distribution('django-altimeter')
direct = json.loads(installed.read_text('direct_url.json'))
assert direct['archive_info']['hashes']['sha256'] == os.environ['WHEEL_SHA256']
assert not direct.get('dir_info', {}).get('editable', False)
assert sys.platform == 'linux', 'Run this proof in a Linux verification image'

def configuration(enabled=True):
    endpoint = os.environ['RECEIVER_ENDPOINT']
    return Config(enabled=True, role='web', service_name='process-reference',
        deployment_environment='test', release='v1',
        integrations=frozenset({'process'}) if enabled else frozenset(),
        traces=SignalConfig(True, endpoint + '/v1/traces', (), 'collector'),
        metrics=SignalConfig(True, endpoint + '/v1/metrics', (), 'collector'),
        metric_export_interval_millis=60000, batch_schedule_delay_millis=60000,
        export_timeout=1,
        policy=replace(DEFAULT_POLICY,
            metric_instruments=frozenset({
                ('django_altimeter', 'control.process.calls')})))

def cpu_seconds():
    usage = resource.getrusage(resource.RUSAGE_SELF)
    return usage.ru_utime + usage.ru_stime

def burn(seconds):
    deadline = cpu_seconds() + seconds
    value = 17
    while cpu_seconds() < deadline:
        for _ in range(1000):
            value = (value * 31 + 7) % 1000003
    return value

def collect(runtime, name):
    before = time.time_ns()
    wall = time.monotonic()
    cpu = cpu_seconds()
    runtime.meter_provider.get_meter('django_altimeter').create_counter(
        'control.process.calls').add(1, {'tenant.id': 'PROCESS_PRIVATE_CANARY'})
    assert runtime.meter_provider.force_flush(timeout_millis=2000)
    return {'name': name, 'before': before, 'after': time.time_ns(),
            'wall': wall, 'cpu': cpu,
            'rss': (int(Path('/proc/self/statm').read_text().split()[1])
                    * os.sysconf('SC_PAGE_SIZE')),
            'threads': len(tuple(Path('/proc/self/task').iterdir()))}

def emit_control(runtime):
    with runtime.tracer.start_as_current_span('GET', attributes={
        'http.request.method': 'GET', 'private': 'PROCESS_PRIVATE_CANARY'}):
        pass
    assert runtime.tracer_provider.force_flush(timeout_millis=2000)
"""


def _run(wheel, receiver, workspace, source, **environment):
    workspace.mkdir(parents=True, exist_ok=True)
    result = subprocess.run(
        [str(wheel.python), "-I", "-c", _BOOTSTRAP + source, _CANARY],
        cwd=workspace,
        env={
            "PATH": os.environ.get("PATH", ""),
            "WHEEL_SHA256": wheel.sha256,
            "RECEIVER_ENDPOINT": receiver.endpoint,
            "PRIVATE_TOKEN": _CANARY,
            **environment,
        },
        text=True,
        capture_output=True,
        timeout=30,
        check=False,
    )
    print(result.stderr, end="")
    assert _CANARY not in result.stderr
    assert result.returncode == 0, result.stdout + result.stderr
    return json.loads(result.stdout.splitlines()[-1])


def _attributes(values):
    return {
        item.key: getattr(item.value, item.value.WhichOneof("value")) for item in values
    }


def _points(receiver, instance=None, phase=None):
    rows = []
    for request in receiver.requests:
        assert _CANARY.encode() not in request.message.SerializeToString()
        if request.path != "/v1/metrics":
            continue
        for resource in request.message.resource_metrics:
            attributes = _attributes(resource.resource.attributes)
            if instance is not None and attributes["service.instance.id"] != instance:
                continue
            for scope in resource.scope_metrics:
                assert scope.scope.name == "django_altimeter"
                for metric in scope.metrics:
                    stream = getattr(metric, metric.WhichOneof("data"))
                    for point in stream.data_points:
                        if (
                            phase is None
                            or phase["before"] <= point.time_unix_nano <= phase["after"]
                        ):
                            rows.append((attributes, metric, point))
    return rows


def _values(receiver, name, instance, phase):
    return [
        point
        for _, metric, point in _points(receiver, instance, phase)
        if metric.name == name
    ]


def _number(point):
    return getattr(point, point.WhichOneof("value"))


def test_controlled_workload_moves_cpu_rss_threads_and_preserves_schema(
    process_wheel, otlp_receiver, tmp_path
):
    result = _run(
        process_wheel,
        otlp_receiver,
        tmp_path,
        r"""
# Exercise the native runtime composition already present before P1; no new
# process module import or new constructor keyword is required for this red.
runtime = ProcessRuntime(configuration())
assert runtime.initialize().enabled_signals == ('traces', 'metrics')
emit_control(runtime)
phases = [collect(runtime, 'baseline')]
stop = threading.Event()
ready = threading.Barrier(4)
def wait():
    ready.wait(timeout=5)
    stop.wait(timeout=10)
threads = [threading.Thread(target=wait) for _ in range(3)]
files = []
try:
    for thread in threads: thread.start()
    ready.wait(timeout=5)
    payload = bytearray(32 * 1024 * 1024)
    for index in range(0, len(payload), 4096): payload[index] = 1
    private = Path('PROCESS_PRIVATE_CANARY.txt')
    private.write_text('PROCESS_PRIVATE_CANARY')
    files = [private.open('rb') for _ in range(12)]
    answer = burn(.20)
    phases.append(collect(runtime, 'loaded'))
    phases.append(collect(runtime, 'held'))
finally:
    stop.set()
    for thread in threads: thread.join(timeout=5)
    assert not any(thread.is_alive() for thread in threads)
    for file in files: file.close()
phases.append(collect(runtime, 'released'))
assert runtime.shutdown().completed
print(json.dumps({'instance': runtime.instance_id, 'pid': os.getpid(),
                  'phases': phases, 'answer': answer, 'application': 'completed'}))
""",
    )
    assert result["application"] == "completed"
    instance = result["instance"]
    baseline, loaded, held, released = result["phases"]
    control = _values(otlp_receiver, _CONTROL, instance, baseline)
    assert len(control) == 1 and control[0].as_int == 1
    observed = {metric.name for _, metric, _ in _points(otlp_receiver)}
    assert observed == {*_SCHEMA, _CONTROL}
    for resource, metric, point in _points(otlp_receiver):
        assert resource == {
            "service.name": "process-reference",
            "deployment.environment.name": "test",
            "service.version": "v1",
            "altimeter.role": "web",
            "service.instance.id": instance,
        }
        assert metric.WhichOneof("data") == "sum"
        if metric.name == _CONTROL:
            assert not point.attributes
            continue
        assert metric.unit == _SCHEMA[metric.name]
        dimensions = _attributes(point.attributes)
        if metric.name == _CPU:
            assert dimensions in ({"cpu.mode": "user"}, {"cpu.mode": "system"})
        elif metric.name in _GC:
            assert dimensions in (
                {"altimeter.gc.generation": str(index)} for index in range(3)
            )
        else:
            assert dimensions == {}
        snapshot = metric.name in {_RSS, _THREADS, _FDS}
        assert metric.sum.aggregation_temporality == (2 if snapshot else 1)
        assert metric.sum.is_monotonic is not snapshot
        assert _number(point) >= 0
        assert not point.exemplars

    def value(name, phase):
        points = _values(otlp_receiver, name, instance, phase)
        assert len(points) == 1
        return _number(points[0])

    cpu = sum(
        _number(point) for point in _values(otlp_receiver, _CPU, instance, loaded)
    )
    assert 0.18 <= cpu <= loaded["cpu"] - baseline["cpu"] + 0.08
    elapsed_wall = loaded["wall"] - baseline["wall"]
    assert (
        elapsed_wall > 0 and 0 < cpu / elapsed_wall < 2
    )  # CPU cores used, not a percent.
    assert value(_RSS, loaded) - value(_RSS, baseline) >= 24 * 1024 * 1024
    assert (
        value(_RSS, held) >= value(_RSS, loaded) * 0.9
    )  # Current snapshot, not a delta.
    assert abs(value(_RSS, loaded) - loaded["rss"]) < 8 * 1024 * 1024
    assert value(_THREADS, loaded) >= value(_THREADS, baseline) + 3
    assert value(_THREADS, held) == value(_THREADS, loaded)
    assert value(_THREADS, released) <= value(_THREADS, baseline)
    assert value(_FDS, loaded) >= value(_FDS, baseline) + 10
    assert value(_FDS, released) <= value(_FDS, loaded) - 10


def test_recycled_fork_children_keep_independent_deltas_and_current_values(
    process_wheel, otlp_receiver, tmp_path
):
    result = _run(
        process_wheel,
        otlp_receiver,
        tmp_path,
        r"""
gc.disable()
shared = bytearray(24 * 1024 * 1024)
for index in range(0, len(shared), 4096): shared[index] = 1
for _ in range(20): gc.collect(2)
parent_collections = gc.get_stats()[2]['collections']
assert get_process_runtime() is None
children = []
for ordinal in range(2):
    read_fd, write_fd = os.pipe()
    pid = os.fork()
    if pid == 0:
        os.close(read_fd)
        try:
            assert setup(configuration()).enabled_signals == ('traces', 'metrics')
            runtime = get_process_runtime()
            assert runtime is not None
            emit_control(runtime)
            phases = [collect(runtime, 'baseline')]
            payload = bytearray((16 + ordinal * 16) * 1024 * 1024)
            for index in range(0, len(payload), 4096): payload[index] = 1
            burn(.12)
            before_gc = gc.get_stats()
            for _ in range(10):
                cycle = []
                cycle.append(cycle)
            del cycle
            gc.collect(2)
            after_gc = gc.get_stats()
            phases.append(collect(runtime, 'loaded'))
            phases.append(collect(runtime, 'held'))
            assert shutdown().completed
            report = {'pid': os.getpid(), 'instance': runtime.instance_id,
                      'phases': phases, 'before_gc': before_gc,
                      'after_gc': after_gc}
            with os.fdopen(write_fd, 'w') as writer:
                json.dump(report, writer)
            os._exit(0)
        except BaseException:
            import traceback
            traceback.print_exc()
            os._exit(1)
    os.close(write_fd)
    with os.fdopen(read_fd) as reader:
        data = reader.read(65537)
    waited, status = os.waitpid(pid, 0)
    assert waited == pid and os.waitstatus_to_exitcode(status) == 0, status
    assert len(data) <= 65536
    children.append(json.loads(data))
assert get_process_runtime() is None
print(json.dumps({'pid': os.getpid(), 'children': children,
                  'parent_collections': parent_collections}))
""",
    )
    children = result["children"]
    assert len({child["pid"] for child in children} | {result["pid"]}) == 3
    instances = {child["instance"] for child in children}
    assert len(instances) == 2
    assert {
        resource["service.instance.id"] for resource, _, _ in _points(otlp_receiver)
    } == instances
    assert result["parent_collections"] >= 20
    for child in children:
        instance = child["instance"]
        baseline, loaded, held = child["phases"]
        for phase in (baseline, loaded, held):
            points = _values(otlp_receiver, _CONTROL, instance, phase)
            assert len(points) == 1 and points[0].as_int == 1
        for phase, expected_cpu in ((baseline, 0), (loaded, 0.1)):
            points = _values(otlp_receiver, _CPU, instance, phase)
            assert len(points) == 2
            value = sum(_number(point) for point in points)
            assert value == 0 if expected_cpu == 0 else value >= expected_cpu
        for name in _GC:
            statistic = name.rsplit(".", 1)[1]
            for phase in (baseline, loaded, held):
                points = _values(otlp_receiver, name, instance, phase)
                assert len(points) == 3
                for point in points:
                    generation = int(
                        _attributes(point.attributes)["altimeter.gc.generation"]
                    )
                    expected = (
                        child["after_gc"][generation][statistic]
                        - child["before_gc"][generation][statistic]
                        if phase is loaded
                        else 0
                    )
                    assert _number(point) == expected
        for name in (_RSS, _THREADS, _FDS):
            loaded_points = _values(otlp_receiver, name, instance, loaded)
            held_points = _values(otlp_receiver, name, instance, held)
            assert len(loaded_points) == len(held_points) == 1
            assert _number(held_points[0]) >= _number(loaded_points[0]) * 0.9
        rss = _values(otlp_receiver, _RSS, instance, loaded)
        assert abs(_number(rss[0]) - loaded["rss"]) < 8 * 1024 * 1024
    # Aggregation retains a latest snapshot per writer, not a sum over time.
    latest = {
        child["instance"]: _number(
            _values(otlp_receiver, _RSS, child["instance"], child["phases"][-1])[0]
        )
        for child in children
    }
    assert set(latest) == instances
    assert sum(latest.values()) > max(latest.values()) > 0
    # Both children exited normally. Neither their new UUIDs nor their summed
    # RSS (including shared inherited pages) imply a crash or physical RAM use.


def test_initialized_parent_never_reads_or_exports_child_process_values(
    process_wheel, otlp_receiver, tmp_path
):
    result = _run(
        process_wheel,
        otlp_receiver,
        tmp_path,
        r"""
assert setup(configuration()).enabled_signals == ('traces', 'metrics')
runtime = get_process_runtime()
assert runtime is not None
emit_control(runtime)
baseline = collect(runtime, 'parent-before')
read_fd, write_fd = os.pipe()
pid = os.fork()
if pid == 0:
    os.close(read_fd)
    try:
        payload = bytearray(64 * 1024 * 1024)
        for index in range(0, len(payload), 4096): payload[index] = 1
        burn(.20)
        reads = []
        observing = True
        def audit(event, arguments):
            if (observing and event in ('open', 'os.listdir')
                    and isinstance(arguments[0], (str, bytes))
                    and os.fsdecode(arguments[0]).startswith('/proc/')):
                reads.append(os.fsdecode(arguments[0]))
        sys.addaudithook(audit)
        runtime.meter_provider.force_flush(timeout_millis=1000)
        inherited = setup(configuration())
        observing = False
        assert not inherited.enabled_signals
        assert get_process_runtime() is None
        assert 'setup.metrics_inherited' in inherited.codes
        assert not reads, reads
        with os.fdopen(write_fd, 'w') as writer:
            json.dump({'pid': os.getpid(), 'reads': reads,
                       'codes': inherited.codes}, writer)
        os._exit(0)
    except BaseException:
        import traceback
        traceback.print_exc()
        os._exit(1)
os.close(write_fd)
with os.fdopen(read_fd) as reader:
    child = json.loads(reader.read(65536))
waited, status = os.waitpid(pid, 0)
assert waited == pid and os.waitstatus_to_exitcode(status) == 0, status
after = collect(runtime, 'parent-after')
assert shutdown().completed
print(json.dumps({'instance': runtime.instance_id, 'pid': os.getpid(),
                  'child': child, 'baseline': baseline, 'after': after}))
""",
    )
    assert result["child"]["pid"] != result["pid"]
    assert result["child"]["reads"] == []
    assert {
        resource["service.instance.id"] for resource, _, _ in _points(otlp_receiver)
    } == {result["instance"]}
    before = _values(otlp_receiver, _RSS, result["instance"], result["baseline"])
    after = _values(otlp_receiver, _RSS, result["instance"], result["after"])
    assert len(before) == len(after) == 1
    assert abs(_number(after[0]) - _number(before[0])) < 16 * 1024 * 1024
    cpu = _values(otlp_receiver, _CPU, result["instance"], result["after"])
    assert len(cpu) == 2
    assert sum(_number(point) for point in cpu) < 0.15
    assert len(_values(otlp_receiver, _CONTROL, result["instance"], None)) >= 2


@pytest.mark.parametrize("failure", ["permission", "missing"])
def test_real_fd_source_gap_is_independent_safe_and_recovers(
    process_wheel, otlp_receiver, tmp_path, failure
):
    result = _run(
        process_wheel,
        otlp_receiver,
        tmp_path,
        r"""
import tempfile
import psutil
# psutil's supported procfs root redirects only this subprocess. Every positive
# reading still comes from the live kernel; no process values are fabricated.
with tempfile.TemporaryDirectory(
        prefix='PROCESS_PRIVATE_CANARY-', dir='/tmp') as temporary:
    root = Path(temporary)
    root.chmod(0o755)
    process = root / str(os.getpid())
    process.mkdir(mode=0o755)
    (root / 'stat').symlink_to('/proc/stat')
    for name in ('stat', 'statm', 'status', 'fd'):
        (process / name).symlink_to('/proc/self/' + name)
    psutil.PROCFS_PATH = temporary
    assert setup(configuration()).enabled_signals == ('traces', 'metrics')
    runtime = get_process_runtime()
    assert runtime is not None
    emit_control(runtime)
    phases = [collect(runtime, 'healthy')]
    fd_source = process / 'fd'
    fd_source.unlink()
    original_uid = os.geteuid()
    failure = os.environ['SOURCE_FAILURE']
    if failure == 'permission':
        fd_source.mkdir(mode=0)
        if original_uid == 0:
            os.seteuid(65534)
    expected_error = PermissionError if failure == 'permission' else FileNotFoundError
    try:
        try:
            os.listdir(fd_source)
        except expected_error:
            actual_failure = expected_error.__name__
        else:
            raise AssertionError('The kernel did not deny the selected FD source')
        burn(.10)
        phases.append(collect(runtime, 'source-gap'))
        emit_control(runtime)
    finally:
        if os.geteuid() != original_uid:
            os.seteuid(original_uid)
        if failure == 'permission':
            fd_source.chmod(0o755)
            fd_source.rmdir()
        fd_source.symlink_to('/proc/self/fd')
    phases.append(collect(runtime, 'recovered'))
    assert shutdown().completed
    print(json.dumps({'instance': runtime.instance_id, 'phases': phases,
                      'failure': actual_failure, 'application': 'completed'}))
""",
        SOURCE_FAILURE=failure,
    )
    assert result["application"] == "completed"
    assert result["failure"] == (
        "PermissionError" if failure == "permission" else "FileNotFoundError"
    )
    instance = result["instance"]
    healthy, gap, recovered = result["phases"]
    for phase in (healthy, recovered):
        fd_points = _values(otlp_receiver, _FDS, instance, phase)
        assert len(fd_points) == 1 and _number(fd_points[0]) > 0
    assert not _values(otlp_receiver, _FDS, instance, gap)
    for phase in (healthy, gap, recovered):
        names = {
            metric.name for _, metric, _ in _points(otlp_receiver, instance, phase)
        }
        assert names == {*_SCHEMA, _CONTROL} - ({_FDS} if phase is gap else set())
        assert (
            sum(
                _number(point)
                for point in _values(otlp_receiver, _CONTROL, instance, phase)
            )
            == 1
        )
        for name in (_RSS, _THREADS):
            points = _values(otlp_receiver, name, instance, phase)
            assert len(points) == 1 and _number(points[0]) > 0
    assert (
        sum(_number(point) for point in _values(otlp_receiver, _CPU, instance, gap))
        >= 0.09
    )
    spans = [
        span
        for request in otlp_receiver.requests
        if request.path == "/v1/traces"
        for resource in request.message.resource_spans
        for scope in resource.scope_spans
        for span in scope.spans
    ]
    assert len(spans) == 2
    assert all(span.name == "GET" for span in spans)


def test_source_gap_diagnostic_cannot_deadlock_reentrant_native_collection(
    process_wheel, otlp_receiver, tmp_path
):
    result = _run(
        process_wheel,
        otlp_receiver,
        tmp_path,
        r"""
import logging
import signal
import tempfile
import psutil
with tempfile.TemporaryDirectory(
        prefix='PROCESS_PRIVATE_CANARY-', dir='/tmp') as temporary:
    root = Path(temporary)
    process = root / str(os.getpid())
    process.mkdir()
    (root / 'stat').symlink_to('/proc/stat')
    for name in ('stat', 'statm', 'status', 'fd'):
        (process / name).symlink_to('/proc/self/' + name)
    psutil.PROCFS_PATH = temporary
    assert setup(configuration()).enabled_signals == ('traces', 'metrics')
    runtime = get_process_runtime()
    assert runtime is not None
    phases = [collect(runtime, 'healthy')]
    watchdog = []
    handler_calls = []
    def timed_out(signum, frame):
        watchdog.append('native_collection_deadlocked')
        raise TimeoutError('test_watchdog')
    signal.signal(signal.SIGALRM, timed_out)
    class FlushHandler(logging.Handler):
        def emit(self, record):
            handler_calls.append(record.getMessage())
            runtime.meter_provider.force_flush(timeout_millis=100)
    logger = logging.getLogger('django_altimeter.process')
    handler = FlushHandler()
    logger.addHandler(handler)
    try:
        # Safe neighbor: the real application handler can flush normally.
        logger.warning('control')
        assert handler_calls == ['control']
        (process / 'fd').unlink()
        signal.setitimer(signal.ITIMER_REAL, 1)
        phases.append(collect(runtime, 'source-gap'))
    finally:
        signal.setitimer(signal.ITIMER_REAL, 0)
        logger.removeHandler(handler)
    (process / 'fd').symlink_to('/proc/self/fd')
    phases.append(collect(runtime, 'recovered'))
    assert shutdown().completed
    print(json.dumps({'instance': runtime.instance_id, 'phases': phases,
                      'watchdog': watchdog, 'handler_calls': handler_calls}))
""",
    )
    assert result["watchdog"] == []
    instance = result["instance"]
    healthy, gap, recovered = result["phases"]
    assert not _values(otlp_receiver, _FDS, instance, gap)
    for phase in (healthy, gap, recovered):
        control = _values(otlp_receiver, _CONTROL, instance, phase)
        assert len(control) == 1 and _number(control[0]) == 1
        assert _values(otlp_receiver, _RSS, instance, phase)
    assert _values(otlp_receiver, _FDS, instance, recovered)
