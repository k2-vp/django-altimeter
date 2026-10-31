"""Predeclared real Celery measurements; explicit ASGI-image performance gate.

One prefork slot and one producer; 1024-byte argument/result, one safe log and
one unapproved free-text canary log (stdout only),
no application database/cache. Three balanced repetitions: disabled/healthy/
hanging, healthy/hanging/disabled, hanging/disabled/healthy; keep every run.
Reuse: 10 warmup, 100 measured completed tasks, including publish/result cost.
Recycle: max_tasks_per_child=1, first child warmup then four measured children
per run; compare pooled median setup with disabled, added target <50ms.
Measure startup signal interval, whole-worker SIGTERM-to-exit and drain hook
interval (2s total budget, 0.2s scheduling allowance). Never connect/getaddrinfo/
sendto during setup. All modes use the same copied wheel and dependency pins.

Traces/logs/metrics on, sampling1, export intervals250ms, timeout200ms, drain2s;
span/log queues512, batch128, record16KiB, request1MiB, response64KiB,
worker series2000. RSS sums parent+child Linux resident pages every100ms.
Separate60s hanging reuse workload: last30s slope<=256KiB/s; final10s median
<=seconds10..20 median+16MiB. Require80% sampling coverage and max gap1s.
Report every count/error/timeout, throughput, setup/exit cost and RSS before
asserting acceptance. This is local real-process evidence, not deployment data.
Original reference machine: Linux arm64 ASGI container on Apple M5 Max,
128 GiB host RAM; Python3.13/Django6.0 and the declared pinned dependencies.
"""

import json
import os
import statistics
import threading
import time
from pathlib import Path

import pytest
from support.celery_worker import running_worker
from support.otlp_receiver import OTLPReceiver
from support.uvicorn_server import install_wheel

RUN_ORDER = (
    ("disabled", "healthy", "hanging"),
    ("healthy", "hanging", "disabled"),
    ("hanging", "disabled", "healthy"),
)
WARMUP_TASKS = 10
MEASURE_TASKS = 100
RECYCLED_CHILDREN = 4
SOAK_SECONDS = 60.0
PAYLOAD = "x" * 1024


def _record_counts(receiver):
    counts = {"traces": 0, "logs": 0, "metrics": 0}
    for request in receiver.requests:
        if request.path == "/v1/traces":
            counts["traces"] += sum(
                len(scope.spans)
                for resource in request.message.resource_spans
                for scope in resource.scope_spans
            )
        elif request.path == "/v1/logs":
            counts["logs"] += sum(
                len(scope.log_records)
                for resource in request.message.resource_logs
                for scope in resource.scope_logs
            )
        elif request.path == "/v1/metrics":
            counts["metrics"] += sum(
                len(getattr(metric, metric.WhichOneof("data")).data_points)
                for resource in request.message.resource_metrics
                for scope in resource.scope_metrics
                for metric in scope.metrics
            )
    return counts


def _resident_bytes(pid):
    pages = int(Path(f"/proc/{pid}/statm").read_text().split()[1])
    return pages * os.sysconf("SC_PAGE_SIZE")


def _worker_rss(pid):
    total = _resident_bytes(pid)
    children = Path(f"/proc/{pid}/task/{pid}/children").read_text().split()
    for child in children:
        try:
            total += _resident_bytes(int(child))
        except FileNotFoundError:
            # Native max_tasks_per_child can reap a child between the two reads.
            if Path(f"/proc/{child}").exists():
                raise
    return total


def _coverage(samples, begin, end):
    times = [moment for moment, _ in samples if begin <= moment <= end]
    edges = [begin, *times, end]
    gaps = [right - left for left, right in zip(edges, edges[1:], strict=False)]
    return {
        "samples": len(times),
        "expected": (end - begin) / 0.1,
        "fraction": len(times) / ((end - begin) / 0.1),
        "max_gap_seconds": max(gaps),
    }


def _plateau(samples):
    early = [value for moment, value in samples if 10 <= moment <= 20]
    final = [value for moment, value in samples if 50 <= moment <= 60]
    tail = [(moment, value) for moment, value in samples if 30 <= moment <= 60]
    if not early or not final or len(tail) < 2:
        return None
    average_time = statistics.mean(moment for moment, _ in tail)
    average_rss = statistics.mean(value for _, value in tail)
    divisor = sum((moment - average_time) ** 2 for moment, _ in tail)
    if divisor == 0:
        return None
    return {
        "slope_bytes_per_second": sum(
            (moment - average_time) * (value - average_rss) for moment, value in tail
        )
        / divisor,
        "median_growth_bytes": statistics.median(final) - statistics.median(early),
        "coverage": {
            name: _coverage(samples, begin, end)
            for name, begin, end in (
                ("early", 10, 20),
                ("tail", 30, 60),
                ("final", 50, 60),
            )
        },
    }


def _run(workspace, wheel, mode, *, recycle=None, seconds=0):
    with OTLPReceiver() as receiver:
        hanging_responses = []
        if mode == "hanging":
            for _ in range(32768):
                hanging_responses.append(receiver.queue_response(hang=True))
        with running_worker(
            workspace,
            wheel=wheel,
            receiver=receiver,
            mode=mode,
            recycle=recycle,
            extra_env={"REFERENCE_CELERY_MEASURE": "1"},
        ) as worker:
            samples = []
            errors = []
            stopped = threading.Event()
            started = time.monotonic()

            def sample():
                while not stopped.is_set():
                    try:
                        samples.append(
                            (
                                time.monotonic() - started,
                                _worker_rss(worker.process.pid),
                            )
                        )
                    except Exception as error:
                        errors.append(type(error).__name__)
                        return
                    stopped.wait(0.1)

            sampler = threading.Thread(target=sample)
            sampler.start()
            try:
                report = worker.client(
                    {
                        "action": "workload",
                        "warmup": 1 if recycle else WARMUP_TASKS,
                        "count": RECYCLED_CHILDREN if recycle else MEASURE_TASKS,
                        "seconds": seconds,
                        "payload": PAYLOAD,
                    },
                    timeout=180,
                )
            finally:
                elapsed = time.monotonic() - started
                stopped.set()
                sampler.join(timeout=2)
            report.update(
                mode=mode,
                recycle=recycle,
                rss_samples=samples,
                rss_errors=errors,
                rss_coverage=_coverage(samples, 0, elapsed),
                rss_median=statistics.median(value for _, value in samples)
                if samples
                else None,
            )
        report["exit_seconds"] = worker.exit_seconds
        report["setups"] = [
            event for event in worker.events if event["event"] == "setup"
        ]
        report["drains"] = [
            event for event in worker.events if event["event"] == "drain"
        ]
        report["task_pids"] = sorted(
            {event["pid"] for event in worker.events if event["event"] == "postrun"}
        )
        report["collector_requests"] = len(receiver.requests)
        report["decoded_records"] = _record_counts(receiver)
        report["collector_signals"] = sorted(
            {request.path for request in receiver.requests}
        )
        report["hanging_peer_closures"] = sum(
            response.client_closed.is_set() for response in hanging_responses
        )
        return report


def _display(report):
    return {
        **{
            key: value
            for key, value in report.items()
            if key not in ("samples", "rss_samples")
        },
        "latency_samples": len(report["samples"]),
        "rss_sample_count": len(report["rss_samples"]),
    }


@pytest.mark.performance
@pytest.mark.integration
def test_celery_worker_overhead_and_failure_plateau(tmp_path):
    wheel = install_wheel(tmp_path / "wheel", extras=("celery", "redis"))
    reports = []
    for repetition, order in enumerate(RUN_ORDER):
        for mode in order:
            for recycle in (None, 1):
                report = _run(
                    tmp_path / f"{repetition}-{mode}-{recycle}",
                    wheel,
                    mode,
                    recycle=recycle,
                )
                report["repetition"] = repetition
                reports.append(report)
                print("WORKER_RUN " + json.dumps(_display(report)), flush=True)
    soak = _run(tmp_path / "soak", wheel, "hanging", seconds=SOAK_SECONDS)
    plateau = _plateau(soak["rss_samples"])
    print(
        "WORKER_SOAK " + json.dumps({**_display(soak), "plateau": plateau}), flush=True
    )
    setup = {
        mode: [
            event["elapsed"]
            for report in reports
            if report["mode"] == mode and report["recycle"]
            # First child is the predeclared warmup; final idle replacement is
            # not one of the four measured task executions.
            for event in report["setups"][1 : 1 + RECYCLED_CHILDREN]
        ]
        for mode in RUN_ORDER[0]
    }
    baseline = statistics.median(setup["disabled"])
    added = {
        mode: statistics.median(values) - baseline for mode, values in setup.items()
    }
    rss_baseline = statistics.median(
        report["rss_median"]
        for report in reports
        if report["mode"] == "disabled" and not report["recycle"]
    )
    extra_rss = {
        mode: statistics.median(
            report["rss_median"]
            for report in reports
            if report["mode"] == mode and not report["recycle"]
        )
        - rss_baseline
        for mode in RUN_ORDER[0]
    }
    print(
        "WORKER_SUMMARY "
        + json.dumps(
            {
                "wheel_sha256": wheel.sha256,
                "evidence": "local real-process",
                "setup_samples": setup,
                "added_setup_seconds": added,
                "extra_rss_bytes": extra_rss,
            }
        ),
        flush=True,
    )
    for report in [*reports, soak]:
        assert report["completed"] > 0
        assert report["errors"] == report["timeouts"] == 0
        assert not report["rss_errors"]
        assert report["rss_coverage"]["fraction"] >= 0.8
        assert report["rss_coverage"]["max_gap_seconds"] <= 1
        assert all(not event["network"] for event in report["setups"])
        if report["mode"] == "disabled":
            assert all(event["instance_id"] is None for event in report["setups"])
            assert all(event["enabled_signals"] == [] for event in report["setups"])
            assert report["collector_requests"] == 0
        else:
            assert all(event["instance_id"] is not None for event in report["setups"])
            assert all(
                set(event["enabled_signals"]) == {"traces", "logs", "metrics"}
                for event in report["setups"]
            )
            assert report["collector_requests"] > 0
            if report["mode"] == "healthy":
                assert all(count > 0 for count in report["decoded_records"].values())
                assert report["collector_signals"] == [
                    "/v1/logs",
                    "/v1/metrics",
                    "/v1/traces",
                ]
                child_pids = {event["pid"] for event in report["setups"]}
                assert all(
                    event["completed"] is True
                    for event in report["drains"]
                    if event["pid"] in child_pids
                )
                assert all(
                    event["export_codes"]
                    == {
                        "traces": "export.accepted",
                        "logs": "export.accepted",
                        "metrics": "export.accepted",
                    }
                    for event in report["drains"]
                    if event["pid"] in report["task_pids"]
                )
                assert set(report["task_pids"]) <= {
                    event["pid"] for event in report["drains"]
                }
            else:
                assert report["hanging_peer_closures"] > 0
        assert report["drains"]
        assert all(event["elapsed"] <= 2.2 for event in report["drains"])
    assert all(len(values) == 12 for values in setup.values())
    assert added["healthy"] < 0.05 and added["hanging"] < 0.05
    assert plateau is not None
    assert plateau["slope_bytes_per_second"] <= 256 * 1024
    assert plateau["median_growth_bytes"] <= 16 * 1024 * 1024
    assert all(
        window["fraction"] >= 0.8 and window["max_gap_seconds"] <= 1
        for window in plateau["coverage"].values()
    )


def test_worker_harness_rejects_missing_plateau_windows():
    assert _plateau([(0, 1), (60, 1)]) is None


def test_worker_harness_reports_growth_and_sample_coverage():
    samples = [(index / 10, 10_000_000 + index * 10) for index in range(601)]
    plateau = _plateau(samples)
    assert plateau["slope_bytes_per_second"] == pytest.approx(100)
    assert plateau["median_growth_bytes"] == pytest.approx(4000)
    assert all(window["fraction"] >= 1 for window in plateau["coverage"].values())
    assert all(
        window["max_gap_seconds"] == pytest.approx(0.1)
        for window in plateau["coverage"].values()
    )
