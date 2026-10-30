"""Predeclared real-server overhead workload; run in the ASGI verification image.

One uvicorn asyncio/h11 worker, four persistent HTTP/1.1 connections, a static
1024-byte response, no database, authentication, application cache, or request
body. Each fresh process receives 200 warmup requests and one second to settle.
Three balanced repetitions use ten measured seconds per mode; every run must
complete at least 1000 requests. Latency includes sending through full body read.
Pool individual latencies per mode, take nearest-rank p95, and compare both
instrumented modes with disabled: increase <= max(2 ms, 0.05 * disabled p95).
No averaging percentiles. Long-response correctness uses separate tests.

All runs use the same copied wheel, dependency pins, machine and workload.
Traces are fully sampled, metrics enabled, logs disabled; export and metric
intervals are 250 ms, exporter deadline 200 ms, shutdown budget two seconds.
The native limits remain queue=512, batch=128, record=16 KiB, request=1 MiB,
response=64 KiB and 2000 attribute sets per instrument (including overflow).

A separate 60-second failing-export run measures Linux resident pages at 50 ms
intervals. Declare a plateau only if the last 30 seconds' least-squares slope
is <= 256 KiB/s and the median final ten seconds is no more than 16 MiB above
the median seconds 10..20. This checks this workload, not arbitrary lifetime
RSS. A macOS host run reads ps RSS in KiB; the required image gate uses Linux
/proc. HTTP 503/Retry-After:0 responses remain in force through teardown.
"""

import importlib.metadata
import json
import math
import os
import platform
import statistics
import subprocess
import sys
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from contextlib import contextmanager
from http.client import HTTPConnection, HTTPException
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path

import pytest
from support.otlp_receiver import OTLPReceiver
from support.uvicorn_server import install_wheel, running_server

pytestmark = pytest.mark.integration
CONCURRENCY = 4
BODY = b"x" * 1024
WARMUP_REQUESTS = 200
MEASURE_SECONDS = 10.0
SOAK_SECONDS = 60.0
FAILURE_RESPONSES = 32768
RUN_ORDER = (
    ("disabled", "enabled", "failing_export"),
    ("enabled", "failing_export", "disabled"),
    ("failing_export", "disabled", "enabled"),
)
_VERSIONS = r"""
import importlib.metadata
import json

names = (
    "Django",
    "django-ninja",
    "uvicorn",
    "opentelemetry-sdk",
    "requests",
    "urllib3",
)
print(json.dumps({name: importlib.metadata.version(name) for name in names}))
"""


def _rss(pid: int) -> int:
    if platform.system() == "Linux":
        pages = int(Path(f"/proc/{pid}/statm").read_text().split()[1])
        return pages * os.sysconf("SC_PAGE_SIZE")
    result = subprocess.run(
        ["ps", "-o", "rss=", "-p", str(pid)],
        check=True,
        stdout=subprocess.PIPE,
        text=True,
        timeout=2,
    )
    return int(result.stdout.strip()) * 1024


def _p95(samples: list[float]) -> float | None:
    if not samples:
        return None
    return sorted(samples)[math.ceil(0.95 * len(samples)) - 1]


def _workload(port: int, pid: int, seconds: float) -> dict:
    def warmup(_: int) -> None:
        connection = HTTPConnection("127.0.0.1", port, timeout=5)
        try:
            for _ in range(WARMUP_REQUESTS // CONCURRENCY):
                connection.request("GET", "/api/benchmark")
                response = connection.getresponse()
                assert response.status == 200 and response.read() == BODY
        finally:
            connection.close()

    with ThreadPoolExecutor(max_workers=CONCURRENCY) as pool:
        list(pool.map(warmup, range(CONCURRENCY)))
    time.sleep(1)
    rss_errors = []

    def read_rss(phase):
        try:
            return _rss(pid)
        except Exception as error:
            rss_errors.append({"phase": phase, "type": type(error).__name__})
            return None

    baseline = read_rss("baseline")
    started = time.monotonic()
    until = started + seconds
    rss_samples = [(0.0, baseline)] if baseline is not None else []
    sampling_done = threading.Event()

    def sample() -> None:
        while not sampling_done.is_set():
            resident = read_rss("sample")
            if resident is None:
                return
            rss_samples.append((time.monotonic() - started, resident))
            sampling_done.wait(0.05)

    def request_loop(_: int) -> tuple[list[float], int, int]:
        connection = HTTPConnection("127.0.0.1", port, timeout=5)
        latencies = []
        errors = aborts = 0
        try:
            while time.monotonic() < until:
                begin = time.perf_counter()
                try:
                    connection.request("GET", "/api/benchmark")
                    response = connection.getresponse()
                    body = response.read()
                    if response.status == 200 and body == BODY:
                        latencies.append(time.perf_counter() - begin)
                    else:
                        errors += 1
                except (OSError, HTTPException):
                    aborts += 1
                    connection.close()
        finally:
            connection.close()
        return latencies, errors, aborts

    sampler = threading.Thread(target=sample)
    sampler.start()
    try:
        with ThreadPoolExecutor(max_workers=CONCURRENCY) as pool:
            results = list(pool.map(request_loop, range(CONCURRENCY)))
    finally:
        elapsed = time.monotonic() - started
        sampling_done.set()
        sampler.join(timeout=2)
    ending = read_rss("end")
    if ending is not None:
        rss_samples.append((time.monotonic() - started, ending))
    samples = [value for values, _, _ in results for value in values]
    errors = sum(errors for _, errors, _ in results)
    aborts = sum(aborts for _, _, aborts in results)
    completed = len(samples) + errors
    p95 = _p95(samples)
    coverage = {"measurement": _rss_coverage(rss_samples, 0, seconds)}
    if seconds == SOAK_SECONDS:
        coverage.update(
            {
                "early_10_20": _rss_coverage(rss_samples, 10, 20),
                "tail_30_60": _rss_coverage(rss_samples, 30, 60),
                "final_50_60": _rss_coverage(rss_samples, 50, 60),
            }
        )
    return {
        "latencies": samples,
        "elapsed": elapsed,
        "completed": completed,
        "successful": len(samples),
        "latency_samples": len(samples),
        "attempts": completed + aborts,
        "errors": errors,
        "aborts": aborts,
        "throughput": completed / elapsed,
        "p95_ms": p95 * 1000 if p95 is not None else None,
        "rss_baseline": baseline,
        "rss_peak": max((value for _, value in rss_samples), default=None),
        "rss_end": ending,
        "rss_samples": rss_samples,
        "rss_errors": rss_errors,
        "rss_sampler_alive": sampler.is_alive(),
        "rss_coverage": coverage,
    }


def _run(mode, duration, wheel, workspace):
    with OTLPReceiver() as receiver:
        if mode == "failing_export":
            for _ in range(FAILURE_RESPONSES):
                receiver.queue_response(status=503, headers={"Retry-After": "0"})
        with running_server(
            workspace,
            wheel=wheel,
            receiver=receiver,
            enabled=mode != "disabled",
            extra_env={
                "ALTIMETER_BATCH_SCHEDULE_DELAY_MILLIS": "250",
                "ALTIMETER_METRIC_EXPORT_INTERVAL_MILLIS": "250",
                "ALTIMETER_EXPORT_TIMEOUT": "0.2",
            },
        ) as server:
            result = _workload(server.port, server.process.pid, duration)
        result["otlp_requests"] = len(receiver.requests)
        result["attempted_server_spans"] = sum(
            span.kind == 2
            for request in receiver.requests
            if request.path == "/v1/traces"
            for resource in request.message.resource_spans
            for scope in resource.scope_spans
            for span in scope.spans
        )
        totals = {
            "altimeter.http.server.requests": 0,
            "http.server.request.duration": 0,
        }
        for request in receiver.requests:
            if request.path != "/v1/metrics":
                continue
            for resource in request.message.resource_metrics:
                for scope in resource.scope_metrics:
                    for metric in scope.metrics:
                        if metric.name == "altimeter.http.server.requests":
                            totals[metric.name] += sum(
                                point.as_int for point in metric.sum.data_points
                            )
                        elif metric.name == "http.server.request.duration":
                            totals[metric.name] += sum(
                                point.count for point in metric.histogram.data_points
                            )
        result["attempted_metric_counts"] = totals
    result["mode"] = mode
    _report_and_validate_run(result)
    return result


def _rss_coverage(samples, start, end):
    times = [timestamp for timestamp, _ in samples]
    window = [timestamp for timestamp in times if start <= timestamp < end]
    missing = [
        second
        for second in range(math.floor(start), math.ceil(end))
        if not any(
            max(start, second) <= timestamp < min(end, second + 1)
            for timestamp in window
        )
    ]
    bracketed = any(timestamp <= start for timestamp in times) and any(
        timestamp >= end for timestamp in times
    )
    return {
        "window_seconds": [start, end],
        "sample_count": len(window),
        "nominal_sample_count": math.ceil((end - start) / 0.05),
        "first_sample": min(window, default=None),
        "last_sample": max(window, default=None),
        "missing_one_second_intervals": missing,
        "bracketed": bracketed,
        "complete": bracketed and not missing,
    }


def _report_and_validate_run(result):
    # Report all collected countermetrics before any acceptance/evidence gate.
    print(
        "WEB_RUN "
        + json.dumps(
            {
                key: value
                for key, value in result.items()
                if key not in {"latencies", "rss_samples"}
            }
        ),
        flush=True,
    )
    assert not result["rss_errors"], ("RSS sampling failed", result["rss_errors"])
    assert not result["rss_sampler_alive"], "RSS sampler did not stop"
    assert all(window["complete"] for window in result["rss_coverage"].values()), (
        "Incomplete RSS window coverage",
        result["rss_coverage"],
    )
    assert result["latency_samples"] > 0, "No successful completed latency samples"
    assert result["p95_ms"] is not None
    assert result["completed"] >= 1000
    assert result["errors"] == result["aborts"] == 0
    assert result["latency_samples"] == result["completed"]
    mode = result["mode"]
    totals = result["attempted_metric_counts"]
    if mode == "enabled":
        expected = result["completed"] + WARMUP_REQUESTS
        assert totals == dict.fromkeys(totals, expected), totals
    # A retryable receiver captures attempts, which are not accepted totals.
    if mode != "disabled":
        assert result["attempted_server_spans"] > 0
        assert all(total > 0 for total in totals.values())
        assert result["otlp_requests"] > 0, "The full boundary must emit real OTLP"
    else:
        assert result["otlp_requests"] == 0
    if mode == "failing_export":
        assert result["otlp_requests"] < FAILURE_RESPONSES


def _server_versions(python: Path) -> dict[str, str]:
    """Read installed distributions from the interpreter serving requests."""
    result = subprocess.run(
        [str(python), "-I", "-c", _VERSIONS],
        check=True,
        stdout=subprocess.PIPE,
        text=True,
        timeout=60,
    )
    return json.loads(result.stdout)


@pytest.mark.performance
def test_native_web_overhead_and_failure_plateau(tmp_path):
    wheel = install_wheel(tmp_path / "wheel")
    print(
        "WEB_BUILD "
        + json.dumps(
            {
                "wheel_sha256": wheel.sha256,
                "platform": platform.platform(),
                "python": platform.python_version(),
                "cpu_count": os.cpu_count(),
                "versions": _server_versions(wheel.python),
            }
        ),
        flush=True,
    )
    runs = []
    for block, order in enumerate(RUN_ORDER):
        for mode in order:
            runs.append(
                _run(mode, MEASURE_SECONDS, wheel, tmp_path / f"{block}-{mode}")
            )
    summaries = {}
    for mode in RUN_ORDER[0]:
        matched = [run for run in runs if run["mode"] == mode]
        samples = [value for run in matched for value in run["latencies"]]
        summaries[mode] = {
            "p95_ms": _p95(samples) * 1000,
            "completed": sum(run["completed"] for run in matched),
            "latency_samples": len(samples),
            "throughput": len(samples) / sum(run["elapsed"] for run in matched),
            "rss_peak": max(run["rss_peak"] for run in matched),
            "errors": sum(run["errors"] for run in matched),
            "aborts": sum(run["aborts"] for run in matched),
        }
    baseline = summaries["disabled"]
    for summary in summaries.values():
        summary["extra_rss"] = summary["rss_peak"] - baseline["rss_peak"]
        summary["p95_increase_ms"] = summary["p95_ms"] - baseline["p95_ms"]
    budget = max(2.0, 0.05 * baseline["p95_ms"])
    print(
        "WEB_SUMMARY " + json.dumps({"budget_ms": budget, "modes": summaries}),
        flush=True,
    )
    # Collect the independent RSS result even if the latency budget fails.
    soak = _run("failing_export", SOAK_SECONDS, wheel, tmp_path / "failure-soak")
    tail = [(t, rss) for t, rss in soak["rss_samples"] if t >= 30]
    mean_time = statistics.mean(t for t, _ in tail)
    mean_rss = statistics.mean(rss for _, rss in tail)
    slope = sum((t - mean_time) * (rss - mean_rss) for t, rss in tail) / sum(
        (t - mean_time) ** 2 for t, _ in tail
    )
    early = statistics.median(rss for t, rss in soak["rss_samples"] if 10 <= t < 20)
    late = statistics.median(rss for t, rss in soak["rss_samples"] if t >= 50)
    print(
        "WEB_PLATEAU "
        + json.dumps(
            {
                "rss_coverage": soak["rss_coverage"],
                "rss_sample_count": len(soak["rss_samples"]),
                "slope_bytes_per_second": slope,
                "median_growth_bytes": late - early,
                "rss_method": (
                    "Linux resident pages"
                    if platform.system() == "Linux"
                    else "ps RSS KiB"
                ),
                "rss_sample_interval_seconds": 0.05,
            }
        ),
        flush=True,
    )
    assert slope <= 256 * 1024
    assert late - early <= 16 * 1024 * 1024
    for mode in ("enabled", "failing_export"):
        assert summaries[mode]["p95_increase_ms"] <= budget, summaries


@contextmanager
def _harness_http_server(*, errors_after_warmup=False):
    """Real HTTP boundary for harness checks, not performance acceptance data."""
    request_count = 0
    lock = threading.Lock()

    class Handler(BaseHTTPRequestHandler):
        def do_GET(self):
            nonlocal request_count
            with lock:
                request_count += 1
                status = (
                    500
                    if errors_after_warmup and request_count > WARMUP_REQUESTS
                    else 200
                )
            self.send_response(status)
            self.send_header("Content-Length", str(len(BODY)))
            self.end_headers()
            self.wfile.write(BODY)

        def log_message(self, format, *args):
            pass

    server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
    thread = threading.Thread(target=server.serve_forever)
    thread.start()
    try:
        yield server.server_port
    finally:
        server.shutdown()
        server.server_close()
        thread.join(timeout=2)
        assert not thread.is_alive()


def test_harness_reports_all_error_responses_before_failing(capsys):
    with _harness_http_server(errors_after_warmup=True) as port:
        result = _workload(port, os.getpid(), 0.05)
    assert result["completed"] == result["errors"] > 0
    assert result["aborts"] == result["latency_samples"] == 0
    assert result["p95_ms"] is None
    result["mode"] = "disabled"
    with pytest.raises(AssertionError):
        _report_and_validate_run(result)
    report = json.loads(capsys.readouterr().out.split("WEB_RUN ", 1)[1])
    assert report["completed"] == report["errors"] == result["completed"]
    assert report["latency_samples"] == 0 and report["p95_ms"] is None
    assert report["throughput"] > 0
    assert report["rss_peak"] > 0


def test_harness_reports_and_propagates_rss_reader_failure(capsys):
    with _harness_http_server() as port:
        result = _workload(port, -123456, 0.05)
    assert result["rss_errors"]
    assert any(error["phase"] == "sample" for error in result["rss_errors"])
    result["mode"] = "disabled"
    with pytest.raises(AssertionError, match="RSS sampling failed"):
        _report_and_validate_run(result)
    report = json.loads(capsys.readouterr().out.split("WEB_RUN ", 1)[1])
    assert report["rss_errors"]
    assert report["completed"] > 0


def test_harness_rejects_truncated_rss_windows():
    complete = [(step * 0.05, 1024) for step in range(1201)]
    truncated = [(timestamp, rss) for timestamp, rss in complete if timestamp <= 50.1]
    for start, end in ((10, 20), (30, 60), (50, 60)):
        assert _rss_coverage(complete, start, end)["complete"]
    for start, end in ((30, 60), (50, 60)):
        coverage = _rss_coverage(truncated, start, end)
        assert not coverage["complete"]
        assert not coverage["bracketed"]
        assert 59 in coverage["missing_one_second_intervals"]


def test_harness_reports_versions_from_given_interpreter():
    names = (
        "Django",
        "django-ninja",
        "uvicorn",
        "opentelemetry-sdk",
        "requests",
        "urllib3",
    )
    assert _server_versions(Path(sys.executable)) == {
        name: importlib.metadata.version(name) for name in names
    }
