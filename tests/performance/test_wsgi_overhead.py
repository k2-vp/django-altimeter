"""Predeclared gunicorn gthread acceptance; explicitly run in the WSGI image.

One no-preload gunicorn 23.0.0 worker with four threads; four persistent HTTP/1.1
connections request a static 1024-byte DRF response, with no request body, auth,
database work or application cache. The optional client installers are active;
combined real driver/client/log work has separate functional tests. One copied
wheel, its pinned dependencies and the same machine serve every run.

Reuse the ASGI workload unchanged: 200 warmups/fresh process, 1s settling, 10s/mode,
>=1000 completions/run, three balanced orders disabled/enabled/failing_export,
enabled/failing_export/disabled, failing_export/disabled/enabled. Retain every
latency; pool samples per mode for nearest-rank p95. Both instrumented modes
must increase p95 by no more than max(2ms, 5% of disabled p95). Report throughput,
errors/aborts, sample counts, RSS and actual server versions before assertions.

Full trace sampling and metrics; logs disabled; 250ms batch/metric intervals,
200ms export deadline, 2s shared shutdown. Native ceilings: 512 queued records,
128/batch, 16KiB admitted record, 1MiB request, 64KiB response, 2000 metric attribute
sets including overflow. Failure receiver returns 503/Retry-After:0 throughout.

Independent 60s failure soak samples the serving child's Linux resident pages
every 50ms, with the shared complete-window coverage checks. Last 30s regression
slope must be <=256KiB/s; median final 10s growth <=16MiB versus seconds 10..20. This
measures this workload's plateau, not arbitrary lifetime memory. No observer
runs in the serving process or arbiter during these measurements.
"""

import json
import os
import platform
import statistics

import pytest
from performance.test_web_overhead import (
    FAILURE_RESPONSES,
    MEASURE_SECONDS,
    RUN_ORDER,
    SOAK_SECONDS,
    _p95,
    _report_and_validate_run,
    _server_versions,
    _workload,
)
from support.gunicorn_server import install_wheel, running_server
from support.otlp_receiver import OTLPReceiver
from support.reference_work import WORK_EXTRAS

pytestmark = pytest.mark.integration


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
            observe_supervisor=False,
            extra_env={
                "ALTIMETER_INTEGRATIONS": "wsgi,django,psycopg,httpx,requests,redis",
                "REFERENCE_REQUEST_WORK": "1",
            },
        ) as server:
            status, _, body = server.request("/health/")
            assert status == 200
            worker_pid = json.loads(body)["pid"]
            assert worker_pid != server.process.pid
            result = _workload(server.port, worker_pid, duration)
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


@pytest.mark.performance
def test_wsgi_web_overhead_and_failure_plateau(tmp_path):
    wheel = install_wheel(tmp_path / "wheel", extras=WORK_EXTRAS)
    print(
        "WSGI_BUILD "
        + json.dumps(
            {
                "wheel_sha256": wheel.sha256,
                "platform": platform.platform(),
                "python": platform.python_version(),
                "cpu_count": os.cpu_count(),
                "versions": _server_versions(
                    wheel.python,
                    names=(
                        "Django",
                        "djangorestframework",
                        "gunicorn",
                        "opentelemetry-sdk",
                        "requests",
                        "urllib3",
                        "psycopg",
                        "redis",
                        "httpx",
                    ),
                ),
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
            "p50_ms": statistics.median(samples) * 1000,
            "p95_ms": _p95(samples) * 1000,
            "completed": sum(run["completed"] for run in matched),
            "latency_samples": len(samples),
            "throughput": len(samples) / sum(run["elapsed"] for run in matched),
            "rss_peak": max(run["rss_peak"] for run in matched),
            "errors": sum(run["errors"] for run in matched),
            "aborts": sum(run["aborts"] for run in matched),
        }
    baseline = summaries["disabled"]
    budget = max(2.0, 0.05 * baseline["p95_ms"])
    for summary in summaries.values():
        summary["extra_rss"] = summary["rss_peak"] - baseline["rss_peak"]
        summary["p95_increase_ms"] = summary["p95_ms"] - baseline["p95_ms"]
    print(
        "WSGI_SUMMARY " + json.dumps({"budget_ms": budget, "modes": summaries}),
        flush=True,
    )
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
        "WSGI_PLATEAU "
        + json.dumps(
            {
                "rss_coverage": soak["rss_coverage"],
                "rss_sample_count": len(soak["rss_samples"]),
                "slope_bytes_per_second": slope,
                "median_growth_bytes": late - early,
                "rss_method": "serving child Linux resident pages"
                if platform.system() == "Linux"
                else "serving child ps RSS KiB",
                "rss_sample_interval_seconds": 0.05,
            }
        ),
        flush=True,
    )
    assert slope <= 256 * 1024
    assert late - early <= 16 * 1024 * 1024
    for mode in ("enabled", "failing_export"):
        assert summaries[mode]["p95_increase_ms"] <= budget, summaries
