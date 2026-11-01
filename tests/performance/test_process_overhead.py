"""Predeclared process-collection overhead on the existing real web profiles.

One uvicorn asyncio/h11 process or one gunicorn gthread worker with four
threads; four persistent HTTP/1.1 connections, static 1024-byte response, no
request body/auth/database/cache. Reuse the existing web workload: 200 warmups,
1s settle, 10s measured, >=1000 successful completions/run. Four paired orders:
off/on, on/off, on/off, off/on. Keep every run and every successful latency.
Only process collection changes: web traces (sampling 1) and metrics stay on;
logs off. One copied wheel, pinned dependencies and machine per profile.

Pool individual latency samples per mode; nearest-rank p95 increase must be
<=max(2ms, 5% of process-off p95), the existing web budget. Report median p50,
throughput, counts/errors/aborts, serving-process RSS and extra peak RSS; no
additional throughput/RSS threshold. No observer in the serving process.
Linux resident-page samples every50ms, complete/bracketed measurement windows,
no sampling errors. Zero samples, <1000 completions or any HTTP error/abort
invalidates the run. All reports print before acceptance assertions.

Both modes must retain exact HTTP request/histogram totals and real SERVER
spans. On must export positive CPU/RSS/thread snapshots with the serving UUID;
off must export no process instruments. Collection/export interval250ms,
deadline200ms, shutdown2s, queues512/batch128, record16KiB, request1MiB,
response64KiB; process instruments admit14 fixed series/writer, no labels
from requests. Run only after the implementation lands, in each matching image.
Evidence class: local real-process; original machine Apple M5 Max/128GiB,
Linux arm64 verification images. No threshold or workload retuning after runs.
"""

import json
import statistics

import pytest
from performance.test_web_overhead import (
    WARMUP_REQUESTS,
    _p95,
    _server_versions,
    _workload,
)
from support.otlp_receiver import OTLPReceiver
from support.process_web import (
    PROCESS_NAMES,
    http_counts,
    install_process_wheel,
    metric_points,
    running_process_web,
    server_spans,
    web_profile,
)

RUN_ORDER = (("off", "on"), ("on", "off"), ("on", "off"), ("off", "on"))
MEASURE_SECONDS = 10.0


def _run(workspace, wheel, mode):
    with OTLPReceiver() as receiver:
        with running_process_web(
            workspace, wheel=wheel, receiver=receiver, process_enabled=mode == "on"
        ) as (server, health):
            result = _workload(server.port, health["pid"], MEASURE_SECONDS)
        points = metric_points(receiver)
        result.update(
            mode=mode,
            instance_id=health["instance_id"],
            http_counts=http_counts(receiver),
            server_spans=len(server_spans(receiver)),
            process_points=[
                {
                    "name": metric.name,
                    "value": point.as_int
                    if point.WhichOneof("value") == "as_int"
                    else point.as_double,
                    "instance_id": next(
                        (
                            item.value.string_value
                            for item in resource.attributes
                            if item.key == "service.instance.id"
                        ),
                        None,
                    ),
                }
                for resource, metric, point in points
                if metric.name in PROCESS_NAMES
            ],
        )
        return result


def _validate_run(result):
    assert result["completed"] >= 1000
    assert result["latency_samples"] == result["completed"]
    assert result["errors"] == result["aborts"] == 0
    assert not result["rss_errors"] and not result["rss_sampler_alive"]
    assert all(window["complete"] for window in result["rss_coverage"].values())
    expected = result["completed"] + WARMUP_REQUESTS
    assert result["http_counts"] == {
        "altimeter.http.server.requests": expected,
        "http.server.request.duration": expected,
    }
    assert result["server_spans"] > 0
    assert result["instance_id"] is not None
    if result["mode"] == "on":
        for name in (
            "process.cpu.time",
            "process.memory.usage",
            "process.thread.count",
        ):
            assert any(
                point["name"] == name and point["value"] > 0
                for point in result["process_points"]
            )
        assert all(
            point["instance_id"] == result["instance_id"]
            for point in result["process_points"]
        )
    else:
        assert result["process_points"] == []


@pytest.mark.integration
@pytest.mark.performance
def test_process_collection_overhead(tmp_path):
    wheel = install_process_wheel(tmp_path / "wheel")
    print(
        "PROCESS_BUILD "
        + json.dumps(
            {
                "wheel_sha256": wheel.sha256,
                "profile": web_profile(),
                "versions": _server_versions(
                    wheel.python, names=("Django", "opentelemetry-sdk", "psutil")
                ),
            }
        ),
        flush=True,
    )
    reports = []
    for repetition, order in enumerate(RUN_ORDER):
        for mode in order:
            report = _run(tmp_path / f"{repetition}-{mode}", wheel, mode)
            report["repetition"] = repetition
            reports.append(report)
            print(
                "PROCESS_RUN "
                + json.dumps(
                    {
                        key: value
                        for key, value in report.items()
                        if key not in ("latencies", "rss_samples", "process_points")
                    }
                    | {"process_point_count": len(report["process_points"])}
                ),
                flush=True,
            )
    summary = {}
    for mode in ("off", "on"):
        matching = [report for report in reports if report["mode"] == mode]
        latencies = [value for report in matching for value in report["latencies"]]
        summary[mode] = {
            "completed": sum(report["completed"] for report in matching),
            "samples": len(latencies),
            "p50_ms": statistics.median(latencies) * 1000 if latencies else None,
            "p95_ms": _p95(latencies) * 1000 if latencies else None,
            "throughput": len(latencies)
            / sum(report["elapsed"] for report in matching),
            "rss_peak": max(
                (
                    report["rss_peak"]
                    for report in matching
                    if report["rss_peak"] is not None
                ),
                default=None,
            ),
            "errors": sum(report["errors"] for report in matching),
            "aborts": sum(report["aborts"] for report in matching),
        }
    baseline = summary["off"]["p95_ms"]
    budget = max(2, 0.05 * baseline) if baseline is not None else None
    increase = (
        summary["on"]["p95_ms"] - baseline
        if baseline is not None and summary["on"]["p95_ms"] is not None
        else None
    )
    extra_rss = (
        summary["on"]["rss_peak"] - summary["off"]["rss_peak"]
        if all(summary[mode]["rss_peak"] is not None for mode in ("off", "on"))
        else None
    )
    print(
        "PROCESS_SUMMARY "
        + json.dumps(
            {
                "modes": summary,
                "budget_ms": budget,
                "increase_ms": increase,
                "extra_rss_bytes": extra_rss,
            }
        ),
        flush=True,
    )
    for report in reports:
        _validate_run(report)
    assert increase is not None and budget is not None and increase <= budget


def _valid_report(mode):
    """Unit-only inputs for validity checks, never measurement evidence."""
    return {
        "mode": mode,
        "completed": 1000,
        "latency_samples": 1000,
        "errors": 0,
        "aborts": 0,
        "rss_errors": [],
        "rss_sampler_alive": False,
        "rss_coverage": {"measurement": {"complete": True}},
        "http_counts": {
            "altimeter.http.server.requests": 1000 + WARMUP_REQUESTS,
            "http.server.request.duration": 1000 + WARMUP_REQUESTS,
        },
        "server_spans": 1000,
        "instance_id": "writer-1",
        "process_points": [
            {"name": name, "value": 1, "instance_id": "writer-1"}
            for name in (
                "process.cpu.time",
                "process.memory.usage",
                "process.thread.count",
            )
        ]
        if mode == "on"
        else [],
    }


@pytest.mark.parametrize("mode", ["off", "on"])
def test_harness_accepts_complete_matching_mode(mode):
    _validate_run(_valid_report(mode))


@pytest.mark.parametrize(
    "fault",
    [
        "missing_process",
        "wrong_writer",
        "lost_http",
        "zero_samples",
        "rss_failure",
        "rss_gap",
        "active_when_off",
    ],
)
def test_harness_rejects_invalid_measurements(fault):
    report = _valid_report("on")
    if fault == "missing_process":
        report["process_points"] = []
    elif fault == "wrong_writer":
        report["process_points"][0]["instance_id"] = "other-writer"
    elif fault == "lost_http":
        report["http_counts"]["altimeter.http.server.requests"] -= 1
    elif fault == "zero_samples":
        report["latency_samples"] = 0
    elif fault == "rss_failure":
        report["rss_errors"] = ["read_failed"]
    elif fault == "rss_gap":
        report["rss_coverage"]["measurement"]["complete"] = False
    elif fault == "active_when_off":
        report["mode"] = "off"
    with pytest.raises(AssertionError):
        _validate_run(report)
