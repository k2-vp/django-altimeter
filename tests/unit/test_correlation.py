"""Immutable reporter snapshots and provider-less context isolation."""

from concurrent.futures import ThreadPoolExecutor
from dataclasses import FrozenInstanceError, asdict

import pytest
from opentelemetry import trace

from django_altimeter.context import Correlation, get_correlation


def test_snapshot_is_immutable_plain_data_for_existing_reporters():
    snapshot = Correlation("1" * 32, "2" * 16, 1)
    assert asdict(snapshot) == {
        "trace_id": "1" * 32,
        "span_id": "2" * 16,
        "trace_flags": 1,
    }
    with pytest.raises(FrozenInstanceError):
        snapshot.trace_id = "3" * 32


def test_reporter_uses_passed_snapshot_without_current_context_lookup():
    snapshot = Correlation("1" * 32, "2" * 16, 0)
    foreign = trace.NonRecordingSpan(trace.SpanContext(3, 4, False))

    def report(captured):
        with trace.use_span(foreign):
            assert get_correlation() is None
            return asdict(captured)

    with ThreadPoolExecutor(max_workers=1) as reporter:
        assert reporter.submit(report, snapshot).result(timeout=2) == asdict(snapshot)
    assert get_correlation() is None


def test_logging_import_does_not_start_threads_or_network():
    import subprocess
    import sys
    import textwrap

    result = subprocess.run(
        [
            sys.executable,
            "-c",
            textwrap.dedent("""
            import sys
            import threading
            starts = []
            connects = []
            original = threading.Thread.start
            def start(self, *args, **kwargs):
                starts.append(self.name)
                return original(self, *args, **kwargs)
            threading.Thread.start = start
            def audit(event, args):
                if event == 'socket.connect':
                    connects.append(event)
            sys.addaudithook(audit)
            import django_altimeter.logging
            assert starts == [], starts
            assert connects == [], connects
            assert 'opentelemetry.sdk._logs' not in sys.modules
        """),
        ],
        capture_output=True,
        text=True,
        timeout=10,
        check=False,
    )
    print(result.stderr, end="")
    assert result.returncode == 0, result.stdout + result.stderr
