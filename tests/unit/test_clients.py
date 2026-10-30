"""Pure admission and import boundaries; integration tests prove real I/O."""

import subprocess
import sys

import pytest


def test_importing_clients_is_lazy_and_starts_no_work():
    result = subprocess.run(
        [
            sys.executable,
            "-c",
            """
import sys, threading
before = set(threading.enumerate())
network = []
sys.addaudithook(lambda event,args: network.append(event) if event in {
    'socket.connect','socket.bind'} else None)
import django_altimeter.clients
assert set(threading.enumerate()) == before
assert not network
assert not {'django','requests','httpx','redis'} & sys.modules.keys()
""",
        ],
        capture_output=True,
        text=True,
        check=False,
    )
    assert result.returncode == 0, result.stderr


class Hostile:
    def __len__(self):
        raise AssertionError("Must not inspect user values")

    def __str__(self):
        raise AssertionError("Must not format user values")


@pytest.mark.parametrize(
    "value", [Hostile(), "X" * 100000, b"X" * 100000, ["GET"], {"GET": 1}]
)
def test_unknown_command_never_inspects_arguments(value):
    from django_altimeter.clients import _redis_operation

    assert _redis_operation((value, Hostile())) == "DB"
    assert _redis_operation(("GET", Hostile())) == "GET"


def test_request_header_view_keeps_bounded_storage_and_never_reads_values():
    result = subprocess.run(
        [
            sys.executable,
            "-c",
            """
import tracemalloc
from django_altimeter.clients import _RequestHeaders
class KeysOnly(dict):
    def __getitem__(self, key):
        raise AssertionError('Header policy must not read application values')
    def items(self):
        raise AssertionError('Header policy must scan keys only')
    def values(self):
        raise AssertionError('Header policy must scan keys only')
source = KeysOnly({f'X-Business-{index}': object() for index in range(50000)})
source.update({'TraceParent': object(), b'Baggage': object(), 'X-Tenant-Id': object()})
tracemalloc.start()
headers = source
for _ in range(2000):
    headers = _RequestHeaders(headers, {})
assert sum(1 for _ in headers) == 50000
_, peak = tracemalloc.get_traced_memory()
assert peak < 65536, peak
""",
        ],
        capture_output=True,
        text=True,
        check=False,
        timeout=10,
    )
    assert result.returncode == 0, result.stderr


@pytest.mark.parametrize("bytes_names", [False, True])
def test_request_header_view_preserves_business_edits_and_redirect_copy(bytes_names):
    from django_altimeter.clients import _RequestHeaders

    parent_key = b"TraceParent" if bytes_names else "TraceParent"
    identity_key = b"X-Tenant-Id" if bytes_names else "X-Tenant-Id"

    class PrivateValues(dict):
        def __getitem__(self, key):
            assert key not in (parent_key, identity_key)
            return super().__getitem__(key)

    source = PrivateValues(
        {
            parent_key: Hostile(),
            identity_key: Hostile(),
            "X-Application": "original",
        }
    )
    headers = _RequestHeaders(source, {"traceparent": "owned"})
    assert headers[parent_key] == "owned"
    assert identity_key not in headers
    headers["X-Application"] = "changed"
    headers["X-Extra"] = "kept"
    assert headers.copy() == {
        "X-Application": "changed",
        "X-Extra": "kept",
        "traceparent": "owned",
    }
    del headers["X-Extra"]
    assert "X-Extra" not in source
    assert source["X-Application"] == "changed"
    renewed = _RequestHeaders(headers, {})
    assert renewed.copy() == {"X-Application": "changed"}
