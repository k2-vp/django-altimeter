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
