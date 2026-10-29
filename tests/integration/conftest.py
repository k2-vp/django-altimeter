"""Shared real-socket fixtures for integration tests."""

from collections.abc import Iterator

import pytest
from support.otlp_receiver import OTLPReceiver


@pytest.fixture
def otlp_receiver() -> Iterator[OTLPReceiver]:
    """Decode protobuf exports on loopback and stop the server after each test."""
    with OTLPReceiver() as receiver:
        yield receiver
