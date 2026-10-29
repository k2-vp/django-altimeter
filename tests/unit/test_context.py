"""Unit evidence for bounded input before the native SDK reads attributes."""

import tracemalloc
from collections.abc import Mapping, Sequence
from dataclasses import FrozenInstanceError, replace

import pytest
from opentelemetry.sdk.resources import Resource
from opentelemetry.sdk.trace import TracerProvider

from django_altimeter.context import Correlation, _bounded_attributes
from django_altimeter.privacy import DEFAULT_LIMITS


class HostileSequence(Sequence):
    accesses = []

    def __len__(self):
        self.accesses.append("len")
        raise AssertionError("User accessor was called")

    def __getitem__(self, key):
        self.accesses.append("getitem")
        raise AssertionError("User accessor was called")

    def __iter__(self):
        self.accesses.append("iter")
        raise AssertionError("User accessor was called")

    def __str__(self):
        self.accesses.append("str")
        raise AssertionError("User accessor was called")


class HostileMapping(Mapping):
    accesses = []

    def __len__(self):
        self.accesses.append("len")
        raise AssertionError("User accessor was called")

    def __getitem__(self, key):
        self.accesses.append("getitem")
        raise AssertionError("User accessor was called")

    def __iter__(self):
        self.accesses.append("iter")
        raise AssertionError("User accessor was called")


class HostileDict(dict):
    accesses = []

    def __len__(self):
        self.accesses.append("len")
        raise AssertionError("User accessor was called")

    def items(self):
        self.accesses.append("items")
        raise AssertionError("User accessor was called")


class HostileString(str):
    accesses = []

    def __len__(self):
        self.accesses.append("len")
        raise AssertionError("User accessor was called")

    def encode(self, *args, **kwargs):
        self.accesses.append("encode")
        raise AssertionError("User accessor was called")


@pytest.fixture(autouse=True)
def clear_accesses():
    for kind in (HostileSequence, HostileMapping, HostileDict, HostileString):
        kind.accesses.clear()
    yield
    for kind in (HostileSequence, HostileMapping, HostileDict, HostileString):
        assert kind.accesses == []


@pytest.mark.parametrize(
    "value",
    [
        HostileSequence(),
        HostileMapping(),
        HostileDict(),
        HostileString("secret"),
        object(),
        b"secret",
        ["secret"],
        ("secret",),
        {"secret": "value"},
        None,
        float("nan"),
        float("inf"),
        float("-inf"),
        2**63,
        -(2**63) - 1,
        1 << 100_000,
        "x" * 257,
        "\N{GRINNING FACE}" * 65,
        "\ud800",
    ],
    ids=[
        "hostile-sequence",
        "hostile-mapping",
        "dict-subclass",
        "str-subclass",
        "object",
        "bytes",
        "list",
        "tuple",
        "dict",
        "none",
        "nan",
        "inf",
        "negative-inf",
        "integer-overflow",
        "integer-underflow",
        "huge-int",
        "long-ascii",
        "long-utf8",
        "surrogate",
    ],
)
def test_bad_value_never_reaches_native_provider_and_safe_neighbor_survives(value):
    provider = TracerProvider(resource=Resource({}), shutdown_on_exit=False)
    attributes = {"http.response.status_code": 200, "candidate": value}
    try:
        with provider.get_tracer("django_altimeter").start_as_current_span(
            "work.execute", attributes=_bounded_attributes(attributes, DEFAULT_LIMITS)
        ) as current:
            assert current.get_span_context().is_valid
            assert current.attributes == {"http.response.status_code": 200}
    finally:
        provider.shutdown()


@pytest.mark.parametrize("attributes", [HostileMapping(), HostileDict(), []])
def test_unknown_attribute_mapping_is_not_inspected(attributes):
    assert _bounded_attributes(attributes, DEFAULT_LIMITS) == {}
    assert _bounded_attributes({"duration_ms": 1.25}, DEFAULT_LIMITS) == {
        "duration_ms": 1.25
    }


def test_exact_scalar_boundaries_are_retained():
    attributes = {
        "text": "\N{GRINNING FACE}" * 64,
        "bool": True,
        "minimum": -(2**63),
        "maximum": 2**63 - 1,
        "float": 1.25,
        "empty": "",
    }
    result = _bounded_attributes(attributes, DEFAULT_LIMITS)
    assert result == attributes
    assert result is not attributes


@pytest.mark.parametrize(
    "key",
    ["", "x" * 129, "\N{GRINNING FACE}" * 33, "\ud800", 7, HostileString("key")],
    ids=["empty", "ascii-size", "utf8-size", "surrogate", "integer", "str-subclass"],
)
def test_invalid_keys_drop_only_their_value(key):
    assert _bounded_attributes(
        {key: HostileSequence(), "safe": True}, DEFAULT_LIMITS
    ) == {"safe": True}


def test_lowered_limits_and_oversized_mapping_keep_traversal_bounded():
    limits = replace(
        DEFAULT_LIMITS, max_attributes=2, max_key_bytes=4, max_string_bytes=4
    )
    assert _bounded_attributes({"a": "\N{GRINNING FACE}", "b": "abcd"}, limits) == {
        "a": "\N{GRINNING FACE}",
        "b": "abcd",
    }
    assert _bounded_attributes({"a": "abcde", "b": "ok"}, limits) == {"b": "ok"}
    assert _bounded_attributes({"a": 1, "b": 2, "c": HostileSequence()}, limits) == {}
    assert _bounded_attributes({"safe": True}, replace(limits, max_attributes=0)) == {}


def test_oversized_values_are_not_copied_before_rejection():
    peaks = []
    for size in (1_024, 1024 * 1024, 8 * 1024 * 1024):
        attributes = {"safe": 200, "string": "x" * size, "list": [1] * size}
        tracemalloc.start()
        try:
            result = _bounded_attributes(attributes, DEFAULT_LIMITS)
            _, peak = tracemalloc.get_traced_memory()
            assert result == {"safe": 200}
            peaks.append(peak)
        finally:
            tracemalloc.stop()
    assert max(peaks) < 32 * 1024, peaks


def test_correlation_is_a_frozen_value():
    correlation = Correlation("1" * 32, "2" * 16, 1)
    with pytest.raises(FrozenInstanceError):
        correlation.trace_id = "3" * 32
    assert correlation == Correlation("1" * 32, "2" * 16, 1)


def test_reserved_identity_keys_cannot_be_supplied_as_span_attributes():
    assert _bounded_attributes(
        {
            "posthogDistinctId": "untrusted",
            "altimeter.tenant_id": "untrusted",
            "safe": 200,
        },
        DEFAULT_LIMITS,
    ) == {"safe": 200}


def test_numeric_subclasses_are_rejected_without_conversion_or_comparison():
    calls = []

    class CustomInt(int):
        def __int__(self):
            calls.append("int")
            raise AssertionError("User conversion was called")

        def __le__(self, other):
            calls.append("compare")
            raise AssertionError("User comparison was called")

    class CustomFloat(float):
        def __float__(self):
            calls.append("float")
            raise AssertionError("User conversion was called")

    for value in (CustomInt(1), CustomFloat(1.0)):
        assert _bounded_attributes({"bad": value, "safe": True}, DEFAULT_LIMITS) == {
            "safe": True
        }
    assert calls == []


def test_identity_is_frozen_and_does_not_display_opaque_values():
    from django_altimeter.context import Identity

    identity = Identity("opaque-person", "opaque-tenant")
    with pytest.raises(FrozenInstanceError):
        identity.tenant_id = "replacement"
    assert identity == Identity("opaque-person", "opaque-tenant")
    assert "opaque" not in repr(identity)


def test_inactive_span_does_not_retain_rejected_inputs():
    import weakref

    from django_altimeter.context import span

    payload = HostileSequence()
    reference = weakref.ref(payload)
    attributes = {"bad": payload}
    del payload
    with span("unregistered.operation", attributes=attributes):
        del attributes
        assert reference() is None, "Rejected input retained across the body"
