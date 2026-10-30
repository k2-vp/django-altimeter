"""Real Django metadata hooks; server behavior is proved by H1 integration."""

import os
import subprocess
import sys

import pytest

_MIDDLEWARE_PROBE = r"""
import asyncio
import json
import sys
from dataclasses import replace
from time import monotonic

from asgiref.sync import iscoroutinefunction, sync_to_async
from django.conf import settings
settings.configure(INSTALLED_APPS=[], DEFAULT_CHARSET="utf-8", SECRET_KEY="unit-only")
import django as framework
framework.setup()
from django.http import HttpResponse, StreamingHttpResponse
from django.test import AsyncRequestFactory, RequestFactory
from django.urls import path, resolve
from django.utils.functional import SimpleLazyObject


from django_altimeter import django
from django_altimeter.conf import Config
from django_altimeter.http import RequestState, request_state
from django_altimeter.privacy import PrivacyPolicy

mode, case = sys.argv[1:]
canary = "request-secret-canary"
route = "items/<str:item_id>/"
policy = PrivacyPolicy(attribute_values=frozenset({("http.route", route)}))
config = Config(policy=policy, excluded_routes=frozenset({"/health/"}))
state = RequestState(monotonic(), "GET", config=config)
request = (AsyncRequestFactory() if case == "scope" else RequestFactory()).get(
    "/items/" + canary + "/?secret=" + canary
)
request.resolver_match = resolve(
    "/items/" + canary + "/",
    urlconf=(path(route, lambda request, item_id: None),),
)
def forbidden_user():
    raise AssertionError("user accessor was invoked")
request.user = SimpleLazyObject(forbidden_user)
if case == "unresolved":
    request.resolver_match = None
elif case == "unapproved":
    config = replace(config, policy=PrivacyPolicy())
    state.config = config
elif case == "excluded":
    state.excluded = True  # The native entry boundary owns static exclusions.
    request.resolver_match = resolve(
        "/health/", urlconf=(path("health/", lambda r: None),)
    )
elif case == "finalized":
    state.finalized = True
elif case == "no_config":
    state.config = None
elif case == "oversized":
    request.resolver_match.route = "x" * 1_000_000
elif case == "fault":
    class BrokenResolver:
        @property
        def route(self):
            raise RuntimeError(canary)
    request.resolver_match = BrokenResolver()

class GuardedRequest(type(request)):
    def __getattribute__(self, name):
        if name in {"user", "path", "path_info", "body", "GET", "POST", "COOKIES"}:
            raise AssertionError("sensitive request accessor was invoked")
        return super().__getattribute__(name)
request.__class__ = GuardedRequest

consumed = []
def chunks():
    consumed.append("started")
    yield b"first"
    yield b"second"
async def async_chunks():
    consumed.append("started")
    yield b"first"
    yield b"second"
streaming = case == "streaming"
response = (
    StreamingHttpResponse(async_chunks() if mode == "async" else chunks())
    if streaming else HttpResponse(
        b"x" * 262144 if case == "large_buffered" else b"safe",
        status=404 if case == "unresolved" else 200,
    )
)
calls = []
app_error = asyncio.CancelledError() if case == "cancel" else RuntimeError(canary)
def app(request):
    calls.append(request)
    assert state.response_ready_at is None
    if case in {"exception", "cancel"}:
        raise app_error
    return response
async def async_app(request):
    await asyncio.sleep(0)
    if mode == "bridge":
        return await sync_to_async(app)(request)
    return app(request)

factory = django.RequestMetadataMiddleware
middleware = factory(async_app if mode == "async" else app)
async def bridge_call():
    return await sync_to_async(middleware)(request)
if case == "scope":
    request.scope["django_altimeter.request_state"] = state
    token = request_state.set(None)
elif case in {"unowned", "finalized"}:
    token = request_state.set(None)
else:
    token = request_state.set(state)
before = monotonic()
try:
    try:
        actual = (
            middleware(request) if mode == "sync"
            else asyncio.run(bridge_call() if mode == "bridge" else middleware(request))
        )
    except (RuntimeError, asyncio.CancelledError) as caught:
        assert case in {"exception", "cancel"}
        assert caught is app_error
        assert state.response_ready_at is None
    else:
        assert case not in {"exception", "cancel"}
        assert actual is response
        assert consumed == []
        if case in {"unowned", "finalized"}:
            assert state.response_ready_at is None and state.route is None
        elif case != "fault":
            assert state.response_ready_at is not None
            assert before <= state.response_ready_at <= monotonic()
            assert state.response_mode == ("streaming" if streaming else "buffered")
            assert state.resolved is (case != "unresolved")
            assert state.excluded is (case == "excluded")
            assert state.route == (
                None if case in {
                    "unresolved", "unapproved", "excluded", "oversized", "no_config"
                }
                else route
            )
            assert canary not in repr(state)
    assert len(calls) == 1
    if mode == "async":
        assert iscoroutinefunction(middleware)
finally:
    request_state.reset(token)
assert request_state.get() is None
print(json.dumps({"ok": True, "case": case, "mode": mode}))
"""


@pytest.mark.parametrize("mode", ["sync", "async", "bridge"])
@pytest.mark.parametrize(
    "case",
    [
        "matched",
        "unresolved",
        "unapproved",
        "excluded",
        "oversized",
        "finalized",
        "no_config",
        "fault",
        "streaming",
        "large_buffered",
        "scope",
        "unowned",
        "exception",
        "cancel",
    ],
)
def test_middleware_metadata_and_application_behavior(mode, case):
    result = subprocess.run(
        [sys.executable, "-c", _MIDDLEWARE_PROBE, mode, case],
        env={"PATH": os.environ.get("PATH", "")},
        capture_output=True,
        text=True,
        timeout=10,
        check=False,
    )
    assert result.returncode == 0, result.stdout + result.stderr
    assert "request-secret-canary" not in result.stdout + result.stderr
    if case == "fault":
        assert "http.metadata_failed" in result.stderr
