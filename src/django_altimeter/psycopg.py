"""Bounded driver spans on psycopg connections created after explicit setup.

Use psycopg's public cursor factories; never format queries, iterate parameters,
add SQL comments, or instrument a connection that predates installation.
"""

from __future__ import annotations

import os
from collections.abc import Mapping
from contextlib import AbstractContextManager, nullcontext
from dataclasses import dataclass
from functools import wraps
from importlib import import_module
from threading import Lock
from typing import TYPE_CHECKING, Any

from opentelemetry import trace

from django_altimeter.context import _diagnostic, client_span
from django_altimeter.privacy import is_allowed_attribute, normalize_span_name

if TYPE_CHECKING:
    from django_altimeter.runtime import ProcessRuntime

_install_lock = Lock()


@dataclass(frozen=True)
class DriverSetupResult:
    installed: bool = False
    codes: tuple[str, ...] = ()


def _operation(query: object, runtime: ProcessRuntime) -> str:
    limits = runtime.config.limits
    if type(query) is bytes:
        if len(query) > limits.max_string_bytes:
            return "DB"
        try:
            query = query.decode("utf-8")
        except UnicodeDecodeError:
            return "DB"
    # Composable objects belong to the driver. Inspecting or formatting one here
    # could adapt application values a second time or call arbitrary user code.
    if type(query) is not str:
        return "DB"
    name = normalize_span_name(query, policy=runtime.config.policy, limits=limits)
    return (
        name
        if is_allowed_attribute(
            "db.operation.name", name, policy=runtime.config.policy, limits=limits
        )
        else "DB"
    )


def _query_context(
    runtime: ProcessRuntime, args: tuple[Any, ...], kwargs: Mapping[str, Any]
) -> AbstractContextManager[trace.Span]:
    try:
        from opentelemetry.instrumentation.utils import is_instrumentation_enabled

        from django_altimeter.runtime import get_owned_trace_runtime

        if get_owned_trace_runtime() is not runtime or not is_instrumentation_enabled():
            return nullcontext(trace.INVALID_SPAN)
        name = _operation(args[0] if args else kwargs.get("query"), runtime)
        return client_span(
            name,
            attributes={"db.system.name": "postgresql", "db.operation.name": name},
        )
    except Exception:
        _diagnostic("driver.psycopg_span_failed")
        return nullcontext(trace.INVALID_SPAN)


def _cursor_factory(
    base: type[Any], runtime: ProcessRuntime, *, asynchronous: bool
) -> type[Any]:
    # Reused factories and subclasses already execute through this runtime's
    # wrapper. Preserve them instead of adding another observation layer.
    if getattr(base, "_altimeter_cursor_runtime", None) is runtime:
        return base
    # No global class cache retains application connection types.
    if asynchronous:

        class AsyncCursor(base):  # type: ignore[misc]
            _altimeter_cursor_runtime = runtime

            async def execute(self, *args: Any, **kwargs: Any) -> Any:
                with _query_context(runtime, args, kwargs):
                    return await super().execute(*args, **kwargs)

            async def executemany(self, *args: Any, **kwargs: Any) -> Any:
                with _query_context(runtime, args, kwargs):
                    return await super().executemany(*args, **kwargs)

        return AsyncCursor

    class Cursor(base):  # type: ignore[misc]
        _altimeter_cursor_runtime = runtime

        def execute(self, *args: Any, **kwargs: Any) -> Any:
            with _query_context(runtime, args, kwargs):
                return super().execute(*args, **kwargs)

        def executemany(self, *args: Any, **kwargs: Any) -> Any:
            with _query_context(runtime, args, kwargs):
                return super().executemany(*args, **kwargs)

    return Cursor


def _instrument_connection(
    connection: Any, runtime: ProcessRuntime, *, asynchronous: bool
) -> None:
    try:
        from django_altimeter.runtime import get_owned_trace_runtime

        if (
            get_owned_trace_runtime() is not runtime
            or getattr(connection, "_altimeter_runtime", None) is runtime
        ):
            return
        cursor = _cursor_factory(
            connection.cursor_factory, runtime, asynchronous=asynchronous
        )
        server_cursor = _cursor_factory(
            connection.server_cursor_factory, runtime, asynchronous=asynchronous
        )
        connection.cursor_factory = cursor
        connection.server_cursor_factory = server_cursor
        connection._altimeter_runtime = runtime
    except Exception:
        _diagnostic("driver.psycopg_connection_failed")


def install(runtime: ProcessRuntime) -> DriverSetupResult:
    """Install once on the declared owned writer; return only fixed local codes."""
    try:
        codes = (
            ("setup.psycopg2_unsupported",)
            if "psycopg2" in runtime.config.integrations
            else ()
        )
        if "psycopg" not in runtime.config.integrations:
            return DriverSetupResult(codes=codes)
        if (
            runtime.pid != os.getpid()
            or "traces" not in runtime.result.enabled_signals
            or trace.get_tracer_provider() is not runtime.tracer_provider
        ):
            return DriverSetupResult(codes=codes + ("setup.psycopg_unowned",))
        try:
            native: Any = import_module("psycopg")
        except ImportError:
            return DriverSetupResult(codes=codes + ("setup.psycopg_missing",))
        with _install_lock:
            module_connect = native.connect
            sync_connect = native.Connection.connect
            async_connect = native.AsyncConnection.connect
            if getattr(sync_connect, "_altimeter_runtime", None) is runtime:
                return DriverSetupResult(True, codes)
            if (
                module_connect != sync_connect
                or hasattr(sync_connect, "__wrapped__")
                or hasattr(async_connect, "__wrapped__")
                or not hasattr(sync_connect, "__func__")
                or not hasattr(async_connect, "__func__")
            ):
                return DriverSetupResult(codes=codes + ("setup.psycopg_conflict",))
            original_sync = sync_connect.__func__
            original_async = async_connect.__func__

            @wraps(original_sync)
            def connect(cls: Any, *args: Any, **kwargs: Any) -> Any:
                connection = original_sync(cls, *args, **kwargs)
                _instrument_connection(connection, runtime, asynchronous=False)
                return connection

            @wraps(original_async)
            async def connect_async(cls: Any, *args: Any, **kwargs: Any) -> Any:
                connection = await original_async(cls, *args, **kwargs)
                _instrument_connection(connection, runtime, asynchronous=True)
                return connection

            connect._altimeter_runtime = runtime  # type: ignore[attr-defined]
            native.Connection.connect = classmethod(connect)
            native.AsyncConnection.connect = classmethod(connect_async)
            native.connect = native.Connection.connect
        return DriverSetupResult(True, codes)
    except Exception:
        return DriverSetupResult(codes=("setup.psycopg_failed",))
