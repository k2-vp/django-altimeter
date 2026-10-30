"""Explicit, process-owned OpenTelemetry setup and safe application helpers."""

from __future__ import annotations

from collections.abc import Mapping
from contextlib import AbstractContextManager
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from opentelemetry.trace import Span

    from .context import Correlation
    from .runtime import SetupResult, ShutdownResult

__all__ = ["enrich", "get_correlation", "setup", "shutdown", "span"]


def setup(*, role: str | None = None) -> SetupResult:
    """Initialize telemetry explicitly after Django setup in the serving process."""
    from .runtime import SetupResult
    from .runtime import setup as setup_runtime

    try:
        from .django import load_django_config

        result = load_django_config(role=role)
        if result.config is None:
            return SetupResult(codes=result.codes)
        return setup_runtime(result.config)
    except Exception:
        return SetupResult(codes=("setup.failed",))


def shutdown(*, timeout: float = 2.0) -> ShutdownResult:
    """Drain owned telemetry within one shared timeout across all signals."""
    from .runtime import shutdown as shutdown_runtime

    return shutdown_runtime(timeout=timeout)


def get_correlation() -> Correlation | None:
    """Return correlation only for tracing owned by this process."""
    from .context import get_correlation as current_correlation

    return current_correlation()


def span(
    name: str, *, attributes: Mapping[str, object] | None = None
) -> AbstractContextManager[Span]:
    """Start a registered span with bounded attributes; preserve application errors."""
    from .context import span as application_span

    return application_span(name, attributes=attributes)


def enrich(subject: object) -> None:
    """Resolve approved scalar identity facts at an authenticated boundary."""
    from .context import enrich as enrich_span

    enrich_span(subject)
