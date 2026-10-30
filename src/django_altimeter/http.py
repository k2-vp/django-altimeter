"""Request-local facts shared by native protocol adapters and metadata hooks."""

from contextvars import ContextVar
from dataclasses import dataclass, field

from django_altimeter.conf import Config


@dataclass(slots=True)
class RequestState:
    """Mutable lifetime state; the protocol adapter alone owns finalization.

    Times are monotonic seconds. A resolved route is an approved template,
    never a raw URL. No request, response, body or user object is retained here.
    """

    started_at: float
    method: str
    surface: str = "django"
    config: Config | None = field(default=None, repr=False)
    route: str | None = None
    resolved: bool = False
    excluded: bool = False
    status: int | None = None
    response_ready_at: float | None = None
    response_mode: str = "unknown"
    concurrent_at_start: int = 0
    finalized: bool = False


request_state: ContextVar[RequestState | None] = ContextVar(
    "django_altimeter.request_state", default=None
)


def get_request_state() -> RequestState | None:
    """Read this execution context's request state without touching Django."""
    return request_state.get()
