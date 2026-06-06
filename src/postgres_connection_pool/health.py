"""Health checks used to validate a connection before it is handed out.

A :data:`HealthCheck` is any callable that takes a connection and returns ``True``
if the connection is still usable. When a check returns ``False`` (or raises), the
pool discards the dead connection and transparently creates a fresh one, so callers
never receive a broken connection.

Part of Postgres Connection Pool by Viprasol Tech Private Limited (https://viprasol.com).
"""

from __future__ import annotations

from collections.abc import Callable
from typing import Protocol, TypeVar, runtime_checkable

C = TypeVar("C")

#: A predicate that returns ``True`` when ``conn`` is healthy, ``False`` otherwise.
HealthCheck = Callable[[C], bool]


@runtime_checkable
class Pingable(Protocol):
    """A connection that can report its own liveness via ``ping()``.

    Real database drivers rarely expose exactly this method, so the pool accepts
    any callable health check. :class:`Pingable` is provided as a convenient shape
    for the bundled :func:`ping_healthy` check and for the in-memory fakes used in
    tests.
    """

    def ping(self) -> bool:
        """Return ``True`` if the connection is alive."""
        ...


def always_healthy(conn: object) -> bool:
    """Treat every connection as healthy.

    This is the default check: it performs no validation and is appropriate when
    the underlying driver already guarantees liveness or when validation is too
    expensive to run on every acquire.

    Args:
        conn: The connection to validate (ignored).

    Returns:
        Always ``True``.
    """
    return True


def ping_healthy(conn: object) -> bool:
    """Validate a connection by calling its ``ping()`` method.

    A connection is considered healthy when it exposes a callable ``ping()`` that
    returns a truthy value. Any exception raised by ``ping()`` is swallowed and
    reported as *unhealthy* so the pool can recycle the connection instead of
    propagating a low-level driver error to the caller.

    Args:
        conn: The connection to validate. Expected to satisfy :class:`Pingable`.

    Returns:
        ``True`` if ``conn.ping()`` returns a truthy value, ``False`` otherwise.
    """
    ping = getattr(conn, "ping", None)
    if not callable(ping):
        return False
    try:
        return bool(ping())
    except Exception:
        return False
