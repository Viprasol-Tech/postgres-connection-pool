"""An in-memory fake connection and factory for offline tests and the CLI demo.

These types stand in for a real database driver so the pool can be exercised
without a network, credentials, or a running Postgres instance. They model the
small surface the pool relies on: ``ping()`` for health checks and ``close()`` for
teardown.

Part of Postgres Connection Pool by Viprasol Tech Private Limited (https://viprasol.com).
"""

from __future__ import annotations

import itertools
from dataclasses import dataclass, field


class FakeConnection:
    """A lightweight stand-in for a real database connection.

    Attributes:
        conn_id: A monotonically increasing identifier, unique per process, handy
            for asserting reuse in tests and for human-readable demo output.

    Args:
        conn_id: The identifier assigned to this connection.
        alive: Whether the connection currently reports itself healthy.
    """

    def __init__(self, conn_id: int, *, alive: bool = True) -> None:
        self.conn_id = conn_id
        self._alive = alive
        self.closed = False

    def ping(self) -> bool:
        """Return ``True`` if the connection is alive and not closed."""
        return self._alive and not self.closed

    def kill(self) -> None:
        """Simulate the server dropping the connection.

        Subsequent :meth:`ping` calls return ``False``, so the pool will discard
        and recreate this connection on the next acquire.
        """
        self._alive = False

    def close(self) -> None:
        """Mark the connection as closed and release its (fake) resources."""
        self.closed = True

    def __repr__(self) -> str:
        state = "closed" if self.closed else ("alive" if self._alive else "dead")
        return f"FakeConnection(id={self.conn_id}, {state})"


@dataclass
class FakeConnectionFactory:
    """A callable factory that hands out :class:`FakeConnection` instances.

    Use it as the ``factory`` argument to
    :class:`~postgres_connection_pool.pool.ConnectionPool`. Each call returns a new
    connection with a fresh, incrementing ``conn_id``.

    Attributes:
        created: Every connection this factory has produced, in creation order.
            Useful for assertions in tests.
    """

    created: list[FakeConnection] = field(default_factory=list)
    _counter: itertools.count[int] = field(default_factory=lambda: itertools.count(1), repr=False)

    def __call__(self) -> FakeConnection:
        """Create and record a new fake connection."""
        conn = FakeConnection(next(self._counter))
        self.created.append(conn)
        return conn
