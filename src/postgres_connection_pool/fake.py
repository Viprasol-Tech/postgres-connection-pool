"""An in-memory fake connection and factory for offline tests and the CLI demo.

These types stand in for a real database driver so the pool can be exercised
without a network, credentials, or a running Postgres instance. They model the
small surface the pool relies on: ``ping()`` for health checks and ``close()`` for
teardown.

Part of Postgres Connection Pool by Viprasol Tech Private Limited (https://viprasol.com).
"""

from __future__ import annotations

import itertools
from collections.abc import Callable
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
        # Transaction bookkeeping, exercised by the transaction helper tests.
        self.executed: list[str] = []
        self.commits = 0
        self.rollbacks = 0

    def ping(self) -> bool:
        """Return ``True`` if the connection is alive and not closed."""
        return self._alive and not self.closed

    def execute(self, sql: str) -> str:
        """Record and "run" a statement, returning the SQL for easy assertions.

        Raises:
            RuntimeError: If the connection has been closed or killed.
        """
        if self.closed:
            raise RuntimeError("cannot execute on a closed connection")
        if not self._alive:
            raise RuntimeError("cannot execute on a dead connection")
        self.executed.append(sql)
        return sql

    def commit(self) -> None:
        """Record a successful transaction commit."""
        self.commits += 1

    def rollback(self) -> None:
        """Record a transaction rollback."""
        self.rollbacks += 1

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


@dataclass
class FlakyConnectionFactory:
    """A factory that fails its first ``fail_times`` calls, then succeeds.

    Handy for exercising retry-on-acquire and the circuit breaker without any real
    I/O. Each failure raises :class:`ConnectionError`; once the failure budget is
    spent, it behaves like :class:`FakeConnectionFactory`.

    Attributes:
        fail_times: How many leading calls should raise before the factory recovers.
        exc_factory: Builds the exception raised on a failing call. Defaults to
            ``ConnectionError``.
        attempts: Total number of times the factory has been called.
        created: Successfully created connections, in creation order.
    """

    fail_times: int = 0
    exc_factory: Callable[[int], Exception] = field(
        default=lambda attempt: ConnectionError(f"simulated connect failure #{attempt}")
    )
    attempts: int = 0
    created: list[FakeConnection] = field(default_factory=list)
    _counter: itertools.count[int] = field(default_factory=lambda: itertools.count(1), repr=False)

    def __call__(self) -> FakeConnection:
        """Raise until the failure budget is spent, then create a connection."""
        self.attempts += 1
        if self.attempts <= self.fail_times:
            raise self.exc_factory(self.attempts)
        conn = FakeConnection(next(self._counter))
        self.created.append(conn)
        return conn
