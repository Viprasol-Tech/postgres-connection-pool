"""A generic, thread-safe connection pool over a pluggable connection factory.

The pool is deliberately agnostic about what a "connection" is: you supply a
``factory`` callable that creates one (e.g. ``lambda: psycopg.connect(dsn)``) and,
optionally, a ``health_check`` that validates a connection before it is handed out.
This keeps the pool fully testable offline -- the test suite and the bundled CLI
demo use an in-memory :class:`~postgres_connection_pool.fake.FakeConnection` and
never touch a real database.

Key behaviours:

* **Reuse** -- idle connections are returned to callers before new ones are made.
* **Bounded** -- at most ``max_size`` connections exist at once.
* **Blocking with timeout** -- when the pool is exhausted, :meth:`acquire` waits up
  to ``timeout`` seconds for a connection to be released, then raises
  :class:`~postgres_connection_pool.errors.PoolTimeoutError`.
* **Self-healing** -- a connection that fails its health check is discarded and a
  fresh one is created in its place.

Part of Postgres Connection Pool by Viprasol Tech Private Limited (https://viprasol.com).
"""

from __future__ import annotations

import contextlib
import threading
import time
from collections import deque
from collections.abc import Callable, Iterator
from contextlib import contextmanager
from dataclasses import dataclass
from typing import Generic, TypeVar

from postgres_connection_pool.errors import PoolClosedError, PoolTimeoutError
from postgres_connection_pool.health import HealthCheck, always_healthy

C = TypeVar("C")


@dataclass(frozen=True)
class PoolStats:
    """A point-in-time snapshot of pool occupancy.

    Attributes:
        size: Total live connections owned by the pool (idle + in use).
        in_use: Connections currently checked out by callers.
        idle: Connections sitting in the pool ready to be reused.
        max_size: The configured upper bound on ``size``.
    """

    size: int
    in_use: int
    idle: int
    max_size: int


class ConnectionPool(Generic[C]):
    """A bounded, thread-safe pool of reusable connections.

    Args:
        factory: A zero-argument callable that creates a brand-new connection.
        max_size: Maximum number of connections that may exist at once. Must be
            >= 1.
        timeout: Seconds to wait in :meth:`acquire` for a connection to become
            available when the pool is exhausted. ``None`` waits forever.
        health_check: A predicate run on an idle connection before it is handed
            out. If it returns ``False`` (or raises), the connection is discarded
            and a replacement is created. Defaults to
            :func:`~postgres_connection_pool.health.always_healthy`.
        close_fn: Optional callable used to close a connection when it is discarded
            or when :meth:`close_all` runs. If omitted, the pool calls the
            connection's own ``close()`` method when present.

    Raises:
        ValueError: If ``max_size`` is less than 1.
    """

    def __init__(
        self,
        factory: Callable[[], C],
        *,
        max_size: int = 10,
        timeout: float | None = 5.0,
        health_check: HealthCheck[C] = always_healthy,
        close_fn: Callable[[C], None] | None = None,
    ) -> None:
        if max_size < 1:
            raise ValueError("max_size must be >= 1")
        self._factory = factory
        self._max_size = max_size
        self._timeout = timeout
        self._health_check = health_check
        self._close_fn = close_fn

        self._idle: deque[C] = deque()
        self._in_use: set[int] = set()
        self._size = 0
        self._closed = False

        self._lock = threading.Lock()
        # Signalled whenever a connection is released or discarded, so blocked
        # acquirers can wake up and retry.
        self._available = threading.Condition(self._lock)

    # -- public API ---------------------------------------------------------

    @property
    def max_size(self) -> int:
        """The configured maximum number of connections."""
        return self._max_size

    @property
    def closed(self) -> bool:
        """Whether :meth:`close_all` has been called."""
        return self._closed

    def stats(self) -> PoolStats:
        """Return a consistent snapshot of current pool occupancy."""
        with self._lock:
            idle = len(self._idle)
            in_use = len(self._in_use)
            return PoolStats(size=self._size, in_use=in_use, idle=idle, max_size=self._max_size)

    def acquire(self, timeout: float | None = -1.0) -> C:
        """Check out a connection, creating or reusing one as needed.

        Resolution order:

        1. Reuse an idle connection that passes its health check.
        2. Otherwise, if the pool is below ``max_size``, create a new connection.
        3. Otherwise, block until a connection is released or ``timeout`` elapses.

        Args:
            timeout: Per-call override for the wait timeout, in seconds. The
                sentinel ``-1.0`` (the default) means "use the pool's configured
                ``timeout``". ``None`` waits forever; ``0`` polls without blocking.

        Returns:
            A live connection owned by the caller until :meth:`release` is called.

        Raises:
            PoolClosedError: If the pool has been closed.
            PoolTimeoutError: If no connection becomes available within the wait.
        """
        effective_timeout = self._timeout if timeout == -1.0 else timeout
        deadline = None if effective_timeout is None else time.monotonic() + effective_timeout

        with self._available:
            while True:
                if self._closed:
                    raise PoolClosedError("cannot acquire from a closed pool")

                conn = self._take_healthy_idle_locked()
                if conn is not None:
                    self._in_use.add(id(conn))
                    return conn

                if self._size < self._max_size:
                    conn = self._create_locked()
                    self._in_use.add(id(conn))
                    return conn

                # Pool is full and nothing is idle: wait for a release.
                if deadline is None:
                    self._available.wait()
                    continue
                remaining = deadline - time.monotonic()
                if remaining <= 0 or not self._available.wait(timeout=remaining):
                    raise PoolTimeoutError(
                        f"timed out after {effective_timeout}s waiting for a connection "
                        f"(max_size={self._max_size})"
                    )

    def release(self, conn: C) -> None:
        """Return a previously acquired connection to the pool.

        The connection becomes available for reuse and any acquirer blocked in
        :meth:`acquire` is woken. If the pool is already closed, the connection is
        closed immediately instead of being retained.

        Args:
            conn: A connection previously returned by :meth:`acquire`.

        Raises:
            ValueError: If ``conn`` is not currently checked out from this pool.
        """
        with self._available:
            key = id(conn)
            if key not in self._in_use:
                raise ValueError("connection was not acquired from this pool")
            self._in_use.discard(key)

            if self._closed:
                self._destroy_locked(conn)
            else:
                self._idle.append(conn)
            self._available.notify()

    @contextmanager
    def connection(self, timeout: float | None = -1.0) -> Iterator[C]:
        """Acquire a connection for the duration of a ``with`` block.

        The connection is guaranteed to be released back to the pool when the
        block exits, even if the body raises.

        Args:
            timeout: Same semantics as :meth:`acquire`.

        Yields:
            A live connection.

        Example:
            >>> with pool.connection() as conn:
            ...     conn.execute("SELECT 1")
        """
        conn = self.acquire(timeout=timeout)
        try:
            yield conn
        finally:
            self.release(conn)

    def close_all(self) -> None:
        """Close every connection and mark the pool as closed.

        Idle connections are closed immediately. Connections still checked out are
        closed as they are released. After this call, :meth:`acquire` raises
        :class:`~postgres_connection_pool.errors.PoolClosedError`. Idempotent.
        """
        with self._available:
            self._closed = True
            while self._idle:
                self._destroy_locked(self._idle.popleft())
            self._available.notify_all()

    # -- context-manager protocol for the pool itself -----------------------

    def __enter__(self) -> ConnectionPool[C]:
        return self

    def __exit__(self, *exc: object) -> None:
        self.close_all()

    # -- internal helpers (call only while holding the lock) ----------------

    def _take_healthy_idle_locked(self) -> C | None:
        """Pop idle connections until a healthy one is found; discard dead ones.

        Returns ``None`` when no idle connection survives its health check, so the
        caller can decide whether to create a new one.
        """
        while self._idle:
            candidate = self._idle.popleft()
            if self._is_healthy(candidate):
                return candidate
            # Dead connection: drop it and let the caller create a replacement.
            self._destroy_locked(candidate)
        return None

    def _is_healthy(self, conn: C) -> bool:
        try:
            return bool(self._health_check(conn))
        except Exception:
            return False

    def _create_locked(self) -> C:
        conn = self._factory()
        self._size += 1
        return conn

    def _destroy_locked(self, conn: C) -> None:
        self._size -= 1
        if self._size < 0:
            self._size = 0
        self._close_connection(conn)

    def _close_connection(self, conn: C) -> None:
        if self._close_fn is not None:
            self._close_fn(conn)
            return
        close = getattr(conn, "close", None)
        if callable(close):
            # Closing is best-effort; a failure here must not mask other work.
            with contextlib.suppress(Exception):
                close()
