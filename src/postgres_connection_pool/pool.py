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
* **Warm pool** -- ``min_size`` connections can be created up front and replenished
  on release so the steady-state pool never falls below a floor.
* **Blocking with timeout** -- when the pool is exhausted, :meth:`acquire` waits up
  to ``timeout`` seconds for a connection to be released, then raises
  :class:`~postgres_connection_pool.errors.PoolTimeoutError`.
* **Self-healing** -- a connection that fails its health check is discarded and a
  fresh one is created in its place.
* **Recycling** -- a connection older than ``max_lifetime`` is retired the next time
  it is acquired or released, defeating server-side idle timeouts and stale state.
* **Resilient creation** -- transient ``factory`` failures are retried, and an
  optional :class:`~postgres_connection_pool.breaker.CircuitBreaker` fails fast when
  the backing database is down instead of hammering it.

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

from postgres_connection_pool.breaker import CircuitBreaker
from postgres_connection_pool.errors import PoolClosedError, PoolTimeoutError
from postgres_connection_pool.health import HealthCheck, always_healthy

C = TypeVar("C")


@dataclass(frozen=True)
class PoolStats:
    """A point-in-time snapshot of pool occupancy and lifetime counters.

    Attributes:
        size: Total live connections owned by the pool (idle + in use).
        in_use: Connections currently checked out by callers.
        idle: Connections sitting in the pool ready to be reused.
        max_size: The configured upper bound on ``size``.
        min_size: The configured floor the pool tries to keep warm.
        created: Lifetime count of connections the factory has produced.
        reused: Lifetime count of acquires satisfied by an existing idle connection.
        recycled: Lifetime count of connections retired for exceeding ``max_lifetime``.
        discarded_unhealthy: Lifetime count of connections dropped for failing a
            health check.
        acquire_retries: Lifetime count of retried connection-creation attempts.
        timeouts: Lifetime count of acquires that ended in ``PoolTimeoutError``.
    """

    size: int
    in_use: int
    idle: int
    max_size: int
    min_size: int
    created: int
    reused: int
    recycled: int
    discarded_unhealthy: int
    acquire_retries: int
    timeouts: int


class ConnectionPool(Generic[C]):
    """A bounded, thread-safe pool of reusable connections.

    Args:
        factory: A zero-argument callable that creates a brand-new connection.
        max_size: Maximum number of connections that may exist at once. Must be
            >= 1.
        min_size: Number of connections to keep warm. The pool creates this many up
            front (see ``warmup``) and replenishes back up to it on release. Must be
            between 0 and ``max_size``.
        timeout: Seconds to wait in :meth:`acquire` for a connection to become
            available when the pool is exhausted. ``None`` waits forever.
        health_check: A predicate run on an idle connection before it is handed
            out. If it returns ``False`` (or raises), the connection is discarded
            and a replacement is created. Defaults to
            :func:`~postgres_connection_pool.health.always_healthy`.
        close_fn: Optional callable used to close a connection when it is discarded
            or when :meth:`close_all` runs. If omitted, the pool calls the
            connection's own ``close()`` method when present.
        max_lifetime: Maximum age, in seconds, of a connection before it is recycled
            (closed and recreated) on its next acquire or release. ``None`` disables
            lifetime-based recycling.
        max_create_retries: Number of *extra* attempts to make when ``factory``
            raises while creating a connection. ``0`` means no retries.
        retry_backoff: Seconds to sleep between creation retries. ``0`` retries
            immediately.
        breaker: Optional :class:`~postgres_connection_pool.breaker.CircuitBreaker`
            guarding connection creation. When it is open, :meth:`acquire` fails
            fast with :class:`~postgres_connection_pool.errors.CircuitOpenError`.
        warmup: If ``True`` (the default), create ``min_size`` connections eagerly in
            the constructor so the first acquires are instant.

    Raises:
        ValueError: If the size or retry parameters are out of range.
    """

    def __init__(
        self,
        factory: Callable[[], C],
        *,
        max_size: int = 10,
        min_size: int = 0,
        timeout: float | None = 5.0,
        health_check: HealthCheck[C] = always_healthy,
        close_fn: Callable[[C], None] | None = None,
        max_lifetime: float | None = None,
        max_create_retries: int = 0,
        retry_backoff: float = 0.0,
        breaker: CircuitBreaker | None = None,
        warmup: bool = True,
    ) -> None:
        if max_size < 1:
            raise ValueError("max_size must be >= 1")
        if min_size < 0:
            raise ValueError("min_size must be >= 0")
        if min_size > max_size:
            raise ValueError("min_size must not exceed max_size")
        if max_lifetime is not None and max_lifetime <= 0:
            raise ValueError("max_lifetime must be > 0 or None")
        if max_create_retries < 0:
            raise ValueError("max_create_retries must be >= 0")
        if retry_backoff < 0:
            raise ValueError("retry_backoff must be >= 0")

        self._factory = factory
        self._max_size = max_size
        self._min_size = min_size
        self._timeout = timeout
        self._health_check = health_check
        self._close_fn = close_fn
        self._max_lifetime = max_lifetime
        self._max_create_retries = max_create_retries
        self._retry_backoff = retry_backoff
        self._breaker = breaker

        self._idle: deque[C] = deque()
        self._in_use: set[int] = set()
        # Birth timestamp per live connection, keyed by id(), for lifetime recycling.
        self._born: dict[int, float] = {}
        self._size = 0
        self._closed = False

        # Lifetime counters surfaced through stats().
        self._created = 0
        self._reused = 0
        self._recycled = 0
        self._discarded_unhealthy = 0
        self._acquire_retries = 0
        self._timeouts = 0

        self._lock = threading.Lock()
        # Signalled whenever a connection is released or discarded, so blocked
        # acquirers can wake up and retry.
        self._available = threading.Condition(self._lock)

        if warmup and min_size > 0:
            self._warmup(min_size)

    # -- public API ---------------------------------------------------------

    @property
    def max_size(self) -> int:
        """The configured maximum number of connections."""
        return self._max_size

    @property
    def min_size(self) -> int:
        """The configured minimum (warm) number of connections."""
        return self._min_size

    @property
    def closed(self) -> bool:
        """Whether :meth:`close_all` has been called."""
        return self._closed

    def stats(self) -> PoolStats:
        """Return a consistent snapshot of current pool occupancy and counters."""
        with self._lock:
            return PoolStats(
                size=self._size,
                in_use=len(self._in_use),
                idle=len(self._idle),
                max_size=self._max_size,
                min_size=self._min_size,
                created=self._created,
                reused=self._reused,
                recycled=self._recycled,
                discarded_unhealthy=self._discarded_unhealthy,
                acquire_retries=self._acquire_retries,
                timeouts=self._timeouts,
            )

    def acquire(self, timeout: float | None = -1.0) -> C:
        """Check out a connection, creating or reusing one as needed.

        Resolution order:

        1. Reuse an idle connection that is fresh and passes its health check.
        2. Otherwise, if the pool is below ``max_size``, create a new connection
           (with retries and circuit-breaker protection).
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
            CircuitOpenError: If the circuit breaker is open.
        """
        effective_timeout = self._timeout if timeout == -1.0 else timeout
        deadline = None if effective_timeout is None else time.monotonic() + effective_timeout

        with self._available:
            while True:
                if self._closed:
                    raise PoolClosedError("cannot acquire from a closed pool")

                conn = self._take_usable_idle_locked()
                if conn is not None:
                    self._reused += 1
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
                    self._timeouts += 1
                    raise PoolTimeoutError(
                        f"timed out after {effective_timeout}s waiting for a connection "
                        f"(max_size={self._max_size})"
                    )

    def release(self, conn: C) -> None:
        """Return a previously acquired connection to the pool.

        The connection becomes available for reuse and any acquirer blocked in
        :meth:`acquire` is woken. A connection past its ``max_lifetime`` is recycled
        instead of being retained. If the pool is closed, the connection is closed
        immediately.

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
            elif self._is_expired_locked(conn):
                self._recycled += 1
                self._destroy_locked(conn)
                self._replenish_locked()
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

    @contextmanager
    def transaction(
        self,
        timeout: float | None = -1.0,
        *,
        commit: Callable[[C], None] | None = None,
        rollback: Callable[[C], None] | None = None,
    ) -> Iterator[C]:
        """Acquire a connection and run a ``with`` block as one transaction.

        On normal exit the connection is committed; if the body raises, it is rolled
        back and the exception re-propagates. The connection is always released back
        to the pool afterwards.

        By default the helper calls the connection's own ``commit()`` / ``rollback()``
        methods (the shape every DB-API driver exposes); pass ``commit`` / ``rollback``
        callables to override that for drivers with a different surface.

        Args:
            timeout: Same semantics as :meth:`acquire`.
            commit: Optional callable invoked with the connection to commit.
            rollback: Optional callable invoked with the connection to roll back.

        Yields:
            A live connection inside an open transaction.

        Example:
            >>> with pool.transaction() as conn:
            ...     conn.execute("INSERT INTO t VALUES (1)")
        """
        do_commit = commit if commit is not None else _call_method("commit")
        do_rollback = rollback if rollback is not None else _call_method("rollback")
        conn = self.acquire(timeout=timeout)
        try:
            yield conn
        except BaseException:
            with contextlib.suppress(Exception):
                do_rollback(conn)
            raise
        else:
            do_commit(conn)
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

    def _warmup(self, count: int) -> None:
        """Eagerly create ``count`` idle connections at construction time."""
        with self._available:
            for _ in range(count):
                if self._size >= self._max_size:
                    break
                self._idle.append(self._create_locked())

    def _take_usable_idle_locked(self) -> C | None:
        """Pop idle connections until a fresh, healthy one is found.

        Expired or dead connections are discarded (and counted) so the caller can
        decide whether to create a replacement. Returns ``None`` when no idle
        connection survives.
        """
        while self._idle:
            candidate = self._idle.popleft()
            if self._is_expired_locked(candidate):
                self._recycled += 1
                self._destroy_locked(candidate)
                continue
            if self._is_healthy(candidate):
                return candidate
            # Dead connection: drop it and let the caller create a replacement.
            self._discarded_unhealthy += 1
            self._destroy_locked(candidate)
        return None

    def _is_expired_locked(self, conn: C) -> bool:
        if self._max_lifetime is None:
            return False
        born = self._born.get(id(conn))
        if born is None:
            return False
        return (time.monotonic() - born) >= self._max_lifetime

    def _is_healthy(self, conn: C) -> bool:
        try:
            return bool(self._health_check(conn))
        except Exception:
            return False

    def _replenish_locked(self) -> None:
        """Top the idle pool back up to ``min_size`` after a recycle."""
        while self._size < self._min_size and not self._closed:
            try:
                self._idle.append(self._create_locked())
            except Exception:
                # A replenish failure must not break release(); the pool will try
                # again to grow on the next acquire.
                break

    def _create_locked(self) -> C:
        """Create a connection with retries and optional circuit-breaker guarding.

        Must be called while holding the lock. Transient ``factory`` failures are
        retried up to ``max_create_retries`` times; a configured circuit breaker can
        short-circuit creation with :class:`CircuitOpenError`. On success the new
        connection's birth time is recorded for lifetime recycling.
        """
        attempt = 0
        while True:
            try:
                conn = self._make_connection()
            except Exception:
                if attempt >= self._max_create_retries:
                    raise
                attempt += 1
                self._acquire_retries += 1
                if self._retry_backoff > 0:
                    time.sleep(self._retry_backoff)
                continue
            self._size += 1
            self._created += 1
            self._born[id(conn)] = time.monotonic()
            return conn

    def _make_connection(self) -> C:
        """Invoke the factory, routing through the breaker when one is configured."""
        if self._breaker is not None:
            return self._breaker.call(self._factory)
        return self._factory()

    def _destroy_locked(self, conn: C) -> None:
        self._size -= 1
        if self._size < 0:
            self._size = 0
        self._born.pop(id(conn), None)
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


def _call_method(name: str) -> Callable[[object], None]:
    """Build a callable that invokes ``conn.<name>()`` if the method exists."""

    def invoke(conn: object) -> None:
        method = getattr(conn, name, None)
        if callable(method):
            method()

    return invoke
