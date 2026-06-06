"""A small, dependency-free circuit breaker for guarding connection creation.

When a backing database becomes unreachable, naively retrying ``factory()`` on
every :meth:`~postgres_connection_pool.pool.ConnectionPool.acquire` turns a brief
outage into a storm of slow, failing calls. A :class:`CircuitBreaker` short-circuits
that: after ``failure_threshold`` consecutive failures it *opens* and fails fast for
``reset_timeout`` seconds, then lets a single trial call through (the *half-open*
state) to probe recovery before fully closing again.

The breaker is intentionally generic -- it wraps any zero-argument callable -- so it
can guard a real ``psycopg.connect`` just as easily as the in-memory fakes used in
the test suite.

Part of Postgres Connection Pool by Viprasol Tech Private Limited (https://viprasol.com).
"""

from __future__ import annotations

import threading
import time
from collections.abc import Callable
from dataclasses import dataclass
from enum import Enum
from typing import TypeVar

from postgres_connection_pool.errors import CircuitOpenError

T = TypeVar("T")


class BreakerState(str, Enum):
    """The three states of a :class:`CircuitBreaker`.

    Attributes:
        CLOSED: Normal operation -- calls pass through and failures are counted.
        OPEN: Tripped -- calls fail fast with :class:`CircuitOpenError` until the
            reset timeout elapses.
        HALF_OPEN: Probing -- a single trial call is allowed through to test whether
            the dependency has recovered.
    """

    CLOSED = "closed"
    OPEN = "open"
    HALF_OPEN = "half_open"


@dataclass(frozen=True)
class BreakerStats:
    """A point-in-time snapshot of breaker counters.

    Attributes:
        state: The current :class:`BreakerState`.
        consecutive_failures: Failures recorded since the last success.
        total_failures: Lifetime count of failed calls.
        total_successes: Lifetime count of successful calls.
        total_rejections: Calls rejected because the breaker was open.
        opened_count: How many times the breaker has transitioned to ``OPEN``.
    """

    state: BreakerState
    consecutive_failures: int
    total_failures: int
    total_successes: int
    total_rejections: int
    opened_count: int


class CircuitBreaker:
    """A thread-safe circuit breaker around a callable.

    Args:
        failure_threshold: Number of consecutive failures that trips the breaker
            from ``CLOSED`` to ``OPEN``. Must be >= 1.
        reset_timeout: Seconds the breaker stays ``OPEN`` before allowing a single
            half-open trial call. Must be > 0.
        time_fn: Monotonic clock used for timing, overridable in tests. Defaults to
            :func:`time.monotonic`.

    Raises:
        ValueError: If ``failure_threshold`` < 1 or ``reset_timeout`` <= 0.
    """

    def __init__(
        self,
        *,
        failure_threshold: int = 5,
        reset_timeout: float = 30.0,
        time_fn: Callable[[], float] = time.monotonic,
    ) -> None:
        if failure_threshold < 1:
            raise ValueError("failure_threshold must be >= 1")
        if reset_timeout <= 0:
            raise ValueError("reset_timeout must be > 0")
        self._failure_threshold = failure_threshold
        self._reset_timeout = reset_timeout
        self._time_fn = time_fn

        self._state = BreakerState.CLOSED
        self._consecutive_failures = 0
        self._total_failures = 0
        self._total_successes = 0
        self._total_rejections = 0
        self._opened_count = 0
        self._opened_at: float | None = None
        self._lock = threading.Lock()

    @property
    def state(self) -> BreakerState:
        """The breaker's current state, accounting for an elapsed reset timeout."""
        with self._lock:
            self._maybe_half_open_locked()
            return self._state

    def stats(self) -> BreakerStats:
        """Return a consistent snapshot of the breaker's counters."""
        with self._lock:
            self._maybe_half_open_locked()
            return BreakerStats(
                state=self._state,
                consecutive_failures=self._consecutive_failures,
                total_failures=self._total_failures,
                total_successes=self._total_successes,
                total_rejections=self._total_rejections,
                opened_count=self._opened_count,
            )

    def call(self, fn: Callable[[], T]) -> T:
        """Invoke ``fn`` through the breaker.

        Args:
            fn: A zero-argument callable to execute.

        Returns:
            Whatever ``fn`` returns.

        Raises:
            CircuitOpenError: If the breaker is open and the reset timeout has not
                yet elapsed.
            Exception: Re-raises any exception ``fn`` raises (after recording it).
        """
        self._before_call()
        try:
            result = fn()
        except Exception:
            self.record_failure()
            raise
        self.record_success()
        return result

    def _before_call(self) -> None:
        with self._lock:
            self._maybe_half_open_locked()
            if self._state is BreakerState.OPEN:
                self._total_rejections += 1
                retry_in = self._retry_in_locked()
                raise CircuitOpenError(
                    f"circuit breaker is open; retry in {retry_in:.2f}s "
                    f"(opened {self._opened_count}x, "
                    f"{self._consecutive_failures} consecutive failures)"
                )

    def record_success(self) -> None:
        """Record a successful call, resetting failures and closing the breaker."""
        with self._lock:
            self._total_successes += 1
            self._consecutive_failures = 0
            self._state = BreakerState.CLOSED
            self._opened_at = None

    def record_failure(self) -> None:
        """Record a failed call, tripping the breaker once the threshold is hit.

        A failure while *half-open* re-opens the breaker immediately, regardless of
        the consecutive-failure count.
        """
        with self._lock:
            self._total_failures += 1
            self._consecutive_failures += 1
            if (
                self._state is BreakerState.HALF_OPEN
                or self._consecutive_failures >= self._failure_threshold
            ):
                self._open_locked()

    def _open_locked(self) -> None:
        if self._state is not BreakerState.OPEN:
            self._opened_count += 1
        self._state = BreakerState.OPEN
        self._opened_at = self._time_fn()

    def _maybe_half_open_locked(self) -> None:
        if (
            self._state is BreakerState.OPEN
            and self._opened_at is not None
            and self._time_fn() - self._opened_at >= self._reset_timeout
        ):
            self._state = BreakerState.HALF_OPEN

    def _retry_in_locked(self) -> float:
        if self._opened_at is None:
            return 0.0
        remaining = self._reset_timeout - (self._time_fn() - self._opened_at)
        return max(0.0, remaining)
