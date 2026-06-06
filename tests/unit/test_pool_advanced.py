"""Unit tests for the advanced pool features added in 0.2.0."""

from __future__ import annotations

import threading
import time

import pytest

from postgres_connection_pool import (
    CircuitBreaker,
    CircuitOpenError,
    ConnectionPool,
    PoolTimeoutError,
)
from postgres_connection_pool.fake import (
    FakeConnection,
    FakeConnectionFactory,
    FlakyConnectionFactory,
)

# -- warmup / min_size ------------------------------------------------------


def test_warmup_creates_min_size_connections_eagerly() -> None:
    factory = FakeConnectionFactory()
    pool: ConnectionPool[FakeConnection] = ConnectionPool(factory, max_size=5, min_size=3)
    stats = pool.stats()
    assert stats.idle == 3
    assert stats.size == 3
    assert len(factory.created) == 3


def test_warmup_can_be_disabled() -> None:
    factory = FakeConnectionFactory()
    pool: ConnectionPool[FakeConnection] = ConnectionPool(
        factory, max_size=5, min_size=3, warmup=False
    )
    assert pool.stats().idle == 0
    assert len(factory.created) == 0


def test_min_size_cannot_exceed_max_size() -> None:
    with pytest.raises(ValueError, match="min_size must not exceed"):
        ConnectionPool(FakeConnectionFactory(), max_size=2, min_size=3)


def test_replenish_keeps_floor_after_recycle() -> None:
    factory = FakeConnectionFactory()
    pool: ConnectionPool[FakeConnection] = ConnectionPool(
        factory, max_size=5, min_size=2, max_lifetime=0.01
    )
    conn = pool.acquire()
    time.sleep(0.02)  # let the connection age past max_lifetime
    pool.release(conn)  # expired on release -> recycled, then replenished
    stats = pool.stats()
    assert stats.recycled == 1
    # Pool should be topped back up to min_size.
    assert stats.size >= pool.min_size


# -- max_lifetime / recycling ----------------------------------------------


def test_expired_idle_connection_is_recycled_on_acquire() -> None:
    factory = FakeConnectionFactory()
    pool: ConnectionPool[FakeConnection] = ConnectionPool(factory, max_size=2, max_lifetime=0.01)
    first = pool.acquire()
    pool.release(first)
    time.sleep(0.02)
    second = pool.acquire()
    assert second is not first  # the stale one was recycled
    assert first.closed is True
    assert pool.stats().recycled == 1
    assert len(factory.created) == 2


def test_fresh_connection_is_reused_within_lifetime() -> None:
    factory = FakeConnectionFactory()
    pool: ConnectionPool[FakeConnection] = ConnectionPool(factory, max_size=2, max_lifetime=10.0)
    first = pool.acquire()
    pool.release(first)
    second = pool.acquire()
    assert second is first
    assert pool.stats().recycled == 0


def test_invalid_max_lifetime() -> None:
    with pytest.raises(ValueError, match="max_lifetime"):
        ConnectionPool(FakeConnectionFactory(), max_size=1, max_lifetime=0)


# -- retry on acquire -------------------------------------------------------


def test_acquire_retries_transient_factory_failures() -> None:
    flaky = FlakyConnectionFactory(fail_times=2)
    pool: ConnectionPool[FakeConnection] = ConnectionPool(flaky, max_size=2, max_create_retries=3)
    conn = pool.acquire()
    assert isinstance(conn, FakeConnection)
    assert flaky.attempts == 3  # 2 failures + 1 success
    assert pool.stats().acquire_retries == 2
    assert pool.stats().created == 1


def test_acquire_raises_when_retries_exhausted() -> None:
    flaky = FlakyConnectionFactory(fail_times=5)
    pool: ConnectionPool[FakeConnection] = ConnectionPool(flaky, max_size=2, max_create_retries=2)
    with pytest.raises(ConnectionError):
        pool.acquire()
    assert flaky.attempts == 3  # initial + 2 retries
    assert pool.stats().size == 0


def test_invalid_retry_params() -> None:
    with pytest.raises(ValueError, match="max_create_retries"):
        ConnectionPool(FakeConnectionFactory(), max_size=1, max_create_retries=-1)
    with pytest.raises(ValueError, match="retry_backoff"):
        ConnectionPool(FakeConnectionFactory(), max_size=1, retry_backoff=-1.0)


# -- circuit breaker integration -------------------------------------------


def test_breaker_opens_and_acquire_fails_fast() -> None:
    breaker = CircuitBreaker(failure_threshold=2, reset_timeout=60.0)
    down = FlakyConnectionFactory(fail_times=99)
    pool: ConnectionPool[FakeConnection] = ConnectionPool(down, max_size=2, breaker=breaker)

    for _ in range(2):
        with pytest.raises(ConnectionError):
            pool.acquire()
    # Breaker has now tripped; further acquires fail fast without calling factory.
    attempts_before = down.attempts
    with pytest.raises(CircuitOpenError):
        pool.acquire()
    assert down.attempts == attempts_before  # factory was not invoked again


def test_breaker_closes_after_recovery() -> None:
    # A manually advanced clock keeps the half-open transition deterministic.
    clock = {"now": 0.0}
    breaker = CircuitBreaker(failure_threshold=2, reset_timeout=10.0, time_fn=lambda: clock["now"])
    flaky = FlakyConnectionFactory(fail_times=2)
    pool: ConnectionPool[FakeConnection] = ConnectionPool(flaky, max_size=2, breaker=breaker)

    for _ in range(2):
        with pytest.raises(ConnectionError):
            pool.acquire()
    clock["now"] = 20.0  # advance past reset_timeout -> breaker half-opens
    conn = pool.acquire()  # half-open trial succeeds -> breaker closes
    assert isinstance(conn, FakeConnection)
    assert breaker.state.value == "closed"


# -- richer stats -----------------------------------------------------------


def test_stats_track_reuse_and_timeouts() -> None:
    factory = FakeConnectionFactory()
    pool: ConnectionPool[FakeConnection] = ConnectionPool(factory, max_size=1, timeout=0.0)
    a = pool.acquire()
    pool.release(a)
    pool.acquire()  # reuse
    with pytest.raises(PoolTimeoutError):
        pool.acquire(timeout=0.0)
    stats = pool.stats()
    assert stats.reused == 1
    assert stats.timeouts == 1
    assert stats.created == 1


def test_stats_track_unhealthy_discards() -> None:
    from postgres_connection_pool import ping_healthy

    factory = FakeConnectionFactory()
    pool: ConnectionPool[FakeConnection] = ConnectionPool(
        factory, max_size=2, health_check=ping_healthy
    )
    conn = pool.acquire()
    pool.release(conn)
    conn.kill()
    pool.acquire()
    assert pool.stats().discarded_unhealthy == 1


def test_concurrent_acquire_release_is_consistent() -> None:
    factory = FakeConnectionFactory()
    pool: ConnectionPool[FakeConnection] = ConnectionPool(factory, max_size=4, timeout=2.0)
    errors: list[Exception] = []

    def worker() -> None:
        try:
            for _ in range(50):
                with pool.connection() as conn:
                    conn.ping()
        except Exception as exc:  # pragma: no cover - failure path
            errors.append(exc)

    threads = [threading.Thread(target=worker) for _ in range(8)]
    for t in threads:
        t.start()
    for t in threads:
        t.join(timeout=10.0)
    assert not errors
    stats = pool.stats()
    assert stats.in_use == 0
    assert stats.size <= pool.max_size
