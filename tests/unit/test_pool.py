"""Unit tests for the connection pool behaviour."""

from __future__ import annotations

import threading
import time

import pytest

from postgres_connection_pool import (
    ConnectionPool,
    PoolClosedError,
    PoolTimeoutError,
    ping_healthy,
)
from postgres_connection_pool.fake import FakeConnection, FakeConnectionFactory


def test_acquire_creates_connection() -> None:
    factory = FakeConnectionFactory()
    pool: ConnectionPool[FakeConnection] = ConnectionPool(factory, max_size=2)
    conn = pool.acquire()
    assert isinstance(conn, FakeConnection)
    assert pool.stats().size == 1
    assert pool.stats().in_use == 1
    assert len(factory.created) == 1


def test_release_returns_connection_to_pool() -> None:
    factory = FakeConnectionFactory()
    pool: ConnectionPool[FakeConnection] = ConnectionPool(factory, max_size=2)
    conn = pool.acquire()
    pool.release(conn)
    stats = pool.stats()
    assert stats.in_use == 0
    assert stats.idle == 1
    assert stats.size == 1


def test_idle_connection_is_reused() -> None:
    factory = FakeConnectionFactory()
    pool: ConnectionPool[FakeConnection] = ConnectionPool(factory, max_size=4)
    first = pool.acquire()
    pool.release(first)
    second = pool.acquire()
    assert second is first
    # No new connection should have been created on reuse.
    assert len(factory.created) == 1


def test_max_size_cap_creates_distinct_connections() -> None:
    factory = FakeConnectionFactory()
    pool: ConnectionPool[FakeConnection] = ConnectionPool(factory, max_size=3)
    conns = [pool.acquire() for _ in range(3)]
    assert len({id(c) for c in conns}) == 3
    assert pool.stats().size == 3
    assert len(factory.created) == 3


def test_exhaustion_raises_timeout() -> None:
    factory = FakeConnectionFactory()
    pool: ConnectionPool[FakeConnection] = ConnectionPool(factory, max_size=1, timeout=0.05)
    held = pool.acquire()
    start = time.monotonic()
    with pytest.raises(PoolTimeoutError):
        pool.acquire()
    elapsed = time.monotonic() - start
    # It should have actually waited approximately the timeout window.
    assert elapsed >= 0.04
    pool.release(held)


def test_zero_timeout_does_not_block() -> None:
    factory = FakeConnectionFactory()
    pool: ConnectionPool[FakeConnection] = ConnectionPool(factory, max_size=1)
    pool.acquire()
    with pytest.raises(PoolTimeoutError):
        pool.acquire(timeout=0.0)


def test_release_wakes_blocked_acquirer() -> None:
    factory = FakeConnectionFactory()
    pool: ConnectionPool[FakeConnection] = ConnectionPool(factory, max_size=1, timeout=2.0)
    held = pool.acquire()
    acquired: list[FakeConnection] = []

    def worker() -> None:
        acquired.append(pool.acquire())

    t = threading.Thread(target=worker)
    t.start()
    time.sleep(0.05)  # let the worker block on acquire()
    pool.release(held)
    t.join(timeout=2.0)
    assert not t.is_alive()
    assert acquired and acquired[0] is held


def test_dead_connection_is_recycled() -> None:
    factory = FakeConnectionFactory()
    pool: ConnectionPool[FakeConnection] = ConnectionPool(
        factory, max_size=2, health_check=ping_healthy
    )
    conn = pool.acquire()
    pool.release(conn)
    conn.kill()  # server dropped the connection while idle
    fresh = pool.acquire()
    assert fresh is not conn
    assert fresh.ping() is True
    assert conn.closed is True  # the dead one was closed on discard
    # Pool size stays at one live connection (one discarded, one created).
    assert pool.stats().size == 1
    assert len(factory.created) == 2


def test_context_manager_releases() -> None:
    factory = FakeConnectionFactory()
    pool: ConnectionPool[FakeConnection] = ConnectionPool(factory, max_size=1)
    with pool.connection() as conn:
        assert pool.stats().in_use == 1
        assert isinstance(conn, FakeConnection)
    assert pool.stats().in_use == 0
    assert pool.stats().idle == 1


def test_context_manager_releases_on_exception() -> None:
    factory = FakeConnectionFactory()
    pool: ConnectionPool[FakeConnection] = ConnectionPool(factory, max_size=1)
    with pytest.raises(RuntimeError), pool.connection():
        raise RuntimeError("boom")
    assert pool.stats().in_use == 0
    assert pool.stats().idle == 1


def test_close_all_closes_idle_and_blocks_acquire() -> None:
    factory = FakeConnectionFactory()
    pool: ConnectionPool[FakeConnection] = ConnectionPool(factory, max_size=2)
    a = pool.acquire()
    b = pool.acquire()
    pool.release(a)
    pool.close_all()
    assert a.closed is True
    assert pool.closed is True
    with pytest.raises(PoolClosedError):
        pool.acquire()
    # Releasing an in-use connection after close should close it too.
    pool.release(b)
    assert b.closed is True
    assert pool.stats().size == 0


def test_close_all_is_idempotent() -> None:
    factory = FakeConnectionFactory()
    pool: ConnectionPool[FakeConnection] = ConnectionPool(factory, max_size=1)
    pool.close_all()
    pool.close_all()
    assert pool.closed is True


def test_release_foreign_connection_raises() -> None:
    factory = FakeConnectionFactory()
    pool: ConnectionPool[FakeConnection] = ConnectionPool(factory, max_size=1)
    stranger = FakeConnection(999)
    with pytest.raises(ValueError, match="not acquired"):
        pool.release(stranger)


def test_invalid_max_size_raises() -> None:
    with pytest.raises(ValueError, match="max_size"):
        ConnectionPool(FakeConnectionFactory(), max_size=0)


def test_pool_as_context_manager_closes() -> None:
    factory = FakeConnectionFactory()
    with ConnectionPool(factory, max_size=1) as pool:
        conn = pool.acquire()
        pool.release(conn)
    assert pool.closed is True
    assert conn.closed is True
