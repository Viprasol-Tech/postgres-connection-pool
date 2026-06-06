"""Unit tests for the context-managed transaction helper."""

from __future__ import annotations

import pytest

from postgres_connection_pool import ConnectionPool
from postgres_connection_pool.fake import FakeConnection, FakeConnectionFactory


def test_transaction_commits_on_success() -> None:
    factory = FakeConnectionFactory()
    pool: ConnectionPool[FakeConnection] = ConnectionPool(factory, max_size=1)
    with pool.transaction() as conn:
        conn.execute("INSERT INTO t VALUES (1)")
    assert conn.commits == 1
    assert conn.rollbacks == 0
    assert conn.executed == ["INSERT INTO t VALUES (1)"]
    # Connection is released back to the pool, not closed.
    assert pool.stats().in_use == 0
    assert pool.stats().idle == 1


def test_transaction_rolls_back_on_exception() -> None:
    factory = FakeConnectionFactory()
    pool: ConnectionPool[FakeConnection] = ConnectionPool(factory, max_size=1)
    captured: FakeConnection | None = None
    with pytest.raises(RuntimeError, match="boom"), pool.transaction() as conn:
        captured = conn
        raise RuntimeError("boom")
    assert captured is not None
    assert captured.rollbacks == 1
    assert captured.commits == 0
    # Even after a rollback the connection returns to the pool for reuse.
    assert pool.stats().in_use == 0
    assert pool.stats().idle == 1


def test_transaction_releases_even_when_commit_fails() -> None:
    factory = FakeConnectionFactory()
    pool: ConnectionPool[FakeConnection] = ConnectionPool(factory, max_size=1)

    def failing_commit(conn: FakeConnection) -> None:
        raise RuntimeError("commit failed")

    with (
        pytest.raises(RuntimeError, match="commit failed"),
        pool.transaction(commit=failing_commit) as conn,
    ):
        conn.execute("SELECT 1")
    # The connection must still be released despite the commit error.
    assert pool.stats().in_use == 0


def test_transaction_custom_commit_rollback_callables() -> None:
    factory = FakeConnectionFactory()
    pool: ConnectionPool[FakeConnection] = ConnectionPool(factory, max_size=1)
    events: list[str] = []

    with pool.transaction(
        commit=lambda c: events.append("commit"),
        rollback=lambda c: events.append("rollback"),
    ) as conn:
        conn.execute("SELECT 1")
    assert events == ["commit"]
    # The connection's own commit() was not used because we overrode it.
    assert conn.commits == 0


def test_transaction_rollback_swallows_rollback_errors() -> None:
    factory = FakeConnectionFactory()
    pool: ConnectionPool[FakeConnection] = ConnectionPool(factory, max_size=1)

    def bad_rollback(conn: FakeConnection) -> None:
        raise RuntimeError("rollback failed too")

    # The original error should propagate, not the rollback error.
    with pytest.raises(ValueError, match="original"), pool.transaction(rollback=bad_rollback):
        raise ValueError("original")
    assert pool.stats().in_use == 0
