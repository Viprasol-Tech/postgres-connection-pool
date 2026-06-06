"""Unit tests for the in-memory fakes used across the suite and demos."""

from __future__ import annotations

import pytest

from postgres_connection_pool.fake import (
    FakeConnection,
    FakeConnectionFactory,
    FlakyConnectionFactory,
)


def test_fake_connection_execute_records_sql() -> None:
    conn = FakeConnection(1)
    assert conn.execute("SELECT 1") == "SELECT 1"
    assert conn.executed == ["SELECT 1"]


def test_fake_connection_execute_on_closed_raises() -> None:
    conn = FakeConnection(1)
    conn.close()
    with pytest.raises(RuntimeError, match="closed"):
        conn.execute("SELECT 1")


def test_fake_connection_execute_on_dead_raises() -> None:
    conn = FakeConnection(1)
    conn.kill()
    with pytest.raises(RuntimeError, match="dead"):
        conn.execute("SELECT 1")


def test_fake_connection_commit_and_rollback_counters() -> None:
    conn = FakeConnection(1)
    conn.commit()
    conn.commit()
    conn.rollback()
    assert conn.commits == 2
    assert conn.rollbacks == 1


def test_fake_factory_assigns_incrementing_ids() -> None:
    factory = FakeConnectionFactory()
    a, b = factory(), factory()
    assert a.conn_id == 1
    assert b.conn_id == 2
    assert factory.created == [a, b]


def test_flaky_factory_fails_then_recovers() -> None:
    factory = FlakyConnectionFactory(fail_times=2)
    with pytest.raises(ConnectionError):
        factory()
    with pytest.raises(ConnectionError):
        factory()
    conn = factory()
    assert isinstance(conn, FakeConnection)
    assert factory.attempts == 3
    assert factory.created == [conn]


def test_flaky_factory_custom_exception() -> None:
    factory = FlakyConnectionFactory(fail_times=1, exc_factory=lambda n: ValueError(f"nope {n}"))
    with pytest.raises(ValueError, match="nope 1"):
        factory()
    assert isinstance(factory(), FakeConnection)
