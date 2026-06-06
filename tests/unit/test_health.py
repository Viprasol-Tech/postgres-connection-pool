"""Unit tests for the health-check helpers."""

from __future__ import annotations

from postgres_connection_pool.fake import FakeConnection
from postgres_connection_pool.health import always_healthy, ping_healthy


def test_always_healthy_returns_true() -> None:
    assert always_healthy(object()) is True


def test_ping_healthy_on_alive_connection() -> None:
    assert ping_healthy(FakeConnection(1)) is True


def test_ping_healthy_on_dead_connection() -> None:
    conn = FakeConnection(1)
    conn.kill()
    assert ping_healthy(conn) is False


def test_ping_healthy_on_closed_connection() -> None:
    conn = FakeConnection(1)
    conn.close()
    assert ping_healthy(conn) is False


def test_ping_healthy_without_ping_method() -> None:
    assert ping_healthy(object()) is False


def test_ping_healthy_swallows_exceptions() -> None:
    class Boom:
        def ping(self) -> bool:
            raise RuntimeError("driver error")

    assert ping_healthy(Boom()) is False
