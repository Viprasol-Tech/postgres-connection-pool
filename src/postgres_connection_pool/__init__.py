"""Postgres Connection Pool - a generic, thread-safe connection pool by Viprasol Tech."""

from __future__ import annotations

from postgres_connection_pool.breaker import (
    BreakerState,
    BreakerStats,
    CircuitBreaker,
)
from postgres_connection_pool.errors import (
    CircuitOpenError,
    PoolClosedError,
    PoolError,
    PoolTimeoutError,
)
from postgres_connection_pool.health import HealthCheck, always_healthy, ping_healthy
from postgres_connection_pool.pool import ConnectionPool, PoolStats

__version__ = "0.2.0"
__author__ = "Viprasol Tech Private Limited"
__all__ = [
    "BreakerState",
    "BreakerStats",
    "CircuitBreaker",
    "CircuitOpenError",
    "ConnectionPool",
    "HealthCheck",
    "PoolClosedError",
    "PoolError",
    "PoolStats",
    "PoolTimeoutError",
    "__version__",
    "always_healthy",
    "ping_healthy",
]
