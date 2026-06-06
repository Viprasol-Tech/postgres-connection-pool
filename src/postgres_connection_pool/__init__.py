"""Postgres Connection Pool - a generic, thread-safe connection pool by Viprasol Tech."""

from __future__ import annotations

from postgres_connection_pool.errors import PoolClosedError, PoolTimeoutError
from postgres_connection_pool.health import HealthCheck, always_healthy, ping_healthy
from postgres_connection_pool.pool import ConnectionPool, PoolStats

__version__ = "0.1.0"
__author__ = "Viprasol Tech Private Limited"
__all__ = [
    "ConnectionPool",
    "HealthCheck",
    "PoolClosedError",
    "PoolStats",
    "PoolTimeoutError",
    "__version__",
    "always_healthy",
    "ping_healthy",
]
