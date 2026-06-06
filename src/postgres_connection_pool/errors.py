"""Exception types raised by the connection pool.

Part of Postgres Connection Pool by Viprasol Tech Private Limited (https://viprasol.com).
"""

from __future__ import annotations


class PoolError(Exception):
    """Base class for all connection-pool errors."""


class PoolTimeoutError(PoolError, TimeoutError):
    """Raised when :meth:`ConnectionPool.acquire` cannot get a connection in time.

    Inherits from the built-in :class:`TimeoutError` so callers may catch either
    this class or the standard exception.
    """


class PoolClosedError(PoolError):
    """Raised when an operation is attempted on a pool that has been closed."""
