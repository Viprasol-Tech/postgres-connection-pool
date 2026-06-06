"""Command-line interface for Postgres Connection Pool.

``postgres-connection-pool demo`` exercises the pool end to end against an
in-memory fake connection factory -- no database, no credentials, no network. It
shows connection reuse, the ``max_size`` cap, a recycled dead connection, and the
context-manager API.

Part of Postgres Connection Pool by Viprasol Tech Private Limited (https://viprasol.com).
"""

from __future__ import annotations

import contextlib

import typer
from rich.console import Console
from rich.table import Table

from postgres_connection_pool import __version__
from postgres_connection_pool.breaker import CircuitBreaker
from postgres_connection_pool.errors import CircuitOpenError, PoolTimeoutError
from postgres_connection_pool.fake import (
    FakeConnection,
    FakeConnectionFactory,
    FlakyConnectionFactory,
)
from postgres_connection_pool.health import ping_healthy
from postgres_connection_pool.pool import ConnectionPool, PoolStats

app = typer.Typer(add_completion=False, help="Postgres Connection Pool - by Viprasol Tech.")
console = Console()


def _stats_table(stats: PoolStats) -> Table:
    """Render a :class:`PoolStats` snapshot as a compact rich table."""
    table = Table(title="Pool stats", title_style="bold", show_header=True)
    table.add_column("metric", style="cyan")
    table.add_column("value", justify="right")
    for name in (
        "size",
        "in_use",
        "idle",
        "min_size",
        "max_size",
        "created",
        "reused",
        "recycled",
        "discarded_unhealthy",
        "acquire_retries",
        "timeouts",
    ):
        table.add_row(name, str(getattr(stats, name)))
    return table


@app.command()
def version() -> None:
    """Print the installed version."""
    console.print(f"postgres-connection-pool [bold cyan]{__version__}[/] - by Viprasol Tech")


@app.command()
def demo(max_size: int = typer.Option(2, help="Maximum connections in the pool.")) -> None:
    """Run an offline pool demo against a fake connection factory."""
    factory = FakeConnectionFactory()
    pool: ConnectionPool[FakeConnection] = ConnectionPool(
        factory, max_size=max_size, timeout=0.0, health_check=ping_healthy
    )

    console.print(f"[bold]Pool created[/] (max_size={max_size}, health_check=ping)\n")

    # 1) Reuse: acquire, release, acquire again -> same connection comes back.
    first = pool.acquire()
    console.print(f"acquire #1 -> {first!r}")
    pool.release(first)
    again = pool.acquire()
    reused = again is first
    console.print(f"acquire #2 -> {again!r}  (reused: [bold]{reused}[/])")

    # 2) Cap + timeout: fill the pool, then prove acquire raises when exhausted.
    held = [pool.acquire() for _ in range(max_size - 1)]
    console.print(f"\nin use: {pool.stats().in_use}/{max_size} -> pool is full")
    try:
        pool.acquire(timeout=0.0)
        console.print("[red]unexpected: acquire should have timed out[/]")
    except PoolTimeoutError:
        console.print("acquire on full pool -> [bold green]PoolTimeoutError (as expected)[/]")

    # 3) Recycle a dead connection on release + re-acquire.
    again.kill()
    pool.release(again)
    for conn in held:
        pool.release(conn)
    fresh = pool.acquire()
    recycled = fresh is not again
    console.print(f"\nkilled idle conn, re-acquired -> {fresh!r}  (recycled: [bold]{recycled}[/])")
    pool.release(fresh)

    # 4) Context manager auto-releases.
    with pool.connection() as conn:
        console.print(f"\nwith pool.connection() -> {conn!r}")
    console.print(f"after with-block, in_use={pool.stats().in_use}")

    # 5) Transaction helper: commit on success, rollback on error.
    with pool.transaction() as conn:
        conn.execute("INSERT INTO demo VALUES (1)")
    console.print(
        f"\ntransaction() committed -> commits={conn.commits}, rollbacks={conn.rollbacks}"
    )

    pool.close_all()
    console.print(
        f"\nclose_all() done. total connections ever created: [bold]{len(factory.created)}[/]"
    )
    console.print(_stats_table(pool.stats()))


@app.command()
def resilience() -> None:
    """Demo retry-on-acquire and the circuit breaker against a flaky factory."""
    # Factory fails its first two connect attempts, then recovers.
    flaky = FlakyConnectionFactory(fail_times=2)
    pool: ConnectionPool[object] = ConnectionPool(
        flaky, max_size=2, max_create_retries=3, retry_backoff=0.0
    )
    conn = pool.acquire()
    console.print(
        f"retry-on-acquire survived {flaky.fail_times} failures -> got {conn!r} "
        f"(retries={pool.stats().acquire_retries})"
    )
    pool.release(conn)
    pool.close_all()

    # A breaker that trips after 2 consecutive failures and stays open.
    breaker = CircuitBreaker(failure_threshold=2, reset_timeout=60.0)
    down = FlakyConnectionFactory(fail_times=99)
    guarded: ConnectionPool[object] = ConnectionPool(down, max_size=2, breaker=breaker)
    for _ in range(2):
        with contextlib.suppress(Exception):
            guarded.acquire()
    try:
        guarded.acquire()
        console.print("[red]unexpected: breaker should be open[/]")
    except CircuitOpenError:
        console.print(
            f"circuit breaker -> [bold green]open, failing fast[/] "
            f"(state={breaker.state.value}, opened={breaker.stats().opened_count}x)"
        )
    guarded.close_all()


@app.command()
def warmup(min_size: int = typer.Option(3, help="Connections to pre-create.")) -> None:
    """Demo a warm pool created eagerly at startup."""
    factory = FakeConnectionFactory()
    pool: ConnectionPool[object] = ConnectionPool(factory, max_size=5, min_size=min_size)
    stats = pool.stats()
    console.print(
        f"warm pool ready: idle={stats.idle}, created={stats.created} "
        f"(min_size={min_size}) before any acquire"
    )
    console.print(_stats_table(stats))
    pool.close_all()


if __name__ == "__main__":
    app()
