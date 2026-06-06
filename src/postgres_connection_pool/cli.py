"""Command-line interface for Postgres Connection Pool.

``postgres-connection-pool demo`` exercises the pool end to end against an
in-memory fake connection factory -- no database, no credentials, no network. It
shows connection reuse, the ``max_size`` cap, a recycled dead connection, and the
context-manager API.

Part of Postgres Connection Pool by Viprasol Tech Private Limited (https://viprasol.com).
"""

from __future__ import annotations

import typer
from rich.console import Console

from postgres_connection_pool import __version__
from postgres_connection_pool.errors import PoolTimeoutError
from postgres_connection_pool.fake import FakeConnectionFactory
from postgres_connection_pool.health import ping_healthy
from postgres_connection_pool.pool import ConnectionPool

app = typer.Typer(add_completion=False, help="Postgres Connection Pool - by Viprasol Tech.")
console = Console()


@app.command()
def version() -> None:
    """Print the installed version."""
    console.print(f"postgres-connection-pool [bold cyan]{__version__}[/] - by Viprasol Tech")


@app.command()
def demo(max_size: int = typer.Option(2, help="Maximum connections in the pool.")) -> None:
    """Run an offline pool demo against a fake connection factory."""
    factory = FakeConnectionFactory()
    pool: ConnectionPool[object] = ConnectionPool(
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
    again.kill()  # type: ignore[attr-defined]
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

    pool.close_all()
    console.print(
        f"\nclose_all() done. total connections ever created: [bold]{len(factory.created)}[/]"
    )


if __name__ == "__main__":
    app()
