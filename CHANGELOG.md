# Changelog

All notable changes to this project are documented here. Format based on
[Keep a Changelog](https://keepachangelog.com/en/1.1.0/); versioning follows
[SemVer](https://semver.org/).

## [0.2.0] - 2025

### Added
- **Connection recycling** via `max_lifetime`: connections older than the configured
  age are retired (closed + recreated) on their next acquire or release, defeating
  server-side idle timeouts and stale session state.
- **Warm pool / `min_size`**: pre-create a floor of connections at startup
  (`warmup=True`) and replenish back up to `min_size` after a recycle.
- **Retry-on-acquire**: transient `factory` failures are retried up to
  `max_create_retries` times with an optional `retry_backoff`.
- **Circuit breaker** (`CircuitBreaker`, `BreakerState`, `BreakerStats`): guards
  connection creation, failing fast with `CircuitOpenError` after repeated failures
  and probing recovery via a half-open trial call.
- **Context-managed transactions** (`ConnectionPool.transaction`): commits on
  success, rolls back on error, and always releases the connection. Custom
  `commit` / `rollback` callables are supported.
- **Richer stats**: `PoolStats` now also reports `min_size`, `created`, `reused`,
  `recycled`, `discarded_unhealthy`, `acquire_retries`, and `timeouts`.
- New CLI commands: `resilience` (retry + breaker demo) and `warmup` (warm-pool
  demo), plus a rich stats table in the existing `demo`.
- `FlakyConnectionFactory` test helper and `execute` / `commit` / `rollback` on
  `FakeConnection` for offline transaction and resilience testing.

### Changed
- `PoolError` and `CircuitOpenError` are now exported from the package root.

## [0.1.0] - 2025

### Added
- Initial release of postgres-connection-pool: A generic connection pool (acquire/release, max size, timeout, health checks).
