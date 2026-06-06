"""Unit tests for the circuit breaker."""

from __future__ import annotations

import pytest

from postgres_connection_pool import BreakerState, CircuitBreaker, CircuitOpenError


class FakeClock:
    """A manually advanced monotonic clock for deterministic breaker tests."""

    def __init__(self) -> None:
        self.now = 0.0

    def __call__(self) -> float:
        return self.now

    def advance(self, seconds: float) -> None:
        self.now += seconds


def test_breaker_starts_closed() -> None:
    breaker = CircuitBreaker(failure_threshold=2, reset_timeout=5.0)
    assert breaker.state is BreakerState.CLOSED


def test_breaker_passes_through_success() -> None:
    breaker = CircuitBreaker(failure_threshold=2, reset_timeout=5.0)
    assert breaker.call(lambda: 42) == 42
    stats = breaker.stats()
    assert stats.total_successes == 1
    assert stats.consecutive_failures == 0


def test_breaker_opens_after_threshold() -> None:
    clock = FakeClock()
    breaker = CircuitBreaker(failure_threshold=3, reset_timeout=5.0, time_fn=clock)

    def boom() -> None:
        raise ConnectionError("down")

    for _ in range(3):
        with pytest.raises(ConnectionError):
            breaker.call(boom)
    assert breaker.state is BreakerState.OPEN
    assert breaker.stats().opened_count == 1


def test_open_breaker_fails_fast() -> None:
    clock = FakeClock()
    breaker = CircuitBreaker(failure_threshold=1, reset_timeout=5.0, time_fn=clock)
    with pytest.raises(ConnectionError):
        breaker.call(lambda: (_ for _ in ()).throw(ConnectionError("x")))
    # Now open: the next call should be rejected without invoking the callable.
    called = False

    def should_not_run() -> int:
        nonlocal called
        called = True
        return 1

    with pytest.raises(CircuitOpenError):
        breaker.call(should_not_run)
    assert called is False
    assert breaker.stats().total_rejections == 1


def test_breaker_half_opens_after_timeout_then_closes_on_success() -> None:
    clock = FakeClock()
    breaker = CircuitBreaker(failure_threshold=1, reset_timeout=10.0, time_fn=clock)
    with pytest.raises(ValueError):
        breaker.call(lambda: (_ for _ in ()).throw(ValueError("x")))
    assert breaker.state is BreakerState.OPEN

    clock.advance(10.0)
    assert breaker.state is BreakerState.HALF_OPEN
    # A successful trial call closes the breaker again.
    assert breaker.call(lambda: "ok") == "ok"
    assert breaker.state is BreakerState.CLOSED


def test_half_open_failure_reopens_immediately() -> None:
    clock = FakeClock()
    breaker = CircuitBreaker(failure_threshold=5, reset_timeout=10.0, time_fn=clock)
    # Trip via half-open path: one failure is below threshold, so force open by time.
    with pytest.raises(RuntimeError):
        breaker.call(lambda: (_ for _ in ()).throw(RuntimeError("x")))
    # Still closed (1 < 5). Drive to open manually through failures.
    for _ in range(4):
        with pytest.raises(RuntimeError):
            breaker.call(lambda: (_ for _ in ()).throw(RuntimeError("x")))
    assert breaker.state is BreakerState.OPEN

    clock.advance(10.0)
    assert breaker.state is BreakerState.HALF_OPEN
    with pytest.raises(RuntimeError):
        breaker.call(lambda: (_ for _ in ()).throw(RuntimeError("x")))
    # A failure while half-open re-opens regardless of the threshold.
    assert breaker.state is BreakerState.OPEN
    assert breaker.stats().opened_count == 2


def test_success_resets_consecutive_failures() -> None:
    breaker = CircuitBreaker(failure_threshold=3, reset_timeout=5.0)
    with pytest.raises(ValueError):
        breaker.call(lambda: (_ for _ in ()).throw(ValueError("x")))
    with pytest.raises(ValueError):
        breaker.call(lambda: (_ for _ in ()).throw(ValueError("x")))
    breaker.call(lambda: 1)
    assert breaker.stats().consecutive_failures == 0
    assert breaker.state is BreakerState.CLOSED


def test_invalid_breaker_params() -> None:
    with pytest.raises(ValueError, match="failure_threshold"):
        CircuitBreaker(failure_threshold=0)
    with pytest.raises(ValueError, match="reset_timeout"):
        CircuitBreaker(reset_timeout=0)
