from __future__ import annotations
from datetime import datetime, timezone

import pytest

from src.utils.retry import retry_with_backoff, CircuitBreaker
from src.utils.errors import RetryExhaustedError


class TestRetryWithBackoff:
    def test_success_no_retry(self):
        call_count = 0

        @retry_with_backoff(max_attempts=3)
        def work():
            nonlocal call_count
            call_count += 1
            return "done"

        result = work()
        assert result == "done"
        assert call_count == 1

    def test_retry_then_success(self):
        call_count = 0

        @retry_with_backoff(max_attempts=3, base_delay=0.01)
        def work():
            nonlocal call_count
            call_count += 1
            if call_count < 3:
                raise ValueError("not yet")
            return "ok"

        result = work()
        assert result == "ok"
        assert call_count == 3

    def test_exhaust_retries(self):
        call_count = 0

        @retry_with_backoff(max_attempts=2, base_delay=0.01)
        def work():
            nonlocal call_count
            call_count += 1
            raise ValueError("always fails")

        with pytest.raises(RetryExhaustedError):
            work()
        assert call_count == 2

    def test_specific_exceptions(self):
        @retry_with_backoff(max_attempts=2, base_delay=0.01, exceptions=(KeyError,))
        def work():
            raise ValueError("wrong exception")

        with pytest.raises(ValueError):
            work()


class TestCircuitBreaker:
    def test_closed_to_open(self):
        cb = CircuitBreaker(failure_threshold=2, recovery_timeout=0.1)

        def fail():
            raise RuntimeError("fail")

        with pytest.raises(RuntimeError):
            cb.call(fail)
        with pytest.raises(RuntimeError):
            cb.call(fail)
        assert cb.state == "open"

        with pytest.raises(RuntimeError, match="Circuit breaker is OPEN"):
            cb.call(fail)

    def test_half_open_to_closed(self):
        import time
        cb = CircuitBreaker(failure_threshold=1, recovery_timeout=0.05)

        def fail():
            raise RuntimeError("fail")

        with pytest.raises(RuntimeError):
            cb.call(fail)
        assert cb.state == "open"

        time.sleep(0.06)

        def succeed():
            return "ok"

        result = cb.call(succeed)
        assert result == "ok"
        assert cb.state == "closed"
