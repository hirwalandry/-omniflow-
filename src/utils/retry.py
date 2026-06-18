import time
import random
import logging
from functools import wraps
from typing import Callable, Optional, Type, Tuple

from .errors import RetryExhaustedError

logger = logging.getLogger(__name__)


def retry_with_backoff(
    max_attempts: int = 3,
    base_delay: float = 1.0,
    max_delay: float = 60.0,
    backoff_factor: float = 2.0,
    jitter: bool = True,
    exceptions: Tuple[Type[Exception], ...] = (Exception,),
) -> Callable:
    def decorator(func: Callable) -> Callable:
        @wraps(func)
        def wrapper(*args, **kwargs):
            last_exception = None
            for attempt in range(1, max_attempts + 1):
                try:
                    return func(*args, **kwargs)
                except exceptions as e:
                    last_exception = e
                    if attempt == max_attempts:
                        logger.error(f"All {max_attempts} attempts failed for {func.__name__}: {e}")
                        raise RetryExhaustedError(str(e)) from e
                    delay = min(base_delay * (backoff_factor ** (attempt - 1)), max_delay)
                    if jitter:
                        delay *= 1 + random.random() * 0.5
                    logger.warning(f"Attempt {attempt}/{max_attempts} failed for {func.__name__}: {e}. Retrying in {delay:.2f}s")
                    time.sleep(delay)
            return None
        return wrapper
    return decorator


class CircuitBreaker:
    def __init__(
        self,
        failure_threshold: int = 5,
        recovery_timeout: float = 30.0,
        half_open_max_attempts: int = 2,
    ):
        self.failure_threshold = failure_threshold
        self.recovery_timeout = recovery_timeout
        self.half_open_max_attempts = half_open_max_attempts
        self.failure_count = 0
        self.last_failure_time: Optional[float] = None
        self.state = "closed"

    def call(self, func: Callable, *args, **kwargs):
        if self.state == "open":
            if time.monotonic() - (self.last_failure_time or 0) > self.recovery_timeout:
                self.state = "half-open"
                self.failure_count = 0
            else:
                raise RuntimeError("Circuit breaker is OPEN")

        try:
            result = func(*args, **kwargs)
            if self.state == "half-open":
                self.state = "closed"
                self.failure_count = 0
            return result
        except Exception as e:
            self.failure_count += 1
            self.last_failure_time = time.monotonic()
            if self.failure_count >= self.failure_threshold:
                self.state = "open"
            raise e
