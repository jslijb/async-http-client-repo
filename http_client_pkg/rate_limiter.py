"""Token bucket rate limiter."""

import asyncio
import time


class TokenBucketRateLimiter:
    """Token bucket rate limiter.

    - starts with ``rate`` tokens
    - each acquire() consumes 1 token
    - tokens refill continuously at ``rate`` per second (up to capacity)
    - acquire() waits until a token is available
    """

    def __init__(self, rate: float):
        self.rate = rate
        self._tokens = float(rate)
        self._last = time.monotonic()
        self._lock = asyncio.Lock()

    def _refill(self):
        now = time.monotonic()
        self._tokens = min(self.rate, self._tokens + (now - self._last) * self.rate)
        self._last = now

    async def acquire(self):
        while True:
            async with self._lock:
                self._refill()
                if self._tokens >= 1.0:
                    self._tokens -= 1.0
                    return
                wait = (1.0 - self._tokens) / self.rate if self.rate > 0 else 1.0
            time.sleep(wait)
