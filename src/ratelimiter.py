# Simple token-bucket rate limiter for asyncio
import asyncio
import time
from typing import Optional

class TokenBucket:
    """
    Token bucket rate limiter.
    - rate: tokens per second
    - capacity: max tokens
    Usage:
      await bucket.acquire()
    """
    def __init__(self, rate: float, capacity: Optional[float] = None):
        self._rate = float(rate)
        self._capacity = float(capacity) if capacity is not None else float(rate)
        self._tokens = float(self._capacity)
        self._last = time.monotonic()
        self._lock = asyncio.Lock()

    async def acquire(self):
        """
        Acquire one token, waiting if necessary.
        """
        async with self._lock:
            while True:
                now = time.monotonic()
                elapsed = now - self._last
                self._last = now
                # refill
                self._tokens = min(self._capacity, self._tokens + elapsed * self._rate)
                if self._tokens >= 1.0:
                    self._tokens -= 1.0
                    return
                # compute wait time for next token
                to_wait = (1.0 - self._tokens) / self._rate
                await asyncio.sleep(to_wait)

    async def __aenter__(self):
        await self.acquire()
        return self

    async def __aexit__(self, exc_type, exc, tb):
        return False
