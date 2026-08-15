"""Sliding-window rate limiter."""

from __future__ import annotations

import threading
import time


class SlidingWindowLimiter:
    """Allows at most ``limit`` calls per ``window`` seconds per source key.

    Uses a deque of timestamps; old timestamps fall out of the window.
    """

    def __init__(self, limit: int = 10, window: float = 1.0):
        if limit <= 0 or window <= 0:
            raise ValueError("limit and window must be positive")
        self.limit = limit
        self.window = window
        self._hits: dict[str, list[float]] = {}
        self._lock = threading.Lock()

    def _prune(self, source: str, now: float):
        ts = self._hits.setdefault(source, [])
        cutoff = now - self.window
        # remove everything strictly older than the cutoff
        while ts and ts[0] < cutoff:
            ts.pop(0)

    def allow(self, source: str, now: float | None = None) -> bool:
        """True if a call from ``source`` may proceed now."""
        now = time.time() if now is None else now
        with self._lock:
            self._prune(source, now)
            ts = self._hits[source]
            if len(ts) <= self.limit:
                ts.append(now)
                return True
            ts.append(now)
            return False

    def pending(self, source: str, now: float | None = None) -> int:
        """Number of recorded calls still inside the window for ``source``."""
        now = time.time() if now is None else now
        with self._lock:
            self._prune(source, now)
            return len(self._hits.get(source, []))
