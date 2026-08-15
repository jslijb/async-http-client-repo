"""Per-host connection pool.

Known defect: the pool is keyed by the bare hostname, ignoring the port, so
requests to different ports of the same host block each other instead of each
endpoint having an independent limit.
"""

import asyncio


class ConnectionPool:
    """Limits concurrent connections per host using per-host semaphores."""

    def __init__(self, max_connections: int = 10):
        self.max_connections = max_connections
        self._sems = {}
        self._guard = asyncio.Lock()

    async def _sem(self, host: str) -> asyncio.Semaphore:
        async with self._guard:
            if host not in self._sems:
                self._sems[host] = asyncio.Semaphore(self.max_connections)
            return self._sems[host]

    async def acquire(self, host: str):
        await (await self._sem(host)).acquire()

    async def release(self, host: str):
        (await self._sem(host)).release()
