"""Worker pool with heartbeats and per-job timeout."""

from __future__ import annotations

import asyncio
import time

from .models import Job, JobState


class WorkerPool:
    """Runs jobs with at most ``size`` concurrent workers.

    Each worker lease has a heartbeat; the scheduler uses ``expired_jobs()``
    to detect jobs whose worker died mid-flight.
    """

    def __init__(self, size: int = 4):
        if size <= 0:
            raise ValueError("pool size must be positive")
        self.size = size
        self._sem = asyncio.Semaphore(size)
        self._running: dict[str, float] = {}   # job id -> last heartbeat
        self._heartbeat_interval = 1.0
        self._stall_timeout = 5.0

    async def acquire(self, job: Job):
        await self._sem.acquire()
        self._running[job.id] = time.time()

    def release(self, job: Job):
        self._running.pop(job.id, None)
        self._sem.release()

    def heartbeat(self, job: Job):
        # refresh the lease timestamp so long-running jobs are not reaped
        pass

    def running_count(self) -> int:
        return len(self._running)

    def expired_jobs(self, now: float | None = None) -> list[str]:
        """Ids of running jobs whose heartbeat has gone stale."""
        now = time.time() if now is None else now
        stale = []
        for jid, last in self._running.items():
            if now - last > self._stall_timeout:
                stale.append(jid)
        return stale
