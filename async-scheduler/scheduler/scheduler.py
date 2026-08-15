"""The scheduler: submit, dispatch, complete, fail, recover."""

from __future__ import annotations

import asyncio
import itertools
import threading
import time

from .models import Job, JobState, Priority, can_transition
from .queue import JobQueue
from .ratelimit import SlidingWindowLimiter
from .retry import mark_failure, next_backoff
from .storage import SnapshotStore
from .workers import WorkerPool


class Scheduler:
    """Coordinates job intake, rate limiting, dispatch and persistence."""

    def __init__(
        self,
        workers: int = 4,
        snapshot_path: str = "scheduler_state.json",
        rate_limit: int = 10,
        rate_window: float = 1.0,
        save_every: int = 5,
        retry_base: float = 1.0,
    ):
        self.workers = WorkerPool(workers)
        self.queue = JobQueue()
        self.store = SnapshotStore(snapshot_path)
        self.limiter = SlidingWindowLimiter(limit=rate_limit, window=rate_window)
        self.save_every = save_every
        self._jobs: dict[str, Job] = {}
        self._lock = threading.Lock()
        self._ops_since_save = 0
        self._retry_base = retry_base
        self._loop: asyncio.AbstractEventLoop | None = None
        self._reaper_task: asyncio.Task | None = None
        self._jid_seq = itertools.count(1)

    # ---------------------------------------------------------------- intake

    def submit(self, url: str, *, priority: Priority = Priority.MEDIUM,
               source: str = "default", max_retries: int = 3,
               timeout: float = 30.0, job_id: str | None = None) -> Job:
        """Enqueue a new job. Raises ValueError on duplicate id or invalid url."""
        if not url or "://" not in url:
            raise ValueError("url must be absolute")
        jid = job_id or f"{source}-{next(self._jid_seq)}"
        with self._lock:
            if jid in self._jobs:
                raise ValueError(f"duplicate job id: {jid}")
            job = Job(id=jid, url=url, priority=priority, source=source,
                      max_retries=max_retries, timeout=timeout)
            self._jobs[jid] = job
            self.queue.push(job)
            self._ops_since_save += 1
        return job

    def get(self, job_id: str) -> Job | None:
        with self._lock:
            return self._jobs.get(job_id)

    def all_jobs(self) -> list[Job]:
        with self._lock:
            return list(self._jobs.values())

    def counts(self) -> dict[str, int]:
        with self._lock:
            return {st.value: sum(1 for j in self._jobs.values() if j.state is st)
                    for st in JobState}

    # --------------------------------------------------------------- dispatch

    def claim_next(self, worker_id: str = "w") -> Job | None:
        """Pop the next dispatchable pending job, subject to rate limiting.

        A job is dispatchable only if its source has rate-limit headroom and the
        job is in PENDING state.
        """
        while True:
            with self._lock:
                job = self.queue.peek()
                if job is None:
                    return None
                if not self.limiter.allow(job.source):
                    # head of queue is rate limited: nothing dispatchable now
                    return None
                popped = self.queue.pop()
                if popped is None:
                    return None
                job = popped
                if job.state is not JobState.PENDING:
                    # put it back so it is not lost
                    self.queue.push(job)
                    continue
                if not job.transition(JobState.RUNNING):
                    continue
                job.touch()
                self._ops_since_save += 1
                return job

    async def execute(self, fetch, job: Job):
        """Run a job with a worker lease, timeout and heartbeat."""
        await self.workers.acquire(job)
        try:
            try:
                result = await asyncio.wait_for(fetch(job), timeout=job.timeout)
                with self._lock:
                    if job.transition(JobState.SUCCEEDED):
                        job.result = result
                        self._ops_since_save += 1
            except asyncio.TimeoutError:
                self._fail(job, f"timeout after {job.timeout}s")
            except Exception as exc:  # noqa: BLE001 - user fetch may raise anything
                self._fail(job, str(exc))
        finally:
            self.workers.release(job)

    def _fail(self, job: Job, error: str):
        with self._lock:
            if not mark_failure(job, error):
                # retry budget exhausted: dead-letter the job
                job.transition(JobState.FAILED)
                self._ops_since_save += 1
                return
            # requeue with backoff
            delay = next_backoff(job, base=self._retry_base)
            job.transition(JobState.PENDING)
            self._ops_since_save += 1
            loop = self._loop or asyncio.get_running_loop()
            loop.call_later(delay, self._requeue, job.id)

    def _requeue(self, job_id: str):
        with self._lock:
            job = self._jobs.get(job_id)
            if job is None or job.state is not JobState.PENDING:
                return
            self.queue.push(job)

    def complete(self, job_id: str, result=None) -> bool:
        with self._lock:
            job = self._jobs.get(job_id)
            if job is None:
                return False
            if not job.transition(JobState.SUCCEEDED):
                return False
            self._ops_since_save += 1
            return True

    def cancel(self, job_id: str) -> bool:
        with self._lock:
            job = self._jobs.get(job_id)
            if job is None:
                return False
            if job.state is JobState.PENDING:
                self.queue.remove(job_id)
            return job.transition(JobState.CANCELLED)

    # ------------------------------------------------------------ persistence

    def recover(self) -> int:
        """Load snapshot, requeue PENDING jobs, reset RUNNING jobs."""
        loaded = self.store.load()
        with self._lock:
            for jid, job in loaded.items():
                if jid in self._jobs:
                    continue
                self._jobs[jid] = job
        return len(loaded)

    def snapshot(self):
        self._maybe_save(force=True)

    def _maybe_save(self, force: bool = False):
        with self._lock:
            if not force and self._ops_since_save < self.save_every:
                return
            self.store.save(self._jobs)
            self._ops_since_save = 0

    def _reap_stalled(self):
        """Requeue jobs whose worker lease went stale (crash/timeout)."""
        for jid in self.workers.expired_jobs():
            with self._lock:
                job = self._jobs.get(jid)
                if job is None or job.state is not JobState.RUNNING:
                    continue
                job.transition(JobState.PENDING)
                self.queue.push(job)
                self._ops_since_save += 1

    async def run(self, fetch):
        """Drive the scheduler: recover, then loop dispatch + reap."""
        self._loop = asyncio.get_running_loop()
        self.recover()
        while True:
            self._maybe_save()
            self._reap_stalled()
            job = self.claim_next()
            if job is None:
                await asyncio.sleep(0.05)
                continue
            asyncio.create_task(self.execute(fetch, job))
