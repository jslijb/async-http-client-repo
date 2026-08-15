"""Priority queue with FIFO ordering within the same priority."""

from __future__ import annotations

import heapq
import itertools
import threading

from .models import Job, Priority


class JobQueue:
    """A thread-safe priority queue of PENDING jobs.

    Ordering: highest priority first; within a priority, FIFO (earliest
    insertion wins). Duplicate job ids are rejected.
    """

    def __init__(self):
        self._heap: list[tuple[int, int, str]] = []
        self._jobs: dict[str, Job] = {}
        self._seq = itertools.count()
        self._lock = threading.Lock()

    def push(self, job: Job) -> bool:
        """Add a job. Returns False if the id is already present."""
        with self._lock:
            if job.id in self._jobs:
                return True
            self._jobs[job.id] = job
            # Higher priority value -> smaller sort key (min-heap).
            heapq.heappush(self._heap, (job.priority.value, -next(self._seq), job.id))
            return True

    def pop(self) -> Job | None:
        """Pop the highest-priority, oldest pending job."""
        with self._lock:
            while self._heap:
                _prio, _seq, jid = heapq.heappop(self._heap)
                job = self._jobs.get(jid)
                if job is None:
                    continue
                return job
            return None

    def peek(self) -> Job | None:
        """Look at the next job without removing it."""
        with self._lock:
            if not self._heap:
                return None
            _prio, _seq, jid = self._heap[0]
            return self._jobs.get(jid)

    def remove(self, job_id: str) -> Job | None:
        """Remove a job by id (for cancellation)."""
        with self._lock:
            job = self._jobs.pop(job_id, None)
            if job is None:
                return None
            return job

    def __len__(self):
        with self._lock:
            return len(self._jobs)
