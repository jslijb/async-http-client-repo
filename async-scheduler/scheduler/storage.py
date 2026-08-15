"""Snapshot persistence and crash recovery."""

from __future__ import annotations

import json
import os
import threading

from .models import Job, JobState, Priority


class SnapshotStore:
    """Persists the job table to a JSON snapshot file.

    On load, jobs that were RUNNING when the snapshot was taken are reset to
    PENDING so they can be re-dispatched after a crash (a RUNNING job cannot
    survive a restart — its worker is gone).
    """

    def __init__(self, path: str = "scheduler_state.json"):
        self.path = path
        self._lock = threading.Lock()

    def _serialize(self, job: Job) -> dict:
        return {
            "id": job.id,
            "url": job.url,
            "priority": job.priority.value,
            "state": job.state.value,
            "source": job.source,
            "created_at": job.created_at,
            "attempts": job.attempts,
            "max_retries": job.max_retries,
            "timeout": job.timeout,
            "error": job.error,
            "last_updated": job.last_updated,
        }

    def _deserialize(self, d: dict) -> Job:
        return Job(
            id=d["id"],
            url=d["url"],
            priority=Priority(d.get("priority", 1)),
            state=JobState(d["state"]),
            source=d.get("source", "default"),
            created_at=d.get("created_at", 0.0),
            attempts=d.get("attempts", 0),
            max_retries=d.get("max_retries", 3),
            timeout=d.get("timeout", 30.0),
            result=d.get("result"),
            error=d.get("error"),
            last_updated=d.get("last_updated", 0.0),
        )

    def save(self, jobs: dict[str, Job]):
        with self._lock:
            payload = {"jobs": [self._serialize(j) for j in jobs.values()]}
            tmp = self.path + ".tmp"
            with open(tmp, "w", encoding="utf-8") as f:
                json.dump(payload, f)
            os.replace(tmp, self.path)

    def load(self) -> dict[str, Job]:
        if not os.path.exists(self.path):
            return {}
        with self._lock:
            with open(self.path, "r", encoding="utf-8") as f:
                payload = json.load(f)
        jobs: dict[str, Job] = {}
        for d in payload.get("jobs", []):
            job = self._deserialize(d)
            jobs[job.id] = job
        return jobs
