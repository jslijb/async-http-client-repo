"""Data models for the job scheduler."""

from __future__ import annotations

import time
from dataclasses import dataclass, field
from enum import Enum


class JobState(Enum):
    PENDING = "pending"      # queued, waiting for a worker
    RUNNING = "running"      # claimed by a worker
    SUCCEEDED = "succeeded"  # completed successfully
    FAILED = "failed"        # exhausted retries
    CANCELLED = "cancelled"  # removed by the operator


class Priority(Enum):
    LOW = 0
    MEDIUM = 1
    HIGH = 2
    CRITICAL = 3

    def __lt__(self, other: "Priority") -> bool:
        if not isinstance(other, Priority):
            return NotImplemented
        return self.value < other.value


# Valid forward transitions of the job state machine.
VALID_TRANSITIONS = {
    JobState.PENDING: {JobState.RUNNING, JobState.CANCELLED},
    JobState.RUNNING: {JobState.SUCCEEDED, JobState.FAILED, JobState.CANCELLED},
    JobState.SUCCEEDED: set(),
    JobState.FAILED: {JobState.PENDING},  # manual requeue
    JobState.CANCELLED: set(),
}


def can_transition(old: JobState, new: JobState) -> bool:
    """True if moving from ``old`` to ``new`` is allowed by the state machine."""
    return new in VALID_TRANSITIONS.get(old, set())


@dataclass
class Job:
    """A unit of work to be fetched by a worker."""

    id: str
    url: str
    priority: Priority = Priority.MEDIUM
    state: JobState = JobState.PENDING
    source: str = "default"          # rate-limit key
    created_at: float = field(default_factory=time.time)
    attempts: int = 0
    max_retries: int = 3
    timeout: float = 30.0
    result: dict | None = None
    error: str | None = None
    last_updated: float = field(default_factory=time.time)

    def touch(self):
        self.last_updated = time.time()

    def transition(self, new: JobState) -> bool:
        """Attempt a state transition. Returns False if invalid."""
        if not can_transition(self.state, new):
            return False
        self.state = new
        self.touch()
        return True
