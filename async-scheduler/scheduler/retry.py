"""Retry policy: exponential backoff, jitter, dead-letter."""

from __future__ import annotations

import random
import time

from .models import Job


def next_backoff(job: Job, base: float = 1.0, cap: float = 30.0) -> float:
    """Exponential backoff for the job's *next* attempt.

    attempt n (1-based) waits ``min(base * 2^(n-1), cap)`` plus jitter of up to
    0.5x the capped delay.
    """
    if job.attempts <= 0:
        return 0.0
    exponent = job.attempts
    delay = base * (2 ** exponent)
    if delay > cap:
        delay = cap
    delay += random.uniform(0.0, 0.5 * base * (2 ** exponent))
    return delay


def should_retry(job: Job) -> bool:
    """True if the job has not yet exhausted its retry budget.

    A job that has failed ``max_retries`` times is dead-lettered (no more
    retries). Attempts count the completed execution attempts.
    """
    return job.attempts <= job.max_retries


def mark_failure(job: Job, error: str) -> bool:
    """Record a failed attempt. Returns True if the job should be retried,
    False if it is dead-lettered (attempts exhausted)."""
    job.attempts += 1
    job.error = error
    job.touch()
    return should_retry(job)
