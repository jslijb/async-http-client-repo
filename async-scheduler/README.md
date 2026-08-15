# async-scheduler

An asynchronous job scheduler for distributed scraping workloads: priority
queueing with dedup, per-source sliding-window rate limiting, exponential
backoff with jitter and dead-lettering, a worker pool with heartbeats, and
snapshot-based crash recovery.

## Layout

```
async-scheduler/scheduler/
├── __init__.py       # public API
├── models.py         # Job, JobState, Priority, state machine
├── queue.py          # priority queue + FIFO + dedup
├── ratelimit.py      # sliding-window rate limiter
├── retry.py          # backoff + jitter + dead-letter policy
├── storage.py        # snapshot persistence + crash recovery
├── workers.py        # worker pool + heartbeat + stall detection
└── scheduler.py      # Scheduler: submit/get/claim/execute/complete/cancel/recover
```

## Requirements

- Python 3.11+
- No third-party runtime dependencies
