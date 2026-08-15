# async-http-client

A small asynchronous HTTP client library for Python, built on `aiohttp`.

Provides:

- `AsyncHttpClient` — async GET/POST/PUT/DELETE/PATCH with per-request timeout
- `TokenBucketRateLimiter` — token-bucket request pacing
- `ConnectionPool` — per-host connection concurrency limiting
- A custom exception hierarchy (`HttpClientError` and subclasses)

## Layout

```
http_client_pkg/
├── __init__.py            # package exports
├── client.py              # AsyncHttpClient
├── exceptions.py          # exception hierarchy
├── rate_limiter.py        # TokenBucketRateLimiter
└── connection_pool.py     # ConnectionPool
```

## Usage

```python
import asyncio
from http_client_pkg import AsyncHttpClient

async def main():
    async with AsyncHttpClient(base_url="http://example.com", timeout=10.0) as client:
        result = await client.get("/api/items")
        print(result["status"], result["json"])

asyncio.run(main())
```

## Requirements

- Python 3.11+
- `aiohttp`
