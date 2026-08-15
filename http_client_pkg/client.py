"""Async HTTP client with retry, rate limiting and connection pooling.

Known bugs (all described in the issue tracker / task instruction):

Known defects (tracked in the project issue list / described in the task
instruction):

- 429 responses are not treated as retryable rate limiting.
- Request-specific headers replace defaults instead of merging.
- Per-request timeout is accepted but ignored.
- close() is not safe to call repeatedly or before any request.
- Waiting on the rate limiter freezes the event loop.
- The connection pool does not isolate different ports of the same host.
"""

import asyncio
import random
from urllib.parse import urlparse

import aiohttp

from .connection_pool import ConnectionPool
from .exceptions import (
    ConnectionError as ClientConnError,
    HttpResponseError,
    RateLimitError,
    RequestTimeout,
)
from .rate_limiter import TokenBucketRateLimiter

RETRYABLE = (RequestTimeout, RateLimitError, ClientConnError)


class AsyncHttpClient:
    def __init__(
        self,
        base_url: str = "",
        timeout: float = 30.0,
        max_retries: int = 3,
        retry_delay: float = 1.0,
        rate_limit: float = 10.0,
        max_connections: int = 10,
        headers: dict = None,
    ):
        self.base_url = base_url
        self.timeout = timeout
        self.max_retries = max_retries
        self.retry_delay = retry_delay
        self.rate_limit = rate_limit
        self.max_connections = max_connections
        self.headers = dict(headers) if headers else {}
        self._limiter = TokenBucketRateLimiter(rate_limit)
        self._pool = ConnectionPool(max_connections)
        self._session = None

    async def _get_session(self) -> aiohttp.ClientSession:
        if self._session is None or self._session.closed:
            self._session = aiohttp.ClientSession()
        return self._session

    @staticmethod
    def _join(base_url: str, path: str) -> str:
        if path.startswith(("http://", "https://")):
            return path
        return base_url.rstrip("/") + "/" + path.lstrip("/")

    async def get(self, path, params=None, headers=None):
        return await self.request("GET", path, params=params, headers=headers)

    async def post(self, path, data=None, json=None, headers=None):
        return await self.request("POST", path, data=data, json=json, headers=headers)

    async def put(self, path, data=None, json=None, headers=None):
        return await self.request("PUT", path, data=data, json=json, headers=headers)

    async def delete(self, path, headers=None):
        return await self.request("DELETE", path, headers=headers)

    async def patch(self, path, data=None, json=None, headers=None):
        return await self.request("PATCH", path, data=data, json=json, headers=headers)

    async def request(
        self,
        method,
        path,
        params=None,
        data=None,
        json=None,
        headers=None,
        timeout=None,
    ):
        url = self._join(self.base_url, path)
        # Symptom: callers cannot shorten the timeout for a single request;
        # passing a per-request timeout has no effect.
        total = self.timeout
        # Symptom: default headers (auth, content-type) vanish whenever a
        # request passes its own headers.
        merged = headers if headers else self.headers
        # Symptom: requests to different ports on the same host block each
        # other, as if they shared one connection pool.
        endpoint = urlparse(url).hostname or "localhost"
        delay = self.retry_delay

        for attempt in range(self.max_retries + 1):
            await self._limiter.acquire()
            await self._pool.acquire(endpoint)
            try:
                try:
                    session = await self._get_session()
                    async with session.request(
                        method,
                        url,
                        params=params,
                        data=data,
                        json=json,
                        headers=merged or None,
                        timeout=aiohttp.ClientTimeout(total=total),
                    ) as resp:
                        text = await resp.text()
                        try:
                            parsed = await resp.json()
                        except Exception:
                            parsed = None
                        # Symptom: a 429 "rate limited" response is not
                        # retried even though it is a transient condition.
                        if resp.status >= 400:
                            raise HttpResponseError(
                                f"HTTP {resp.status} on {method} {url}",
                                status=resp.status,
                            )
                        return {
                            "status": resp.status,
                            "headers": dict(resp.headers),
                            "json": parsed,
                            "text": text,
                        }
                except asyncio.TimeoutError:
                    raise RequestTimeout(
                        f"request timed out after {total}s: {method} {url}"
                    )
                except aiohttp.ClientError as exc:
                    raise ClientConnError(
                        f"connection error for {method} {url}: {exc}"
                    )
            except RETRYABLE as exc:
                if attempt == self.max_retries:
                    raise
                capped = min(delay, 30.0)
                await asyncio.sleep(capped + random.uniform(0.0, 0.5 * capped))
                delay *= 2
            finally:
                await self._pool.release(endpoint)
        raise ClientConnError("unreachable")

    async def close(self):
        # Symptom: calling close() twice, or before any request, crashes with
        # an AttributeError instead of being a safe no-op.
        await self._session.close()
        self._session = None

    async def __aenter__(self):
        return self

    async def __aexit__(self, *exc):
        await self.close()
