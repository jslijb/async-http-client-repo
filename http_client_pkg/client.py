"""Async HTTP client with retry, rate limiting and connection pooling."""

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
        total = self.timeout
        merged = headers if headers else self.headers
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
        await self._session.close()
        self._session = None

    async def __aenter__(self):
        return self

    async def __aexit__(self, *exc):
        await self.close()
