"""Exception hierarchy for the async HTTP client library."""


class HttpClientError(Exception):
    """Base exception for all HTTP client errors."""

    pass


class RequestTimeout(HttpClientError):
    """Request exceeded the timeout limit."""

    pass


class RateLimitError(HttpClientError):
    """Rate limit exceeded (e.g. the server returned HTTP 429); should retry."""

    pass


class ConnectionError(HttpClientError):
    """Failed to establish a connection."""

    pass


class HttpResponseError(HttpClientError):
    """HTTP response indicated an error (4xx/5xx)."""

    def __init__(self, message="HTTP error", status=None):
        super().__init__(message)
        self.status = status
