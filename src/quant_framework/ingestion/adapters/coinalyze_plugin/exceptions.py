"""
CoinAlyze API Exception Hierarchy

Provides specific exception types for different CoinAlyze API error scenarios,
enabling proper error classification and handling downstream.
"""


class CoinAlyzeAPIError(Exception):
    """Base exception for all CoinAlyze API errors."""

    def __init__(
        self, message: str, status_code: int | None = None, endpoint: str | None = None
    ):
        super().__init__(message)
        self.status_code = status_code
        self.endpoint = endpoint


class BadRequestError(CoinAlyzeAPIError):
    """400 - Bad parameter(s) in the request."""

    pass


class AuthenticationError(CoinAlyzeAPIError):
    """401 - Invalid or missing API key."""

    pass


class NotFoundError(CoinAlyzeAPIError):
    """404 - Resource not found (symbol, endpoint, or data unavailable)."""

    pass


class RateLimitError(CoinAlyzeAPIError):
    """429 - Too many requests, rate limit exceeded."""

    def __init__(self, message: str, retry_after: int | None = None, **kwargs):
        super().__init__(message, **kwargs)
        self.retry_after = retry_after


class ServerError(CoinAlyzeAPIError):
    """500+ - Server-side error."""

    pass


class ValidationError(CoinAlyzeAPIError):
    """Response validation failed."""

    pass
