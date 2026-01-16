"""Concrete error handlers for CoinAlyze error mapping.

Each HTTP status code (or range) gets its own handler class.
Add new error types by creating handler and registering with ErrorMapperChain.
"""

from typing import Any

from quant_framework.ingestion.adapters.coinalyze_plugin.strategies import (
    ErrorMapperChain,
    IErrorHandler,
)


# Domain-specific exceptions
class CoinAlyzeAPIError(Exception):
    """Base exception for CoinAlyze API errors."""

    def __init__(
        self,
        message: str,
        status_code: int,
        endpoint: str,
        retry_after: float | None = None,
    ):
        super().__init__(message)
        self.status_code = status_code
        self.endpoint = endpoint
        self.retry_after = retry_after


class BadRequestError(CoinAlyzeAPIError):
    """400 - Malformed request."""

    pass


class AuthenticationError(CoinAlyzeAPIError):
    """401 - Invalid API key."""

    pass


class NotFoundError(CoinAlyzeAPIError):
    """404 - Resource not found."""

    pass


class RateLimitError(CoinAlyzeAPIError):
    """429 - Rate limit exceeded."""

    pass


class ServerError(CoinAlyzeAPIError):
    """5xx - Server error."""

    pass


# Error handlers
class BadRequestHandler(IErrorHandler):
    """Handle 400 Bad Request errors."""

    def can_handle(self, status_code: int, body: Any) -> bool:
        return status_code == 400

    def handle(self, status_code: int, body: Any, endpoint: str) -> Exception:
        message = "Bad request"
        if isinstance(body, dict):
            message = body.get("message", message)
        return BadRequestError(message, status_code, endpoint)


class AuthenticationHandler(IErrorHandler):
    """Handle 401 Unauthorized errors."""

    def can_handle(self, status_code: int, body: Any) -> bool:
        return status_code == 401

    def handle(self, status_code: int, body: Any, endpoint: str) -> Exception:
        message = "Invalid API key or authentication failed"
        if isinstance(body, dict):
            message = body.get("message", message)
        return AuthenticationError(message, status_code, endpoint)


class NotFoundHandler(IErrorHandler):
    """Handle 404 Not Found errors."""

    def can_handle(self, status_code: int, body: Any) -> bool:
        return status_code == 404

    def handle(self, status_code: int, body: Any, endpoint: str) -> Exception:
        message = f"Endpoint not found: {endpoint}"
        if isinstance(body, dict):
            message = body.get("message", message)
        return NotFoundError(message, status_code, endpoint)


class RateLimitHandler(IErrorHandler):
    """Handle 429 Too Many Requests errors."""

    def can_handle(self, status_code: int, body: Any) -> bool:
        return status_code == 429

    def handle(
        self,
        status_code: int,
        body: Any,
        endpoint: str,
    ) -> Exception:
        message = "Rate limit exceeded"
        retry_after = None

        if isinstance(body, dict):
            message = body.get("message", message)
            # Extract retry-after if provided in response
            retry_after_str = body.get("retry_after", body.get("retryAfter"))
            if retry_after_str:
                try:
                    retry_after = float(retry_after_str)
                except (ValueError, TypeError):
                    pass

        return RateLimitError(message, status_code, endpoint, retry_after=retry_after)


class ServerErrorHandler(IErrorHandler):
    """Handle 5xx Server errors."""

    def can_handle(self, status_code: int, body: Any) -> bool:
        return 500 <= status_code < 600

    def handle(self, status_code: int, body: Any, endpoint: str) -> Exception:
        message = "Server error"
        if isinstance(body, dict):
            message = body.get("message", message)
        return ServerError(message, status_code, endpoint)


def create_error_mapper_chain() -> ErrorMapperChain:
    """Factory to create pre-configured error mapper chain.

    Returns:
        Chain with all standard error handlers registered
    """
    chain = ErrorMapperChain()
    chain.register(BadRequestHandler())
    chain.register(AuthenticationHandler())
    chain.register(NotFoundHandler())
    chain.register(RateLimitHandler())
    chain.register(ServerErrorHandler())
    return chain
