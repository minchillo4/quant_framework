"""
CoinAlyze Error Mapper

Maps HTTP status codes and response bodies to specific exception types,
providing context-rich error messages for debugging.
"""

from typing import Any

from .exceptions import (
    AuthenticationError,
    BadRequestError,
    CoinAlyzeAPIError,
    NotFoundError,
    RateLimitError,
    ServerError,
)


class CoinAlyzeErrorMapper:
    """Maps HTTP status codes to appropriate exception types."""

    @staticmethod
    def extract_error_message(response_body: Any) -> str:
        """Extract error message from response body."""
        if isinstance(response_body, str):
            return response_body
        elif isinstance(response_body, dict):
            # Try common error message keys
            return (
                response_body.get("error")
                or response_body.get("message")
                or str(response_body)
            )
        else:
            return str(response_body)

    @staticmethod
    def map_error(
        status_code: int,
        response_body: Any,
        endpoint: str,
        retry_after: str | None = None,
    ) -> CoinAlyzeAPIError:
        """
        Map HTTP status code to specific exception with context.

        Args:
            status_code: HTTP status code
            response_body: Response body (dict, str, or other)
            endpoint: API endpoint that was called
            retry_after: Retry-After header value if present

        Returns:
            Appropriate CoinAlyzeAPIError subclass instance
        """
        error_msg = CoinAlyzeErrorMapper.extract_error_message(response_body)

        if status_code == 400:
            return BadRequestError(
                f"Bad parameters for {endpoint}: {error_msg}",
                status_code=status_code,
                endpoint=endpoint,
            )
        elif status_code == 401:
            return AuthenticationError(
                f"Invalid API key for {endpoint}: {error_msg}",
                status_code=status_code,
                endpoint=endpoint,
            )
        elif status_code == 404:
            return NotFoundError(
                f"Resource not found for {endpoint}: {error_msg}",
                status_code=status_code,
                endpoint=endpoint,
            )
        elif status_code == 429:
            retry_after_int = None
            if retry_after:
                try:
                    retry_after_int = int(retry_after)
                except ValueError:
                    pass

            return RateLimitError(
                f"Rate limit exceeded for {endpoint}: {error_msg}",
                retry_after=retry_after_int,
                status_code=status_code,
                endpoint=endpoint,
            )
        elif status_code >= 500:
            return ServerError(
                f"Server error {status_code} for {endpoint}: {error_msg}",
                status_code=status_code,
                endpoint=endpoint,
            )
        else:
            return CoinAlyzeAPIError(
                f"Unexpected error {status_code} for {endpoint}: {error_msg}",
                status_code=status_code,
                endpoint=endpoint,
            )
