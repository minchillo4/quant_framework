"""Validation and error handling abstractions.

Separates validation concerns, error classification, and retry decisions
from HTTP client logic. Allows different validation/error strategies per
API provider.
"""

from dataclasses import dataclass
from typing import Any, Protocol


@dataclass
class ValidationResult:
    """Result of response validation."""

    is_valid: bool
    error_message: str | None = None
    error_code: str | None = None


class IResponseValidator(Protocol):
    """Abstraction for response validation.

    Single Responsibility: Validate that response structure/data is correct.
    Does NOT:
    - Handle HTTP errors
    - Decide on retry
    - Handle API-specific errors
    """

    def validate(self, endpoint: str, data: Any) -> ValidationResult:
        """Validate response data structure and required fields.

        Args:
            endpoint: API endpoint (e.g., "ohlcv-history", "open-interest-history")
            data: JSON-decoded response body

        Returns:
            ValidationResult with is_valid flag and optional error details
        """
        ...


class IErrorMapper(Protocol):
    """Abstraction for error classification and mapping.

    Single Responsibility: Map HTTP status codes and response bodies to
    domain-specific exceptions.
    """

    def map_error(
        self,
        status_code: int,
        body: Any,
        endpoint: str,
    ) -> Exception:
        """Map HTTP error response to domain exception.

        Args:
            status_code: HTTP status code
            body: Response body (may be JSON or text)
            endpoint: API endpoint that was called

        Returns:
            Domain-specific exception (e.g., RateLimitError, AuthenticationError)
        """
        ...


class IRetryHandler(Protocol):
    """Abstraction for retry decision and delay calculation.

    Single Responsibility: Decide if a request should be retried and
    calculate delay before next attempt.
    """

    def should_retry(self, status_code: int) -> bool:
        """Determine if request with given status code should be retried.

        Args:
            status_code: HTTP status code from failed request

        Returns:
            True if request should be retried, False otherwise
        """
        ...

    def get_retry_delay(
        self,
        attempt_number: int,
        status_code: int,
        response_headers: dict[str, str] | None = None,
    ) -> float:
        """Calculate delay before next retry attempt.

        Respects Retry-After header if present (e.g., for 429 responses).

        Args:
            attempt_number: Current attempt number (1-indexed)
            status_code: HTTP status code that triggered retry
            response_headers: Response headers that may contain Retry-After

        Returns:
            Delay in seconds
        """
        ...
