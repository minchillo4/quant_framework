"""
CoinAlyze Retry Handler

Intelligent retry logic that distinguishes between retryable errors
(rate limits, server errors) and non-retryable errors (bad requests, not found).
"""


class RetryHandler:
    """Determines retry behavior for different error types."""

    # Status codes that should be retried (temporary failures)
    RETRYABLE_STATUS_CODES = (429, 500, 502, 503, 504)

    # Status codes that should NOT be retried (permanent failures)
    NON_RETRYABLE_STATUS_CODES = (400, 401, 404)

    @classmethod
    def should_retry(cls, status_code: int) -> bool:
        """
        Determine if an error should be retried.

        Args:
            status_code: HTTP status code

        Returns:
            True if error is retryable, False otherwise
        """
        # Explicit non-retryable errors fail immediately
        if status_code in cls.NON_RETRYABLE_STATUS_CODES:
            return False

        # Explicit retryable errors should be retried
        if status_code in cls.RETRYABLE_STATUS_CODES:
            return True

        # Unknown 5xx errors should be retried as server issues
        if status_code >= 500:
            return True

        # All other errors (2xx success, 3xx redirects, other 4xx) should not retry
        return False

    @classmethod
    def get_retry_delay(
        cls, attempt: int, status_code: int, retry_after: str | None = None
    ) -> float:
        """
        Calculate retry delay with exponential backoff.

        Args:
            attempt: Current retry attempt (0-indexed)
            status_code: HTTP status code
            retry_after: Retry-After header value if present

        Returns:
            Number of seconds to wait before retrying
        """
        # Honor Retry-After header for rate limits
        if status_code == 429 and retry_after:
            try:
                return int(retry_after)
            except ValueError:
                pass

        # Exponential backoff: 2^attempt
        base_delay = 2**attempt

        # Different max delays for different error types
        if status_code == 429:
            # Rate limits: allow up to 60s delay
            max_delay = 60
        else:
            # Server errors: max 30s delay
            max_delay = 30

        return min(base_delay, max_delay)
