"""Concrete validation and retry handler implementations.

Extracted from CoinalyzeClient to support dependency injection.
"""

from typing import Any

from quant_framework.ingestion.config.value_objects import (
    RetryConfig,
    ValidationConfig,
)
from quant_framework.ingestion.ports.validators import (
    IResponseValidator,
    IRetryHandler,
    ValidationResult,
)


class CoinAlyzeResponseValidator(IResponseValidator):
    """Validates CoinAlyze API responses.

    Extracted from original validation logic.
    """

    def __init__(self, config: ValidationConfig):
        """Initialize validator.

        Args:
            config: Validation configuration
        """
        self.config = config

    def validate(self, endpoint: str, data: Any) -> ValidationResult:
        """Validate response data structure.

        Args:
            endpoint: API endpoint (e.g., "ohlcv-history")
            data: JSON-decoded response body

        Returns:
            ValidationResult with validity and optional error message
        """
        if not isinstance(data, dict):
            return ValidationResult(
                is_valid=False,
                error_message="Response body is not a JSON object",
                error_code="INVALID_STRUCTURE",
            )

        # Endpoint-specific validation
        if endpoint == "ohlcv-history":
            return self._validate_ohlcv_response(data)
        elif endpoint == "open-interest-history":
            return self._validate_oi_response(data)
        else:
            return self._validate_generic_response(data)

    def _validate_ohlcv_response(self, data: dict) -> ValidationResult:
        """Validate OHLCV response structure."""
        if "data" not in data:
            return ValidationResult(
                is_valid=False,
                error_message="Missing 'data' field in response",
                error_code="MISSING_DATA_FIELD",
            )

        candles = data.get("data", [])
        if not isinstance(candles, list):
            return ValidationResult(
                is_valid=False,
                error_message="'data' field must be an array",
                error_code="INVALID_DATA_TYPE",
            )

        # Check first candle structure (if present)
        if candles:
            first = candles[0]
            required_fields = {"t", "o", "h", "l", "c", "v"}
            missing = required_fields - set(first.keys())
            if missing:
                return ValidationResult(
                    is_valid=False,
                    error_message=f"Missing required OHLC fields: {missing}",
                    error_code="MISSING_OHLC_FIELDS",
                )

        return ValidationResult(is_valid=True)

    def _validate_oi_response(self, data: dict) -> ValidationResult:
        """Validate Open Interest response structure."""
        if "data" not in data:
            return ValidationResult(
                is_valid=False,
                error_message="Missing 'data' field in response",
                error_code="MISSING_DATA_FIELD",
            )

        oi_data = data.get("data", [])
        if not isinstance(oi_data, list):
            return ValidationResult(
                is_valid=False,
                error_message="'data' field must be an array",
                error_code="INVALID_DATA_TYPE",
            )

        # Check first record
        if oi_data:
            first = oi_data[0]
            required_fields = {"t", "o"}  # time and open interest
            missing = required_fields - set(first.keys())
            if missing:
                return ValidationResult(
                    is_valid=False,
                    error_message=f"Missing required OI fields: {missing}",
                    error_code="MISSING_OI_FIELDS",
                )

        return ValidationResult(is_valid=True)

    def _validate_generic_response(self, data: dict) -> ValidationResult:
        """Validate generic response has expected structure."""
        # Generic validation just checks it's a dict
        return ValidationResult(is_valid=True)


class CoinAlyzeRetryHandler(IRetryHandler):
    """Handles retry logic for CoinAlyze API requests.

    Extracted from original retry logic in client.
    """

    def __init__(self, config: RetryConfig):
        """Initialize retry handler.

        Args:
            config: Retry configuration
        """
        self.config = config

    def should_retry(self, status_code: int) -> bool:
        """Determine if status code should be retried.

        Args:
            status_code: HTTP status code

        Returns:
            True if should retry
        """
        return status_code in self.config.retryable_status_codes

    def get_retry_delay(
        self,
        attempt_number: int,
        status_code: int,
        response_headers: dict[str, str] | None = None,
    ) -> float:
        """Calculate delay before retry.

        Respects Retry-After header if present (429 responses).

        Args:
            attempt_number: Current attempt (1-indexed)
            status_code: HTTP status code
            response_headers: Response headers that may have Retry-After

        Returns:
            Delay in seconds
        """
        # Check for Retry-After header (for 429 responses)
        if response_headers and "Retry-After" in response_headers:
            try:
                return float(response_headers["Retry-After"])
            except (ValueError, TypeError):
                pass

        # Exponential backoff: base_delay * multiplier^(attempt - 1)
        delay = self.config.base_delay * (
            self.config.backoff_multiplier ** (attempt_number - 1)
        )

        # Cap at max_delay
        return min(delay, self.config.max_delay)
