"""
CoinAlyze Response Validator

Validates API response structure to ensure schema compliance before processing.
Helps catch data quality issues early.
"""

from typing import Any


class ResponseValidator:
    """Validates CoinAlyze API response structures."""

    @staticmethod
    def validate_ohlc_response(data: Any) -> tuple[bool, str]:
        """
        Validate OHLC response structure.

        Expected format:
        [
            {
                "symbol": "BTCUSDT_PERP.A",
                "history": [
                    {"t": timestamp, "o": open, "h": high, "l": low, "c": close, "v": volume},
                    ...
                ]
            },
            ...
        ]

        Args:
            data: Response data to validate

        Returns:
            Tuple of (is_valid, error_message)
        """
        if not isinstance(data, list):
            return False, f"Response must be a list, got {type(data).__name__}"

        if len(data) == 0:
            # Empty list is valid (no data for the range)
            return True, "Valid (empty)"

        for idx, item in enumerate(data):
            if not isinstance(item, dict):
                return False, f"Item {idx} must be a dict, got {type(item).__name__}"

            if "symbol" not in item:
                return False, f"Item {idx} missing required 'symbol' field"

            if "history" not in item:
                return False, f"Item {idx} missing required 'history' field"

            if not isinstance(item["history"], list):
                return (
                    False,
                    f"Item {idx} 'history' must be a list, got {type(item['history']).__name__}",
                )

            # Validate history entries (if not empty)
            if item["history"]:
                first_entry = item["history"][0]
                required_fields = ["t", "o", "h", "l", "c"]
                for field in required_fields:
                    if field not in first_entry:
                        return (
                            False,
                            f"Item {idx} history entry missing required field '{field}'",
                        )

        return True, "Valid"

    @staticmethod
    def validate_oi_response(data: Any) -> tuple[bool, str]:
        """
        Validate Open Interest response structure.

        OI uses the same structure as OHLC (OHLC format where 'c' is the OI value).

        Args:
            data: Response data to validate

        Returns:
            Tuple of (is_valid, error_message)
        """
        # OI response has same structure as OHLC
        return ResponseValidator.validate_ohlc_response(data)

    @staticmethod
    def validate_response(endpoint: str, data: Any) -> tuple[bool, str]:
        """
        Dispatch validation based on endpoint.

        Args:
            endpoint: API endpoint name
            data: Response data to validate

        Returns:
            Tuple of (is_valid, error_message)
        """
        if endpoint == "ohlcv-history":
            return ResponseValidator.validate_ohlc_response(data)
        elif endpoint == "open-interest-history":
            return ResponseValidator.validate_oi_response(data)
        else:
            # Unknown endpoint, accept any data
            return True, "No validation defined for endpoint"
