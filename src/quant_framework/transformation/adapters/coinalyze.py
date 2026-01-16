"""Coinalyze-specific normalizers for OHLCV and Open Interest data.

Coinalyze provides cryptocurrency derivatives data including:
- OHLCV candles for perpetual futures
- Open Interest with long/short breakdown
- Funding rates
- Liquidations

This module converts Coinalyze API responses to storage schema objects.
"""

from decimal import Decimal
from typing import Any

from quant_framework.transformation.normalizers import (
    BaseOHLCVNormalizer,
    BaseOpenInterestNormalizer,
    NormalizationError,
)


class CoinalyzeOHLCVNormalizer(BaseOHLCVNormalizer):
    """Normalizer for Coinalyze OHLCV candle data.

    Coinalyze returns OHLCV as dict:
    {
        "time": 1704067200000,  # Unix milliseconds
        "open": "42000.5",      # String or float
        "high": "42500.0",
        "low": "41500.25",
        "close": "42200.75",
        "volume": "1234567890"  # In USD
    }
    """

    def _extract_timestamp(self, raw_ohlcv: dict) -> int:
        """Extract timestamp from Coinalyze OHLCV.

        Args:
            raw_ohlcv: Coinalyze OHLCV dict

        Returns:
            Unix timestamp in milliseconds

        Raises:
            KeyError: If time field not found
        """
        # Coinalyze uses "time" instead of "timestamp"
        return int(raw_ohlcv["time"])

    def _extract_prices(
        self, raw_ohlcv: dict
    ) -> tuple[Decimal, Decimal, Decimal, Decimal]:
        """Extract OHLC prices from Coinalyze OHLCV.

        Args:
            raw_ohlcv: Coinalyze OHLCV dict

        Returns:
            (open, high, low, close) as Decimal objects

        Raises:
            KeyError: If price fields not found
        """
        try:
            open_price = Decimal(str(raw_ohlcv["open"]))
            high = Decimal(str(raw_ohlcv["high"]))
            low = Decimal(str(raw_ohlcv["low"]))
            close = Decimal(str(raw_ohlcv["close"]))

            return (open_price, high, low, close)
        except (KeyError, ValueError) as e:
            raise NormalizationError(
                f"Failed to extract Coinalyze prices: {str(e)}"
            ) from e

    def _extract_volume(self, raw_ohlcv: dict) -> Decimal:
        """Extract volume from Coinalyze OHLCV.

        Args:
            raw_ohlcv: Coinalyze OHLCV dict

        Returns:
            Volume as Decimal (in USD)

        Raises:
            KeyError: If volume field not found
        """
        try:
            # Coinalyze volume is in USD
            volume = Decimal(str(raw_ohlcv["volume"]))
            return volume
        except (KeyError, ValueError) as e:
            raise NormalizationError(
                f"Failed to extract Coinalyze volume: {str(e)}"
            ) from e


class CoinalyzeOINormalizer(BaseOpenInterestNormalizer):
    """Normalizer for Coinalyze Open Interest data.

    Coinalyze returns OI as dict:
    {
        "time": 1704067200000,
        "open_interest": "1234567890.50",  # Total OI in USD
        "long_oi": "650000000.25",         # Optional
        "short_oi": "584567890.25",        # Optional
        "long_short_ratio": "1.112",       # Optional
        "symbol": "BTCUSDT",
        "exchange": "binance",
        "settlement_currency": "USDT"      # Optional
    }
    """

    def _extract_timestamp(self, raw_oi: dict) -> int:
        """Extract timestamp from Coinalyze OI.

        Args:
            raw_oi: Coinalyze OI dict

        Returns:
            Unix timestamp in milliseconds

        Raises:
            KeyError: If time field not found
        """
        return int(raw_oi["time"])

    def _extract_open_interest_usd(self, raw_oi: dict) -> Decimal:
        """Extract open interest USD value from Coinalyze OI.

        Args:
            raw_oi: Coinalyze OI dict

        Returns:
            Open interest in USD as Decimal

        Raises:
            KeyError: If open_interest field not found
        """
        try:
            oi_usd = Decimal(str(raw_oi["open_interest"]))
            return oi_usd
        except (KeyError, ValueError) as e:
            raise NormalizationError(
                f"Failed to extract Coinalyze open_interest: {str(e)}"
            ) from e

    def _extract_optional_fields(self, raw_oi: dict) -> dict[str, Any]:
        """Extract optional fields from Coinalyze OI.

        Coinalyze provides long/short breakdown and settlement currency.

        Args:
            raw_oi: Coinalyze OI dict

        Returns:
            Dict of optional fields for OpenInterestRecord
        """
        optional = {}

        # Extract long OI if present
        if "long_oi" in raw_oi and raw_oi["long_oi"] is not None:
            try:
                optional["long_oi_usd"] = Decimal(str(raw_oi["long_oi"]))
            except (ValueError, TypeError):
                pass  # Skip invalid values

        # Extract short OI if present
        if "short_oi" in raw_oi and raw_oi["short_oi"] is not None:
            try:
                optional["short_oi_usd"] = Decimal(str(raw_oi["short_oi"]))
            except (ValueError, TypeError):
                pass

        # Extract long/short ratio if present
        if "long_short_ratio" in raw_oi and raw_oi["long_short_ratio"] is not None:
            try:
                optional["long_short_ratio"] = Decimal(str(raw_oi["long_short_ratio"]))
            except (ValueError, TypeError):
                pass

        # Extract settlement currency if present
        if "settlement_currency" in raw_oi and raw_oi["settlement_currency"]:
            optional["settlement_currency"] = str(raw_oi["settlement_currency"]).upper()

        return optional
