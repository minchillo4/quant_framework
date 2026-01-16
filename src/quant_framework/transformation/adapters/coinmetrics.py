"""CoinMetrics-specific normalizers for OHLCV data.

CoinMetrics provides institutional-grade cryptocurrency market data including:
- OHLCV candles for spot and derivatives markets
- Reference rates
- Network metrics
- On-chain data

This module converts CoinMetrics API responses to storage schema objects.
"""

from decimal import Decimal

from quant_framework.transformation.normalizers import (
    BaseOHLCVNormalizer,
    NormalizationError,
)


class CoinMetricsOHLCVNormalizer(BaseOHLCVNormalizer):
    """Normalizer for CoinMetrics OHLCV candle data.

    CoinMetrics returns OHLCV as dict:
    {
        "time": "2024-01-01T00:00:00.000Z",  # ISO 8601 string
        "price_open": "42000.5",
        "price_high": "42500.0",
        "price_low": "41500.25",
        "price_close": "42200.75",
        "volume": "1234.56789"  # Base currency volume
    }

    Alternative format (with Unix timestamp):
    {
        "timestamp": 1704067200,  # Unix seconds (not milliseconds!)
        "open": 42000.5,
        "high": 42500.0,
        "low": 41500.25,
        "close": 42200.75,
        "volume": 1234.56789
    }
    """

    def _extract_timestamp(self, raw_ohlcv: dict) -> int:
        """Extract timestamp from CoinMetrics OHLCV.

        Handles both ISO 8601 strings and Unix timestamps.

        Args:
            raw_ohlcv: CoinMetrics OHLCV dict

        Returns:
            Unix timestamp in milliseconds

        Raises:
            KeyError: If timestamp field not found
            ValueError: If timestamp format invalid
        """
        # Try Unix timestamp first (seconds, not milliseconds)
        if "timestamp" in raw_ohlcv:
            unix_seconds = int(raw_ohlcv["timestamp"])
            return unix_seconds * 1000  # Convert to milliseconds

        # Try ISO 8601 time string
        if "time" in raw_ohlcv:
            from datetime import datetime

            time_str = raw_ohlcv["time"]
            try:
                # Parse ISO 8601 format
                dt = datetime.fromisoformat(time_str.replace("Z", "+00:00"))
                return int(dt.timestamp() * 1000)  # Convert to milliseconds
            except ValueError as e:
                raise NormalizationError(
                    f"Invalid CoinMetrics time format: {time_str}"
                ) from e

        raise NormalizationError(
            "CoinMetrics OHLCV missing timestamp field (expected 'time' or 'timestamp')"
        )

    def _extract_prices(
        self, raw_ohlcv: dict
    ) -> tuple[Decimal, Decimal, Decimal, Decimal]:
        """Extract OHLC prices from CoinMetrics OHLCV.

        Handles both prefixed (price_open) and unprefixed (open) formats.

        Args:
            raw_ohlcv: CoinMetrics OHLCV dict

        Returns:
            (open, high, low, close) as Decimal objects

        Raises:
            KeyError: If price fields not found
        """
        try:
            # Try prefixed format (price_open, price_high, etc.)
            if "price_open" in raw_ohlcv:
                open_price = Decimal(str(raw_ohlcv["price_open"]))
                high = Decimal(str(raw_ohlcv["price_high"]))
                low = Decimal(str(raw_ohlcv["price_low"]))
                close = Decimal(str(raw_ohlcv["price_close"]))
            # Try unprefixed format (open, high, low, close)
            elif "open" in raw_ohlcv:
                open_price = Decimal(str(raw_ohlcv["open"]))
                high = Decimal(str(raw_ohlcv["high"]))
                low = Decimal(str(raw_ohlcv["low"]))
                close = Decimal(str(raw_ohlcv["close"]))
            else:
                raise NormalizationError(
                    "CoinMetrics OHLCV missing price fields (expected 'price_open' or 'open')"
                )

            return (open_price, high, low, close)
        except (KeyError, ValueError) as e:
            raise NormalizationError(
                f"Failed to extract CoinMetrics prices: {str(e)}"
            ) from e

    def _extract_volume(self, raw_ohlcv: dict) -> Decimal:
        """Extract volume from CoinMetrics OHLCV.

        Args:
            raw_ohlcv: CoinMetrics OHLCV dict

        Returns:
            Volume as Decimal (in base currency)

        Raises:
            KeyError: If volume field not found
        """
        try:
            # CoinMetrics volume is in base currency (e.g., BTC for BTC/USDT)
            volume = Decimal(str(raw_ohlcv["volume"]))
            return volume
        except (KeyError, ValueError) as e:
            raise NormalizationError(
                f"Failed to extract CoinMetrics volume: {str(e)}"
            ) from e
