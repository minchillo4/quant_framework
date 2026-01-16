"""CCXT-specific normalizers for OHLCV and Trade data.

CCXT is a unified cryptocurrency exchange library that provides:
- Standardized OHLCV format: [timestamp, open, high, low, close, volume]
- Standardized trade format: {'timestamp': ..., 'price': ..., 'amount': ..., 'side': ...}

This module converts CCXT responses to storage schema objects.
"""

from decimal import Decimal

from quant_framework.transformation.normalizers import (
    BaseOHLCVNormalizer,
    BaseTradeNormalizer,
    NormalizationError,
)


class CCXTOHLCVNormalizer(BaseOHLCVNormalizer):
    """Normalizer for CCXT OHLCV candle data.

    CCXT returns OHLCV as: [timestamp, open, high, low, close, volume]
    - timestamp: Unix milliseconds (int)
    - open, high, low, close, volume: floats (need conversion to Decimal)

    Example:
        [1704067200000, 42000.5, 42500.0, 41500.25, 42200.75, 1234.56]
    """

    def _extract_timestamp(self, raw_ohlcv: dict | list) -> int:
        """Extract timestamp from CCXT OHLCV.

        Args:
            raw_ohlcv: CCXT OHLCV array [timestamp, o, h, l, c, v]

        Returns:
            Unix timestamp in milliseconds

        Raises:
            KeyError: If timestamp not found
            ValueError: If timestamp invalid
        """
        if isinstance(raw_ohlcv, dict):
            # Some CCXT methods return dict format
            return int(raw_ohlcv["timestamp"])
        elif isinstance(raw_ohlcv, list):
            # Standard CCXT array format
            return int(raw_ohlcv[0])
        else:
            raise NormalizationError(f"Unexpected CCXT OHLCV format: {type(raw_ohlcv)}")

    def _extract_prices(
        self, raw_ohlcv: dict | list
    ) -> tuple[Decimal, Decimal, Decimal, Decimal]:
        """Extract OHLC prices from CCXT OHLCV.

        Args:
            raw_ohlcv: CCXT OHLCV array [timestamp, o, h, l, c, v]

        Returns:
            (open, high, low, close) as Decimal objects

        Raises:
            KeyError: If price fields not found
            ValueError: If prices invalid
        """
        try:
            if isinstance(raw_ohlcv, dict):
                # Dict format: {'open': ..., 'high': ..., 'low': ..., 'close': ...}
                open_price = Decimal(str(raw_ohlcv["open"]))
                high = Decimal(str(raw_ohlcv["high"]))
                low = Decimal(str(raw_ohlcv["low"]))
                close = Decimal(str(raw_ohlcv["close"]))
            elif isinstance(raw_ohlcv, list):
                # Array format: [timestamp, open, high, low, close, volume]
                open_price = Decimal(str(raw_ohlcv[1]))
                high = Decimal(str(raw_ohlcv[2]))
                low = Decimal(str(raw_ohlcv[3]))
                close = Decimal(str(raw_ohlcv[4]))
            else:
                raise NormalizationError(
                    f"Unexpected CCXT OHLCV format: {type(raw_ohlcv)}"
                )

            return (open_price, high, low, close)
        except (IndexError, KeyError, ValueError) as e:
            raise NormalizationError(f"Failed to extract CCXT prices: {str(e)}") from e

    def _extract_volume(self, raw_ohlcv: dict | list) -> Decimal:
        """Extract volume from CCXT OHLCV.

        Args:
            raw_ohlcv: CCXT OHLCV array [timestamp, o, h, l, c, v]

        Returns:
            Volume as Decimal

        Raises:
            KeyError: If volume field not found
            ValueError: If volume invalid
        """
        try:
            if isinstance(raw_ohlcv, dict):
                volume = Decimal(str(raw_ohlcv["volume"]))
            elif isinstance(raw_ohlcv, list):
                volume = Decimal(str(raw_ohlcv[5]))
            else:
                raise NormalizationError(
                    f"Unexpected CCXT OHLCV format: {type(raw_ohlcv)}"
                )

            return volume
        except (IndexError, KeyError, ValueError) as e:
            raise NormalizationError(f"Failed to extract CCXT volume: {str(e)}") from e


class CCXTTradeNormalizer(BaseTradeNormalizer):
    """Normalizer for CCXT trade data.

    CCXT returns trades as dict:
    {
        'id': '12345',
        'timestamp': 1704067200000,
        'datetime': '2024-01-01T00:00:00.000Z',
        'symbol': 'BTC/USDT',
        'order': None,
        'type': 'limit',
        'side': 'buy',
        'takerOrMaker': 'taker',
        'price': 42000.5,
        'amount': 0.123,
        'cost': 5166.0615,
        'fee': {'cost': 5.166, 'currency': 'USDT'},
        'info': {...}  # Raw exchange response
    }
    """

    def _extract_timestamp(self, raw_trade: dict) -> int:
        """Extract timestamp from CCXT trade.

        Args:
            raw_trade: CCXT trade dict

        Returns:
            Unix timestamp in milliseconds

        Raises:
            KeyError: If timestamp not found
        """
        return int(raw_trade["timestamp"])

    def _extract_price(self, raw_trade: dict) -> Decimal:
        """Extract trade price from CCXT trade.

        Args:
            raw_trade: CCXT trade dict

        Returns:
            Trade price as Decimal

        Raises:
            KeyError: If price not found
        """
        return Decimal(str(raw_trade["price"]))

    def _extract_quantity(self, raw_trade: dict) -> Decimal:
        """Extract trade quantity from CCXT trade.

        Args:
            raw_trade: CCXT trade dict

        Returns:
            Trade quantity as Decimal

        Raises:
            KeyError: If amount not found
        """
        return Decimal(str(raw_trade["amount"]))

    def _extract_side(self, raw_trade: dict) -> str:
        """Extract trade side from CCXT trade.

        Args:
            raw_trade: CCXT trade dict

        Returns:
            Trade side as 'buy' or 'sell'

        Raises:
            KeyError: If side not found
        """
        side = raw_trade["side"].lower()
        if side not in ("buy", "sell"):
            raise NormalizationError(
                f"Invalid CCXT trade side: {side} (expected 'buy' or 'sell')"
            )
        return side
