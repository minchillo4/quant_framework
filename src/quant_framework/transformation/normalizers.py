"""Base normalizer implementations for common data sources.

Provides:
- BaseOHLCVNormalizer: Template for converting API OHLCV → OHLCVRecord
- BaseOpenInterestNormalizer: Template for converting API OI → OpenInterestRecord
- BaseTradeNormalizer: Template for converting API trades → TradeRecord

Each source (CCXT, Coinalyze, CoinMetrics) creates subclass with source-specific logic.
"""

from decimal import Decimal
from typing import Any

from quant_framework.storage.schemas import (
    OHLCVRecord,
    OpenInterestRecord,
    TradeRecord,
)


class NormalizationError(Exception):
    """Raised when normalization of raw data fails."""

    pass


class BaseOHLCVNormalizer:
    """Base normalizer for OHLCV data.

    Source adapters override:
    - _extract_timestamp(): Get timestamp from API format
    - _extract_prices(): Get o, h, l, c from API format
    - _extract_volume(): Get volume from API format
    """

    def __init__(self, symbol: str, exchange: str):
        """Initialize normalizer.

        Args:
            symbol: Trading symbol (e.g., "BTC/USDT")
            exchange: Exchange name (e.g., "binance", "coinbase")
        """
        self.symbol = symbol
        self.exchange = exchange

    async def normalize_single(self, raw_ohlcv: dict) -> OHLCVRecord:
        """Normalize single OHLCV record from API.

        Args:
            raw_ohlcv: Raw OHLCV record from API

        Returns:
            OHLCVRecord with validated fields

        Raises:
            NormalizationError: If extraction fails
        """
        try:
            timestamp = self._extract_timestamp(raw_ohlcv)
            open_price, high, low, close = self._extract_prices(raw_ohlcv)
            volume = self._extract_volume(raw_ohlcv)

            return OHLCVRecord(
                symbol=self.symbol,
                exchange=self.exchange,
                timestamp=timestamp,
                open=open_price,
                high=high,
                low=low,
                close=close,
                volume=volume,
            )
        except (KeyError, ValueError, TypeError) as e:
            raise NormalizationError(
                f"Failed to normalize OHLCV for {self.symbol}: {str(e)}"
            ) from e

    async def normalize_batch(self, raw_ohlcvs: list[dict]) -> list[OHLCVRecord]:
        """Normalize batch of OHLCV records.

        Args:
            raw_ohlcvs: List of raw OHLCV records from API

        Returns:
            List of OHLCVRecord objects

        Raises:
            NormalizationError: If batch normalization fails
        """
        records = []
        errors = []

        for i, raw in enumerate(raw_ohlcvs):
            try:
                record = await self.normalize_single(raw)
                records.append(record)
            except NormalizationError as e:
                errors.append(f"Index {i}: {str(e)}")

        if errors and len(errors) == len(raw_ohlcvs):
            raise NormalizationError(f"Batch normalization failed: {errors}")

        return records

    def _extract_timestamp(self, raw_ohlcv: dict) -> int:
        """Extract timestamp (Unix milliseconds) from API format.

        Override in subclass to handle source-specific formats.

        Args:
            raw_ohlcv: Raw OHLCV record

        Returns:
            Unix timestamp in milliseconds

        Raises:
            KeyError: If timestamp field not found
        """
        raise NotImplementedError(
            f"{self.__class__.__name__} must implement _extract_timestamp()"
        )

    def _extract_prices(
        self, raw_ohlcv: dict
    ) -> tuple[Decimal, Decimal, Decimal, Decimal]:
        """Extract OHLC prices from API format.

        Override in subclass to handle source-specific formats.

        Args:
            raw_ohlcv: Raw OHLCV record

        Returns:
            (open, high, low, close) as Decimal objects

        Raises:
            KeyError: If price fields not found
        """
        raise NotImplementedError(
            f"{self.__class__.__name__} must implement _extract_prices()"
        )

    def _extract_volume(self, raw_ohlcv: dict) -> Decimal:
        """Extract volume from API format.

        Override in subclass to handle source-specific formats.

        Args:
            raw_ohlcv: Raw OHLCV record

        Returns:
            Volume as Decimal

        Raises:
            KeyError: If volume field not found
        """
        raise NotImplementedError(
            f"{self.__class__.__name__} must implement _extract_volume()"
        )


class BaseOpenInterestNormalizer:
    """Base normalizer for Open Interest data.

    Source adapters override:
    - _extract_timestamp(): Get timestamp from API format
    - _extract_open_interest_usd(): Get OI USD value from API format
    - _extract_optional_fields(): Get optional fields (long/short OI, etc.)
    """

    def __init__(self, symbol: str, exchange: str):
        """Initialize normalizer.

        Args:
            symbol: Trading symbol (e.g., "BTC/USDT")
            exchange: Exchange name (e.g., "binance", "bybit")
        """
        self.symbol = symbol
        self.exchange = exchange

    async def normalize_single(self, raw_oi: dict) -> OpenInterestRecord:
        """Normalize single OI record from API.

        Args:
            raw_oi: Raw OI record from API

        Returns:
            OpenInterestRecord with validated fields

        Raises:
            NormalizationError: If extraction fails
        """
        try:
            timestamp = self._extract_timestamp(raw_oi)
            open_interest_usd = self._extract_open_interest_usd(raw_oi)

            # Optional fields (source-specific)
            optional_fields = self._extract_optional_fields(raw_oi)

            return OpenInterestRecord(
                symbol=self.symbol,
                exchange=self.exchange,
                timestamp=timestamp,
                open_interest_usd=open_interest_usd,
                **optional_fields,
            )
        except (KeyError, ValueError, TypeError) as e:
            raise NormalizationError(
                f"Failed to normalize OI for {self.symbol}: {str(e)}"
            ) from e

    async def normalize_batch(self, raw_ois: list[dict]) -> list[OpenInterestRecord]:
        """Normalize batch of OI records.

        Args:
            raw_ois: List of raw OI records from API

        Returns:
            List of OpenInterestRecord objects

        Raises:
            NormalizationError: If batch normalization fails
        """
        records = []
        errors = []

        for i, raw in enumerate(raw_ois):
            try:
                record = await self.normalize_single(raw)
                records.append(record)
            except NormalizationError as e:
                errors.append(f"Index {i}: {str(e)}")

        if errors and len(errors) == len(raw_ois):
            raise NormalizationError(f"Batch normalization failed: {errors}")

        return records

    def _extract_timestamp(self, raw_oi: dict) -> int:
        """Extract timestamp (Unix milliseconds) from API format.

        Override in subclass to handle source-specific formats.

        Args:
            raw_oi: Raw OI record

        Returns:
            Unix timestamp in milliseconds

        Raises:
            KeyError: If timestamp field not found
        """
        raise NotImplementedError(
            f"{self.__class__.__name__} must implement _extract_timestamp()"
        )

    def _extract_open_interest_usd(self, raw_oi: dict) -> Decimal:
        """Extract open interest USD value from API format.

        Override in subclass to handle source-specific formats.

        Args:
            raw_oi: Raw OI record

        Returns:
            Open interest in USD as Decimal

        Raises:
            KeyError: If OI field not found
        """
        raise NotImplementedError(
            f"{self.__class__.__name__} must implement _extract_open_interest_usd()"
        )

    def _extract_optional_fields(self, raw_oi: dict) -> dict[str, Any]:
        """Extract optional fields (long/short OI, settlement currency, etc.).

        Override in subclass to extract source-specific optional fields.

        Args:
            raw_oi: Raw OI record

        Returns:
            Dict of optional fields to include in OpenInterestRecord
            (e.g., {"long_oi_usd": ..., "settlement_currency": ...})
        """
        return {}


class BaseTradeNormalizer:
    """Base normalizer for Trade data.

    Source adapters override:
    - _extract_timestamp(): Get timestamp from API format
    - _extract_price(): Get price from API format
    - _extract_quantity(): Get trade quantity from API format
    - _extract_side(): Get trade side (buy/sell) from API format
    """

    def __init__(self, symbol: str, exchange: str):
        """Initialize normalizer.

        Args:
            symbol: Trading symbol (e.g., "BTC/USDT")
            exchange: Exchange name (e.g., "binance")
        """
        self.symbol = symbol
        self.exchange = exchange

    async def normalize_single(self, raw_trade: dict) -> TradeRecord:
        """Normalize single trade record from API.

        Args:
            raw_trade: Raw trade record from API

        Returns:
            TradeRecord with validated fields

        Raises:
            NormalizationError: If extraction fails
        """
        try:
            timestamp = self._extract_timestamp(raw_trade)
            price = self._extract_price(raw_trade)
            quantity = self._extract_quantity(raw_trade)
            side = self._extract_side(raw_trade)

            return TradeRecord(
                symbol=self.symbol,
                exchange=self.exchange,
                timestamp=timestamp,
                price=price,
                quantity=quantity,
                side=side,
            )
        except (KeyError, ValueError, TypeError) as e:
            raise NormalizationError(
                f"Failed to normalize trade for {self.symbol}: {str(e)}"
            ) from e

    async def normalize_batch(self, raw_trades: list[dict]) -> list[TradeRecord]:
        """Normalize batch of trade records.

        Args:
            raw_trades: List of raw trade records from API

        Returns:
            List of TradeRecord objects

        Raises:
            NormalizationError: If batch normalization fails
        """
        records = []
        errors = []

        for i, raw in enumerate(raw_trades):
            try:
                record = await self.normalize_single(raw)
                records.append(record)
            except NormalizationError as e:
                errors.append(f"Index {i}: {str(e)}")

        if errors and len(errors) == len(raw_trades):
            raise NormalizationError(f"Batch normalization failed: {errors}")

        return records

    def _extract_timestamp(self, raw_trade: dict) -> int:
        """Extract timestamp (Unix milliseconds) from API format.

        Override in subclass.

        Args:
            raw_trade: Raw trade record

        Returns:
            Unix timestamp in milliseconds
        """
        raise NotImplementedError(
            f"{self.__class__.__name__} must implement _extract_timestamp()"
        )

    def _extract_price(self, raw_trade: dict) -> Decimal:
        """Extract trade price from API format.

        Override in subclass.

        Args:
            raw_trade: Raw trade record

        Returns:
            Trade price as Decimal
        """
        raise NotImplementedError(
            f"{self.__class__.__name__} must implement _extract_price()"
        )

    def _extract_quantity(self, raw_trade: dict) -> Decimal:
        """Extract trade quantity from API format.

        Override in subclass.

        Args:
            raw_trade: Raw trade record

        Returns:
            Trade quantity as Decimal
        """
        raise NotImplementedError(
            f"{self.__class__.__name__} must implement _extract_quantity()"
        )

    def _extract_side(self, raw_trade: dict) -> str:
        """Extract trade side (buy/sell) from API format.

        Override in subclass.

        Args:
            raw_trade: Raw trade record

        Returns:
            Trade side as 'buy' or 'sell'
        """
        raise NotImplementedError(
            f"{self.__class__.__name__} must implement _extract_side()"
        )
