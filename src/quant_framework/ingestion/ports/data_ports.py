"""
Data port protocols for capability-based adapters.

Ports define the raw data contracts that adapters must satisfy before
preprocessing/normalization. This enables layered architecture and
provider-agnostic orchestration.
"""

from __future__ import annotations

from collections.abc import Iterable
from datetime import datetime
from typing import Any, Protocol

from quant_framework.shared.models.instruments import Instrument


class OHLCVPort(Protocol):
    """Raw OHLCV data port."""

    async def fetch_ohlcv(
        self,
        instrument: Instrument,
        timeframe: str,
        start: datetime | None = None,
        end: datetime | None = None,
        limit: int | None = None,
    ) -> Iterable[dict[str, Any]]:
        """Fetch raw OHLCV candles for the given instrument."""


class OpenInterestPort(Protocol):
    """Raw open interest data port."""

    async def fetch_open_interest(
        self,
        instrument: Instrument,
        timeframe: str,
        start: datetime | None = None,
        end: datetime | None = None,
        limit: int | None = None,
    ) -> Iterable[dict[str, Any]]:
        """Fetch raw open interest snapshots for the given instrument."""


class OnChainDataPort(Protocol):
    """Simple port following existing pattern."""

    async def fetch_onchain_history(
        self, asset: str, start_date: datetime, end_date: datetime
    ) -> dict:
        """
        Returns: {"csv_path": "minio/path", "metadata": {...}}
        Follows same pattern as other adapters.
        """


class EconomicDataPort(Protocol):
    """Raw economic/financial indicator data port."""

    async def fetch_treasury_yields(
        self,
        instrument: Instrument,
        maturity: str,
        timeframe: str = "daily",
        start: datetime | None = None,
        end: datetime | None = None,
        limit: int | None = None,
    ) -> Iterable[dict[str, Any]]:
        """
        Fetch treasury yield data using AlphaVantage TREASURY_YIELD endpoint.

        Args:
            instrument: Treasury yield instrument
            maturity: AlphaVantage maturity ("3month", "2year", "5year", "7year", "10year", "30year")
            timeframe: Fixed to "daily" for AlphaVantage
            start: Optional start date
            end: Optional end date
            limit: Optional record limit

        Returns:
            Iterable with raw fields: {"timestamp": ms, "date": str, "value": float, ...}
        """

    async def fetch_equity_ohlcv(
        self,
        instrument: Instrument,
        timeframe: str = "1d",
        start: datetime | None = None,
        end: datetime | None = None,
        limit: int | None = None,
    ) -> Iterable[dict[str, Any]]:
        """
        Fetch equity OHLCV data using AlphaVantage TIME_SERIES_DAILY endpoint.

        Args:
            instrument: Equity/stock instrument
            timeframe: Fixed to "1d" for AlphaVantage
            start: Optional start date
            end: Optional end date
            limit: Optional record limit (max 100 for compact)

        Returns:
            Iterable with raw fields: {"timestamp": ms, "date": str, "open": float, "high": float, "low": float, "close": float, "volume": float, ...}
        """
