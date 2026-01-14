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
