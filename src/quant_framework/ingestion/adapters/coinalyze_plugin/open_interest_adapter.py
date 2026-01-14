"""CoinAlyze Open Interest Adapter - Fetches OI data from CoinAlyze API."""

import logging
from collections.abc import Iterable
from datetime import datetime
from typing import Any

from quant_framework.ingestion.ports.data_ports import OpenInterestPort

from .base import CoinalyzeAdapterBase
from .mappers import get_coinalyze_interval, get_coinalyze_symbol

logger = logging.getLogger(__name__)


class CoinalyzeOpenInterestAdapter(CoinalyzeAdapterBase, OpenInterestPort):
    """Open Interest data from CoinAlyze API."""

    capabilities = {OpenInterestPort}

    async def fetch_open_interest(
        self,
        instrument,
        timeframe: str,
        start: datetime,
        end: datetime,
        limit: int | None = None,
    ) -> Iterable[dict[str, Any]]:
        """
        Fetch raw Open Interest data from CoinAlyze.

        Args:
            instrument: Instrument with venue, symbol, etc.
            timeframe: Timeframe string (e.g., "1h", "5m")
            start: Start datetime
            end: End datetime
            limit: Maximum number of records (optional)

        Returns:
            Raw OI data from CoinAlyze API (list of dicts with "history" key)
        """
        if not self.validate_instrument(instrument):
            logger.warning(f"Instrument validation failed: {instrument}")
            raise ValueError(f"Invalid instrument: {instrument}")

        self.venue = instrument.venue

        symbol = get_coinalyze_symbol(instrument)
        interval = get_coinalyze_interval(timeframe)

        params = {
            "symbols": symbol,
            "interval": interval,
            "from": int(start.timestamp()),
            "to": int(end.timestamp()),
            "convert_to_usd": "false",
        }

        logger.info(f"Fetching Open Interest for {instrument.instrument_id} ({symbol})")

        return await self.client.fetch_data_async("open-interest-history", params)
