"""CCXT-backed Open Interest adapter implementing OpenInterestPort."""

from __future__ import annotations

import asyncio
import logging
from collections.abc import Iterable
from datetime import datetime
from typing import Any

from quant_framework.ingestion.adapters.ccxt_plugin import CCXTAdapterBase
from quant_framework.ingestion.ports.data_ports import OpenInterestPort

logger = logging.getLogger(__name__)


class CCXTOpenInterestAdapter(CCXTAdapterBase, OpenInterestPort):
    """Implements OpenInterestPort using a provided CCXT client (or compatible wrapper)."""

    capabilities = {OpenInterestPort}

    async def fetch_open_interest(
        self,
        instrument,
        timeframe: str,
        start: datetime | None = None,
        end: datetime | None = None,
        limit: int | None = None,
    ) -> Iterable[dict[str, Any]]:
        if not self.validate_instrument(instrument):
            raise ValueError(f"Invalid instrument: {instrument}")

        await self.connect()
        fetch = getattr(self.client, "fetch_open_interest_history", None) or getattr(
            self.client, "fetchOpenInterestHistory", None
        )
        if not callable(fetch):
            raise NotImplementedError(
                "CCXT client does not support fetch_open_interest_history"
            )

        since = int(start.timestamp() * 1000) if start else None

        # Allow exchange-specific params via adapter config
        params: dict[str, Any] = {}
        params.update(self.config.get("open_interest_params", {}))

        symbol = instrument.raw_symbol or instrument.symbol
        result = await asyncio.to_thread(
            fetch,
            symbol,
            timeframe,
            since,
            limit,
            params,
        )
        return result
