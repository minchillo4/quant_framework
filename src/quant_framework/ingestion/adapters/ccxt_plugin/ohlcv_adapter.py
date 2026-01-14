"""
CCXT OHLCV capability adapter.

Implements the OHLCVPort using a provided CCXT client.
"""

from __future__ import annotations

import asyncio
from collections.abc import Iterable
from datetime import datetime
from typing import Any

from quant_framework.ingestion.adapters.ccxt_plugin import CCXTAdapterBase
from quant_framework.ingestion.models.enums import DataProvider
from quant_framework.ingestion.ports.data_ports import OHLCVPort
from quant_framework.shared.models.enums import AssetClass
from quant_framework.shared.models.instruments import Instrument


class CCXTOHLCVAdapter(CCXTAdapterBase, OHLCVPort):
    """CCXT-backed OHLCV adapter implementing OHLCVPort."""

    provider: DataProvider
    supported_asset_classes = {AssetClass.CRYPTO}
    capabilities = {OHLCVPort}

    async def fetch_ohlcv(
        self,
        instrument: Instrument,
        timeframe: str,
        start: datetime | None = None,
        end: datetime | None = None,
        limit: int | None = None,
    ) -> Iterable[list[Any]]:
        if not self.validate_instrument(instrument):
            raise ValueError(f"Invalid instrument: {instrument}")

        await self.connect()
        fetch = getattr(self.client, "fetch_ohlcv", None) or getattr(
            self.client, "fetchOHLCV", None
        )
        if not callable(fetch):
            raise NotImplementedError("CCXT client does not support fetch_ohlcv")

        since = int(start.timestamp() * 1000) if start else None
        # CCXT does not support explicit end; rely on limit+since; upstream orchestrator will paginate
        result = await asyncio.to_thread(
            fetch,
            instrument.raw_symbol or instrument.symbol,
            timeframe,
            since,
            limit,
        )
        return result
