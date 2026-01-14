"""CoinAlyze OHLCV Adapter - BRONZE LAYER (raw only)."""

import logging
from collections.abc import Iterable
from datetime import datetime
from typing import Any

from quant_framework.ingestion.ports.data_ports import OHLCVPort

from .base import CoinalyzeAdapterBase
from .mappers import get_coinalyze_interval, get_coinalyze_symbol

logger = logging.getLogger(__name__)


class CoinalyzeOHLCVAdapter(CoinalyzeAdapterBase, OHLCVPort):
    """OHLCV data from CoinAlyze API - BRONZE (no normalization)."""

    capabilities = {OHLCVPort}

    async def fetch_ohlcv(
        self,
        instrument,
        timeframe: str,
        start: datetime,
        end: datetime,
        limit: int | None = None,
    ) -> Iterable[list[Any]]:
        """
        Fetch **raw** OHLCV data from CoinAlyze.
        Returns exactly what the API returns (Bronze layer).
        """
        if not self.validate_instrument(instrument):
            raise ValueError(f"Invalid instrument: {instrument}")

        symbol = get_coinalyze_symbol(instrument)
        interval = get_coinalyze_interval(timeframe)

        params = {
            "symbols": symbol,
            "interval": interval,
            "from": int(start.timestamp()),
            "to": int(end.timestamp()),
            "convert_to_usd": "false",
        }
        logger.debug("🔍 Adapter params: %s", params)
        logger.debug(f"Bronze fetch: {instrument.instrument_id} ({symbol})")

        # 🔥 BRONZE: retorna JSON cru sem tocar
        logger.debug(
            "🔍 Adapter fetch_ohlcv: %s %s", instrument.instrument_id, timeframe
        )
        raw = await self.client.fetch_data_async("ohlcv-history", params)

        # 🔥 BRONZE: garante que é lista de dicts com "history"
        if not isinstance(raw, list) or not raw or "history" not in raw[0]:
            raise ValueError(f"Bronze invalid response: {raw}")

        return raw  # ← exatamente o que a API devolveu
