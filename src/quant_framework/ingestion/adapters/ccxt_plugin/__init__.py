"""
CCXT plugin adapters for capability-based ingestion.

This module provides a thin wrapper that treats CCXT as a plugin rather
than a core dependency. Specific capability adapters (OHLCV, Open
Interest, etc.) inherit from this base and implement the relevant ports.
"""

from __future__ import annotations

import asyncio
import logging
from typing import Any

from quant_framework.ingestion.adapters.base import BaseAdapter
from quant_framework.ingestion.models.enums import ClientType, ConnectionType

logger = logging.getLogger(__name__)


class CCXTAdapterBase(BaseAdapter):
    """Base class for CCXT-backed adapters used as plugins."""

    client_type: ClientType = ClientType.WRAPPER
    connection_type: ConnectionType = ConnectionType.REST

    def __init__(self, client: Any, *args: Any, **kwargs: Any):
        super().__init__(*args, **kwargs)
        self.client = client

    async def connect(self) -> None:
        """Initialize CCXT client markets lazily."""
        if self._connected:
            return
        load_markets = getattr(self.client, "load_markets", None)
        if callable(load_markets):
            await asyncio.to_thread(load_markets)
        self._connected = True

    async def close(self) -> None:
        """Close CCXT client if supported."""
        if not self._connected:
            return
        close_fn = getattr(self.client, "close", None)
        if callable(close_fn):
            try:
                maybe_coro = close_fn()
                if asyncio.iscoroutine(maybe_coro):
                    await maybe_coro
            except Exception as exc:  # pragma: no cover - best-effort cleanup
                logger.warning("Error closing CCXT client: %s", exc)
        self._connected = False
