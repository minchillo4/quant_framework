"""
Lightweight configuration exports for quant_framework.

This module intentionally avoids heavy dependencies and missing modules.
It exposes:
    - config_registry: pulled from the shared config.settings
    - CCXTExchangeMapper: optional; falls back to a no-op mapper
    - EnhancedSymbolRegistry: minimal registry for symbol lookups
"""

from dataclasses import dataclass
from enum import Enum
from pathlib import Path
from typing import Any

import yaml

from config.settings import config_registry


class EndpointType(str, Enum):
    """Supported endpoint types for market data resolution."""

    OHLC = "ohlc"
    OPEN_INTEREST = "open_interest"


@dataclass(frozen=True)
class SymbolConfig:
    """Minimal symbol config exposed by the registry."""

    canonical: str


class EnhancedSymbolRegistry:
    """Minimal registry backed by the project YAML config."""

    def __init__(self, config_path: str | Path = "config/assets/crypto_universe.yaml"):
        self._path = Path(config_path)
        if not self._path.exists():
            alt = Path("/opt/airflow") / config_path
            self._path = alt if alt.exists() else self._path
        self._doc = self._load_yaml(self._path)

    @staticmethod
    def _load_yaml(path: Path) -> dict[str, Any]:
        try:
            with path.open("r") as f:
                return yaml.safe_load(f) or {}
        except Exception:
            return {"symbols": []}

    def get_active_symbols(self, exchange: str) -> list[SymbolConfig]:
        """Return canonical symbols active for the given exchange."""

        results: list[SymbolConfig] = []
        for entry in self._doc.get("symbols", []):
            if not entry.get("active", True):
                continue
            for ex in entry.get("exchanges", []):
                if ex.get("exchange") == exchange:
                    results.append(SymbolConfig(canonical=entry.get("canonical", "")))
                    break

        seen: set[str] = set()
        deduped: list[SymbolConfig] = []
        for cfg in results:
            if cfg.canonical and cfg.canonical not in seen:
                seen.add(cfg.canonical)
                deduped.append(cfg)
        return deduped

    def get_exchange_symbol(
        self,
        canonical: str,
        exchange: str,
        endpoint: EndpointType = EndpointType.OHLC,
    ) -> str | None:
        """Resolve the concrete exchange symbol for a canonical asset."""

        for entry in self._doc.get("symbols", []):
            if entry.get("canonical") != canonical:
                continue
            for ex in entry.get("exchanges", []):
                if ex.get("exchange") != exchange:
                    continue
                sym_map = ex.get("symbols", {})
                return sym_map.get(endpoint.value)
        return None

    def get_supported_settlement_currencies(
        self, canonical: str, exchange: str, market_type: str
    ) -> list[str]:
        """Return settlement currencies available for the given triple."""

        settlements: list[str] = []
        for entry in self._doc.get("symbols", []):
            if entry.get("canonical") != canonical:
                continue
            for ex in entry.get("exchanges", []):
                if (
                    ex.get("exchange") == exchange
                    and ex.get("market_type") == market_type
                ):
                    cur = ex.get("settlement_currency")
                    if cur:
                        settlements.append(cur)
        return settlements


class CCXTExchangeMapper:
    """No-op mapper used when CCXTExchangeMapper is not installed."""

    def map(self, exchange: str) -> str:  # pragma: no cover - minimal stub
        return exchange


__all__ = [
    "config_registry",
    "CCXTExchangeMapper",
    "EnhancedSymbolRegistry",
    "EndpointType",
    "SymbolConfig",
]
