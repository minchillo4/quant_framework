"""
AlphaVantage plugin for economic and equity data ingestion.

This plugin provides adapters for:
- Treasury yield data (TREASURY_YIELD endpoint)
- Equity OHLCV data (TIME_SERIES_DAILY endpoint)
- Rate limiting and API key rotation
- Raw field preservation for maximum flexibility
"""

from .client import AlphaVantageClient
from .base import AlphaVantageAdapterBase
from .treasury_adapter import AlphaVantageTreasuryAdapter
from .equity_adapter import AlphaVantageEquityAdapter
from .mappers import map_alpha_vantage_symbol, TREASURY_MATURITIES
from .dependency_container import AlphaVantageDependencyContainer
from .factory_registration import (
    register_all_alpha_vantage_factories,
    create_alpha_vantage_treasury_adapter,
    create_alpha_vantage_equity_adapter,
    create_alpha_vantage_preprocessor,
)

__all__ = [
    "AlphaVantageClient",
    "AlphaVantageAdapterBase",
    "AlphaVantageTreasuryAdapter",
    "AlphaVantageEquityAdapter",
    "AlphaVantageDependencyContainer",
    "map_alpha_vantage_symbol",
    "TREASURY_MATURITIES",
    "register_all_alpha_vantage_factories",
    "create_alpha_vantage_treasury_adapter",
    "create_alpha_vantage_equity_adapter",
    "create_alpha_vantage_preprocessor",
]
