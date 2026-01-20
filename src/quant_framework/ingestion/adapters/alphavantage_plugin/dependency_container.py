"""
AlphaVantage dependency container.

Provides dependency injection for AlphaVantage components following the existing
pattern established by CoinalyzeDependencyContainer.
"""

from __future__ import annotations

from typing import Any

from quant_framework.config.state import AlphaVantageConfig
from quant_framework.ingestion.adapters.alphavantage_plugin.client import (
    AlphaVantageClient,
)
from quant_framework.ingestion.adapters.alphavantage_plugin.treasury_adapter import (
    AlphaVantageTreasuryAdapter,
)
from quant_framework.ingestion.adapters.alphavantage_plugin.equity_adapter import (
    AlphaVantageEquityAdapter,
)
from quant_framework.ingestion.preprocessing.providers import AlphaVantagePreprocessor


class AlphaVantageDependencyContainer:
    """
    Dependency injection container for AlphaVantage components.

    Provides factory methods for creating AlphaVantage clients, adapters,
    and preprocessors with proper configuration and dependency wiring.
    """

    def __init__(
        self,
        base_url: str,
        api_keys: list[str],
        rate_limit: int = 5,
        request_interval: float = 12.0,
    ):
        """
        Initialize AlphaVantage dependency container.

        Args:
            base_url: AlphaVantage API base URL
            api_keys: List of AlphaVantage API keys for rotation
            rate_limit: Rate limit in requests per minute
            request_interval: Interval between requests in seconds
        """
        self.base_url = base_url
        self.api_keys = api_keys
        self.rate_limit = rate_limit
        self.request_interval = request_interval

    def create_config(self) -> AlphaVantageConfig:
        """Create AlphaVantage configuration."""
        return AlphaVantageConfig(
            base_url=self.base_url,
            api_keys=self.api_keys,
            rate_limit=self.rate_limit,
            request_interval=self.request_interval,
        )

    def create_client(self) -> AlphaVantageClient:
        """Create AlphaVantage HTTP client."""
        config = self.create_config()
        return AlphaVantageClient(config)

    def create_treasury_adapter(self) -> AlphaVantageTreasuryAdapter:
        """Create AlphaVantage Treasury adapter."""
        client = self.create_client()
        config = self.create_config()
        return AlphaVantageTreasuryAdapter(client, config)

    def create_equity_adapter(self) -> AlphaVantageEquityAdapter:
        """Create AlphaVantage Equity adapter."""
        client = self.create_client()
        config = self.create_config()
        return AlphaVantageEquityAdapter(client, config)

    def create_preprocessor(self) -> AlphaVantagePreprocessor:
        """Create AlphaVantage preprocessor."""
        return AlphaVantagePreprocessor()

    def create_all_components(self) -> dict[str, Any]:
        """Create all AlphaVantage components."""
        return {
            "client": self.create_client(),
            "treasury_adapter": self.create_treasury_adapter(),
            "equity_adapter": self.create_equity_adapter(),
            "preprocessor": self.create_preprocessor(),
            "config": self.create_config(),
        }
