"""
AlphaVantage adapter factory registration.

This module provides factory functions for creating AlphaVantage adapters and preprocessors.
Can be used with the factory pattern or dependency injection containers.
"""

from __future__ import annotations

from quant_framework.config.state import AlphaVantageConfig
from quant_framework.common.factories.adapter_factory import AdapterFactory
from quant_framework.common.factories.preprocessor_factory import PreprocessorFactory
from quant_framework.ingestion.adapters.alphavantage_plugin.client import (
    AlphaVantageClient,
)
from quant_framework.ingestion.adapters.alphavantage_plugin.treasury_adapter import (
    AlphaVantageTreasuryAdapter,
)
from quant_framework.ingestion.adapters.alphavantage_plugin.equity_adapter import (
    AlphaVantageEquityAdapter,
)
from quant_framework.ingestion.models.enums import DataProvider
from quant_framework.ingestion.preprocessing.providers import AlphaVantagePreprocessor


def create_alpha_vantage_client(config: AlphaVantageConfig) -> AlphaVantageClient:
    """Create AlphaVantage HTTP client with configuration."""
    return AlphaVantageClient(config)


def create_alpha_vantage_treasury_adapter(
    config: AlphaVantageConfig,
) -> AlphaVantageTreasuryAdapter:
    """Create AlphaVantage Treasury adapter with client."""
    client = create_alpha_vantage_client(config)
    return AlphaVantageTreasuryAdapter(client, config)


def create_alpha_vantage_equity_adapter(
    config: AlphaVantageConfig,
) -> AlphaVantageEquityAdapter:
    """Create AlphaVantage Equity adapter with client."""
    client = create_alpha_vantage_client(config)
    return AlphaVantageEquityAdapter(client, config)


def create_alpha_vantage_preprocessor() -> AlphaVantagePreprocessor:
    """Create AlphaVantage preprocessor."""
    return AlphaVantagePreprocessor()


def register_alpha_vantage_adapters(adapter_factory: AdapterFactory) -> None:
    """Register AlphaVantage adapters with the factory."""
    adapter_factory.register(
        DataProvider.ALPHA_VANTAGE,
        create_alpha_vantage_treasury_adapter,  # Default to treasury adapter
    )


def register_alpha_vantage_preprocessor(
    preprocessor_factory: PreprocessorFactory,
) -> None:
    """Register AlphaVantage preprocessor with the factory."""
    preprocessor_factory.register(
        DataProvider.ALPHA_VANTAGE, create_alpha_vantage_preprocessor
    )


def register_all_alpha_vantage_factories(
    adapter_factory: AdapterFactory, preprocessor_factory: PreprocessorFactory
) -> None:
    """Register all AlphaVantage components with their respective factories."""
    register_alpha_vantage_adapters(adapter_factory)
    register_alpha_vantage_preprocessor(preprocessor_factory)
