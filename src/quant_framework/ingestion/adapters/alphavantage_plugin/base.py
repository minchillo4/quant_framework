"""
Base AlphaVantage adapter with common functionality.

Provides the foundation for AlphaVantage adapters including:
- Client initialization and connection management
- Common validation logic
- Data lineage configuration
- Shared utility methods
"""

from __future__ import annotations

import logging
from typing import Any

from quant_framework.config.state import AlphaVantageConfig
from quant_framework.ingestion.adapters.base import BaseAdapter
from quant_framework.ingestion.adapters.alphavantage_plugin.client import (
    AlphaVantageClient,
)
from quant_framework.ingestion.models.enums import ClientType, ConnectionType
from quant_framework.shared.models.enums import (
    AssetClass,
    DataVenue,
    WrapperImplementation,
)
from quant_framework.shared.models.instruments import Instrument

logger = logging.getLogger(__name__)


class AlphaVantageAdapterBase(BaseAdapter):
    """
    Base adapter for AlphaVantage data providers.

    Extends BaseAdapter with AlphaVantage-specific configuration and client management.
    All AlphaVantage adapters should inherit from this base class.
    """

    # Data lineage configuration
    venue: DataVenue = DataVenue.ALPHA_VANTAGE
    wrapper: WrapperImplementation = WrapperImplementation.ALPHA_VANTAGE

    # Technical implementation
    client_type: ClientType = ClientType.NATIVE
    connection_type: ConnectionType = ConnectionType.REST

    # Supported asset classes
    supported_asset_classes = {AssetClass.EQUITY, AssetClass.RATES}

    def __init__(self, client: AlphaVantageClient, config: AlphaVantageConfig):
        """
        Initialize AlphaVantage adapter.

        Args:
            client: AlphaVantage HTTP client
            config: AlphaVantage configuration
        """
        self.client = client
        self.config = config
        self._connected = False

        logger.debug(f"Initialized {self.__class__.__name__}")

    async def connect(self) -> None:
        """
        Establish connection to AlphaVantage API.

        For AlphaVantage, this is essentially a no-op since it's a REST API,
        but we validate the connection by checking API key availability.
        """
        if not self.client.api_keys:
            raise ValueError("No API keys configured for AlphaVantage")

        # Test connection with a simple request if needed
        # (AlphaVantage doesn't have a dedicated "ping" endpoint)

        self._connected = True
        logger.info(f"Connected to AlphaVantage API")

    async def close(self) -> None:
        """Close connection to AlphaVantage API."""
        # AlphaVantage uses HTTP, so there's no persistent connection to close
        self._connected = False
        logger.info(f"Closed AlphaVantage API connection")

    def validate_instrument(self, instrument: Instrument) -> bool:
        """
        Validate instrument for AlphaVantage compatibility.

        Args:
            instrument: Instrument to validate

        Returns:
            True if instrument is supported by AlphaVantage
        """
        # Check asset class
        if instrument.asset_class not in self.supported_asset_classes:
            logger.debug(f"Unsupported asset class: {instrument.asset_class}")
            return False

        # Check venue (should be AlphaVantage or generic)
        if instrument.venue not in [DataVenue.ALPHA_VANTAGE, DataVenue.AGGREGATED]:
            logger.debug(f"Unsupported venue: {instrument.venue}")
            return False

        # Check wrapper (should be AlphaVantage)
        if instrument.wrapper != WrapperImplementation.ALPHA_VANTAGE:
            logger.debug(f"Unsupported wrapper: {instrument.wrapper}")
            return False

        # Check for required instrument fields
        if not instrument.symbol:
            logger.debug("Missing symbol for instrument")
            return False

        return True

    def _to_ms(self, timestamp_str: str) -> int:
        """
        Convert AlphaVantage date string to milliseconds timestamp.

        Args:
            timestamp_str: Date string from AlphaVantage (e.g., "2024-01-15")

        Returns:
            Milliseconds since epoch
        """
        from datetime import datetime

        try:
            dt = datetime.strptime(timestamp_str, "%Y-%m-%d")
            return int(dt.timestamp() * 1000)
        except ValueError as e:
            logger.warning(f"Failed to parse timestamp '{timestamp_str}': {e}")
            # Return current time as fallback
            import time

            return int(time.time() * 1000)

    def _parse_float(self, value: Any) -> float:
        """
        Parse float value from AlphaVantage response.

        Args:
            value: Value to parse (could be string, number, or None)

        Returns:
            Parsed float value or 0.0 if parsing fails
        """
        if value is None:
            return 0.0

        try:
            return float(value)
        except (ValueError, TypeError):
            logger.warning(f"Failed to parse float value: {value}")
            return 0.0

    def _get_instrument_info(self, instrument: Instrument) -> dict[str, Any]:
        """
        Get standardized instrument information for logging/debugging.

        Args:
            instrument: Instrument to extract info from

        Returns:
            Dictionary with instrument information
        """
        return {
            "instrument_id": instrument.instrument_id,
            "symbol": instrument.symbol,
            "asset_class": instrument.asset_class,
            "venue": instrument.venue,
            "wrapper": instrument.wrapper,
        }

    async def health_check(self) -> dict[str, Any]:
        """
        Perform health check on AlphaVantage connection.

        Returns:
            Health check results
        """
        try:
            # Check if client is properly initialized
            api_keys_info = self.client.get_api_keys_info()

            return {
                "status": "healthy",
                "connected": self._connected,
                "api_keys": api_keys_info,
                "adapter": self.__class__.__name__,
            }
        except Exception as e:
            return {
                "status": "unhealthy",
                "connected": self._connected,
                "error": str(e),
                "adapter": self.__class__.__name__,
            }

    def __repr__(self) -> str:
        """String representation of the adapter."""
        return (
            f"{self.__class__.__name__}("
            f"venue={self.venue}, "
            f"wrapper={self.wrapper}, "
            f"asset_classes={self.supported_asset_classes}"
            f")"
        )
