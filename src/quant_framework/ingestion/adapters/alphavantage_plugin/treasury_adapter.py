"""
AlphaVantage Treasury Yield adapter.

Implements EconomicDataPort for AlphaVantage TREASURY_YIELD endpoint.
Provides access to US Treasury yield data for various maturities.
"""

from __future__ import annotations

import logging
from collections.abc import Iterable
from datetime import datetime
from typing import Any

from quant_framework.config.state import AlphaVantageConfig
from quant_framework.ingestion.adapters.alphavantage_plugin.base import (
    AlphaVantageAdapterBase,
)
from quant_framework.ingestion.adapters.alphavantage_plugin.client import (
    AlphaVantageClient,
)
from quant_framework.ingestion.ports.data_ports import EconomicDataPort
from quant_framework.shared.models.instruments import Instrument

logger = logging.getLogger(__name__)


class AlphaVantageTreasuryAdapter(AlphaVantageAdapterBase, EconomicDataPort):
    """
    AlphaVantage Treasury Yield adapter implementing EconomicDataPort.

    Fetches US Treasury yield data using AlphaVantage's TREASURY_YIELD function.
    Supports multiple maturities and provides daily yield history.
    """

    capabilities = {EconomicDataPort}

    # Supported Treasury maturities by AlphaVantage
    SUPPORTED_MATURITIES = ["3month", "2year", "5year", "7year", "10year", "30year"]

    def __init__(self, client: AlphaVantageClient, config: AlphaVantageConfig):
        """
        Initialize AlphaVantage Treasury adapter.

        Args:
            client: AlphaVantage HTTP client
            config: AlphaVantage configuration
        """
        super().__init__(client, config)
        logger.debug("Initialized AlphaVantage Treasury adapter")

    async def fetch_treasury_yields(
        self,
        instrument: Instrument,
        maturity: str,
        timeframe: str = "daily",
        start: datetime | None = None,
        end: datetime | None = None,
        limit: int | None = None,
    ) -> Iterable[dict[str, Any]]:
        """
        Fetch treasury yield data using AlphaVantage TREASURY_YIELD endpoint.

        Args:
            instrument: Treasury yield instrument
            maturity: AlphaVantage maturity ("3month", "2year", "5year", "7year", "10year", "30year")
            timeframe: Fixed to "daily" for AlphaVantage
            start: Optional start date (not supported by AlphaVantage for this endpoint)
            end: Optional end date (not supported by AlphaVantage for this endpoint)
            limit: Optional record limit (not supported by AlphaVantage for this endpoint)

        Returns:
            Iterable with raw fields: {"date": str, "value": float, ...}

        Raises:
            ValueError: If parameters are invalid
            AlphaVantageError: If API call fails
        """
        # Validate parameters
        if not self.validate_instrument(instrument):
            raise ValueError(f"Invalid instrument: {instrument}")

        if timeframe != "daily":
            raise ValueError(
                "AlphaVantage only supports daily timeframe for treasury data"
            )

        if maturity not in self.SUPPORTED_MATURITIES:
            raise ValueError(
                f"Invalid maturity '{maturity}'. Supported: {self.SUPPORTED_MATURITIES}"
            )

        # Log start and end dates (AlphaVantage doesn't support date filtering for this endpoint)
        if start or end:
            logger.warning(
                "AlphaVantage TREASURY_YIELD endpoint does not support date filtering. "
                "All available data will be returned and client-side filtering should be applied."
            )

        logger.info(
            f"Fetching treasury yields for {instrument.symbol}, maturity: {maturity}"
        )

        # Build AlphaVantage request
        params = {
            "function": "TREASURY_YIELD",
            "interval": "daily",
            "maturity": maturity,
        }

        # Make API request
        response_data = await self.client.fetch_data_with_retry(params)

        # Parse response
        parsed_data = self._parse_treasury_response(response_data, instrument, maturity)

        logger.info(
            f"Retrieved {len(list(parsed_data))} treasury yield records for {maturity}"
        )

        return parsed_data

    def _parse_treasury_response(
        self, response_data: dict[str, Any], instrument: Instrument, maturity: str
    ) -> Iterable[dict[str, Any]]:
        """
        Parse AlphaVantage TREASURY_YIELD response.

        Args:
            response_data: Raw response from AlphaVantage
            instrument: The instrument being requested
            maturity: Treasury maturity

        Returns:
            Iterable of parsed treasury yield records
        """
        # AlphaVantage TREASURY_YIELD response format:
        # {
        #   "data": [
        #     {"date": "2024-01-15", "value": "4.28"},
        #     {"date": "2024-01-12", "value": "4.26"},
        #     ...
        #   ]
        # }

        if "data" not in response_data:
            raise ValueError("Invalid treasury response format: missing 'data' field")

        data_array = response_data["data"]
        if not isinstance(data_array, list):
            raise ValueError("Invalid treasury response format: 'data' is not a list")

        for record in data_array:
            if not isinstance(record, dict):
                logger.warning(f"Skipping invalid record: {record}")
                continue

            # Extract required fields
            date_str = record.get("date")
            value_str = record.get("value")

            if not date_str or value_str is None:
                logger.warning(f"Skipping record with missing fields: {record}")
                continue

            # Parse numeric value
            try:
                value = float(value_str)
            except (ValueError, TypeError):
                logger.warning(f"Failed to parse yield value: {value_str}")
                continue

            # Create standardized record with raw fields preserved
            parsed_record = {
                # Raw fields from AlphaVantage
                "date": date_str,
                "value": value,
                # Additional metadata
                "maturity": maturity,
                "instrument_id": instrument.instrument_id,
                "symbol": instrument.symbol,
                # Normalized timestamp for consistency
                "timestamp": self._to_ms(date_str),
            }

            yield parsed_record

    def get_supported_maturities(self) -> list[str]:
        """
        Get list of supported treasury maturities.

        Returns:
            List of supported maturity strings
        """
        return self.SUPPORTED_MATURITIES.copy()

    def __repr__(self) -> str:
        """String representation of the treasury adapter."""
        return (
            f"{self.__class__.__name__}("
            f"venue={self.venue}, "
            f"wrapper={self.wrapper}, "
            f"maturities={len(self.SUPPORTED_MATURITIES)}"
            f")"
        )
