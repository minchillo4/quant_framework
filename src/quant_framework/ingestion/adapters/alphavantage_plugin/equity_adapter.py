"""
AlphaVantage Equity OHLCV adapter.

Implements EconomicDataPort for AlphaVantage TIME_SERIES_DAILY endpoint.
Provides access to global equity/stock price data with OHLCV format.
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
from quant_framework.ingestion.adapters.alphavantage_plugin.mappers import (
    map_alpha_vantage_symbol,
)
from quant_framework.shared.models.instruments import Instrument

logger = logging.getLogger(__name__)


class AlphaVantageEquityAdapter(AlphaVantageAdapterBase, EconomicDataPort):
    """
    AlphaVantage Equity OHLCV adapter implementing EconomicDataPort.

    Fetches equity/stock price data using AlphaVantage's TIME_SERIES_DAILY function.
    Supports global equity symbols with compact output (100 latest records).
    """

    capabilities = {EconomicDataPort}

    def __init__(self, client: AlphaVantageClient, config: AlphaVantageConfig):
        """
        Initialize AlphaVantage Equity adapter.

        Args:
            client: AlphaVantage HTTP client
            config: AlphaVantage configuration
        """
        super().__init__(client, config)
        logger.debug("Initialized AlphaVantage Equity adapter")

    async def fetch_equity_ohlcv(
        self,
        instrument: Instrument,
        timeframe: str = "1d",
        start: datetime | None = None,
        end: datetime | None = None,
        limit: int | None = None,
    ) -> Iterable[dict[str, Any]]:
        """
        Fetch equity OHLCV data using AlphaVantage TIME_SERIES_DAILY endpoint.

        Args:
            instrument: Equity/stock instrument
            timeframe: Fixed to "1d" for AlphaVantage
            start: Optional start date (not supported by AlphaVantage for this endpoint)
            end: Optional end date (not supported by AlphaVantage for this endpoint)
            limit: Optional record limit (max 100 for compact output)

        Returns:
            Iterable with raw fields: {"date": str, "1. open": float, "2. high": float, "3. low": float, "4. close": float, "5. volume": float, ...}

        Raises:
            ValueError: If parameters are invalid
            AlphaVantageError: If API call fails
        """
        # Validate parameters
        if not self.validate_instrument(instrument):
            raise ValueError(f"Invalid instrument: {instrument}")

        if timeframe != "1d":
            raise ValueError("AlphaVantage only supports 1d timeframe for equity data")

        # Log start and end dates (AlphaVantage doesn't support date filtering for this endpoint)
        if start or end:
            logger.warning(
                "AlphaVantage TIME_SERIES_DAILY endpoint does not support date filtering. "
                "Latest 100 records will be returned and client-side filtering should be applied."
            )

        # Map symbol to AlphaVantage format
        symbol = map_alpha_vantage_symbol(instrument)

        logger.info(f"Fetching equity OHLCV for {symbol}")

        # Build AlphaVantage request
        params = {
            "function": "TIME_SERIES_DAILY",
            "symbol": symbol,
            "outputsize": "compact",  # Always compact as requested
        }

        # Make API request
        response_data = await self.client.fetch_data_with_retry(params)

        # Parse response
        parsed_data = self._parse_equity_response(response_data, instrument)

        logger.info(f"Retrieved {len(list(parsed_data))} OHLCV records for {symbol}")

        return parsed_data

    def _parse_equity_response(
        self, response_data: dict[str, Any], instrument: Instrument
    ) -> Iterable[dict[str, Any]]:
        """
        Parse AlphaVantage TIME_SERIES_DAILY response.

        Args:
            response_data: Raw response from AlphaVantage
            instrument: The instrument being requested

        Returns:
            Iterable of parsed OHLCV records
        """
        # AlphaVantage TIME_SERIES_DAILY response format:
        # {
        #   "Meta Data": {
        #     "1. Information": "Daily Prices (open, high, low, close) and Volumes",
        #     "2. Symbol": "IBM",
        #     "3. Last Refreshed": "2024-01-15",
        #     "4. Output Size": "Compact",
        #     "5. Time Zone": "US/Eastern"
        #   },
        #   "Time Series (Daily)": {
        #     "2024-01-15": {
        #       "1. open": "173.4300",
        #       "2. high": "174.4800",
        #       "3. low": "172.2000",
        #       "4. close": "173.8000",
        #       "5. volume": "4321500"
        #     },
        #     "2024-01-12": {
        #       "1. open": "172.5000",
        #       "2. high": "173.4500",
        #       "3. low": "171.8000",
        #       "4. close": "172.9000",
        #       "5. volume": "5236800"
        #     },
        #     ...
        #   }
        # }

        time_series_key = "Time Series (Daily)"
        if time_series_key not in response_data:
            raise ValueError(
                f"Invalid equity response format: missing '{time_series_key}' field"
            )

        time_series = response_data[time_series_key]
        if not isinstance(time_series, dict):
            raise ValueError(
                f"Invalid equity response format: '{time_series_key}' is not a dict"
            )

        for date_str, daily_data in time_series.items():
            if not isinstance(daily_data, dict):
                logger.warning(
                    f"Skipping invalid daily data for {date_str}: {daily_data}"
                )
                continue

            # Extract OHLCV fields
            open_str = daily_data.get("1. open")
            high_str = daily_data.get("2. high")
            low_str = daily_data.get("3. low")
            close_str = daily_data.get("4. close")
            volume_str = daily_data.get("5. volume")

            # Validate required fields
            if not all([open_str, high_str, low_str, close_str, volume_str]):
                logger.warning(
                    f"Skipping record with missing OHLCV fields for {date_str}"
                )
                continue

            # Parse numeric values
            try:
                open_price = self._parse_float(open_str)
                high_price = self._parse_float(high_str)
                low_price = self._parse_float(low_str)
                close_price = self._parse_float(close_str)
                volume = self._parse_float(volume_str)
            except Exception as e:
                logger.warning(f"Failed to parse OHLCV values for {date_str}: {e}")
                continue

            # Create standardized record with raw fields preserved
            parsed_record = {
                # Raw fields from AlphaVantage
                "date": date_str,
                "1. open": open_str,
                "2. high": high_str,
                "3. low": low_str,
                "4. close": close_str,
                "5. volume": volume_str,
                # Additional metadata
                "instrument_id": instrument.instrument_id,
                "symbol": instrument.symbol,
                # Normalized timestamp for consistency
                "timestamp": self._to_ms(date_str),
                # Parsed numeric values for convenience
                "open": open_price,
                "high": high_price,
                "low": low_price,
                "close": close_price,
                "volume": volume,
            }

            yield parsed_record

    def get_output_size_info(self) -> dict[str, Any]:
        """
        Get information about output size settings.

        Returns:
            Dictionary with output size information
        """
        return {
            "outputsize": "compact",
            "max_records": 100,
            "description": "AlphaVantage free tier returns latest 100 records only",
        }

    def __repr__(self) -> str:
        """String representation of the equity adapter."""
        return (
            f"{self.__class__.__name__}("
            f"venue={self.venue}, "
            f"wrapper={self.wrapper}, "
            f"outputsize=compact"
            f")"
        )
