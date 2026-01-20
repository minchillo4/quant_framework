"""
CoinAlyze Backfill Adapter
Unified adapter for historical data backfill with rate limiting integration.
"""

import logging
from datetime import datetime

from quant_framework.ingestion.adapters.coinalyze_plugin.client import CoinalyzeClient
from quant_framework.ingestion.adapters.coinalyze_plugin.ohlcv_adapter import (
    CoinalyzeOHLCVAdapter,
)
from quant_framework.ingestion.adapters.coinalyze_plugin.open_interest_adapter import (
    CoinalyzeOpenInterestAdapter,
)
from quant_framework.ingestion.backfill.rate_limiter import IRateLimiter
from quant_framework.shared.models.instruments import Instrument

logger = logging.getLogger(__name__)


class CoinalyzeBackfillAdapter:
    """
    Unified backfill adapter for CoinAlyze data.
    Combines OHLCV and OI capabilities with rate limiting.
    """

    def __init__(
        self, client: CoinalyzeClient, rate_limiter: IRateLimiter, verbose: bool = False
    ):
        """
        Initialize backfill adapter.

        Args:
            client: CoinAlyze API client
            rate_limiter: Rate limiting strategy
            verbose: Enable verbose logging
        """
        self.client = client
        self.rate_limiter = rate_limiter
        self.verbose = verbose

        # Initialize underlying adapters
        self.ohlcv_adapter = CoinalyzeOHLCVAdapter(client)
        self.oi_adapter = CoinalyzeOpenInterestAdapter(client)

        logger.info("CoinAlyze backfill adapter initialized")

    async def fetch_ohlcv_history(
        self,
        symbol: str,
        timeframe: str,
        start_date: datetime,
        end_date: datetime,
        instrument: Instrument,
    ) -> list[dict]:
        """
        Fetch OHLCV history with rate limit validation.

        Args:
            symbol: Trading symbol
            timeframe: Data timeframe
            start_date: Start of date range
            end_date: End of date range
            instrument: Instrument object with venue info

        Returns:
            List of OHLCV records
        """
        # Normalize timeframe for rate limiting
        normalized_tf = self.rate_limiter.normalize_timeframe(timeframe)

        if self.verbose:
            logger.debug(
                f"Fetching OHLCV: {symbol}/{normalized_tf} "
                f"from {start_date} to {end_date}"
            )

        # Fetch data using existing adapter
        try:
            result = await self.ohlcv_adapter.fetch_ohlcv(
                instrument=instrument,
                timeframe=normalized_tf,
                start=start_date,
                end=end_date,
            )

            # Convert result to list if needed
            if isinstance(result, list):
                data = result
            else:
                data = list(result)

            # Extract history from response
            records = []
            for item in data:
                if isinstance(item, dict) and "history" in item:
                    records.extend(item["history"])
                elif isinstance(item, dict):
                    records.append(item)

            # Validate response with rate limiter
            validation = self.rate_limiter.validate_chunk_response(
                points_received=len(records), timeframe=normalized_tf
            )

            if not validation["valid"]:
                logger.error(
                    f"Rate limit validation failed for {symbol}/{normalized_tf}: "
                    f"{validation['warnings']}"
                )
            elif validation["warnings"] and self.verbose:
                for warning in validation["warnings"]:
                    logger.warning(warning)

            # Record performance for adaptive chunking
            chunk_size_days = (end_date - start_date).days
            self.rate_limiter.record_performance(
                timeframe=normalized_tf,
                chunk_size_days=chunk_size_days,
                points_received=len(records),
                success=validation["valid"],
            )

            logger.info(
                f"✅ Fetched {len(records)} OHLCV records for "
                f"{symbol}/{normalized_tf}"
            )

            return records

        except Exception as e:
            logger.error(f"❌ Failed to fetch OHLCV for {symbol}/{normalized_tf}: {e}")
            raise

    async def fetch_oi_history(
        self,
        symbol: str,
        timeframe: str,
        start_date: datetime,
        end_date: datetime,
        instrument: Instrument,
    ) -> list[dict]:
        """
        Fetch Open Interest history with rate limit validation.

        Args:
            symbol: Trading symbol
            timeframe: Data timeframe
            start_date: Start of date range
            end_date: End of date range
            instrument: Instrument object with venue info

        Returns:
            List of OI records
        """
        # Normalize timeframe for rate limiting
        normalized_tf = self.rate_limiter.normalize_timeframe(timeframe)

        if self.verbose:
            logger.debug(
                f"Fetching OI: {symbol}/{normalized_tf} "
                f"from {start_date} to {end_date}"
            )

        # Fetch data using existing adapter
        try:
            result = await self.oi_adapter.fetch_open_interest(
                instrument=instrument,
                timeframe=normalized_tf,
                start=start_date,
                end=end_date,
            )

            # Convert result to list if needed
            if isinstance(result, list):
                data = result
            else:
                data = list(result)

            # Extract history from response
            records = []
            for item in data:
                if isinstance(item, dict) and "history" in item:
                    records.extend(item["history"])
                elif isinstance(item, dict):
                    records.append(item)

            # Validate response with rate limiter
            validation = self.rate_limiter.validate_chunk_response(
                points_received=len(records), timeframe=normalized_tf
            )

            if not validation["valid"]:
                logger.error(
                    f"Rate limit validation failed for {symbol}/{normalized_tf}: "
                    f"{validation['warnings']}"
                )
            elif validation["warnings"] and self.verbose:
                for warning in validation["warnings"]:
                    logger.warning(warning)

            # Record performance for adaptive chunking
            chunk_size_days = (end_date - start_date).days
            self.rate_limiter.record_performance(
                timeframe=normalized_tf,
                chunk_size_days=chunk_size_days,
                points_received=len(records),
                success=validation["valid"],
            )

            logger.info(
                f"✅ Fetched {len(records)} OI records for " f"{symbol}/{normalized_tf}"
            )

            return records

        except Exception as e:
            logger.error(f"❌ Failed to fetch OI for {symbol}/{normalized_tf}: {e}")
            raise

    def supports_ohlcv(self) -> bool:
        """Check if adapter supports OHLCV data."""
        return True

    def supports_open_interest(self) -> bool:
        """Check if adapter supports Open Interest data."""
        return True

    def get_performance_summary(self) -> dict:
        """
        Get rate limiter performance summary.

        Returns:
            Performance statistics dictionary
        """
        if hasattr(self.rate_limiter, "get_performance_summary"):
            return self.rate_limiter.get_performance_summary()
        return {}
