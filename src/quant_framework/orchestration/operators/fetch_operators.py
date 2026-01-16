"""
Fetch Operators - Data Retrieval
================================

Operators for fetching data from various adapters (CCXT, Coinalyze, etc.).
"""

import logging
from datetime import datetime
from typing import Any

from quant_framework.orchestration.operators.base import (
    BaseDataOperator,
)
from quant_framework.orchestration.ports import IRateLimiter

logger = logging.getLogger(__name__)


class FetchOHLCVOperator(BaseDataOperator):
    """
    Operator for fetching OHLCV data from an adapter.

    Can be used with any adapter that implements fetch_ohlcv method
    (CCXT, Coinalyze, CoinMetrics, etc.).
    """

    def __init__(
        self,
        task_id: str,
        adapter: Any,  # Data adapter with fetch_ohlcv method
        symbol: str,
        timeframe: str,
        since: int | None = None,
        limit: int | None = None,
        rate_limiter: IRateLimiter | None = None,
        **kwargs,
    ):
        """
        Initialize OHLCV fetch operator.

        Args:
            task_id: Task identifier
            adapter: Data adapter
            symbol: Symbol to fetch
            timeframe: Timeframe
            since: Start timestamp (Unix ms)
            limit: Max records to fetch
            rate_limiter: Optional rate limiter
            **kwargs: Additional args for BaseDataOperator
        """
        super().__init__(task_id, **kwargs)
        self.adapter = adapter
        self.symbol = symbol
        self.timeframe = timeframe
        self.since = since
        self.limit = limit
        self.rate_limiter = rate_limiter

    async def execute(self, context: dict[str, Any] | None = None) -> dict[str, Any]:
        """
        Execute OHLCV fetch.

        Args:
            context: Airflow context dictionary

        Returns:
            Result dictionary for XCom
        """
        start_time = datetime.utcnow()

        self.log_info(f"Fetching OHLCV for {self.symbol} ({self.timeframe})")

        try:
            # Rate limiting
            if self.rate_limiter:
                await self.rate_limiter.acquire()

            # Fetch with retry
            data = await self.execute_with_retry(
                self.adapter.fetch_ohlcv,
                symbol=self.symbol,
                timeframe=self.timeframe,
                since=self.since,
                limit=self.limit,
            )

            if self.rate_limiter:
                await self.rate_limiter.record_success()

            duration = (datetime.utcnow() - start_time).total_seconds()

            self.log_info(
                f"Fetched {len(data) if data else 0} OHLCV records in {duration:.2f}s"
            )

            result = self.create_result(
                success=True,
                records_processed=len(data) if data else 0,
                duration=duration,
                symbol=self.symbol,
                timeframe=self.timeframe,
                data=data,  # Include data for downstream tasks
            )

            return result.to_dict()

        except Exception as e:
            if self.rate_limiter:
                await self.rate_limiter.record_error()

            duration = (datetime.utcnow() - start_time).total_seconds()
            error_msg = f"OHLCV fetch failed: {str(e)}"
            self.log_error(error_msg)

            result = self.create_result(
                success=False,
                duration=duration,
                error=error_msg,
            )

            return result.to_dict()


class FetchOpenInterestOperator(BaseDataOperator):
    """
    Operator for fetching open interest data from an adapter.
    """

    def __init__(
        self,
        task_id: str,
        adapter: Any,  # Data adapter with fetch_oi method
        symbol: str,
        timeframe: str,
        exchange: str,
        since: int | None = None,
        limit: int | None = None,
        rate_limiter: IRateLimiter | None = None,
        **kwargs,
    ):
        """
        Initialize open interest fetch operator.

        Args:
            task_id: Task identifier
            adapter: Data adapter
            symbol: Symbol to fetch
            timeframe: Timeframe
            exchange: Exchange identifier
            since: Start timestamp (Unix ms)
            limit: Max records to fetch
            rate_limiter: Optional rate limiter
            **kwargs: Additional args for BaseDataOperator
        """
        super().__init__(task_id, **kwargs)
        self.adapter = adapter
        self.symbol = symbol
        self.timeframe = timeframe
        self.exchange = exchange
        self.since = since
        self.limit = limit
        self.rate_limiter = rate_limiter

    async def execute(self, context: dict[str, Any] | None = None) -> dict[str, Any]:
        """
        Execute open interest fetch.

        Args:
            context: Airflow context dictionary

        Returns:
            Result dictionary for XCom
        """
        start_time = datetime.utcnow()

        self.log_info(
            f"Fetching OI for {self.symbol} on {self.exchange} ({self.timeframe})"
        )

        try:
            # Rate limiting
            if self.rate_limiter:
                await self.rate_limiter.acquire()

            # Fetch with retry
            data = await self.execute_with_retry(
                self.adapter.fetch_oi,
                symbol=self.symbol,
                timeframe=self.timeframe,
                exchange=self.exchange,
                since=self.since,
                limit=self.limit,
            )

            if self.rate_limiter:
                await self.rate_limiter.record_success()

            duration = (datetime.utcnow() - start_time).total_seconds()

            self.log_info(
                f"Fetched {len(data) if data else 0} OI records in {duration:.2f}s"
            )

            result = self.create_result(
                success=True,
                records_processed=len(data) if data else 0,
                duration=duration,
                symbol=self.symbol,
                timeframe=self.timeframe,
                exchange=self.exchange,
                data=data,
            )

            return result.to_dict()

        except Exception as e:
            if self.rate_limiter:
                await self.rate_limiter.record_error()

            duration = (datetime.utcnow() - start_time).total_seconds()
            error_msg = f"OI fetch failed: {str(e)}"
            self.log_error(error_msg)

            result = self.create_result(
                success=False,
                duration=duration,
                error=error_msg,
            )

            return result.to_dict()


class FetchHistoricalOperator(BaseDataOperator):
    """
    Operator for fetching historical data over a time range.

    Suitable for backfill operations that need a date range.
    """

    def __init__(
        self,
        task_id: str,
        adapter: Any,  # Adapter with fetch_*_history methods
        symbol: str,
        timeframe: str,
        data_type: str,  # "ohlcv" or "open_interest"
        start_date: datetime,
        end_date: datetime,
        exchange: str | None = None,
        rate_limiter: IRateLimiter | None = None,
        **kwargs,
    ):
        """
        Initialize historical data fetch operator.

        Args:
            task_id: Task identifier
            adapter: Data adapter
            symbol: Symbol to fetch
            timeframe: Timeframe
            data_type: Type of data
            start_date: Start date
            end_date: End date
            exchange: Optional exchange identifier
            rate_limiter: Optional rate limiter
            **kwargs: Additional args for BaseDataOperator
        """
        super().__init__(task_id, **kwargs)
        self.adapter = adapter
        self.symbol = symbol
        self.timeframe = timeframe
        self.data_type = data_type
        self.start_date = start_date
        self.end_date = end_date
        self.exchange = exchange
        self.rate_limiter = rate_limiter

    async def execute(self, context: dict[str, Any] | None = None) -> dict[str, Any]:
        """
        Execute historical data fetch.

        Args:
            context: Airflow context dictionary

        Returns:
            Result dictionary for XCom
        """
        start_time = datetime.utcnow()

        self.log_info(
            f"Fetching historical {self.data_type} for {self.symbol} "
            f"from {self.start_date.date()} to {self.end_date.date()}"
        )

        try:
            # Rate limiting
            if self.rate_limiter:
                await self.rate_limiter.acquire()

            # Determine fetch method based on data type
            if self.data_type in ["ohlcv", "ohlc"]:
                fetch_method = self.adapter.fetch_ohlcv_history
                fetch_kwargs = {
                    "symbol": self.symbol,
                    "timeframe": self.timeframe,
                    "start_date": self.start_date,
                    "end_date": self.end_date,
                }
            elif self.data_type == "open_interest":
                fetch_method = self.adapter.fetch_oi_history
                fetch_kwargs = {
                    "symbol": self.symbol,
                    "timeframe": self.timeframe,
                    "start_date": self.start_date,
                    "end_date": self.end_date,
                    "exchange": self.exchange,
                }
            else:
                raise ValueError(f"Unsupported data type: {self.data_type}")

            # Fetch with retry
            data = await self.execute_with_retry(
                fetch_method,
                **fetch_kwargs,
            )

            if self.rate_limiter:
                await self.rate_limiter.record_success()

            duration = (datetime.utcnow() - start_time).total_seconds()

            self.log_info(
                f"Fetched {len(data) if data else 0} historical records in {duration:.2f}s"
            )

            result = self.create_result(
                success=True,
                records_processed=len(data) if data else 0,
                duration=duration,
                symbol=self.symbol,
                timeframe=self.timeframe,
                data_type=self.data_type,
                data=data,
            )

            return result.to_dict()

        except Exception as e:
            if self.rate_limiter:
                await self.rate_limiter.record_error()

            duration = (datetime.utcnow() - start_time).total_seconds()
            error_msg = f"Historical fetch failed: {str(e)}"
            self.log_error(error_msg)

            result = self.create_result(
                success=False,
                duration=duration,
                error=error_msg,
            )

            return result.to_dict()
