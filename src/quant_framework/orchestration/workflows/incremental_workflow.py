"""
Incremental Workflow Implementation
===================================

Coordinates real-time incremental data updates with streaming state management.
"""

import logging
from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Any

from quant_framework.orchestration.ports import (
    ICheckpointStore,
    IRateLimiter,
    IWorkflowContext,
    WorkflowStatus,
)
from quant_framework.orchestration.workflows.base import BaseWorkflow

logger = logging.getLogger(__name__)


@dataclass
class IncrementalRequest:
    """Specification for incremental update request."""

    symbols: list[str]
    timeframes: list[str]
    data_types: list[str]
    source: str = "ccxt"
    exchanges: list[str] | None = None
    lookback_hours: int = 24  # How far back to fetch if no checkpoint


class IncrementalWorkflow(BaseWorkflow):
    """
    Coordinates real-time incremental data updates.

    Fetches recent data since last checkpoint, maintaining streaming state.
    Suitable for hourly/minutely DAGs that keep data fresh.
    """

    def __init__(
        self,
        data_adapter: Any,  # Adapter with fetch_ohlcv/fetch_oi methods
        bronze_writer: Any,  # Writer for bronze layer
        checkpoint_store: ICheckpointStore,
        rate_limiter: IRateLimiter | None = None,
        max_retries: int = 3,
        retry_delay_seconds: int = 5,
    ):
        """
        Initialize incremental workflow.

        Args:
            data_adapter: Adapter for fetching data
            bronze_writer: Writer for bronze layer
            checkpoint_store: Checkpoint storage
            rate_limiter: Optional rate limiter
            max_retries: Max retries per fetch
            retry_delay_seconds: Delay between retries
        """
        super().__init__()
        self.data_adapter = data_adapter
        self.bronze_writer = bronze_writer
        self.checkpoint_store = checkpoint_store
        self.rate_limiter = rate_limiter
        self.max_retries = max_retries
        self.retry_delay_seconds = retry_delay_seconds

        logger.info("IncrementalWorkflow initialized")

    async def validate(self, context: IWorkflowContext) -> tuple[bool, list[str]]:
        """
        Validate incremental update preconditions.

        Args:
            context: Workflow execution context

        Returns:
            Tuple of (is_valid, error_messages)
        """
        errors = []

        request = context.get_parameter("request")
        if not request:
            errors.append("Missing 'request' parameter in context")
            return False, errors

        # Validate request structure
        if isinstance(request, dict):
            required_fields = ["symbols", "timeframes", "data_types"]
            for field in required_fields:
                if field not in request:
                    errors.append(f"Missing required field: {field}")

        return len(errors) == 0, errors

    async def _execute_impl(self, context: IWorkflowContext) -> dict[str, Any]:
        """
        Execute incremental update workflow.

        Args:
            context: Workflow execution context

        Returns:
            Execution result dictionary
        """
        # Get request from context
        request_data = context.get_parameter("request")

        # Convert to IncrementalRequest if dict
        if isinstance(request_data, dict):
            request = IncrementalRequest(**request_data)
        else:
            request = request_data

        context.log_info(
            f"Starting incremental update: {len(request.symbols)} symbols, "
            f"{len(request.timeframes)} timeframes, {len(request.data_types)} data types"
        )

        results = {}
        total_records = 0

        # Process each combination
        for symbol in request.symbols:
            for timeframe in request.timeframes:
                for data_type in request.data_types:
                    # Determine exchanges
                    if data_type == "open_interest" and request.exchanges:
                        exchanges = request.exchanges
                    else:
                        exchanges = ["binance"]

                    for exchange in exchanges:
                        key = f"{symbol}/{timeframe}/{data_type}/{exchange}"

                        try:
                            records = await self._execute_single_update(
                                symbol=symbol,
                                timeframe=timeframe,
                                data_type=data_type,
                                exchange=exchange,
                                source=request.source,
                                lookback_hours=request.lookback_hours,
                                context=context,
                            )

                            results[key] = {
                                "success": True,
                                "records_written": records,
                            }
                            total_records += records

                        except Exception as e:
                            error_msg = f"Incremental update failed for {key}: {str(e)}"
                            context.log_error(error_msg)
                            results[key] = {
                                "success": False,
                                "error": str(e),
                                "records_written": 0,
                            }

        successful = sum(1 for r in results.values() if r.get("success"))

        return {
            "status": WorkflowStatus.SUCCESS.value,
            "results": results,
            "total_combinations": len(results),
            "successful": successful,
            "failed": len(results) - successful,
            "total_records": total_records,
        }

    async def _execute_single_update(
        self,
        symbol: str,
        timeframe: str,
        data_type: str,
        exchange: str,
        source: str,
        lookback_hours: int,
        context: IWorkflowContext,
    ) -> int:
        """
        Execute incremental update for single combination.

        Args:
            symbol: Symbol identifier
            timeframe: Timeframe
            data_type: Type of data
            exchange: Exchange identifier
            source: Data source
            lookback_hours: Lookback period if no checkpoint
            context: Workflow context

        Returns:
            Number of records written
        """
        context.log_info(f"Updating {symbol}/{timeframe}/{data_type}/{exchange}")

        # Load checkpoint
        checkpoint_data = await self.checkpoint_store.load_checkpoint(
            symbol=symbol,
            timeframe=timeframe,
            data_type=data_type,
            source=source,
        )

        # Determine start timestamp
        if checkpoint_data and "last_timestamp" in checkpoint_data:
            since = checkpoint_data["last_timestamp"]
            context.log_info(
                f"Fetching since checkpoint: {datetime.utcfromtimestamp(since/1000)}"
            )
        else:
            # No checkpoint, fetch recent data
            since = int(
                (datetime.utcnow() - timedelta(hours=lookback_hours)).timestamp() * 1000
            )
            context.log_info(f"No checkpoint, fetching last {lookback_hours} hours")

        # Rate limiting
        if self.rate_limiter:
            await self.rate_limiter.acquire()

        # Fetch data with retries
        raw_data = None
        last_error = None

        for attempt in range(self.max_retries):
            try:
                if data_type in ["ohlcv", "ohlc"]:
                    raw_data = await self.data_adapter.fetch_ohlcv(
                        symbol=symbol,
                        timeframe=timeframe,
                        since=since,
                    )
                elif data_type == "open_interest":
                    raw_data = await self.data_adapter.fetch_oi(
                        symbol=symbol,
                        timeframe=timeframe,
                        since=since,
                        exchange=exchange,
                    )
                else:
                    raise ValueError(f"Unsupported data type: {data_type}")

                if self.rate_limiter:
                    await self.rate_limiter.record_success()
                break

            except Exception as e:
                last_error = e
                if self.rate_limiter:
                    await self.rate_limiter.record_error()

                if attempt < self.max_retries - 1:
                    import asyncio

                    await asyncio.sleep(self.retry_delay_seconds)
                else:
                    raise e

        if not raw_data:
            context.log_info(f"No new data for {symbol}/{timeframe}/{data_type}")
            return 0

        # Write to bronze
        result = await self.bronze_writer.write_daily_batch(
            data=raw_data,
            symbol=symbol,
            timeframe=timeframe,
            data_type=data_type,
            exchange=exchange,
            date=datetime.utcnow(),
        )

        if not result.get("success"):
            raise Exception(f"Write failed: {result.get('error')}")

        records_written = result.get("records_written", len(raw_data))

        # Update checkpoint with latest timestamp
        if raw_data:
            # Extract last timestamp (format depends on adapter)
            if isinstance(raw_data[-1], dict):
                last_ts = raw_data[-1].get("timestamp", since)
            elif isinstance(raw_data[-1], (list, tuple)):
                last_ts = raw_data[-1][0]  # CCXT format
            else:
                last_ts = since

            await self.checkpoint_store.save_checkpoint(
                symbol=symbol,
                timeframe=timeframe,
                data_type=data_type,
                source=source,
                last_timestamp=int(last_ts),
            )

        context.log_info(
            f"Written {records_written} records for {symbol}/{timeframe}/{data_type}"
        )

        return records_written
