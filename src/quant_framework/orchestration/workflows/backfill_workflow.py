"""
Backfill Workflow Implementation
================================

Coordinates historical data backfill operations with chunking, checkpointing,
and rate limiting.

Moved from: quant_framework.ingestion.orchestration.backfill.coordinator
"""

import logging
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any

from quant_framework.orchestration.ports import (
    ICheckpointStore,
    IChunkStrategy,
    IProgressReporter,
    IRateLimiter,
    IWorkflowContext,
    WorkflowStatus,
)
from quant_framework.orchestration.workflows.base import BaseWorkflow

logger = logging.getLogger(__name__)


@dataclass
class BackfillRequest:
    """Specification for a backfill request."""

    symbols: list[str]
    timeframes: list[str]
    data_types: list[str]  # ['ohlcv', 'open_interest', 'trades']
    start_date: datetime
    end_date: datetime
    source: str = "coinalyze"
    exchanges: list[str] | None = None  # For OI multi-exchange


@dataclass
class BackfillChunk:
    """Single chunk of backfill work."""

    symbol: str
    timeframe: str
    data_type: str
    exchange: str
    start_date: datetime
    end_date: datetime
    metadata: dict[str, Any] = field(default_factory=dict)


@dataclass
class BackfillResult:
    """Result of a backfill operation."""

    symbol: str
    timeframe: str
    data_type: str
    exchange: str
    total_chunks: int
    successful_chunks: int
    total_records: int
    failed_chunks: list[dict[str, Any]]
    duration_seconds: float
    status: str  # 'completed', 'partial', 'failed'

    def to_dict(self) -> dict[str, Any]:
        """Convert to dictionary."""
        return {
            "symbol": self.symbol,
            "timeframe": self.timeframe,
            "data_type": self.data_type,
            "exchange": self.exchange,
            "total_chunks": self.total_chunks,
            "successful_chunks": self.successful_chunks,
            "total_records": self.total_records,
            "failed_chunks": self.failed_chunks,
            "duration_seconds": self.duration_seconds,
            "status": self.status,
        }


class BackfillWorkflow(BaseWorkflow):
    """
    Coordinates historical data backfill operations.

    Responsibilities:
    - Orchestrate chunk generation, processing, and reporting
    - Coordinate checkpointing and error recovery
    - Apply rate limiting and floor dates
    - Delegate actual data fetching to adapters

    This replaces BackfillCoordinator from ingestion.orchestration.backfill.
    """

    def __init__(
        self,
        data_adapter: Any,  # Adapter with fetch_ohlcv_history/fetch_oi_history
        bronze_writer: Any,  # Writer for MinIO/S3 bronze layer
        checkpoint_store: ICheckpointStore,
        chunk_strategy: IChunkStrategy,
        rate_limiter: IRateLimiter | None = None,
        progress_reporter: IProgressReporter | None = None,
        max_retries: int = 3,
        retry_delay_seconds: int = 5,
        checkpoint_interval_chunks: int = 10,
        global_floor_dates: dict[str, str] | None = None,
    ):
        """
        Initialize backfill workflow.

        Args:
            data_adapter: Adapter for fetching data
            bronze_writer: Writer for bronze layer
            checkpoint_store: Checkpoint storage
            chunk_strategy: Strategy for chunking time ranges
            rate_limiter: Optional rate limiter
            progress_reporter: Optional progress reporter
            max_retries: Max retries per chunk
            retry_delay_seconds: Delay between retries
            checkpoint_interval_chunks: Save checkpoint every N chunks
            global_floor_dates: Dict of symbol -> earliest valid date
        """
        super().__init__()
        self.data_adapter = data_adapter
        self.bronze_writer = bronze_writer
        self.checkpoint_store = checkpoint_store
        self.chunk_strategy = chunk_strategy
        self.rate_limiter = rate_limiter
        self.progress_reporter = progress_reporter
        self.max_retries = max_retries
        self.retry_delay_seconds = retry_delay_seconds
        self.checkpoint_interval_chunks = checkpoint_interval_chunks
        self.global_floor_dates = global_floor_dates or {}

        logger.info("BackfillWorkflow initialized")

    async def validate(self, context: IWorkflowContext) -> tuple[bool, list[str]]:
        """
        Validate backfill preconditions.

        Args:
            context: Workflow execution context

        Returns:
            Tuple of (is_valid, error_messages)
        """
        errors = []

        # Get request from context
        request = context.get_parameter("request")
        if not request:
            errors.append("Missing 'request' parameter in context")
            return False, errors

        # Validate request structure
        if not isinstance(request, BackfillRequest):
            if not isinstance(request, dict):
                errors.append("Request must be BackfillRequest or dict")
                return False, errors

            # Validate dict structure
            required_fields = [
                "symbols",
                "timeframes",
                "data_types",
                "start_date",
                "end_date",
            ]
            for field in required_fields:
                if field not in request:
                    errors.append(f"Missing required field: {field}")

        # Validate date range
        if isinstance(request, BackfillRequest):
            if request.start_date >= request.end_date:
                errors.append("start_date must be before end_date")
        elif isinstance(request, dict):
            start = request.get("start_date")
            end = request.get("end_date")
            if start and end and start >= end:
                errors.append("start_date must be before end_date")

        return len(errors) == 0, errors

    async def _execute_impl(self, context: IWorkflowContext) -> dict[str, Any]:
        """
        Execute backfill workflow.

        Args:
            context: Workflow execution context

        Returns:
            Execution result dictionary
        """
        # Get request from context
        request_data = context.get_parameter("request")

        # Convert to BackfillRequest if dict
        if isinstance(request_data, dict):
            request = BackfillRequest(**request_data)
        else:
            request = request_data

        context.log_info(
            f"Starting backfill: {len(request.symbols)} symbols, "
            f"{len(request.timeframes)} timeframes, {len(request.data_types)} data types"
        )

        results = {}

        # Process each combination
        for symbol in request.symbols:
            for timeframe in request.timeframes:
                for data_type in request.data_types:
                    # Determine exchanges to process
                    if data_type == "open_interest" and request.exchanges:
                        exchanges = request.exchanges
                    else:
                        # OHLCV typically from single exchange
                        exchanges = ["binance"]

                    for exchange in exchanges:
                        key = f"{symbol}/{timeframe}/{data_type}/{exchange}"

                        try:
                            result = await self._execute_single_backfill(
                                symbol=symbol,
                                timeframe=timeframe,
                                data_type=data_type,
                                exchange=exchange,
                                start_date=request.start_date,
                                end_date=request.end_date,
                                source=request.source,
                                context=context,
                            )
                            results[key] = result.to_dict()

                        except Exception as e:
                            error_msg = f"Backfill failed for {key}: {str(e)}"
                            context.log_error(error_msg)
                            results[key] = BackfillResult(
                                symbol=symbol,
                                timeframe=timeframe,
                                data_type=data_type,
                                exchange=exchange,
                                total_chunks=0,
                                successful_chunks=0,
                                total_records=0,
                                failed_chunks=[{"error": str(e)}],
                                duration_seconds=0.0,
                                status="failed",
                            ).to_dict()

        # Calculate overall status
        all_success = all(r["status"] == "completed" for r in results.values())
        any_partial = any(r["status"] == "partial" for r in results.values())

        return {
            "status": WorkflowStatus.SUCCESS.value
            if all_success
            else WorkflowStatus.FAILED.value,
            "results": results,
            "total_combinations": len(results),
            "successful": sum(
                1 for r in results.values() if r["status"] == "completed"
            ),
            "partial": sum(1 for r in results.values() if r["status"] == "partial"),
            "failed": sum(1 for r in results.values() if r["status"] == "failed"),
        }

    async def _execute_single_backfill(
        self,
        symbol: str,
        timeframe: str,
        data_type: str,
        exchange: str,
        start_date: datetime,
        end_date: datetime,
        source: str,
        context: IWorkflowContext,
    ) -> BackfillResult:
        """
        Execute backfill for single symbol/timeframe/data_type combination.

        Args:
            symbol: Symbol identifier
            timeframe: Timeframe
            data_type: Type of data
            exchange: Exchange identifier
            start_date: Start date
            end_date: End date
            source: Data source
            context: Workflow context

        Returns:
            BackfillResult with execution details
        """
        start_time = datetime.utcnow()

        context.log_info(
            f"Backfilling {symbol}/{timeframe}/{data_type}/{exchange} "
            f"from {start_date.date()} to {end_date.date()}"
        )

        # Load checkpoint to resume if needed
        checkpoint_data = await self.checkpoint_store.load_checkpoint(
            symbol=symbol,
            timeframe=timeframe,
            data_type=data_type,
            source=source,
        )

        if checkpoint_data and "last_timestamp" in checkpoint_data:
            # Resume from checkpoint
            last_ts = checkpoint_data["last_timestamp"]
            resume_date = datetime.utcfromtimestamp(last_ts / 1000)
            if resume_date > start_date:
                context.log_info(f"Resuming from checkpoint: {resume_date.date()}")
                start_date = resume_date

        # Apply global floor date if exists
        if symbol in self.global_floor_dates:
            floor_str = self.global_floor_dates[symbol]
            if isinstance(floor_str, dict):
                floor_str = floor_str.get("date", floor_str.get("start_date"))
            floor_date = datetime.fromisoformat(floor_str)
            if start_date < floor_date:
                context.log_info(
                    f"Adjusting start date from {start_date.date()} "
                    f"to floor date {floor_date.date()}"
                )
                start_date = floor_date

        # Generate chunks
        chunk_ranges = self.chunk_strategy.generate_chunks(
            start_date=start_date,
            end_date=end_date,
            timeframe=timeframe,
        )

        context.log_info(f"Generated {len(chunk_ranges)} chunks for processing")

        # Report start
        if self.progress_reporter:
            self.progress_reporter.report_start(total_items=len(chunk_ranges))

        # Process chunks
        total_records = 0
        successful_chunks = 0
        failed_chunks = []

        for idx, (chunk_start, chunk_end) in enumerate(chunk_ranges):
            chunk = BackfillChunk(
                symbol=symbol,
                timeframe=timeframe,
                data_type=data_type,
                exchange=exchange,
                start_date=chunk_start,
                end_date=chunk_end,
            )

            # Rate limiting
            if self.rate_limiter:
                await self.rate_limiter.acquire()

            # Process chunk with retries
            records_written = 0
            success = False
            last_error = None

            for attempt in range(self.max_retries):
                try:
                    records_written = await self._process_chunk(chunk, context)
                    success = True
                    if self.rate_limiter:
                        await self.rate_limiter.record_success()
                    break
                except Exception as e:
                    last_error = e
                    if self.rate_limiter:
                        await self.rate_limiter.record_error()

                    if attempt < self.max_retries - 1:
                        await asyncio.sleep(self.retry_delay_seconds)

            if success:
                successful_chunks += 1
                total_records += records_written

                # Checkpoint every N chunks
                if (idx + 1) % self.checkpoint_interval_chunks == 0:
                    await self.checkpoint_store.save_checkpoint(
                        symbol=symbol,
                        timeframe=timeframe,
                        data_type=data_type,
                        source=source,
                        last_timestamp=int(chunk_end.timestamp() * 1000),
                    )
            else:
                failed_chunks.append(
                    {
                        "chunk_start": chunk_start.isoformat(),
                        "chunk_end": chunk_end.isoformat(),
                        "error": str(last_error),
                    }
                )

            # Report progress
            if self.progress_reporter:
                self.progress_reporter.report_progress(
                    completed_items=idx + 1,
                    message=f"Processed {idx + 1}/{len(chunk_ranges)} chunks",
                )

        # Final checkpoint
        await self.checkpoint_store.save_checkpoint(
            symbol=symbol,
            timeframe=timeframe,
            data_type=data_type,
            source=source,
            last_timestamp=int(end_date.timestamp() * 1000),
            metadata={"status": "completed" if not failed_chunks else "partial"},
        )

        # Determine status
        if failed_chunks:
            status = "partial" if successful_chunks > 0 else "failed"
        else:
            status = "completed"

        duration = (datetime.utcnow() - start_time).total_seconds()

        # Report completion
        if self.progress_reporter:
            self.progress_reporter.report_completion(
                success=status in ["completed", "partial"],
                summary={
                    "total_chunks": len(chunk_ranges),
                    "successful_chunks": successful_chunks,
                    "total_records": total_records,
                },
            )

        return BackfillResult(
            symbol=symbol,
            timeframe=timeframe,
            data_type=data_type,
            exchange=exchange,
            total_chunks=len(chunk_ranges),
            successful_chunks=successful_chunks,
            total_records=total_records,
            failed_chunks=failed_chunks,
            duration_seconds=duration,
            status=status,
        )

    async def _process_chunk(
        self,
        chunk: BackfillChunk,
        context: IWorkflowContext,
    ) -> int:
        """
        Process a single chunk: fetch data and write to bronze.

        Args:
            chunk: Chunk to process
            context: Workflow context

        Returns:
            Number of records written

        Raises:
            Exception: If chunk processing fails
        """
        # Fetch data based on data type
        if chunk.data_type in ["ohlcv", "ohlc"]:
            raw_data = await self.data_adapter.fetch_ohlcv_history(
                symbol=chunk.symbol,
                timeframe=chunk.timeframe,
                start_date=chunk.start_date,
                end_date=chunk.end_date,
            )
        elif chunk.data_type == "open_interest":
            raw_data = await self.data_adapter.fetch_oi_history(
                symbol=chunk.symbol,
                timeframe=chunk.timeframe,
                start_date=chunk.start_date,
                end_date=chunk.end_date,
                exchange=chunk.exchange,
            )
        else:
            raise ValueError(f"Unsupported data type: {chunk.data_type}")

        if not raw_data:
            return 0

        # Write to bronze layer
        result = await self.bronze_writer.write_daily_batch(
            data=raw_data,
            symbol=chunk.symbol,
            timeframe=chunk.timeframe,
            data_type=chunk.data_type,
            exchange=chunk.exchange,
            date=chunk.start_date,
        )

        if not result.get("success"):
            raise Exception(f"Write failed: {result.get('error')}")

        return result.get("records_written", len(raw_data))


# Import asyncio for sleep
import asyncio
