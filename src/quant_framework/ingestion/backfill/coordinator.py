"""
Backfill Coordinator
Orchestrates historical data backfill with dependency injection.
"""

import logging
from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Any

import yaml

from quant_framework.ingestion.backfill.checkpoint_manager import (
    CheckpointManager,
)
from quant_framework.ingestion.backfill.rate_limiter import IRateLimiter
from quant_framework.ingestion.ports.backfill import (
    IBackfillReporter,
    IChunkGenerator,
    IChunkProcessor,
)

logger = logging.getLogger(__name__)


@dataclass
class BackfillRequest:
    """Specification for a backfill request."""

    symbols: list[str]
    timeframes: list[str]
    data_types: list[str]  # ['ohlc', 'oi']
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
    failed_chunks: list[dict]
    duration_seconds: float
    status: str  # 'completed', 'partial', 'failed'


class BackfillCoordinator:
    """
    Coordinates historical data backfill with dependency injection.

    Responsibilities:
    - Orchestrate chunk generation, processing, and reporting
    - Coordinate checkpointing and error recovery
    - Apply rate limiting and floor dates
    - NOT responsible for: chunk generation, processing, reporting (delegated to injected dependencies)
    """

    def __init__(
        self,
        data_adapter: Any,  # CoinalyzeBackfillAdapter
        bronze_writer: Any,  # BronzeWriter
        rate_limiter: IRateLimiter,
        checkpoint_manager: CheckpointManager,
        chunk_generator: IChunkGenerator,
        chunk_processor: IChunkProcessor,
        reporter: IBackfillReporter,
        config_path: str | None = None,
    ):
        """
        Initialize coordinator with injected dependencies.

        Args:
            data_adapter: Adapter implementing fetch methods (type-hinted as Any)
            bronze_writer: Writer for MinIO/S3 bronze layer (type-hinted as Any)
            rate_limiter: Rate limiting strategy
            checkpoint_manager: Checkpoint tracking manager
            chunk_generator: Generator for time-range chunks
            chunk_processor: Processor for individual chunks
            reporter: Reporter for progress and summary logging
            config_path: Path to backfill.yaml config
        """
        self.data_adapter = data_adapter
        self.bronze_writer = bronze_writer
        self.rate_limiter = rate_limiter
        self.checkpoint_manager = checkpoint_manager
        self.chunk_generator = chunk_generator
        self.chunk_processor = chunk_processor
        self.reporter = reporter

        # Load configuration

        if config_path is None:
            from quant_framework.infrastructure.impls import ProjectConfigPathResolver

            path_resolver = ProjectConfigPathResolver()
            config_path = path_resolver.resolve_config_file("backfill.yaml")
        with open(config_path) as f:
            self.config = yaml.safe_load(f)

        # Extract config values
        exec_config = self.config.get("execution", {})
        self.max_retries = exec_config.get("max_retries", 3)
        self.retry_delay = exec_config.get("retry_delay_seconds", 5)
        self.checkpoint_interval = exec_config.get("checkpoint_interval_chunks", 10)

        self.global_floor_dates = self.config.get("global_floor_dates", {})

        logger.info("BackfillCoordinator initialized with dependency injection")

    async def execute_backfill(
        self, request: BackfillRequest
    ) -> dict[str, BackfillResult]:
        """
        Execute complete backfill request.

        Args:
            request: Backfill specification

        Returns:
            Dictionary mapping symbol/timeframe/data_type to results
        """
        logger.info(
            f"🚀 Starting backfill: {len(request.symbols)} symbols, "
            f"{len(request.timeframes)} timeframes, {len(request.data_types)} data types"
        )

        results = {}

        # Process each combination
        for symbol in request.symbols:
            for timeframe in request.timeframes:
                for data_type in request.data_types:
                    # Determine exchanges to process
                    if data_type == "oi" and request.exchanges:
                        exchanges = request.exchanges
                    else:
                        # OHLC typically from single exchange (e.g., binance)
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
                            )
                            results[key] = result

                        except Exception as e:
                            logger.error(f"❌ Backfill failed for {key}: {e}")
                            results[key] = BackfillResult(
                                symbol=symbol,
                                timeframe=timeframe,
                                data_type=data_type,
                                exchange=exchange,
                                total_chunks=0,
                                successful_chunks=0,
                                total_records=0,
                                failed_chunks=[],
                                duration_seconds=0.0,
                                status="failed",
                            )

        # Log summary
        self._log_summary(results)

        return results

    async def _execute_single_backfill(
        self,
        symbol: str,
        timeframe: str,
        data_type: str,
        exchange: str,
        start_date: datetime,
        end_date: datetime,
        source: str,
    ) -> BackfillResult:
        """
        Execute backfill for single symbol/timeframe/data_type combination.
        Delegates to injected chunk generator and processor.
        """
        start_time = datetime.utcnow()

        logger.info(
            f"📊 Backfilling {symbol}/{timeframe}/{data_type}/{exchange} "
            f"from {start_date.date()} to {end_date.date()}"
        )

        # Start or resume checkpoint
        checkpoint = await self.checkpoint_manager.start_backfill(
            symbol=symbol, timeframe=timeframe, data_type=data_type, source=source
        )

        # Adjust start date if resuming
        if checkpoint.last_completed_date > datetime.min:
            start_date = checkpoint.last_completed_date + timedelta(days=1)
            logger.info(f"📍 Resuming from checkpoint: {start_date.date()}")

        # Apply global floor date if exists
        if symbol in self.global_floor_dates:
            floor_date = datetime.fromisoformat(self.global_floor_dates[symbol]["date"])
            if start_date < floor_date:
                logger.info(
                    f"📅 Adjusting start date from {start_date.date()} "
                    f"to floor date {floor_date.date()}"
                )
                start_date = floor_date

        # Generate chunks using injected generator
        chunks = self.chunk_generator.generate_chunks(
            symbol=symbol,
            timeframe=timeframe,
            data_type=data_type,
            start_date=start_date,
            end_date=end_date,
        )

        logger.info(f"📦 Generated {len(chunks)} chunks for processing")

        # Process chunks using injected processor
        total_records = 0
        successful_chunks = 0
        failed_chunks = []

        for i, chunk in enumerate(chunks):
            try:
                # Inject exchange information into chunk
                chunk.exchange = exchange

                result = await self.chunk_processor.process_chunk(chunk)
                total_records += result.records_written
                successful_chunks += 1

                # Report progress via injected reporter
                self.reporter.log_progress(
                    symbol=symbol,
                    timeframe=timeframe,
                    data_type=data_type,
                    chunks_completed=successful_chunks,
                    total_chunks=len(chunks),
                    records_written=result.records_written,
                )

                # Periodic checkpoint save
                if (i + 1) % self.checkpoint_interval == 0:
                    await self.checkpoint_manager.update_progress(
                        checkpoint=checkpoint,
                        last_completed_date=chunk.end_date,
                        records_written=0,  # Already tracked in checkpoint
                    )
                    logger.info(
                        f"💾 Checkpoint saved: {i + 1}/{len(chunks)} chunks processed"
                    )

            except Exception as e:
                logger.error(
                    f"❌ Chunk failed: {chunk.start_date} - {chunk.end_date}: {e}"
                )
                failed_chunks.append({"chunk": chunk, "error": str(e)})

        # Mark as completed
        if successful_chunks == len(chunks):
            await self.checkpoint_manager.complete_backfill(checkpoint)
            status = "completed"
        elif successful_chunks > 0:
            status = "partial"
        else:
            await self.checkpoint_manager.fail_backfill(
                checkpoint, f"All {len(chunks)} chunks failed"
            )
            status = "failed"

        duration = (datetime.utcnow() - start_time).total_seconds()

        result = BackfillResult(
            symbol=symbol,
            timeframe=timeframe,
            data_type=data_type,
            exchange=exchange,
            total_chunks=len(chunks),
            successful_chunks=successful_chunks,
            total_records=total_records,
            failed_chunks=failed_chunks,
            duration_seconds=duration,
            status=status,
        )

        logger.info(
            f"✅ Backfill {status}: {symbol}/{timeframe}/{data_type} - "
            f"{successful_chunks}/{len(chunks)} chunks, "
            f"{total_records} records in {duration:.1f}s"
        )

        return result

    def _log_summary(self, results: dict[str, BackfillResult]) -> None:
        """Log summary of all backfill results using injected reporter."""
        total_records = sum(r.total_records for r in results.values())
        total_duration = sum(r.duration_seconds for r in results.values())

        successful = sum(1 for r in results.values() if r.status == "completed")
        failed = sum(1 for r in results.values() if r.status in ("failed", "partial"))

        # Collect errors
        errors = []
        for key, result in results.items():
            if result.failed_chunks:
                for failed_chunk in result.failed_chunks:
                    errors.append((key, failed_chunk.get("error", "Unknown error")))

        # Create summary and report via injected reporter
        from quant_framework.ingestion.ports.backfill import BackfillSummary

        summary = BackfillSummary(
            total_chunks=len(results),
            successful_chunks=successful,
            failed_chunks=failed,
            total_records_written=total_records,
            total_duration_seconds=total_duration,
            errors=errors,
        )

        self.reporter.log_summary(summary)
