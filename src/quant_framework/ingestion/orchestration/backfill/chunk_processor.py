"""
Backfill chunk processor for data fetching and writing.

Responsibility: Execute a single chunk (fetch data, write to bronze).
Does NOT handle orchestration, progress tracking, or error recovery.
"""

import logging
from collections import defaultdict
from datetime import datetime
from typing import Any

from quant_framework.ingestion.models.enums import DataProvider
from quant_framework.ingestion.ports.backfill import (
    BackfillChunk,
    ChunkResult,
)
from quant_framework.shared.models.instruments import Instrument

logger = logging.getLogger(__name__)


class BackfillChunkProcessor:
    """
    Processes individual backfill chunks.

    Implements IChunkProcessor protocol.

    Single Responsibility: Fetch data for a chunk and write to bronze layer.
    Does NOT orchestrate multiple chunks or handle retries.
    """

    def __init__(
        self,
        data_adapter: Any,  # CoinalyzeBackfillAdapter
        bronze_writer: Any,  # BronzeWriter
    ):
        """
        Initialize chunk processor.

        Args:
            data_adapter: Adapter implementing fetch_ohlcv_history/fetch_oi_history
            bronze_writer: Writer for bronze layer (implements write_daily_batch)
        """
        self.data_adapter = data_adapter
        self.bronze_writer = bronze_writer
        logger.info("BackfillChunkProcessor initialized")

    async def process_chunk(self, chunk: BackfillChunk) -> ChunkResult:
        """
        Process single backfill chunk: fetch data and write to bronze.

        Args:
            chunk: Chunk to process

        Returns:
            ChunkResult with success status and record count
        """
        start_time = datetime.utcnow()

        try:
            logger.debug(
                f"Processing chunk: {chunk.symbol}/{chunk.timeframe}/{chunk.data_type} "
                f"{chunk.start_date.date()} to {chunk.end_date.date()}"
            )

            # Create instrument object if not provided
            if not chunk.instrument:
                chunk.instrument = Instrument(
                    instrument_id=f"binance:{chunk.symbol}",
                    venue="binance",
                    symbol=chunk.symbol,
                    provider=DataProvider.COINALYZE,
                )

            # Fetch data based on type
            if chunk.data_type == "ohlcv" or chunk.data_type == "ohlc":
                records = await self.data_adapter.fetch_ohlcv_history(
                    symbol=chunk.symbol,
                    timeframe=chunk.timeframe,
                    start_date=chunk.start_date,
                    end_date=chunk.end_date,
                    instrument=chunk.instrument,
                )
            elif chunk.data_type == "oi" or chunk.data_type == "open_interest":
                records = await self.data_adapter.fetch_oi_history(
                    symbol=chunk.symbol,
                    timeframe=chunk.timeframe,
                    start_date=chunk.start_date,
                    end_date=chunk.end_date,
                    instrument=chunk.instrument,
                )
            else:
                raise ValueError(f"Unknown data type: {chunk.data_type}")

            if not records:
                logger.warning(
                    f"No records returned for chunk {chunk.symbol}/{chunk.timeframe} "
                    f"{chunk.start_date.date()}"
                )
                return ChunkResult(
                    chunk=chunk,
                    records_written=0,
                    success=True,
                    error=None,
                    duration_seconds=(datetime.utcnow() - start_time).total_seconds(),
                )

            # Group records by date for daily partitioning
            records_by_date = defaultdict(list)

            for record in records:
                # Extract timestamp from record
                ts = self._extract_timestamp(record)

                if ts:
                    date_key = ts.date()
                    records_by_date[date_key].append(record)

            # Write each daily batch to bronze layer
            total_written = 0
            for date_key, daily_records in records_by_date.items():
                result = await self.bronze_writer.write_daily_batch(
                    records=daily_records,
                    symbol=chunk.symbol,
                    timeframe=chunk.timeframe,
                    data_type=chunk.data_type,
                    date=datetime.combine(date_key, datetime.min.time()),
                )

                if result.get("success"):
                    written = result.get("records_written", 0)
                    total_written += written
                    logger.debug(
                        f"✅ Wrote {written} records for {date_key} "
                        f"to {result.get('file_path')}"
                    )
                else:
                    raise RuntimeError(
                        f"Failed to write {date_key}: {result.get('error')}"
                    )

            duration = (datetime.utcnow() - start_time).total_seconds()

            return ChunkResult(
                chunk=chunk,
                records_written=total_written,
                success=True,
                error=None,
                duration_seconds=duration,
            )

        except Exception as e:
            duration = (datetime.utcnow() - start_time).total_seconds()
            logger.error(
                f"❌ Chunk processing failed: {chunk.symbol}/{chunk.timeframe} "
                f"{chunk.start_date.date()}: {e}"
            )

            return ChunkResult(
                chunk=chunk,
                records_written=0,
                success=False,
                error=str(e),
                duration_seconds=duration,
            )

    def _extract_timestamp(self, record: Any) -> datetime | None:
        """
        Extract timestamp from record (dict or object).

        Args:
            record: Record object or dictionary

        Returns:
            Parsed datetime or None if not found
        """
        # Try dictionary access first
        if isinstance(record, dict):
            ts = record.get("timestamp") or record.get("time")
        else:
            # Try object attributes
            ts = getattr(record, "timestamp", None) or getattr(record, "time", None)

        if not ts:
            return None

        # Convert to datetime if needed
        if isinstance(ts, (int, float)):
            try:
                ts = datetime.fromtimestamp(ts)
            except (ValueError, OSError):
                return None
        elif isinstance(ts, str):
            try:
                ts = datetime.fromisoformat(ts)
            except ValueError:
                return None

        return ts if isinstance(ts, datetime) else None
