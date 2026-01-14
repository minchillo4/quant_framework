"""Backfill orchestration abstractions.

Separates orchestration logic from execution concerns:
- Chunk generation: How to split backfill into chunks
- Chunk processing: How to execute individual chunks
- Reporting: How to report progress and results
"""

from dataclasses import dataclass
from datetime import datetime
from typing import Protocol

from quant_framework.shared.models.instruments import Instrument


@dataclass
class BackfillChunk:
    """A time-range chunk for backfill processing."""

    symbol: str
    timeframe: str
    data_type: str  # "ohlcv" or "open_interest"
    start_date: datetime
    end_date: datetime
    instrument: Instrument | None = None


@dataclass
class ChunkResult:
    """Result of processing a single chunk."""

    chunk: BackfillChunk
    records_written: int
    success: bool
    error: str | None = None
    duration_seconds: float = 0.0


class IChunkGenerator(Protocol):
    """Abstraction for chunk generation.

    Single Responsibility: Generate time-range chunks for backfill.
    Does NOT execute chunks.
    """

    def generate_chunks(
        self,
        symbol: str,
        timeframe: str,
        data_type: str,
        start_date: datetime,
        end_date: datetime,
    ) -> list[BackfillChunk]:
        """Generate chunks for backfill date range.

        Args:
            symbol: Trading symbol
            timeframe: Candle timeframe (e.g., "1h", "1d")
            data_type: Type of data (e.g., "ohlcv", "open_interest")
            start_date: Backfill start date
            end_date: Backfill end date

        Returns:
            List of chunks covering the date range
        """
        ...


class IChunkProcessor(Protocol):
    """Abstraction for processing individual chunks.

    Single Responsibility: Execute a chunk (fetch + write).
    Does NOT handle orchestration or progress tracking.
    """

    async def process_chunk(self, chunk: BackfillChunk) -> ChunkResult:
        """Process single backfill chunk.

        Args:
            chunk: Chunk to process

        Returns:
            ChunkResult with success status and record count

        Raises:
            ChunkProcessingError: On non-recoverable processing errors
        """
        ...


@dataclass
class BackfillSummary:
    """Summary statistics after backfill completes."""

    total_chunks: int
    successful_chunks: int
    failed_chunks: int
    total_records_written: int
    total_duration_seconds: float
    errors: list[tuple[str, str]]  # List of (symbol, error_msg)


class IBackfillReporter(Protocol):
    """Abstraction for reporting and logging.

    Single Responsibility: Format and report progress/results.
    Does NOT make backfill decisions.
    """

    def log_progress(
        self,
        symbol: str,
        timeframe: str,
        data_type: str,
        chunks_completed: int,
        total_chunks: int,
        records_written: int,
    ) -> None:
        """Log progress after chunk completion.

        Args:
            symbol: Trading symbol
            timeframe: Candle timeframe
            data_type: Data type being backfilled
            chunks_completed: Number of completed chunks
            total_chunks: Total chunks to process
            records_written: Records written by last chunk
        """
        ...

    def log_summary(self, summary: BackfillSummary) -> None:
        """Log final backfill summary.

        Args:
            summary: Backfill completion summary with statistics
        """
        ...
