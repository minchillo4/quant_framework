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


class ICheckpointManager(Protocol):
    """Abstraction for checkpoint management.

    Single Responsibility: Track backfill progress persistence.
    Enables resumable backfills across restarts.

    Note: This is for DATABASE-BACKED checkpoints used in backfill orchestration.
    For MinIO-backed incremental pipeline checkpoints, see infrastructure.checkpoint.
    """

    async def start_backfill(
        self,
        symbol: str,
        timeframe: str,
        data_type: str,
        source: str,
        start_date: datetime,
    ) -> Any:  # BackfillCheckpoint
        """Initialize new backfill checkpoint.

        Args:
            symbol: Trading symbol
            timeframe: Data timeframe
            data_type: Type of data ('ohlcv', 'open_interest')
            source: Data source name (e.g., 'coinalyze')
            start_date: Backfill start date

        Returns:
            BackfillCheckpoint object
        """
        ...

    async def update_progress(
        self,
        symbol: str,
        timeframe: str,
        data_type: str,
        source: str,
        last_completed_date: datetime,
        chunks_completed: int,
        records_written: int,
    ) -> None:
        """Update checkpoint with latest progress.

        Args:
            symbol: Trading symbol
            timeframe: Data timeframe
            data_type: Type of data
            source: Data source name
            last_completed_date: Latest successfully completed date
            chunks_completed: Total chunks completed so far
            records_written: Total records written so far
        """
        ...

    async def complete_backfill(
        self,
        symbol: str,
        timeframe: str,
        data_type: str,
        source: str,
        total_chunks: int,
        total_records: int,
    ) -> None:
        """Mark backfill as completed.

        Args:
            symbol: Trading symbol
            timeframe: Data timeframe
            data_type: Type of data
            source: Data source name
            total_chunks: Total chunks processed
            total_records: Total records written
        """
        ...

    async def fail_backfill(
        self,
        symbol: str,
        timeframe: str,
        data_type: str,
        source: str,
        error_message: str,
    ) -> None:
        """Mark backfill as failed.

        Args:
            symbol: Trading symbol
            timeframe: Data timeframe
            data_type: Type of data
            source: Data source name
            error_message: Failure reason
        """
        ...

    async def get_last_checkpoint(
        self,
        symbol: str,
        timeframe: str,
        data_type: str,
        source: str,
    ) -> datetime | None:
        """Get last successfully completed date from checkpoint.

        Args:
            symbol: Trading symbol
            timeframe: Data timeframe
            data_type: Type of data
            source: Data source name

        Returns:
            Last completed date or None if no checkpoint exists
        """
        ...


class IBackfillAdapter(Protocol):
    """Abstraction for backfill data adapters.

    Single Responsibility: Fetch historical data from a specific source.
    Must support both OHLCV and open interest data types.
    """

    async def fetch_ohlcv_history(
        self,
        symbol: str,
        timeframe: str,
        start_date: datetime,
        end_date: datetime,
        instrument: Instrument,
    ) -> list[dict]:
        """Fetch OHLCV historical data.

        Args:
            symbol: Trading symbol
            timeframe: Data timeframe (e.g., '1h', '1d')
            start_date: Start of date range
            end_date: End of date range
            instrument: Instrument metadata

        Returns:
            List of OHLCV records as dictionaries
        """
        ...

    async def fetch_oi_history(
        self,
        symbol: str,
        timeframe: str,
        start_date: datetime,
        end_date: datetime,
        instrument: Instrument,
    ) -> list[dict]:
        """Fetch open interest historical data.

        Args:
            symbol: Trading symbol
            timeframe: Data timeframe (e.g., '1h', '5m')
            start_date: Start of date range
            end_date: End of date range
            instrument: Instrument metadata

        Returns:
            List of open interest records as dictionaries
        """
        ...


class IBronzeWriter(Protocol):
    """Abstraction for bronze layer storage.

    Single Responsibility: Write validated data to bronze storage.
    Typically writes daily-partitioned Parquet files to MinIO/S3.

    Note: Implementation currently lives in infra/airflow/dags/common/bronze_writer.py
    but should be moved to quant_framework.storage.bronze in Phase 2.
    """

    async def write_daily_batch(
        self,
        records: list[dict],
        symbol: str,
        timeframe: str,
        data_type: str,
        date: datetime,
        schema_class: type | None = None,
    ) -> dict:
        """Write daily batch of records to bronze storage.

        Args:
            records: List of data records
            symbol: Canonical symbol (e.g., 'BTC', 'ETH')
            timeframe: Timeframe (e.g., '1h', '1d')
            data_type: Data type ('ohlc' or 'oi')
            date: Date for this batch
            schema_class: Optional Pydantic schema for validation

        Returns:
            Dictionary with:
                - success: bool
                - file_path: str
                - records_written: int
                - error: str | None
        """
        ...
