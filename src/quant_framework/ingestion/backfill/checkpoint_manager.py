"""
Checkpoint manager for tracking backfill progress using MinIO storage.
Enables resumable backfills and progress monitoring.
"""

import logging
from dataclasses import dataclass
from datetime import datetime
from typing import Protocol

from quant_framework.storage.repositories.checkpoint import (
    BackfillCheckpointRecord,
    CheckpointRepository,
)

logger = logging.getLogger(__name__)


@dataclass
class BackfillCheckpoint:
    """Represents a backfill progress checkpoint.

    This is the public API dataclass used by BackfillCoordinator.
    Maps to BackfillCheckpointRecord for storage.
    """

    symbol: str
    timeframe: str
    data_type: str
    source: str
    last_completed_date: datetime
    total_chunks_completed: int
    total_records_written: int
    started_at: datetime
    updated_at: datetime
    status: str  # 'in_progress', 'completed', 'failed'
    error_message: str | None = None

    def to_record(self) -> BackfillCheckpointRecord:
        """Convert to storage record."""
        # Convert datetime to milliseconds timestamp
        timestamp_ms = int(self.last_completed_date.timestamp() * 1000)
        if self.last_completed_date == datetime.min:
            timestamp_ms = 0

        return BackfillCheckpointRecord(
            symbol=self.symbol,
            timeframe=self.timeframe,
            data_type=self.data_type,
            source=self.source,
            last_completed_timestamp=timestamp_ms,
            total_chunks_completed=self.total_chunks_completed,
            total_records_written=self.total_records_written,
            started_at=self.started_at,
            updated_at=self.updated_at,
            status=self.status,
            error_message=self.error_message,
        )

    @classmethod
    def from_record(cls, record: BackfillCheckpointRecord) -> "BackfillCheckpoint":
        """Create from storage record."""
        return cls(
            symbol=record.symbol,
            timeframe=record.timeframe,
            data_type=record.data_type,
            source=record.source,
            last_completed_date=record.last_completed_date,
            total_chunks_completed=record.total_chunks_completed,
            total_records_written=record.total_records_written,
            started_at=record.started_at,
            updated_at=record.updated_at,
            status=record.status,
            error_message=record.error_message,
        )


class ICheckpointStore(Protocol):
    """
    Protocol for checkpoint storage.
    Now implemented by MinIO-backed CheckpointRepository.
    """

    async def save_checkpoint(self, checkpoint: BackfillCheckpoint) -> None:
        """Save or update checkpoint."""
        ...

    async def load_checkpoint(
        self, symbol: str, timeframe: str, data_type: str, source: str
    ) -> BackfillCheckpoint | None:
        """Load checkpoint for specific combination."""
        ...

    async def list_checkpoints(
        self, status: str | None = None
    ) -> list[BackfillCheckpoint]:
        """List all checkpoints, optionally filtered by status."""
        ...

    async def delete_checkpoint(
        self, symbol: str, timeframe: str, data_type: str, source: str
    ) -> bool:
        """Delete checkpoint."""
        ...

    async def mark_completed(
        self, symbol: str, timeframe: str, data_type: str, source: str
    ) -> None:
        """Mark backfill as completed."""
        ...


class MinIOCheckpointStore:
    """
    MinIO-backed checkpoint storage adapter.
    Implements ICheckpointStore using CheckpointRepository.
    """

    def __init__(self, repository: CheckpointRepository | None = None):
        """
        Initialize checkpoint store.

        Args:
            repository: CheckpointRepository instance (creates default if None)
        """
        self.repo = repository or CheckpointRepository()

    async def save_checkpoint(self, checkpoint: BackfillCheckpoint) -> None:
        """Save or update checkpoint in MinIO."""
        record = checkpoint.to_record()
        await self.repo.upsert(record)

        logger.debug(
            f"Saved checkpoint: {checkpoint.symbol}/{checkpoint.timeframe}/{checkpoint.data_type} "
            f"- {checkpoint.total_chunks_completed} chunks, {checkpoint.total_records_written} records"
        )

    async def load_checkpoint(
        self, symbol: str, timeframe: str, data_type: str, source: str
    ) -> BackfillCheckpoint | None:
        """Load checkpoint from MinIO."""
        record = await self.repo.find_by_symbol_source(
            symbol, timeframe, data_type, source
        )

        if not record:
            return None

        return BackfillCheckpoint.from_record(record)

    async def list_checkpoints(
        self, status: str | None = None
    ) -> list[BackfillCheckpoint]:
        """List all checkpoints from MinIO."""
        records = await self.repo.find_all(status=status)
        return [BackfillCheckpoint.from_record(r) for r in records]

    async def delete_checkpoint(
        self, symbol: str, timeframe: str, data_type: str, source: str
    ) -> bool:
        """Delete checkpoint from MinIO."""
        deleted = await self.repo.delete(symbol, timeframe, data_type, source)
        if deleted:
            logger.info(
                f"Deleted checkpoint: {symbol}/{timeframe}/{data_type}/{source}"
            )
        return deleted

    async def mark_completed(
        self, symbol: str, timeframe: str, data_type: str, source: str
    ) -> None:
        """Mark backfill as completed."""
        checkpoint = await self.load_checkpoint(symbol, timeframe, data_type, source)
        if checkpoint:
            checkpoint.status = "completed"
            checkpoint.updated_at = datetime.utcnow()
            await self.save_checkpoint(checkpoint)
            logger.info(f"Marked completed: {symbol}/{timeframe}/{data_type}/{source}")


class CheckpointManager:
    """
    High-level checkpoint management with automatic progress tracking.
    Now uses MinIO storage instead of PostgreSQL.
    """

    def __init__(self, checkpoint_store: ICheckpointStore | None = None):
        """
        Initialize manager.

        Args:
            checkpoint_store: Checkpoint storage backend (defaults to MinIOCheckpointStore)
        """
        self.store = checkpoint_store or MinIOCheckpointStore()

    async def start_backfill(
        self, symbol: str, timeframe: str, data_type: str, source: str
    ) -> BackfillCheckpoint:
        """
        Start new backfill or resume existing one.

        Returns:
            Existing or new checkpoint
        """
        # Check for existing checkpoint
        existing = await self.store.load_checkpoint(
            symbol, timeframe, data_type, source
        )

        if existing and existing.status == "in_progress":
            logger.info(
                f"Resuming backfill from checkpoint: {symbol}/{timeframe}/{data_type} "
                f"- Last completed: {existing.last_completed_date}"
            )
            return existing

        # Create new checkpoint
        now = datetime.utcnow()
        checkpoint = BackfillCheckpoint(
            symbol=symbol,
            timeframe=timeframe,
            data_type=data_type,
            source=source,
            last_completed_date=datetime.min,  # Will be updated on first chunk
            total_chunks_completed=0,
            total_records_written=0,
            started_at=now,
            updated_at=now,
            status="in_progress",
        )

        await self.store.save_checkpoint(checkpoint)
        logger.info(f"Started new backfill: {symbol}/{timeframe}/{data_type}")

        return checkpoint

    async def update_progress(
        self,
        checkpoint: BackfillCheckpoint,
        last_completed_date: datetime,
        records_written: int,
    ) -> None:
        """
        Update checkpoint with progress.

        Args:
            checkpoint: Current checkpoint
            last_completed_date: Latest date completed
            records_written: Number of records written in this update
        """
        checkpoint.last_completed_date = last_completed_date
        checkpoint.total_chunks_completed += 1
        checkpoint.total_records_written += records_written
        checkpoint.updated_at = datetime.utcnow()

        await self.store.save_checkpoint(checkpoint)

        logger.debug(
            f"Updated progress: {checkpoint.symbol}/{checkpoint.timeframe} "
            f"- {checkpoint.total_chunks_completed} chunks, "
            f"{checkpoint.total_records_written} records"
        )

    async def complete_backfill(self, checkpoint: BackfillCheckpoint) -> None:
        """
        Mark backfill as completed.

        Args:
            checkpoint: Checkpoint to complete
        """
        checkpoint.status = "completed"
        checkpoint.updated_at = datetime.utcnow()
        await self.store.save_checkpoint(checkpoint)

        logger.info(
            f"✅ Backfill completed: {checkpoint.symbol}/{checkpoint.timeframe}/{checkpoint.data_type} "
            f"- {checkpoint.total_chunks_completed} chunks, "
            f"{checkpoint.total_records_written} total records"
        )

    async def fail_backfill(
        self, checkpoint: BackfillCheckpoint, error_message: str
    ) -> None:
        """
        Mark backfill as failed.

        Args:
            checkpoint: Checkpoint to fail
            error_message: Error description
        """
        checkpoint.status = "failed"
        checkpoint.error_message = error_message
        checkpoint.updated_at = datetime.utcnow()
        await self.store.save_checkpoint(checkpoint)

        logger.error(
            f"❌ Backfill failed: {checkpoint.symbol}/{checkpoint.timeframe}/{checkpoint.data_type} "
            f"- {error_message}"
        )

    async def get_progress_summary(self) -> dict[str, any]:
        """
        Get overall progress summary.

        Returns:
            Dictionary with progress statistics
        """
        all_checkpoints = await self.store.list_checkpoints()

        in_progress = [c for c in all_checkpoints if c.status == "in_progress"]
        completed = [c for c in all_checkpoints if c.status == "completed"]
        failed = [c for c in all_checkpoints if c.status == "failed"]

        return {
            "total": len(all_checkpoints),
            "in_progress": len(in_progress),
            "completed": len(completed),
            "failed": len(failed),
            "total_records_written": sum(
                c.total_records_written for c in all_checkpoints
            ),
            "checkpoints": {
                "in_progress": [
                    f"{c.symbol}/{c.timeframe}/{c.data_type}" for c in in_progress
                ],
                "completed": [
                    f"{c.symbol}/{c.timeframe}/{c.data_type}" for c in completed
                ],
                "failed": [f"{c.symbol}/{c.timeframe}/{c.data_type}" for c in failed],
            },
        }
