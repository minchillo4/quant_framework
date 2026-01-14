"""
Checkpoint manager for tracking backfill progress.
Enables resumable backfills and progress monitoring.
"""

import logging
from dataclasses import dataclass
from datetime import datetime
from typing import Protocol

logger = logging.getLogger(__name__)


@dataclass
class BackfillCheckpoint:
    """Represents a backfill progress checkpoint."""

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


class ICheckpointStore(Protocol):
    """
    Protocol for checkpoint storage.
    Enables different backend implementations (database, file, cache).
    """

    async def save_checkpoint(self, checkpoint: BackfillCheckpoint) -> None:
        """
        Save or update checkpoint.

        Args:
            checkpoint: Checkpoint data to save
        """
        ...

    async def load_checkpoint(
        self, symbol: str, timeframe: str, data_type: str, source: str
    ) -> BackfillCheckpoint | None:
        """
        Load checkpoint for specific combination.

        Args:
            symbol: Trading symbol
            timeframe: Data timeframe
            data_type: Type of data ('ohlc', 'oi')
            source: Data source name

        Returns:
            Checkpoint if exists, None otherwise
        """
        ...

    async def list_checkpoints(
        self, status: str | None = None
    ) -> list[BackfillCheckpoint]:
        """
        List all checkpoints, optionally filtered by status.

        Args:
            status: Filter by status ('in_progress', 'completed', 'failed')

        Returns:
            List of checkpoints
        """
        ...

    async def delete_checkpoint(
        self, symbol: str, timeframe: str, data_type: str, source: str
    ) -> bool:
        """
        Delete checkpoint.

        Args:
            symbol: Trading symbol
            timeframe: Data timeframe
            data_type: Type of data
            source: Data source name

        Returns:
            True if deleted, False if not found
        """
        ...

    async def mark_completed(
        self, symbol: str, timeframe: str, data_type: str, source: str
    ) -> None:
        """
        Mark backfill as completed.

        Args:
            symbol: Trading symbol
            timeframe: Data timeframe
            data_type: Type of data
            source: Data source name
        """
        ...


class DatabaseCheckpointStore:
    """
    Database-backed checkpoint storage.
    Uses system.backfill_checkpoints table.
    """

    def __init__(self, db_adapter: "IDatabaseAdapter"):
        """
        Initialize checkpoint store.

        Args:
            db_adapter: Database adapter instance
        """
        self.db = db_adapter
        self.table = "system.backfill_checkpoints"

    async def save_checkpoint(self, checkpoint: BackfillCheckpoint) -> None:
        """Save or update checkpoint in database."""
        query = f"""
            INSERT INTO {self.table} (
                symbol, timeframe, data_type, source,
                last_completed_date, total_chunks_completed, total_records_written,
                started_at, updated_at, status, error_message
            )
            VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11)
            ON CONFLICT (symbol, timeframe, data_type, source)
            DO UPDATE SET
                last_completed_date = EXCLUDED.last_completed_date,
                total_chunks_completed = EXCLUDED.total_chunks_completed,
                total_records_written = EXCLUDED.total_records_written,
                updated_at = EXCLUDED.updated_at,
                status = EXCLUDED.status,
                error_message = EXCLUDED.error_message
        """

        await self.db.execute_query(
            query,
            checkpoint.symbol,
            checkpoint.timeframe,
            checkpoint.data_type,
            checkpoint.source,
            checkpoint.last_completed_date,
            checkpoint.total_chunks_completed,
            checkpoint.total_records_written,
            checkpoint.started_at,
            checkpoint.updated_at,
            checkpoint.status,
            checkpoint.error_message,
        )

        logger.debug(
            f"Saved checkpoint: {checkpoint.symbol}/{checkpoint.timeframe}/{checkpoint.data_type} "
            f"- {checkpoint.total_chunks_completed} chunks, {checkpoint.total_records_written} records"
        )

    async def load_checkpoint(
        self, symbol: str, timeframe: str, data_type: str, source: str
    ) -> BackfillCheckpoint | None:
        """Load checkpoint from database."""
        query = f"""
            SELECT 
                symbol, timeframe, data_type, source,
                last_completed_date, total_chunks_completed, total_records_written,
                started_at, updated_at, status, error_message
            FROM {self.table}
            WHERE symbol = $1 
              AND timeframe = $2 
              AND data_type = $3 
              AND source = $4
        """

        result = await self.db.fetch_one(query, symbol, timeframe, data_type, source)

        if not result:
            return None

        return BackfillCheckpoint(
            symbol=result["symbol"],
            timeframe=result["timeframe"],
            data_type=result["data_type"],
            source=result["source"],
            last_completed_date=result["last_completed_date"],
            total_chunks_completed=result["total_chunks_completed"],
            total_records_written=result["total_records_written"],
            started_at=result["started_at"],
            updated_at=result["updated_at"],
            status=result["status"],
            error_message=result.get("error_message"),
        )

    async def list_checkpoints(
        self, status: str | None = None
    ) -> list[BackfillCheckpoint]:
        """List all checkpoints from database."""
        if status:
            query = f"""
                SELECT 
                    symbol, timeframe, data_type, source,
                    last_completed_date, total_chunks_completed, total_records_written,
                    started_at, updated_at, status, error_message
                FROM {self.table}
                WHERE status = $1
                ORDER BY updated_at DESC
            """
            results = await self.db.fetch_all(query, status)
        else:
            query = f"""
                SELECT 
                    symbol, timeframe, data_type, source,
                    last_completed_date, total_chunks_completed, total_records_written,
                    started_at, updated_at, status, error_message
                FROM {self.table}
                ORDER BY updated_at DESC
            """
            results = await self.db.fetch_all(query)

        return [
            BackfillCheckpoint(
                symbol=row["symbol"],
                timeframe=row["timeframe"],
                data_type=row["data_type"],
                source=row["source"],
                last_completed_date=row["last_completed_date"],
                total_chunks_completed=row["total_chunks_completed"],
                total_records_written=row["total_records_written"],
                started_at=row["started_at"],
                updated_at=row["updated_at"],
                status=row["status"],
                error_message=row.get("error_message"),
            )
            for row in results
        ]

    async def delete_checkpoint(
        self, symbol: str, timeframe: str, data_type: str, source: str
    ) -> bool:
        """Delete checkpoint from database."""
        query = f"""
            DELETE FROM {self.table}
            WHERE symbol = $1 
              AND timeframe = $2 
              AND data_type = $3 
              AND source = $4
        """

        await self.db.execute_query(query, symbol, timeframe, data_type, source)
        logger.info(f"Deleted checkpoint: {symbol}/{timeframe}/{data_type}/{source}")
        return True

    async def mark_completed(
        self, symbol: str, timeframe: str, data_type: str, source: str
    ) -> None:
        """Mark backfill as completed."""
        query = f"""
            UPDATE {self.table}
            SET status = 'completed',
                updated_at = NOW()
            WHERE symbol = $1 
              AND timeframe = $2 
              AND data_type = $3 
              AND source = $4
        """

        await self.db.execute_query(query, symbol, timeframe, data_type, source)
        logger.info(f"Marked completed: {symbol}/{timeframe}/{data_type}/{source}")


class CheckpointManager:
    """
    High-level checkpoint management with automatic progress tracking.
    """

    def __init__(self, checkpoint_store: ICheckpointStore):
        """
        Initialize manager.

        Args:
            checkpoint_store: Checkpoint storage backend
        """
        self.store = checkpoint_store

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
