"""Checkpoint repository for backfill progress tracking.

Stores and retrieves backfill checkpoint records from PostgreSQL.
Enables resumable backfills and progress monitoring across restarts.

Note: This is separate from MinIO-backed checkpoints in infrastructure.checkpoint
which are used for incremental streaming pipelines.

Checkpoint Table Schema:
  system.backfill_checkpoints:
    - id: SERIAL PRIMARY KEY
    - symbol: VARCHAR NOT NULL
    - timeframe: VARCHAR NOT NULL
    - data_type: VARCHAR NOT NULL (ohlcv, open_interest)
    - source: VARCHAR NOT NULL (coinalyze, ccxt, coinmetrics)
    - last_completed_date: TIMESTAMP NOT NULL
    - total_chunks_completed: INTEGER DEFAULT 0
    - total_records_written: BIGINT DEFAULT 0
    - started_at: TIMESTAMP NOT NULL
    - updated_at: TIMESTAMP NOT NULL
    - status: VARCHAR DEFAULT 'in_progress' (in_progress, completed, failed)
    - error_message: TEXT
    - UNIQUE (symbol, timeframe, data_type, source)
"""

import logging
from dataclasses import dataclass
from datetime import datetime

from quant_framework.infrastructure.database.ports import DatabaseAdapter

logger = logging.getLogger(__name__)


@dataclass
class BackfillCheckpointRecord:
    """Checkpoint record for backfill orchestration.

    Represents progress state for a single backfill job.
    Enables resumption from last successful chunk on restarts.
    """

    symbol: str
    timeframe: str
    data_type: str  # "ohlcv" or "open_interest"
    source: str  # "coinalyze", "ccxt", "coinmetrics"
    last_completed_date: datetime
    total_chunks_completed: int
    total_records_written: int
    started_at: datetime
    updated_at: datetime
    status: str  # "in_progress", "completed", "failed"
    error_message: str | None = None
    id: int | None = None  # PK set by database


class CheckpointRepository:
    """Repository for backfill checkpoint persistence.

    Provides CRUD operations for checkpoint tracking in PostgreSQL.
    Used by BackfillCoordinator to enable resumable backfills.
    """

    def __init__(self, db: DatabaseAdapter):
        """Initialize checkpoint repository.

        Args:
            db: DatabaseAdapter instance for SQL execution
        """
        self.db = db
        logger.info("CheckpointRepository initialized")

    async def create(
        self, checkpoint: BackfillCheckpointRecord
    ) -> BackfillCheckpointRecord:
        """Create new checkpoint record.

        Args:
            checkpoint: Checkpoint data to persist

        Returns:
            Checkpoint with id populated from database

        Raises:
            UniqueConstraintError: If checkpoint already exists for this combination
        """
        query = """
            INSERT INTO system.backfill_checkpoints
            (symbol, timeframe, data_type, source, last_completed_date,
             total_chunks_completed, total_records_written, started_at, updated_at,
             status, error_message)
            VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11)
            RETURNING id
        """

        try:
            result = await self.db.fetchval(
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

            checkpoint.id = result
            logger.debug(
                f"✅ Created checkpoint: {checkpoint.symbol}/{checkpoint.timeframe} "
                f"({checkpoint.source})"
            )
            return checkpoint

        except Exception as e:
            logger.error(f"❌ Failed to create checkpoint: {e}")
            raise

    async def update(self, checkpoint: BackfillCheckpointRecord) -> None:
        """Update existing checkpoint record.

        Args:
            checkpoint: Checkpoint data to update (must have id)

        Raises:
            ValueError: If checkpoint has no id
            NotFoundError: If checkpoint doesn't exist
        """
        if not checkpoint.id:
            raise ValueError("Cannot update checkpoint without id")

        query = """
            UPDATE system.backfill_checkpoints
            SET last_completed_date = $1,
                total_chunks_completed = $2,
                total_records_written = $3,
                updated_at = $4,
                status = $5,
                error_message = $6
            WHERE id = $7
        """

        try:
            await self.db.execute(
                query,
                checkpoint.last_completed_date,
                checkpoint.total_chunks_completed,
                checkpoint.total_records_written,
                checkpoint.updated_at,
                checkpoint.status,
                checkpoint.error_message,
                checkpoint.id,
            )
            logger.debug(f"✅ Updated checkpoint id={checkpoint.id}")
        except Exception as e:
            logger.error(f"❌ Failed to update checkpoint id={checkpoint.id}: {e}")
            raise

    async def find_by_symbol_source(
        self,
        symbol: str,
        timeframe: str,
        data_type: str,
        source: str,
    ) -> BackfillCheckpointRecord | None:
        """Get checkpoint for specific symbol/timeframe/source combination.

        Args:
            symbol: Trading symbol
            timeframe: Data timeframe
            data_type: Type of data
            source: Data source name

        Returns:
            Checkpoint record if exists, None otherwise
        """
        query = """
            SELECT id, symbol, timeframe, data_type, source,
                   last_completed_date, total_chunks_completed,
                   total_records_written, started_at, updated_at,
                   status, error_message
            FROM system.backfill_checkpoints
            WHERE symbol = $1 AND timeframe = $2
              AND data_type = $3 AND source = $4
            LIMIT 1
        """

        try:
            row = await self.db.fetchrow(query, symbol, timeframe, data_type, source)

            if not row:
                logger.debug(f"No checkpoint found for {symbol}/{timeframe}/{source}")
                return None

            return BackfillCheckpointRecord(
                id=row["id"],
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
                error_message=row["error_message"],
            )
        except Exception as e:
            logger.error(f"❌ Failed to find checkpoint: {e}")
            raise

    async def find_all(
        self, status: str | None = None
    ) -> list[BackfillCheckpointRecord]:
        """Get all checkpoints, optionally filtered by status.

        Args:
            status: Filter by status (in_progress, completed, failed) or None for all

        Returns:
            List of checkpoint records
        """
        if status:
            query = """
                SELECT id, symbol, timeframe, data_type, source,
                       last_completed_date, total_chunks_completed,
                       total_records_written, started_at, updated_at,
                       status, error_message
                FROM system.backfill_checkpoints
                WHERE status = $1
                ORDER BY updated_at DESC
            """
            rows = await self.db.fetch(query, status)
        else:
            query = """
                SELECT id, symbol, timeframe, data_type, source,
                       last_completed_date, total_chunks_completed,
                       total_records_written, started_at, updated_at,
                       status, error_message
                FROM system.backfill_checkpoints
                ORDER BY updated_at DESC
            """
            rows = await self.db.fetch(query)

        checkpoints = []
        for row in rows:
            checkpoints.append(
                BackfillCheckpointRecord(
                    id=row["id"],
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
                    error_message=row["error_message"],
                )
            )

        logger.debug(f"Found {len(checkpoints)} checkpoints (status={status})")
        return checkpoints

    async def delete(self, checkpoint_id: int) -> bool:
        """Delete checkpoint by id.

        Args:
            checkpoint_id: Checkpoint id to delete

        Returns:
            True if deleted, False if not found
        """
        query = "DELETE FROM system.backfill_checkpoints WHERE id = $1"

        try:
            result = await self.db.execute(query, checkpoint_id)
            logger.debug(f"Deleted checkpoint id={checkpoint_id}")
            return result == "DELETE 1"
        except Exception as e:
            logger.error(f"❌ Failed to delete checkpoint id={checkpoint_id}: {e}")
            raise
