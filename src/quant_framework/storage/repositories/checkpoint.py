"""Unified checkpoint repository using MinIO storage.

Handles both streaming and backfill checkpoint tracking.
Checkpoints stored in: s3://bronze/_checkpoints/{source}/{data_type}/{symbol}_{timeframe}.json

Replaces PostgreSQL-based checkpoint tracking with simpler MinIO-backed approach.
"""

import logging
from dataclasses import dataclass
from datetime import datetime
from typing import Any

from quant_framework.infrastructure.checkpoint.store import (
    CheckpointDocument,
    CheckpointStore,
)

logger = logging.getLogger(__name__)


@dataclass
class BackfillCheckpointRecord:
    """Unified checkpoint for both streaming and backfill.

    Stored in MinIO as JSON, no database dependency required.
    """

    symbol: str
    timeframe: str
    data_type: str  # "ohlcv" or "open_interest"
    source: str  # "coinalyze", "ccxt", "coinmetrics"

    # Progress tracking
    last_completed_timestamp: int  # Unix timestamp in milliseconds
    total_chunks_completed: int
    total_records_written: int

    # Metadata
    started_at: datetime
    updated_at: datetime
    status: str  # "in_progress", "completed", "failed"
    error_message: str | None = None

    # Additional context
    metadata: dict[str, Any] | None = None

    # Legacy compatibility - computed from timestamp
    @property
    def last_completed_date(self) -> datetime:
        """Convert timestamp to datetime for backward compatibility."""
        if self.last_completed_timestamp <= 0:
            return datetime.min
        return datetime.utcfromtimestamp(self.last_completed_timestamp / 1000)

    def to_checkpoint_document(self) -> CheckpointDocument:
        """Convert to MinIO checkpoint document format."""
        return CheckpointDocument(
            source=self.source,
            exchange=self.source,  # For compatibility
            symbol=self.symbol,
            timeframe=self.timeframe,
            market_type=self.data_type,
            last_successful_timestamp=self.last_completed_timestamp,
            last_updated=self.updated_at.isoformat() + "Z",
            total_records=self.total_records_written,
            metadata={
                "total_chunks_completed": self.total_chunks_completed,
                "started_at": self.started_at.isoformat() + "Z",
                "status": self.status,
                "error_message": self.error_message,
                **(self.metadata or {}),
            },
        )

    @classmethod
    def from_checkpoint_document(
        cls, doc: CheckpointDocument
    ) -> "BackfillCheckpointRecord":
        """Create from MinIO checkpoint document."""
        meta = doc.metadata or {}
        return cls(
            symbol=doc.symbol,
            timeframe=doc.timeframe,
            data_type=doc.market_type or "ohlcv",
            source=doc.source,
            last_completed_timestamp=doc.last_successful_timestamp,
            total_chunks_completed=meta.get("total_chunks_completed", 0),
            total_records_written=doc.total_records,
            started_at=datetime.fromisoformat(
                meta.get("started_at", doc.last_updated).rstrip("Z")
            ),
            updated_at=datetime.fromisoformat(doc.last_updated.rstrip("Z")),
            status=meta.get("status", "in_progress"),
            error_message=meta.get("error_message"),
            metadata={
                k: v
                for k, v in meta.items()
                if k
                not in {
                    "total_chunks_completed",
                    "started_at",
                    "status",
                    "error_message",
                }
            },
        )


class CheckpointRepository:
    """MinIO-backed checkpoint repository for both streaming and backfill.

    Eliminates PostgreSQL dependency by storing checkpoints as JSON in MinIO.
    """

    def __init__(self, store: CheckpointStore | None = None):
        """Initialize checkpoint repository.

        Args:
            store: CheckpointStore instance (creates default if None)
        """
        self.store = store or CheckpointStore()
        logger.info("CheckpointRepository initialized (MinIO-backed)")

    def _build_key(
        self, symbol: str, timeframe: str, data_type: str, source: str
    ) -> str:
        """Build MinIO key for checkpoint."""
        return f"_checkpoints/{source}/{data_type}/{symbol}_{timeframe}.json"

    async def create(
        self, checkpoint: BackfillCheckpointRecord
    ) -> BackfillCheckpointRecord:
        """Create new checkpoint in MinIO.

        Args:
            checkpoint: Checkpoint data to persist

        Returns:
            Same checkpoint (MinIO doesn't generate IDs)

        Raises:
            ValueError: If checkpoint already exists for this combination
        """
        key = self._build_key(
            checkpoint.symbol,
            checkpoint.timeframe,
            checkpoint.data_type,
            checkpoint.source,
        )

        # Check if already exists
        if self.store.exists(key):
            logger.warning(f"Checkpoint already exists: {key}")
            raise ValueError(
                f"Checkpoint already exists for {checkpoint.symbol}/{checkpoint.timeframe}"
            )

        doc = checkpoint.to_checkpoint_document()
        self.store.write_atomic(key, doc)

        logger.debug(f"✅ Created checkpoint: {key}")
        return checkpoint

    async def update(self, checkpoint: BackfillCheckpointRecord) -> None:
        """Update existing checkpoint in MinIO.

        Args:
            checkpoint: Checkpoint data to update
        """
        key = self._build_key(
            checkpoint.symbol,
            checkpoint.timeframe,
            checkpoint.data_type,
            checkpoint.source,
        )

        checkpoint.updated_at = datetime.utcnow()
        doc = checkpoint.to_checkpoint_document()

        # Read current for optimistic locking (optional - MinIO doesn't enforce ETag)
        _, etag = self.store.read(key)
        self.store.write_atomic(key, doc, if_match=etag)

        logger.debug(f"✅ Updated checkpoint: {key}")

    async def find_by_symbol_source(
        self,
        symbol: str,
        timeframe: str,
        data_type: str,
        source: str,
    ) -> BackfillCheckpointRecord | None:
        """Get checkpoint from MinIO.

        Args:
            symbol: Trading symbol
            timeframe: Data timeframe
            data_type: Type of data
            source: Data source name

        Returns:
            Checkpoint record if exists, None otherwise
        """
        key = self._build_key(symbol, timeframe, data_type, source)
        doc, _ = self.store.read(key)

        if not doc:
            logger.debug(f"No checkpoint found: {key}")
            return None

        return BackfillCheckpointRecord.from_checkpoint_document(doc)

    async def find_all(
        self, status: str | None = None
    ) -> list[BackfillCheckpointRecord]:
        """Get all checkpoints from MinIO, optionally filtered by status.

        Args:
            status: Filter by status (in_progress, completed, failed) or None for all

        Returns:
            List of checkpoint records
        """
        all_keys = self.store.list("_checkpoints/")
        checkpoints = []

        for key in all_keys:
            doc, _ = self.store.read(key)
            if not doc:
                continue

            checkpoint = BackfillCheckpointRecord.from_checkpoint_document(doc)

            # Apply status filter if provided
            if status is None or checkpoint.status == status:
                checkpoints.append(checkpoint)

        logger.debug(f"Found {len(checkpoints)} checkpoints (status={status})")
        return checkpoints

    async def delete(
        self, symbol: str, timeframe: str, data_type: str, source: str
    ) -> bool:
        """Delete checkpoint from MinIO.

        Args:
            symbol: Trading symbol
            timeframe: Data timeframe
            data_type: Type of data
            source: Data source name

        Returns:
            True if deleted, False if not found
        """
        key = self._build_key(symbol, timeframe, data_type, source)

        if not self.store.exists(key):
            return False

        # Delete object from S3/MinIO
        try:
            self.store.s3.delete_object(Bucket=self.store.bucket, Key=key)
            logger.debug(f"Deleted checkpoint: {key}")
            return True
        except Exception as e:
            logger.error(f"Failed to delete checkpoint {key}: {e}")
            raise

    async def upsert(self, checkpoint: BackfillCheckpointRecord) -> None:
        """Create or update checkpoint (convenience method).

        Args:
            checkpoint: Checkpoint data to save
        """
        key = self._build_key(
            checkpoint.symbol,
            checkpoint.timeframe,
            checkpoint.data_type,
            checkpoint.source,
        )

        checkpoint.updated_at = datetime.utcnow()
        doc = checkpoint.to_checkpoint_document()
        self.store.write_atomic(key, doc)

        logger.debug(f"✅ Upserted checkpoint: {key}")
