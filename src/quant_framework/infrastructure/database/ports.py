"""
Database adapter interfaces and implementations.
Provides abstraction over database operations for dependency injection.
"""

from datetime import datetime
from typing import Any, Protocol

from pydantic import BaseModel


class IDatabaseAdapter(Protocol):
    """
    Protocol defining database operations interface.
    Enables dependency injection and testing with different implementations.
    """

    async def connect(self) -> None:
        """Establish database connection."""
        ...

    async def disconnect(self) -> None:
        """Close database connection."""
        ...

    async def execute_query(
        self, query: str, *args: Any, fetch_one: bool = False, fetch_all: bool = False
    ) -> Any | None:
        """
        Execute arbitrary SQL query.

        Args:
            query: SQL query string
            *args: Query parameters
            fetch_one: Return single row
            fetch_all: Return all rows

        Returns:
            Query result if fetch_one/fetch_all, else None
        """
        ...

    async def fetch_one(self, query: str, *args: Any) -> dict[str, Any] | None:
        """Fetch single row as dictionary."""
        ...

    async def fetch_all(self, query: str, *args: Any) -> list[dict[str, Any]]:
        """Fetch all rows as list of dictionaries."""
        ...

    async def upsert_batch(
        self,
        records: list[BaseModel],
        table: str,
        conflict_keys: list[str],
        data_type: str,
        mode: str = "consumer",
    ) -> int:
        """
        Batch upsert Pydantic models.

        Args:
            records: List of Pydantic model instances
            table: Target table name
            conflict_keys: Columns for ON CONFLICT
            data_type: Type of data for logging
            mode: Operation mode ('consumer', 'backfill')

        Returns:
            Number of rows affected
        """
        ...

    async def update_metadata(
        self,
        data_type: str,
        symbol: str,
        interval: str,
        earliest_ts: datetime | None = None,
        latest_ts: datetime | None = None,
        mode: str = "extraction",
    ) -> bool:
        """
        Update metadata tracking table.

        Args:
            data_type: Type of data ('ohlc', 'open_interest')
            symbol: Trading symbol
            interval: Timeframe
            earliest_ts: Earliest timestamp in batch
            latest_ts: Latest timestamp in batch
            mode: 'extraction', 'backfill', or 'ingestion'

        Returns:
            True if successful
        """
        ...


class DatabaseAdapter:
    """
    Concrete implementation wrapping mnemo_quant.db.Database.
    Adapts existing Database class to IDatabaseAdapter protocol.
    """

    def __init__(self, database: "Database"):
        """
        Initialize adapter.

        Args:
            database: Instance of mnemo_quant.db.Database
        """
        self._db = database

    async def connect(self) -> None:
        """Establish database connection."""
        await self._db.connect()

    async def disconnect(self) -> None:
        """Close database connection."""
        await self._db.disconnect()

    async def execute_query(
        self, query: str, *args: Any, fetch_one: bool = False, fetch_all: bool = False
    ) -> Any | None:
        """Execute arbitrary SQL query."""
        if not self._db.pool:
            raise RuntimeError("Database not connected")

        async with self._db.pool.acquire() as conn:
            if fetch_one:
                result = await conn.fetchrow(query, *args)
                return dict(result) if result else None
            elif fetch_all:
                results = await conn.fetch(query, *args)
                return [dict(row) for row in results]
            else:
                await conn.execute(query, *args)
                return None

    async def fetch_one(self, query: str, *args: Any) -> dict[str, Any] | None:
        """Fetch single row as dictionary."""
        return await self.execute_query(query, *args, fetch_one=True)

    async def fetch_all(self, query: str, *args: Any) -> list[dict[str, Any]]:
        """Fetch all rows as list of dictionaries."""
        result = await self.execute_query(query, *args, fetch_all=True)
        return result if result else []

    async def upsert_batch(
        self,
        records: list[BaseModel],
        table: str,
        conflict_keys: list[str],
        data_type: str,
        mode: str = "consumer",
    ) -> int:
        """Batch upsert Pydantic models."""
        return await self._db.upsert_from_model_batch(
            records=records,
            table=table,
            conflict_keys=conflict_keys,
            data_type=data_type,
            mode=mode,
        )

    async def update_metadata(
        self,
        data_type: str,
        symbol: str,
        interval: str,
        earliest_ts: datetime | None = None,
        latest_ts: datetime | None = None,
        mode: str = "extraction",
    ) -> bool:
        """Update metadata tracking table."""
        return await self._db.update_metadata(
            data_type=data_type,
            symbol=symbol,
            interval=interval,
            earliest_ts=earliest_ts,
            latest_ts=latest_ts,
            mode=mode,
        )

    @property
    def pool(self):
        """Access underlying connection pool."""
        return self._db.pool
