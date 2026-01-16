"""Open Interest repository for perpetual/futures OI data persistence.

Provides CRUD operations for Open Interest data in PostgreSQL/TimescaleDB.
Handles both native OI (perpetual futures) and synthetic OI (e.g., staking).

Table Schema (with timeframe-specific clones):
  market.open_interest (hypertable):
    - time: TIMESTAMP NOT NULL (PRIMARY)
    - symbol: VARCHAR NOT NULL
    - exchange: VARCHAR NOT NULL
    - timeframe: VARCHAR NOT NULL
    - open_interest_usd: NUMERIC(20,2)
    - settlement_currency: VARCHAR (USDT, USDC, etc.)
    - long_oi_usd: NUMERIC(20,2)
    - short_oi_usd: NUMERIC(20,2)
    - long_short_ratio: NUMERIC(10,4)
    - PRIMARY KEY (symbol, exchange, timeframe, time DESC)

Hypertable clones (for common timeframes):
  - market.open_interest_5m
  - market.open_interest_1h
  - market.open_interest_4h
  - market.open_interest_1d
"""

import logging
from typing import Any

from quant_framework.infrastructure.database.ports import DatabaseAdapter
from quant_framework.storage.schemas.time_series import OpenInterestRecord

logger = logging.getLogger(__name__)


class OpenInterestRepository:
    """Repository for Open Interest data.

    Handles persistence of OI data to TimescaleDB hypertables.
    Supports both perpetual futures and synthetic OI tracking.
    """

    def __init__(self, db: DatabaseAdapter):
        """Initialize Open Interest repository.

        Args:
            db: DatabaseAdapter instance for SQL execution
        """
        self.db = db
        logger.info("OpenInterestRepository initialized")

    async def save(self, record: OpenInterestRecord) -> OpenInterestRecord:
        """Save single Open Interest record (upsert).

        Uses ON CONFLICT to handle duplicate timestamps gracefully.
        Idempotent - safe to retry without creating duplicates.

        Args:
            record: OpenInterestRecord to save

        Returns:
            The saved record
        """
        query = """
            INSERT INTO market.open_interest
            (time, symbol, exchange, timeframe, open_interest_usd,
             settlement_currency, long_oi_usd, short_oi_usd, long_short_ratio)
            VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
            ON CONFLICT (symbol, exchange, timeframe, time)
            DO UPDATE SET
                open_interest_usd = EXCLUDED.open_interest_usd,
                settlement_currency = EXCLUDED.settlement_currency,
                long_oi_usd = EXCLUDED.long_oi_usd,
                short_oi_usd = EXCLUDED.short_oi_usd,
                long_short_ratio = EXCLUDED.long_short_ratio
        """

        try:
            await self.db.execute(
                query,
                record.timestamp,
                record.symbol,
                record.exchange,
                record.timeframe,
                str(record.open_interest_usd),
                record.settlement_currency,
                str(record.long_oi_usd) if record.long_oi_usd else None,
                str(record.short_oi_usd) if record.short_oi_usd else None,
                str(record.long_short_ratio) if record.long_short_ratio else None,
            )
            logger.debug(
                f"✅ Saved OI: {record.symbol}/{record.timeframe} @ {record.timestamp}"
            )
            return record
        except Exception as e:
            logger.error(f"❌ Failed to save OI: {e}")
            raise

    async def save_batch(
        self,
        records: list[OpenInterestRecord],
        on_conflict: str = "update",
    ) -> int:
        """Save multiple Open Interest records in batch.

        Uses parameterized multi-row insert for efficiency.
        Significantly faster than individual inserts for backfill (1000+ records/call).

        Args:
            records: List of OpenInterestRecord to save
            on_conflict: How to handle conflicts:
                - "update": Update existing records (default)
                - "ignore": Skip duplicates silently

        Returns:
            Number of records inserted/updated
        """
        if not records:
            return 0

        # Build multi-row VALUES clause
        placeholders = []
        values_flat = []

        for idx, record in enumerate(records):
            base_idx = idx * 9  # 9 parameters per record
            placeholders.append(
                f"(${base_idx+1}, ${base_idx+2}, ${base_idx+3}, ${base_idx+4}, "
                f"${base_idx+5}, ${base_idx+6}, ${base_idx+7}, ${base_idx+8}, "
                f"${base_idx+9})"
            )

            values_flat.extend(
                [
                    record.timestamp,
                    record.symbol,
                    record.exchange,
                    record.timeframe,
                    str(record.open_interest_usd),
                    record.settlement_currency,
                    str(record.long_oi_usd) if record.long_oi_usd else None,
                    str(record.short_oi_usd) if record.short_oi_usd else None,
                    str(record.long_short_ratio) if record.long_short_ratio else None,
                ]
            )

        conflict_clause = (
            """ON CONFLICT (symbol, exchange, timeframe, time)
            DO UPDATE SET
                open_interest_usd = EXCLUDED.open_interest_usd,
                settlement_currency = EXCLUDED.settlement_currency,
                long_oi_usd = EXCLUDED.long_oi_usd,
                short_oi_usd = EXCLUDED.short_oi_usd,
                long_short_ratio = EXCLUDED.long_short_ratio"""
            if on_conflict == "update"
            else "ON CONFLICT DO NOTHING"
        )

        query = f"""
            INSERT INTO market.open_interest
            (time, symbol, exchange, timeframe, open_interest_usd,
             settlement_currency, long_oi_usd, short_oi_usd, long_short_ratio)
            VALUES {','.join(placeholders)}
            {conflict_clause}
        """

        try:
            result = await self.db.execute(query, *values_flat)
            logger.info(
                f"✅ Batch inserted {len(records)} OI records "
                f"({records[0].symbol}/{records[0].timeframe})"
            )
            return len(records)
        except Exception as e:
            logger.error(f"❌ Batch insert failed: {e}")
            raise

    async def find_latest(
        self,
        symbol: str,
        exchange: str,
        timeframe: str,
        limit: int = 100,
    ) -> list[OpenInterestRecord]:
        """Get most recent Open Interest records for a symbol/timeframe.

        Args:
            symbol: Trading symbol
            exchange: Exchange name
            timeframe: Timeframe (5m, 1h, etc)
            limit: Number of records to return (max 10000)

        Returns:
            List of OpenInterestRecord sorted by time DESC (newest first)
        """
        query = """
            SELECT time, symbol, exchange, timeframe, open_interest_usd,
                   settlement_currency, long_oi_usd, short_oi_usd, long_short_ratio
            FROM market.open_interest
            WHERE symbol = $1 AND exchange = $2 AND timeframe = $3
            ORDER BY time DESC
            LIMIT $4
        """

        try:
            rows = await self.db.fetch(query, symbol, exchange, timeframe, limit)

            records = [
                OpenInterestRecord(
                    timestamp=row["time"],
                    symbol=row["symbol"],
                    exchange=row["exchange"],
                    timeframe=row["timeframe"],
                    open_interest_usd=row["open_interest_usd"],
                    settlement_currency=row["settlement_currency"],
                    long_oi_usd=row["long_oi_usd"],
                    short_oi_usd=row["short_oi_usd"],
                    long_short_ratio=row["long_short_ratio"],
                )
                for row in rows
            ]

            logger.debug(f"Found {len(records)} OI records for {symbol}/{timeframe}")
            return records
        except Exception as e:
            logger.error(f"❌ Failed to find OI records: {e}")
            raise

    async def find_range(
        self,
        symbol: str,
        exchange: str,
        timeframe: str,
        start_time: Any,
        end_time: Any,
    ) -> list[OpenInterestRecord]:
        """Get Open Interest records within time range.

        Args:
            symbol: Trading symbol
            exchange: Exchange name
            timeframe: Timeframe
            start_time: Start timestamp (inclusive)
            end_time: End timestamp (inclusive)

        Returns:
            List of OpenInterestRecord in time order (ASC)
        """
        query = """
            SELECT time, symbol, exchange, timeframe, open_interest_usd,
                   settlement_currency, long_oi_usd, short_oi_usd, long_short_ratio
            FROM market.open_interest
            WHERE symbol = $1 AND exchange = $2 AND timeframe = $3
              AND time >= $4 AND time <= $5
            ORDER BY time ASC
        """

        try:
            rows = await self.db.fetch(
                query, symbol, exchange, timeframe, start_time, end_time
            )

            records = [
                OpenInterestRecord(
                    timestamp=row["time"],
                    symbol=row["symbol"],
                    exchange=row["exchange"],
                    timeframe=row["timeframe"],
                    open_interest_usd=row["open_interest_usd"],
                    settlement_currency=row["settlement_currency"],
                    long_oi_usd=row["long_oi_usd"],
                    short_oi_usd=row["short_oi_usd"],
                    long_short_ratio=row["long_short_ratio"],
                )
                for row in rows
            ]

            logger.debug(f"Found {len(records)} OI records in range")
            return records
        except Exception as e:
            logger.error(f"❌ Failed to find OI range: {e}")
            raise

    async def count(self, symbol: str, exchange: str, timeframe: str) -> int:
        """Get total record count for symbol/timeframe.

        Args:
            symbol: Trading symbol
            exchange: Exchange name
            timeframe: Timeframe

        Returns:
            Total record count
        """
        query = """
            SELECT COUNT(*) as cnt
            FROM market.open_interest
            WHERE symbol = $1 AND exchange = $2 AND timeframe = $3
        """

        try:
            result = await self.db.fetchval(query, symbol, exchange, timeframe)
            logger.debug(f"OI count for {symbol}/{timeframe}: {result}")
            return result or 0
        except Exception as e:
            logger.error(f"❌ Failed to count OI: {e}")
            raise
