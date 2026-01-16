"""OHLCV repository for candlestick data persistence.

Provides CRUD operations for OHLCV candlestick data in PostgreSQL/TimescaleDB.
Supports batch upserts for efficient backfill ingestion.

Table Schema (with timeframe-specific clones):
  market.ohlc (hypertable):
    - time: TIMESTAMP NOT NULL (PRIMARY)
    - symbol: VARCHAR NOT NULL
    - exchange: VARCHAR NOT NULL
    - timeframe: VARCHAR NOT NULL
    - open, high, low, close: NUMERIC(20,8)
    - volume: NUMERIC(20,8)
    - quote_volume: NUMERIC(20,2)
    - trades_count: INTEGER
    - PRIMARY KEY (symbol, exchange, timeframe, time DESC)

Hypertable clones (for common timeframes):
  - market.ohlc_5m
  - market.ohlc_1h
  - market.ohlc_4h
  - market.ohlc_1d
"""

import logging
from typing import Any

from quant_framework.infrastructure.database.ports import DatabaseAdapter
from quant_framework.storage.schemas.time_series import OHLCVRecord

logger = logging.getLogger(__name__)


class OHLCVRepository:
    """Repository for OHLCV candlestick data.

    Handles persistence of OHLC data to TimescaleDB hypertables.
    Supports single record save and batch upsert for backfill efficiency.
    """

    def __init__(self, db: DatabaseAdapter):
        """Initialize OHLCV repository.

        Args:
            db: DatabaseAdapter instance for SQL execution
        """
        self.db = db
        logger.info("OHLCVRepository initialized")

    async def save(self, record: OHLCVRecord) -> OHLCVRecord:
        """Save single OHLCV record (upsert).

        Uses ON CONFLICT to handle duplicate timestamps gracefully.
        Idempotent - safe to retry without creating duplicates.

        Args:
            record: OHLCVRecord to save

        Returns:
            The saved record
        """
        query = """
            INSERT INTO market.ohlc
            (time, symbol, exchange, timeframe, open, high, low, close, volume,
             quote_volume, trades_count)
            VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11)
            ON CONFLICT (symbol, exchange, timeframe, time)
            DO UPDATE SET
                open = EXCLUDED.open,
                high = EXCLUDED.high,
                low = EXCLUDED.low,
                close = EXCLUDED.close,
                volume = EXCLUDED.volume,
                quote_volume = EXCLUDED.quote_volume,
                trades_count = EXCLUDED.trades_count
        """

        try:
            await self.db.execute(
                query,
                record.timestamp,
                record.symbol,
                record.exchange,
                record.timeframe,
                str(record.open_price),
                str(record.high_price),
                str(record.low_price),
                str(record.close_price),
                str(record.volume),
                str(record.quote_volume) if record.quote_volume else None,
                record.trades_count,
            )
            logger.debug(
                f"✅ Saved OHLCV: {record.symbol}/{record.timeframe} @ {record.timestamp}"
            )
            return record
        except Exception as e:
            logger.error(f"❌ Failed to save OHLCV: {e}")
            raise

    async def save_batch(
        self,
        records: list[OHLCVRecord],
        on_conflict: str = "update",
    ) -> int:
        """Save multiple OHLCV records in batch.

        Uses parameterized multi-row insert for efficiency.
        Significantly faster than individual inserts for backfill (1000+ records/call).

        Args:
            records: List of OHLCVRecord to save
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
            base_idx = idx * 11  # 11 parameters per record
            placeholders.append(
                f"(${base_idx+1}, ${base_idx+2}, ${base_idx+3}, ${base_idx+4}, "
                f"${base_idx+5}, ${base_idx+6}, ${base_idx+7}, ${base_idx+8}, "
                f"${base_idx+9}, ${base_idx+10}, ${base_idx+11})"
            )

            values_flat.extend(
                [
                    record.timestamp,
                    record.symbol,
                    record.exchange,
                    record.timeframe,
                    str(record.open_price),
                    str(record.high_price),
                    str(record.low_price),
                    str(record.close_price),
                    str(record.volume),
                    str(record.quote_volume) if record.quote_volume else None,
                    record.trades_count,
                ]
            )

        conflict_clause = (
            """ON CONFLICT (symbol, exchange, timeframe, time)
            DO UPDATE SET
                open = EXCLUDED.open,
                high = EXCLUDED.high,
                low = EXCLUDED.low,
                close = EXCLUDED.close,
                volume = EXCLUDED.volume,
                quote_volume = EXCLUDED.quote_volume,
                trades_count = EXCLUDED.trades_count"""
            if on_conflict == "update"
            else "ON CONFLICT DO NOTHING"
        )

        query = f"""
            INSERT INTO market.ohlc
            (time, symbol, exchange, timeframe, open, high, low, close, volume,
             quote_volume, trades_count)
            VALUES {','.join(placeholders)}
            {conflict_clause}
        """

        try:
            result = await self.db.execute(query, *values_flat)
            logger.info(
                f"✅ Batch inserted {len(records)} OHLCV records "
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
    ) -> list[OHLCVRecord]:
        """Get most recent OHLCV records for a symbol/timeframe.

        Args:
            symbol: Trading symbol
            exchange: Exchange name
            timeframe: Timeframe (5m, 1h, etc)
            limit: Number of records to return (max 10000)

        Returns:
            List of OHLCVRecord sorted by time DESC (newest first)
        """
        query = """
            SELECT time, symbol, exchange, timeframe, open, high, low, close,
                   volume, quote_volume, trades_count
            FROM market.ohlc
            WHERE symbol = $1 AND exchange = $2 AND timeframe = $3
            ORDER BY time DESC
            LIMIT $4
        """

        try:
            rows = await self.db.fetch(query, symbol, exchange, timeframe, limit)

            records = [
                OHLCVRecord(
                    timestamp=row["time"],
                    symbol=row["symbol"],
                    exchange=row["exchange"],
                    timeframe=row["timeframe"],
                    open_price=row["open"],
                    high_price=row["high"],
                    low_price=row["low"],
                    close_price=row["close"],
                    volume=row["volume"],
                    quote_volume=row["quote_volume"],
                    trades_count=row["trades_count"],
                )
                for row in rows
            ]

            logger.debug(f"Found {len(records)} OHLCV records for {symbol}/{timeframe}")
            return records
        except Exception as e:
            logger.error(f"❌ Failed to find OHLCV records: {e}")
            raise

    async def find_range(
        self,
        symbol: str,
        exchange: str,
        timeframe: str,
        start_time: Any,
        end_time: Any,
    ) -> list[OHLCVRecord]:
        """Get OHLCV records within time range.

        Args:
            symbol: Trading symbol
            exchange: Exchange name
            timeframe: Timeframe
            start_time: Start timestamp (inclusive)
            end_time: End timestamp (inclusive)

        Returns:
            List of OHLCVRecord in time order (ASC)
        """
        query = """
            SELECT time, symbol, exchange, timeframe, open, high, low, close,
                   volume, quote_volume, trades_count
            FROM market.ohlc
            WHERE symbol = $1 AND exchange = $2 AND timeframe = $3
              AND time >= $4 AND time <= $5
            ORDER BY time ASC
        """

        try:
            rows = await self.db.fetch(
                query, symbol, exchange, timeframe, start_time, end_time
            )

            records = [
                OHLCVRecord(
                    timestamp=row["time"],
                    symbol=row["symbol"],
                    exchange=row["exchange"],
                    timeframe=row["timeframe"],
                    open_price=row["open"],
                    high_price=row["high"],
                    low_price=row["low"],
                    close_price=row["close"],
                    volume=row["volume"],
                    quote_volume=row["quote_volume"],
                    trades_count=row["trades_count"],
                )
                for row in rows
            ]

            logger.debug(f"Found {len(records)} OHLCV records in range")
            return records
        except Exception as e:
            logger.error(f"❌ Failed to find OHLCV range: {e}")
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
            FROM market.ohlc
            WHERE symbol = $1 AND exchange = $2 AND timeframe = $3
        """

        try:
            result = await self.db.fetchval(query, symbol, exchange, timeframe)
            logger.debug(f"OHLCV count for {symbol}/{timeframe}: {result}")
            return result or 0
        except Exception as e:
            logger.error(f"❌ Failed to count OHLCV: {e}")
            raise
