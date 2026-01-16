"""Storage layer for mnemo-quant framework.

Central abstraction for data persistence across:
- Time-series data: OHLCV, Trade, Open Interest
- Relational metadata: Instruments, Exchanges, Markets
- Checkpoint tracking: Backfill progress and state
- Bronze layer: Raw data in Parquet format

Architecture:

    ┌─────────────────────────────────────┐
    │ Application Layer                   │
    │ (Orchestration, Transformation)     │
    └──────────────┬──────────────────────┘
                   │
    ┌──────────────▼──────────────────────┐
    │ Storage Layer (THIS MODULE)         │
    │                                     │
    │  Repositories:                      │
    │  - OHLCVRepository                  │
    │  - OpenInterestRepository           │
    │  - CheckpointRepository             │
    │  - MetadataRepository (future)      │
    │                                     │
    │  Schemas:                           │
    │  - Time-series: OHLCV, Trade, OI    │
    │  - Relational: Instrument, Exchange │
    │  - Document: Checkpoint             │
    └──────────────┬──────────────────────┘
                   │
    ┌──────────────▼──────────────────────┐
    │ Infrastructure Layer                │
    │ (Database, Config, Logging)         │
    │ - PostgreSQL/TimescaleDB (asyncpg)  │
    │ - MinIO/S3 (boto3)                  │
    └─────────────────────────────────────┘

Key Design Principles:

1. **Protocol-Based**: All interfaces use Protocols for flexibility
2. **Async/Await**: All I/O operations are async-first
3. **Batch Operations**: Efficient bulk inserts via parameterized queries
4. **Idempotent**: Upsert semantics avoid duplicate errors
5. **Type Safe**: Pydantic models for validation
6. **Decoupled**: Storage details hidden from application layer

Usage:

    from quant_framework.storage.repositories import OHLCVRepository
    from quant_framework.storage.schemas import OHLCVRecord
    from quant_framework.infrastructure.database import Database

    # Initialize database and repository
    db = Database()
    await db.connect()

    ohlcv_repo = OHLCVRepository(db)

    # Save single record
    record = OHLCVRecord(...)
    await ohlcv_repo.save(record)

    # Batch insert
    records = [OHLCVRecord(...), ...]
    await ohlcv_repo.save_batch(records)

    # Query
    latest = await ohlcv_repo.find_latest("BTC", "binance", "1h")
"""

from .adapters import PostgresAdapter
from .repositories import (
    CheckpointRepository,
    ExchangeRepository,
    InstrumentRepository,
    MarketTypeRepository,
    OHLCVRepository,
    OpenInterestRepository,
)
from .schemas import (
    Exchange,
    Instrument,
    Market,
    OHLCVRecord,
    OpenInterestRecord,
    TradeRecord,
)

__all__ = [
    # Adapters
    "PostgresAdapter",
    # Repositories
    "OHLCVRepository",
    "OpenInterestRepository",
    "CheckpointRepository",
    "InstrumentRepository",
    "ExchangeRepository",
    "MarketTypeRepository",
    # Schemas
    "OHLCVRecord",
    "OpenInterestRecord",
    "TradeRecord",
    "Instrument",
    "Exchange",
    "Market",
]
