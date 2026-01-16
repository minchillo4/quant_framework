"""Repository module for data access layer.

Generic CRUD abstractions and concrete implementations for:
- Time-series data: OHLCV, Trade, OpenInterest
- Relational metadata: Instrument, Exchange, Market
- Checkpoint tracking: Backfill progress and state

All repositories use async/await patterns and protocols for
testability and dependency injection.
"""

from .checkpoint import CheckpointRepository
from .metadata import ExchangeRepository, InstrumentRepository, MarketTypeRepository
from .ohlcv import OHLCVRepository
from .open_interest import OpenInterestRepository

__all__ = [
    "CheckpointRepository",
    "OHLCVRepository",
    "OpenInterestRepository",
    "InstrumentRepository",
    "ExchangeRepository",
    "MarketTypeRepository",
]
