"""Storage schemas for time-series and relational data.

This module exports data models for:
- Time-series data: OHLCV, Trade, OpenInterest
- Relational metadata: Instrument, Exchange, Market

All models use Pydantic for strict validation and type safety.
Models are decoupled from database implementation via generics and protocols.
"""

from .relational import Exchange, Instrument, Market
from .time_series import OHLCVRecord, OpenInterestRecord, TradeRecord

__all__ = [
    # Time-series
    "OHLCVRecord",
    "TradeRecord",
    "OpenInterestRecord",
    # Relational
    "Instrument",
    "Exchange",
    "Market",
]
