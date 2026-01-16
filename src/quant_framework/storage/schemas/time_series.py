"""Time-series data models for storage layer.

Models for:
- OHLCV: Open, High, Low, Close, Volume candlestick data
- Trade: Individual trade executions
- OpenInterest: Perpetual/futures open interest metrics

All models use:
- Pydantic for strict validation
- DECIMAL prices (not float) for financial accuracy
- Timezone-aware UTC datetimes
- Optional fields for sparse data
"""

from datetime import datetime
from decimal import Decimal

from pydantic import BaseModel, Field


class OHLCVRecord(BaseModel):
    """OHLCV candlestick record.

    Represents a single candlestick (open-high-low-close-volume) record
    for a trading symbol at a specific timeframe and timestamp.

    Stored in: market.ohlc (hypertable with timeframe-specific clones)
    """

    timestamp: datetime = Field(..., description="Candle close time (UTC)")
    symbol: str = Field(
        ..., min_length=1, description="Canonical symbol (e.g., BTC, ETH)"
    )
    exchange: str = Field(
        ..., min_length=1, description="Exchange name (e.g., binance, coinalyze)"
    )
    timeframe: str = Field(
        ..., min_length=1, description="Timeframe (e.g., 1m, 5m, 1h, 1d)"
    )

    open_price: Decimal = Field(
        ..., decimal_places=8, description="Open price (DECIMAL for accuracy)"
    )
    high_price: Decimal = Field(
        ..., decimal_places=8, description="Highest price in period"
    )
    low_price: Decimal = Field(
        ..., decimal_places=8, description="Lowest price in period"
    )
    close_price: Decimal = Field(
        ..., decimal_places=8, description="Close price (end of candle)"
    )
    volume: Decimal = Field(
        ..., decimal_places=8, description="Trading volume in base asset"
    )

    quote_volume: Decimal | None = Field(
        None, decimal_places=2, description="Volume in quote asset (optional)"
    )
    trades_count: int | None = Field(
        None, description="Number of trades in this period (optional)"
    )

    class Config:
        """Pydantic configuration."""

        json_encoders = {
            Decimal: str,
            datetime: lambda v: v.isoformat(),
        }


class TradeRecord(BaseModel):
    """Individual trade execution record.

    Represents a single spot market trade between two parties.

    Stored in: market.trades (NOT hypertable, standard partitioning)
    """

    timestamp: datetime = Field(..., description="Trade execution time (UTC)")
    symbol: str = Field(
        ..., min_length=1, description="Canonical symbol (e.g., BTC, ETH)"
    )
    exchange: str = Field(..., min_length=1, description="Exchange name")

    price: Decimal = Field(..., decimal_places=8, description="Trade price (DECIMAL)")
    quantity: Decimal = Field(
        ..., decimal_places=8, description="Trade quantity in base asset"
    )
    quote_amount: Decimal | None = Field(
        None, decimal_places=2, description="Total value in quote asset (optional)"
    )

    side: str = Field(..., description="Trade side: 'buy' or 'sell'")
    buyer_maker: bool | None = Field(
        None, description="Whether buyer was the maker (optional)"
    )

    class Config:
        """Pydantic configuration."""

        json_encoders = {
            Decimal: str,
            datetime: lambda v: v.isoformat(),
        }


class OpenInterestRecord(BaseModel):
    """Open Interest record for perpetual/futures contracts.

    Tracks the total amount of open positions at a point in time.
    For spot markets where OI doesn't apply, settlement_currency indicates
    synthetic OI tracking (e.g., staking amounts, synthetic perpetuals).

    Stored in: market.open_interest (hypertable with timeframe-specific clones)

    Note: This differs from CoinalyzeOISchema which includes derived fields.
    Use this for raw data storage; derive analytics fields in transformations.
    """

    timestamp: datetime = Field(..., description="OI snapshot time (UTC)")
    symbol: str = Field(
        ..., min_length=1, description="Canonical symbol (e.g., BTC, ETH)"
    )
    exchange: str = Field(..., min_length=1, description="Exchange name")
    timeframe: str = Field(..., min_length=1, description="Timeframe (e.g., 5m, 1h)")

    open_interest_usd: Decimal = Field(
        ..., decimal_places=2, description="Open interest in USD (or primary currency)"
    )
    settlement_currency: str | None = Field(
        None, description="Stablecoin used (USDT, USDC, etc.) - None for USD OI"
    )

    long_oi_usd: Decimal | None = Field(
        None, decimal_places=2, description="Long OI in USD (optional)"
    )
    short_oi_usd: Decimal | None = Field(
        None, decimal_places=2, description="Short OI in USD (optional)"
    )
    long_short_ratio: Decimal | None = Field(
        None, decimal_places=4, description="Long/Short ratio (optional)"
    )

    class Config:
        """Pydantic configuration."""

        json_encoders = {
            Decimal: str,
            datetime: lambda v: v.isoformat(),
        }
