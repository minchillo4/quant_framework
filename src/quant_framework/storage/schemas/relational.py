"""Relational/metadata models for storage layer.

Models for reference data that changes infrequently:
- Instrument: Trading symbols, their properties, and normalized names
- Exchange: Venues where trading occurs (Binance, Coinalyze, Coinbase, etc.)
- Market: Market type (spot, linear perpetual, inverse perpetual, etc.)

These models support relational schema normalization to avoid duplication
across time-series tables and enable efficient joins.
"""

from datetime import datetime

from pydantic import BaseModel, Field


class Exchange(BaseModel):
    """Exchange/venue reference data.

    Represents a venue where trading occurs.
    Stored in: market.exchanges (relational table)

    Used by: Time-series tables via exchange_id FK to normalize storage.
    """

    exchange_id: int | None = Field(
        None, description="Database PK (None when creating)"
    )
    name: str = Field(
        ..., min_length=1, description="Exchange name (e.g., 'binance', 'coinalyze')"
    )
    display_name: str = Field(
        ..., description="Human-readable name (e.g., 'Binance', 'CoinAlyze')"
    )

    is_cex: bool = Field(
        True, description="Is centralized exchange (True) or data provider (False)"
    )
    is_active: bool = Field(True, description="Whether exchange is actively monitored")

    country: str | None = Field(None, description="Country of origin (optional)")
    website: str | None = Field(None, description="Official website URL (optional)")
    api_documentation: str | None = Field(None, description="API docs URL (optional)")

    created_at: datetime | None = Field(None, description="Record creation timestamp")
    updated_at: datetime | None = Field(None, description="Record update timestamp")

    class Config:
        """Pydantic configuration."""

        json_encoders = {
            datetime: lambda v: v.isoformat() if v else None,
        }


class Instrument(BaseModel):
    """Trading instrument/symbol reference data.

    Represents a tradeable asset (BTC, ETH, etc.) and its properties.
    Stored in: market.instruments (relational table)

    Provides single source of truth for symbol normalization across data sources.
    Different data providers may use different symbol conventions; this table
    normalizes to a canonical form.
    """

    instrument_id: int | None = Field(
        None, description="Database PK (None when creating)"
    )
    symbol: str = Field(
        ..., min_length=1, description="Canonical symbol (e.g., 'BTC', 'ETH')"
    )
    asset_type: str = Field(
        ..., description="Asset type: 'crypto', 'commodity', 'fiat', etc."
    )

    name: str | None = Field(
        None, description="Full name (e.g., 'Bitcoin', 'Ethereum')"
    )
    decimals: int | None = Field(
        None, ge=0, le=18, description="Token decimals (for crypto, optional)"
    )

    # Symbol mappings to other data sources
    coingecko_id: str | None = Field(None, description="CoinGecko API identifier")
    coinalyze_symbol: str | None = Field(
        None, description="CoinAlyze API symbol representation"
    )
    ccxt_symbol_binance: str | None = Field(
        None, description="Binance/CCXT symbol representation"
    )

    is_active: bool = Field(True, description="Whether instrument is actively tracked")
    is_major: bool = Field(
        False, description="Whether this is a 'major' asset (BTC, ETH, etc.)"
    )

    created_at: datetime | None = Field(None, description="Record creation timestamp")
    updated_at: datetime | None = Field(None, description="Record update timestamp")

    class Config:
        """Pydantic configuration."""

        json_encoders = {
            datetime: lambda v: v.isoformat() if v else None,
        }


class Market(BaseModel):
    """Market type reference data.

    Represents different market categories:
    - 'spot': Regular spot trading
    - 'linear_perpetual': Linear futures (e.g., BTC/USDT perpetual)
    - 'inverse_perpetual': Inverse futures (e.g., BTC/USD perpetual)
    - 'options': Options trading
    - 'synthetic': Synthetic OI (e.g., staking amounts tracked as 'perpetual')

    Stored in: market.market_types (relational table)

    Used to normalize time-series tables across different market types
    and separate OI calculations appropriately.
    """

    market_type_id: int | None = Field(
        None, description="Database PK (None when creating)"
    )
    market_type: str = Field(
        ...,
        min_length=1,
        description="Market type identifier (e.g., 'spot', 'linear_perpetual')",
    )
    display_name: str = Field(
        ..., description="Human-readable name (e.g., 'Spot', 'Linear Perpetual')"
    )

    description: str | None = Field(
        None, description="Detailed description of market type"
    )

    # Categorization
    has_oi: bool = Field(
        False, description="Whether this market type has open interest"
    )
    is_perpetual: bool = Field(
        False, description="Whether this is perpetual/futures (not spot)"
    )
    is_leveraged: bool = Field(
        False, description="Whether margin/leverage trading is available"
    )

    created_at: datetime | None = Field(None, description="Record creation timestamp")
    updated_at: datetime | None = Field(None, description="Record update timestamp")

    class Config:
        """Pydantic configuration."""

        json_encoders = {
            datetime: lambda v: v.isoformat() if v else None,
        }
