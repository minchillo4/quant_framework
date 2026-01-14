# quant_framework/shared/models/instruments.py

from datetime import datetime
from decimal import Decimal
from typing import Any

from pydantic import BaseModel, Field, root_validator, validator

from quant_framework.shared.models.enums import (
    AssetClass,
    DataVenue,
    MarketType,
    WrapperImplementation,
)


class Instrument(BaseModel):
    """
    Universal instrument model supporting all asset classes.

    Pydantic version provides robust validation and automatic field calculation.
    """

    # ========== CORE IDENTIFIERS (Required) ==========
    instrument_id: str = Field(..., description="Unique identifier")
    asset_class: AssetClass
    market_type: MarketType
    venue: DataVenue

    # ========== ASSET COMPOSITION (Required) ==========
    base_asset: str
    quote_asset: str

    # ========== DATA LINEAGE (Optional) ==========
    wrapper: WrapperImplementation = Field(
        default=WrapperImplementation.NONE,
        description="How we access the data (wrapper)",
    )

    # ========== CONTRACT SPECIFICATIONS (Optional) ==========
    contract_type: str | None = Field(default=None)
    contract_size: Decimal | None = Field(default=None)
    tick_size: Decimal | None = Field(default=None)
    lot_size: Decimal | None = Field(default=None)

    # ========== LIFECYCLE (Optional) ==========
    listing_date: datetime | None = Field(default=None)
    expiry_date: datetime | None = Field(default=None)
    is_active: bool = Field(default=True)

    # ========== SPECIAL FLAGS (Optional) ==========
    is_inverse: bool = Field(default=False)

    # ========== VENUE/WRAPPER MAPPING (Optional) ==========
    raw_symbol: str = Field(default="")
    metadata: dict[str, Any] | None = Field(default_factory=dict)

    # ========== CALCULATED FIELD (Not in __init__) ==========
    settlement_currency: str | None = Field(default=None)

    # ==================== VALIDATORS ====================

    @validator("raw_symbol", pre=True, always=True)
    def set_raw_symbol(cls, v, values):
        """Set raw_symbol to instrument_id if not provided"""
        if not v:
            return values.get("instrument_id", "")
        return v

    @validator("metadata", pre=True, always=True)
    def set_metadata(cls, v):
        """Initialize metadata as empty dict if None"""
        return v or {}

    @root_validator(skip_on_failure=True)
    def calculate_settlement_currency(cls, values):
        """Auto-calculate settlement currency based on contract type"""
        settlement = values.get("settlement_currency")

        # Only calculate if settlement_currency is None
        if settlement is None:
            is_inverse = values.get("is_inverse", False)
            base_asset = values.get("base_asset")
            quote_asset = values.get("quote_asset")

            if is_inverse:
                values["settlement_currency"] = base_asset
            else:
                values["settlement_currency"] = quote_asset

        return values

    # ==================== PROPERTIES ====================

    @property
    def symbol(self) -> str:
        """Alias for instrument_id for backward compatibility"""
        return self.instrument_id

    @property
    def full_symbol(self) -> str:
        """Full symbol with venue prefix (e.g., 'binance_usdm:BTCUSDT')"""
        return f"{self.venue.value}:{self.instrument_id}"

    @property
    def data_lineage(self) -> str:
        """Human-readable data lineage string"""
        if self.wrapper == WrapperImplementation.NONE:
            return f"{self.venue.value} (native)"
        return f"{self.venue.value} via {self.wrapper.value}"

    @property
    def is_derivative(self) -> bool:
        """Check if instrument is a derivative"""
        return self.market_type in [
            MarketType.LINEAR_PERPETUAL,
            MarketType.INVERSE_PERPETUAL,
            MarketType.LINEAR_FUTURE,
            MarketType.INVERSE_FUTURE,
            MarketType.OPTION,
        ]

    @property
    def is_perpetual(self) -> bool:
        """Check if instrument is a perpetual swap"""
        return self.market_type in [
            MarketType.LINEAR_PERPETUAL,
            MarketType.INVERSE_PERPETUAL,
        ]

    # ==================== SERIALIZATION ====================

    def to_dict(self) -> dict[str, Any]:
        """Convert to dictionary (Pydantic does this natively)"""
        return self.dict(by_alias=True)

    def to_json(self) -> str:
        """Convert to JSON string"""
        return self.json(by_alias=True, indent=2)

    class Config:
        """Pydantic configuration"""

        validate_assignment = True  # Re-validate on attribute change
        arbitrary_types_allowed = True  # Allow Decimal, datetime


# ==================== TESTING ====================
