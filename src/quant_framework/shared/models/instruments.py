# quant_framework/shared/models/instruments.py

from datetime import datetime
from decimal import Decimal
from typing import Any

from pydantic import (
    BaseModel,
    ConfigDict,
    Field,
    field_validator,
    model_validator,
)

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

    model_config = ConfigDict(validate_assignment=True, arbitrary_types_allowed=True)

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

    @field_validator("raw_symbol", mode="before")
    @classmethod
    def set_raw_symbol(cls, v, info):
        """Set raw_symbol to instrument_id if not provided"""
        if not v:
            return info.data.get("instrument_id", "")
        return v

    @field_validator("metadata", mode="before")
    @classmethod
    def set_metadata(cls, v):
        """Initialize metadata as empty dict if None"""
        return v or {}

    @model_validator(mode="after")
    def calculate_settlement_currency(self):
        """Auto-calculate settlement currency based on contract type"""
        settlement = self.settlement_currency

        # Only calculate if settlement_currency is None
        if settlement is None:
            if self.is_inverse:
                self.settlement_currency = self.base_asset
            else:
                self.settlement_currency = self.quote_asset

        return self

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
        """Convert to dictionary (Pydantic v2 method)"""
        return self.model_dump(by_alias=True)

    def to_json(self) -> str:
        """Convert to JSON string"""
        return self.model_dump_json(by_alias=True, indent=2)


# ==================== TESTING ====================
