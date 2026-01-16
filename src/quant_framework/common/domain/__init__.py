"""
Domain Module - Shared Domain Models
====================================

Re-exports core domain entities (enums, models, instruments) from shared/
for use across all layers.

This provides a unified entry point for domain models while keeping
the authoritative definitions in shared/.
"""

# Re-export from shared.models.enums
from quant_framework.shared.models.enums import (
    AssetClass,
    DataVenue,
    IngestionStatus,
    MarketType,
    WrapperImplementation,
)

# Re-export from shared.models.instruments
from quant_framework.shared.models.instruments import Instrument

__all__ = [
    # Enums
    "AssetClass",
    "DataVenue",
    "MarketType",
    "WrapperImplementation",
    "IngestionStatus",
    # Models
    "Instrument",
]
