"""
Enhanced base adapter with asset-agnostic, capability-based design.

Clarified data lineage through explicit venue + wrapper tracking.
"""

import logging
from abc import ABC, abstractmethod
from typing import TYPE_CHECKING, Any

from quant_framework.ingestion.models.enums import (
    ClientType,
    ConnectionType,
)
from quant_framework.shared.models.enums import (
    AssetClass,
    DataVenue,
    WrapperImplementation,
)
from quant_framework.shared.models.instruments import Instrument

if TYPE_CHECKING:
    pass

logger = logging.getLogger(__name__)


class BaseAdapter(ABC):
    """
    Enhanced base adapter supporting all asset classes and capability-based design.

    Clarifies data lineage through explicit venue + wrapper tracking:
    - venue: WHERE the data originates (the actual exchange/source)
    - wrapper: HOW we access it (CCXT, CoinAlyze, Yahoo Finance, etc.)

    Key improvements over old BaseAdapter:
    1. Asset-agnostic: Works with any asset class via DataVenue enum
    2. Capability-based: Adapters declare which ports they implement
    3. Instrument-centric: Uses Instrument model instead of string symbols
    4. Metadata-rich: Exposes venue, wrapper, client_type, connection_type
    5. Layer separation: Returns raw data; no preprocessing/normalization
    6. Clear lineage: Explicit tracking of data source and access method

    Attributes:
        venue: Data venue (BINANCE_USDM, BYBIT, ICE_DATA, etc.) - WHERE data comes from
        wrapper: Wrapper implementation (COINALYZE, CCXT, YAHOO_FINANCE, NONE) - HOW we access it
        client_type: Type of client implementation (WRAPPER, NATIVE, etc.)
        connection_type: Network protocol (REST, WEBSOCKET, etc.)
        supported_asset_classes: Set of AssetClass values this adapter supports
        capabilities: Set of port classes this adapter implements

    Examples:
        # CoinAlyze adapter accessing Binance USD-M
        class CoinalyzeOHLCVAdapter(BaseAdapter):
            venue = DataVenue.BINANCE_USDM  # Can also be set per-instance
            wrapper = WrapperImplementation.COINALYZE
            client_type = ClientType.WRAPPER
            connection_type = ConnectionType.REST

        # CCXT adapter accessing Bybit
        class CCXTOHLCVAdapter(BaseAdapter):
            venue = DataVenue.BYBIT
            wrapper = WrapperImplementation.CCXT
            client_type = ClientType.WRAPPER
            connection_type = ConnectionType.REST
    """

    # ========== DATA LINEAGE (NEW: Clarified) ==========
    venue: DataVenue  # WHERE: The actual source
    wrapper: WrapperImplementation  # HOW: Access method

    # ========== TECHNICAL IMPLEMENTATION ==========
    client_type: ClientType
    connection_type: ConnectionType

    # ========== CAPABILITIES ==========
    supported_asset_classes: set[AssetClass] = set()
    capabilities: set[type] = set()

    def __init__(
        self,
        api_key: str | None = None,
        api_secret: str | None = None,
        passphrase: str | None = None,
        testnet: bool = False,
        config: dict[str, Any] | None = None,
    ):
        """Initialize adapter state and credentials."""
        self.api_key = api_key
        self.api_secret = api_secret
        self.passphrase = passphrase
        self.testnet = testnet
        self.config = config or {}
        self._connected = False

    @abstractmethod
    async def connect(self) -> None:
        """Establish connection to the data provider (idempotent)."""
        pass

    @abstractmethod
    async def close(self) -> None:
        """Close connection and clean up resources (idempotent)."""
        pass

    def supports_asset_class(self, asset_class: AssetClass) -> bool:
        """Check if this adapter supports the given asset class."""
        return asset_class in self.supported_asset_classes

    def supports_capability(self, port_type: type) -> bool:
        """Check if this adapter implements the given port."""
        return port_type in self.capabilities

    def validate_instrument(self, instrument: Instrument) -> bool:
        """Validate if instrument is supported by this adapter.

        Checks:
        1. Asset class support
        2. Venue match (with flexibility for AGGREGATED venues)
        3. Wrapper match (must match adapter's wrapper)
        4. Instrument is active
        """
        # Check asset class
        if not self.supports_asset_class(instrument.asset_class):
            logger.warning(
                f"{self.__class__.__name__} does not support asset class {instrument.asset_class}"
            )
            return False

        # Check venue match (with flexibility for AGGREGATED)
        # Adapters that access multiple venues (like CoinAlyze) should allow any venue
        if hasattr(self, "supports_multiple_venues") and self.supports_multiple_venues:
            # Multi-venue adapters (CoinAlyze, CCXT) - venue is validated elsewhere
            pass
        elif self.venue != DataVenue.AGGREGATED and instrument.venue != self.venue:
            logger.warning(
                f"Instrument venue {instrument.venue} does not match adapter venue {self.venue}"
            )
            return False

        # Check wrapper match (strict - wrapper must match)
        if instrument.wrapper != self.wrapper:
            logger.warning(
                f"Instrument wrapper {instrument.wrapper} does not match adapter wrapper {self.wrapper}"
            )
            return False

        # Check active status
        if not instrument.is_active:
            logger.warning(f"Instrument {instrument.instrument_id} is not active")
            return False

        return True

    async def __aenter__(self):
        await self.connect()
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        await self.close()

    def __repr__(self) -> str:
        return (
            f"{self.__class__.__name__}("
            f"venue={self.venue.value}, "
            f"wrapper={self.wrapper.value}, "
            f"client_type={self.client_type.value}, "
            f"authenticated={bool(self.api_key)}, "
            f"testnet={self.testnet})"
        )
