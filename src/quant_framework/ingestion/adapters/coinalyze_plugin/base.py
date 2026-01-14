from quant_framework.ingestion.adapters.base import BaseAdapter
from quant_framework.ingestion.models.enums import (
    ClientType,
    ConnectionType,
)
from quant_framework.shared.models.enums import (
    AssetClass,
    DataVenue,
    WrapperImplementation,
)

from .client import CoinalyzeClient


class CoinalyzeAdapterBase(BaseAdapter):
    """Base adapter for CoinAlyze API.

    CoinAlyze is a multi-venue wrapper that provides access to:
    - BINANCE (spot)
    - BINANCE_USDM (linear perpetuals/futures)
    - BINANCE_COINM (inverse perpetuals/futures)
    - BYBIT (unified)
    - GATEIO (unified)
    - HUOBI (unified)
    """

    # Data lineage
    venue: DataVenue  # Set per-instance based on instrument
    wrapper = WrapperImplementation.COINALYZE

    # Technical implementation
    client_type = ClientType.WRAPPER
    connection_type = ConnectionType.REST

    # CoinAlyze supports crypto only
    supported_asset_classes = {AssetClass.CRYPTO}

    # Multi-venue support flag
    supports_multiple_venues = True  # Used in validate_instrument()

    def __init__(self, client: CoinalyzeClient, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.client = client
        # venue is set per-instrument in fetch methods

    async def connect(self) -> None:
        """Connect to CoinAlyze API via client."""
        await self.client.connect()
        self._connected = True

    async def close(self) -> None:
        """Close connection and clean up resources."""
        await self.client.close()
        self._connected = False
