from quant_framework.ingestion.adapters.base import BaseAdapter
from quant_framework.ingestion.models.enums import ClientType, ConnectionType
from quant_framework.shared.models.enums import (
    AssetClass,
    DataVenue,
    WrapperImplementation,
)


class CoinMetricsAdapterBase(BaseAdapter):
    """Base adapter for Coin Metrics API."""

    venue = DataVenue.COINMETRICS
    wrapper = WrapperImplementation.COINMETRICS
    client_type = ClientType.NATIVE
    connection_type = ConnectionType.REST
    supported_asset_classes = {AssetClass.CRYPTO}
