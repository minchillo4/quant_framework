# shared/models/onchain.py - JUST THIS ONE FILE
from datetime import datetime
from decimal import Decimal

from pydantic import BaseModel


class OnChainMetric(BaseModel):
    """Simple on-chain metric for CoinMetrics CSV."""

    timestamp: datetime
    asset: str  # "BTC", "ETH"
    metric: str  # "PriceUSD", "AdrActCnt"
    value: Decimal
    source: str = "coinmetrics_csv"
