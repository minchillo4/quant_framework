"""
Capability-based adapters package.
Exports core BaseAdapter and CCXT plugin capabilities.
"""

from quant_framework.ingestion.adapters.base import BaseAdapter
from quant_framework.ingestion.adapters.ccxt_plugin.ohlcv_adapter import (
    CCXTOHLCVAdapter,
)
from quant_framework.ingestion.adapters.ccxt_plugin.open_interest_adapter import (
    CCXTOpenInterestAdapter,
)

__all__ = [
    "BaseAdapter",
    "CCXTOHLCVAdapter",
    "CCXTOpenInterestAdapter",
]
