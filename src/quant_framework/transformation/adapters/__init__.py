"""Adapter implementations for data source normalization.

This subdirectory contains source-specific normalizer classes:
- coinalyze.py: Normalizers for Coinalyze API
  - CoinalyzeOHLCVNormalizer: Convert Coinalyze candles → OHLCVRecord
  - CoinalyzeOINormalizer: Convert Coinalyze OI → OpenInterestRecord
- ccxt.py: Normalizers for CCXT unified API
  - CCXTOHLCVNormalizer: Convert CCXT candles → OHLCVRecord
  - CCXTTradeNormalizer: Convert CCXT trades → TradeRecord
- coinmetrics.py: Normalizers for CoinMetrics API
  - CoinMetricsOHLCVNormalizer: Convert CM candles → OHLCVRecord

Each adapter extends base normalizer class and implements:
- _extract_timestamp(): Get timestamp from source format
- _extract_prices(): Get OHLC from source format
- _extract_volume(): Get volume from source format
- _extract_optional_fields(): Get source-specific fields
"""

from quant_framework.transformation.adapters.ccxt import (
    CCXTOHLCVNormalizer,
    CCXTTradeNormalizer,
)
from quant_framework.transformation.adapters.coinalyze import (
    CoinalyzeOHLCVNormalizer,
    CoinalyzeOINormalizer,
)
from quant_framework.transformation.adapters.coinmetrics import (
    CoinMetricsOHLCVNormalizer,
)

__all__ = [
    # CCXT adapters
    "CCXTOHLCVNormalizer",
    "CCXTTradeNormalizer",
    # Coinalyze adapters
    "CoinalyzeOHLCVNormalizer",
    "CoinalyzeOINormalizer",
    # CoinMetrics adapters
    "CoinMetricsOHLCVNormalizer",
]
