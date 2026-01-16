"""Source-specific normalizer implementations.

This subdirectory contains concrete normalizer classes for each data source:
- coinalyze.py: CoinalyzeOHLCVNormalizer, CoinalyzeOINormalizer
- ccxt.py: CCXTOHLCVNormalizer, CCXTTradeNormalizer
- coinmetrics.py: CoinMetricsOHLCVNormalizer

Each adapter extends BaseOHLCVNormalizer, BaseOpenInterestNormalizer, etc.
and implements source-specific field extraction.
"""
