"""Compatibility wrapper for CoinAlyze client/fetcher."""

from mnemo_quant.data_sources.coin_alyze.client import CoinalyzeClient
from mnemo_quant.data_sources.coin_alyze.fetcher import CoinAlyzeFetcher

__all__ = ["CoinalyzeClient", "CoinAlyzeFetcher"]
