"""
Symbol and format mapping utilities for AlphaVantage.

Provides mapping functions for converting between internal instrument
representations and AlphaVantage API parameters.
"""

from __future__ import annotations

import logging
from typing import Any

from quant_framework.shared.models.enums import AssetClass
from quant_framework.shared.models.instruments import Instrument

logger = logging.getLogger(__name__)

# AlphaVantage treasury maturity mappings
# Maps internal maturity strings to AlphaVantage API values
TREASURY_MATURITIES = {
    "3month": "3month",
    "2year": "2year",
    "5year": "5year",
    "7year": "7year",
    "10year": "10year",
    "30year": "30year",
    # Additional mappings for convenience
    "3m": "3month",
    "2y": "2year",
    "5y": "5year",
    "7y": "7year",
    "10y": "10year",
    "30y": "30year",
}

# Reverse mapping for normalization
NORMALIZED_MATURITIES = {
    "3month": "3M",
    "2year": "2Y",
    "5year": "5Y",
    "7year": "7Y",
    "10year": "10Y",
    "30year": "30Y",
}


def map_alpha_vantage_symbol(instrument: Instrument) -> str:
    """
    Map internal instrument to AlphaVantage symbol format.

    Args:
        instrument: Internal instrument object

    Returns:
        Symbol string formatted for AlphaVantage API

    Raises:
        ValueError: If instrument cannot be mapped
    """
    if not instrument.symbol:
        raise ValueError("Instrument missing required symbol")

    # Handle different asset classes
    if instrument.asset_class == AssetClass.RATES:
        # For treasury rates, the symbol might include maturity info
        # AlphaVantage uses maturity parameter separately, so we just use the base symbol
        return instrument.symbol.upper()

    elif instrument.asset_class == AssetClass.EQUITY:
        # For equities, AlphaVantage accepts most ticker symbols directly
        # Some international symbols require exchange suffixes
        return _format_equity_symbol(instrument)

    else:
        # For other asset classes, use symbol as-is
        logger.debug(
            f"Using symbol as-is for {instrument.asset_class}: {instrument.symbol}"
        )
        return instrument.symbol.upper()


def _format_equity_symbol(instrument: Instrument) -> str:
    """
    Format equity symbol for AlphaVantage API.

    AlphaVantage supports various international symbols with exchange suffixes:
    - US stocks: AAPL, MSFT, IBM
    - UK (LSE): TSCO.LON
    - Canada (TSX): SHOP.TRT
    - Canada (Venture): GPV.TRV
    - Germany (XETRA): MBG.DEX
    - India (BSE): RELIANCE.BSE
    - China (Shanghai): 600104.SHH
    - China (Shenzhen): 000002.SHZ

    Args:
        instrument: Equity instrument

    Returns:
        Formatted symbol for AlphaVantage
    """
    symbol = instrument.symbol.upper()

    # If the instrument already has exchange information, use it
    if hasattr(instrument, "exchange") and instrument.exchange:
        exchange_suffix = _get_exchange_suffix(instrument.exchange)
        if exchange_suffix:
            return f"{symbol}.{exchange_suffix}"

    # Otherwise, use symbol as-is (likely US stock)
    return symbol


def _get_exchange_suffix(exchange: str) -> str | None:
    """
    Get AlphaVantage exchange suffix for given exchange name.

    Args:
        exchange: Exchange name or identifier

    Returns:
        Exchange suffix for AlphaVantage or None if not applicable
    """
    exchange_mappings = {
        # Major exchanges with known AlphaVantage suffixes
        "LON": "LON",  # London Stock Exchange
        "LSE": "LON",  # London Stock Exchange
        "TRT": "TRT",  # Toronto Stock Exchange
        "TSX": "TRT",  # Toronto Stock Exchange
        "TRV": "TRV",  # Toronto Venture Exchange
        "DEX": "DEX",  # XETRA (Germany)
        "BSE": "BSE",  # Bombay Stock Exchange (India)
        "SHH": "SHH",  # Shanghai Stock Exchange
        "SHZ": "SHZ",  # Shenzhen Stock Exchange
        # Common variations
        "LONDON": "LON",
        "TORONTO": "TRT",
        "XETRA": "DEX",
        "SHANGHAI": "SHH",
        "SHENZHEN": "SHZ",
    }

    return exchange_mappings.get(exchange.upper())


def map_treasury_maturity(maturity: str) -> str:
    """
    Map maturity string to AlphaVantage API format.

    Args:
        maturity: Maturity string (e.g., "3M", "2year", "10y")

    Returns:
        AlphaVantage-compatible maturity string

    Raises:
        ValueError: If maturity is not supported
    """
    normalized = maturity.lower().replace(" ", "")

    if normalized not in TREASURY_MATURITIES:
        supported = list(TREASURY_MATURITIES.keys())
        raise ValueError(f"Unsupported maturity '{maturity}'. Supported: {supported}")

    return TREASURY_MATURITIES[normalized]


def normalize_treasury_maturity(alpha_vantage_maturity: str) -> str:
    """
    Convert AlphaVantage maturity to normalized format.

    Args:
        alpha_vantage_maturity: Maturity from AlphaVantage API

    Returns:
        Normalized maturity string (e.g., "3M", "10Y")
    """
    return NORMALIZED_MATURITIES.get(
        alpha_vantage_maturity, alpha_vantage_maturity.upper()
    )


def validate_alpha_vantage_symbol(symbol: str) -> bool:
    """
    Validate if symbol is compatible with AlphaVantage.

    Args:
        symbol: Symbol to validate

    Returns:
        True if symbol is valid, False otherwise
    """
    if not symbol or not isinstance(symbol, str):
        return False

    # Basic validation - AlphaVantage symbols are alphanumeric
    # and may contain dots for exchange suffixes
    import re

    pattern = r"^[A-Z0-9\.]+$"
    return bool(re.match(pattern, symbol.upper()))


def get_supported_treasury_maturities() -> list[str]:
    """
    Get list of supported treasury maturities.

    Returns:
        List of AlphaVantage-compatible maturity strings
    """
    return list(set(TREASURY_MATURITIES.values()))


def get_exchange_info() -> dict[str, Any]:
    """
    Get information about supported exchanges.

    Returns:
        Dictionary with exchange mapping information
    """
    return {
        "supported_exchanges": list(
            _get_exchange_suffix.__code__.co_consts[1]
            if hasattr(_get_exchange_suffix, "__code__")
            else []
        ),
        "default_exchange": "US",
        "format_examples": {
            "US": "AAPL",
            "UK": "TSCO.LON",
            "Canada": "SHOP.TRT",
            "Germany": "MBG.DEX",
            "India": "RELIANCE.BSE",
            "China": "600104.SHH",
        },
    }
