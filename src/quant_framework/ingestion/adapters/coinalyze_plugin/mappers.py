# quant_framework/ingestion/adapters/coinalyze_plugin/mappers.py

from quant_framework.shared.models.instruments import Instrument

from .formatters import create_symbol_formatter_registry

COINALYZE_INTERVAL_MAP = {
    "1m": "1min",
    "5m": "5min",
    "15m": "15min",
    "30m": "30min",
    "1h": "1hour",
    "4h": "4hour",
    "1d": "daily",
}


def get_coinalyze_interval(timeframe: str) -> str:
    """Map internal timeframe to CoinAlyze interval"""
    if timeframe not in COINALYZE_INTERVAL_MAP:
        raise ValueError(f"Unsupported timeframe {timeframe}")
    return COINALYZE_INTERVAL_MAP[timeframe]


def get_coinalyze_symbol(instrument: Instrument) -> str:
    """
    Format symbol using registry of exchange-specific formatters.
    Takes Instrument → CoinAlyze symbol format
    """
    # Map venue to exchange name
    exchange = instrument.venue.value.replace("_usdm", "").replace("_coinm", "")

    # Determine market type and settlement
    market_type = (
        "linear_perpetual" if not instrument.is_inverse else "inverse_perpetual"
    )
    settlement = (
        instrument.settlement_currency if not instrument.is_inverse else "NATIVE"
    )

    # Use strategy registry with exchange-specific formatters
    registry = create_symbol_formatter_registry()
    formatter = registry.get_formatter(exchange.lower())
    return formatter.format(
        base_asset=instrument.base_asset.upper(),
        market_type=market_type,
        settlement=settlement,
    )
