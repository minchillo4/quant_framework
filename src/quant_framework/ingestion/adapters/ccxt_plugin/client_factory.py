"""CCXT client factory with exchange-specific options.

Creates configured CCXT clients for use with CCXTAdapterBase-based adapters.
"""

from __future__ import annotations

from typing import Any

try:
    import ccxt  # type: ignore
except Exception:  # pragma: no cover - optional dependency at runtime
    ccxt = None  # type: ignore

from quant_framework.shared.models.enums import DataVenue, MarketType


def _build_options(
    venue: DataVenue,
    market_type: MarketType,
    api_key: str | None,
    api_secret: str | None,
    testnet: bool,
    extra_options: dict[str, Any] | None,
) -> dict[str, Any]:
    options: dict[str, Any] = {
        "apiKey": api_key or "",
        "secret": api_secret or "",
        "enableRateLimit": True,
        "timeout": 30000,
    }

    # Testnet toggle if supported by exchange
    if testnet:
        options["sandbox"] = True

    # Exchange-specific tweaks
    if venue == DataVenue.HUOBI:
        # Docker environments may hit SSL chain issues with Huobi/HTX
        options["verify"] = False

    # CCXT "options" bag for unified exchange settings
    xopts: dict[str, Any] = {}
    # Example: defaultType for futures/spot routing when needed
    if venue in {DataVenue.BINANCE_USDM, DataVenue.BINANCE_COINM}:
        xopts["defaultType"] = (
            "delivery" if venue == DataVenue.BINANCE_COINM else "future"
        )

    if xopts:
        options["options"] = xopts

    if extra_options:
        # Shallow-merge at root and options/options if provided
        options.update({k: v for k, v in extra_options.items() if k != "options"})
        if "options" in extra_options:
            options.setdefault("options", {}).update(extra_options["options"])  # type: ignore

    return options


def create_ccxt_client(
    venue: DataVenue,
    market_type: MarketType,
    api_key: str | None = None,
    api_secret: str | None = None,
    testnet: bool = False,
    extra_options: dict[str, Any] | None = None,
):
    """Instantiate a CCXT exchange client with sane defaults.

    The caller is responsible for passing the correct CCXT class corresponding
    to the venue. For now we focus on common venues and Huobi.
    """
    if ccxt is None:
        raise RuntimeError("ccxt is not available in this environment")

    # Map DataVenue to ccxt class name
    venue_to_ccxt = {
        DataVenue.BINANCE: "binance",
        DataVenue.BINANCE_USDM: "binanceusdm",
        DataVenue.BINANCE_COINM: "binancecoinm",
        DataVenue.BYBIT: "bybit",
        DataVenue.GATEIO: "gateio",
        DataVenue.HUOBI: "huobi",
        DataVenue.BITGET: "bitget",
        DataVenue.OKX: "okx",
        DataVenue.KRAKEN: "kraken",
        DataVenue.COINBASE: "coinbase",
    }

    ccxt_class_name = venue_to_ccxt.get(venue)
    if not ccxt_class_name or not hasattr(ccxt, ccxt_class_name):
        raise ValueError(f"Unsupported venue for CCXT client: {venue}")

    exchange_class = getattr(ccxt, ccxt_class_name)
    options = _build_options(
        venue, market_type, api_key, api_secret, testnet, extra_options
    )
    return exchange_class(options)
