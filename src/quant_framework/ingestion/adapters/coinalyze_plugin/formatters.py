"""Concrete symbol formatters for CoinAlyze exchanges.

Each exchange has its own formatting rules.
Add new exchanges by creating new formatter and registering with SymbolFormatterRegistry.
"""

from quant_framework.ingestion.adapters.coinalyze_plugin.strategies import (
    ISymbolFormatter,
    SymbolFormatterRegistry,
)


class BinanceSymbolFormatter(ISymbolFormatter):
    """Format symbols for Binance futures."""

    def format(
        self,
        base_asset: str,
        market_type: str,
        settlement: str | None = None,
    ) -> str:
        """Format symbol for Binance.

        Spot: BTCUSDT
        Perpetual: BTCUSDT (linear) or BTCUSD (inverse)
        """
        if market_type == "spot":
            return f"{base_asset}USDT"
        elif market_type in ("linear_perpetual", "inverse_perpetual"):
            return f"{base_asset}{settlement or 'USDT'}"
        else:
            raise ValueError(f"Unsupported market type for Binance: {market_type}")


class BybitSymbolFormatter(ISymbolFormatter):
    """Format symbols for Bybit futures."""

    def format(
        self,
        base_asset: str,
        market_type: str,
        settlement: str | None = None,
    ) -> str:
        """Format symbol for Bybit.

        Spot: BTCUSDT
        Linear perpetual: BTCUSDT
        Inverse perpetual: BTCUSD
        """
        if market_type == "spot":
            return f"{base_asset}USDT"
        elif market_type == "linear_perpetual":
            return f"{base_asset}USDT"
        elif market_type == "inverse_perpetual":
            return f"{base_asset}USD"
        else:
            raise ValueError(f"Unsupported market type for Bybit: {market_type}")


class GateioSymbolFormatter(ISymbolFormatter):
    """Format symbols for Gate.io futures."""

    def format(
        self,
        base_asset: str,
        market_type: str,
        settlement: str | None = None,
    ) -> str:
        """Format symbol for Gate.io.

        Spot: BTC_USDT
        Perpetual: BTC_USDT (with _PERP suffix when needed)
        """
        if market_type == "spot":
            settlement = settlement or "USDT"
            return f"{base_asset}_{settlement}"
        elif market_type in ("linear_perpetual", "inverse_perpetual"):
            settlement = settlement or "USDT"
            return f"{base_asset}_{settlement}"
        else:
            raise ValueError(f"Unsupported market type for Gate.io: {market_type}")


class HuobiSymbolFormatter(ISymbolFormatter):
    """Format symbols for Huobi futures."""

    def format(
        self,
        base_asset: str,
        market_type: str,
        settlement: str | None = None,
    ) -> str:
        """Format symbol for Huobi.

        Spot: btcusdt (lowercase)
        Perpetual: BTC_USDT
        """
        if market_type == "spot":
            return f"{base_asset.lower()}usdt"
        elif market_type in ("linear_perpetual", "inverse_perpetual"):
            settlement = settlement or "USDT"
            return f"{base_asset}_{settlement}"
        else:
            raise ValueError(f"Unsupported market type for Huobi: {market_type}")


class OkxSymbolFormatter(ISymbolFormatter):
    """Format symbols for OKX futures."""

    def format(
        self,
        base_asset: str,
        market_type: str,
        settlement: str | None = None,
    ) -> str:
        """Format symbol for OKX.

        Spot: BTC-USDT
        Perpetual: BTC-USDT-SWAP or BTC-USD-SWAP
        """
        if market_type == "spot":
            return f"{base_asset}-USDT"
        elif market_type == "linear_perpetual":
            settlement = settlement or "USDT"
            return f"{base_asset}-{settlement}-SWAP"
        elif market_type == "inverse_perpetual":
            return f"{base_asset}-USD-SWAP"
        else:
            raise ValueError(f"Unsupported market type for OKX: {market_type}")


class KrakenSymbolFormatter(ISymbolFormatter):
    """Format symbols for Kraken futures."""

    def format(
        self,
        base_asset: str,
        market_type: str,
        settlement: str | None = None,
    ) -> str:
        """Format symbol for Kraken.

        Spot: XXBTZUSD, XETHZUSD (X-prefix for crypto)
        Perpetual: PF_XBTUSD (futures prefix)
        """
        if market_type == "spot":
            return f"X{base_asset}ZUSD"
        elif market_type in ("linear_perpetual", "inverse_perpetual"):
            return f"PF_X{base_asset}USD"
        else:
            raise ValueError(f"Unsupported market type for Kraken: {market_type}")


def create_symbol_formatter_registry() -> "SymbolFormatterRegistry":
    """Factory to create pre-configured symbol formatter registry.

    Returns:
        Registry with all standard exchanges registered
    """

    registry = SymbolFormatterRegistry()
    registry.register("binance", BinanceSymbolFormatter())
    registry.register("bybit", BybitSymbolFormatter())
    registry.register("gateio", GateioSymbolFormatter())
    registry.register("huobi", HuobiSymbolFormatter())
    registry.register("okx", OkxSymbolFormatter())
    registry.register("kraken", KrakenSymbolFormatter())

    # Set a default formatter (can be overridden per exchange)
    registry.set_default(BinanceSymbolFormatter())

    return registry
