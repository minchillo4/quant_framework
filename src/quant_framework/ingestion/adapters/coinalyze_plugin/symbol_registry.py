import logging

logger = logging.getLogger(__name__)


class CoinAlyzeSymbolRegistry:
    """
    Exchange-specific symbol mapping for CoinAlyze API.

    Each exchange has different symbol format conventions:
    - Binance (A): BTCUSDT_PERP.A, BTCBUSD_PERP.A, BTCUSDC_PERP.A, BTCUSD_PERP.A (inverse)
    - Bybit (6): BTCUSDT.6 (no _PERP), BTCUSDC.6, BTCUSD.6 (inverse)
    - Gate.io (Y): BTC_USDT.Y (underscore separator), BTC_USD.Y (inverse)
    - Huobi (4): BTCUSDT_PERP.4, BTCUSD.4 (inverse)

    Settlement currency "NATIVE" is used for inverse perpetuals (COIN-M futures).
    """

    # Official CoinAlyze exchange codes from API
    # Source: curl https://api.coinalyze.net/v1/exchanges
    COINALYZE_EXCHANGE_CODES = {
        "binance": "A",
        "bybit": "6",
        "gateio": "Y",
        "huobi": "4",
        "okx": "3",
        "deribit": "2",
        "phemex": "7",
        "bitmex": "0",
        "dydx": "8",
        "bitfinex": "F",
        "woox": "W",
    }

    def __init__(self):
        logger.info(
            "Initialized CoinAlyzeSymbolRegistry with exchange-specific formatters"
        )

    def _format_binance_symbol(
        self, base_asset: str, market_type: str, settlement_currency: str | None
    ) -> str:
        """
        Binance symbol formats:
        - Linear USDT: BTCUSDT_PERP.A
        - Linear BUSD: BTCBUSD_PERP.A
        - Linear USDC: BTCUSDC_PERP.A
        - Inverse (NATIVE): BTCUSD_PERP.A (COIN-M futures)
        """
        base = base_asset.upper()

        if market_type == "linear_perpetual":
            if settlement_currency == "USDT":
                return f"{base}USDT_PERP.A"
            elif settlement_currency == "BUSD":
                return f"{base}BUSD_PERP.A"
            elif settlement_currency == "USDC":
                return f"{base}USDC_PERP.A"
            else:
                # Default to USDT
                return f"{base}USDT_PERP.A"
        elif market_type == "inverse_perpetual":
            # Based on pattern, try with _PERP suffix
            return f"{base}USD_PERP.A"
        else:
            raise ValueError(f"Unsupported market type for Binance: {market_type}")

    def _format_bybit_symbol(
        self, base_asset: str, market_type: str, settlement_currency: str | None
    ) -> str:
        """
        Bybit symbol formats (NO _PERP suffix):
        - Linear USDT: BTCUSDT.6 ✅ (confirmed working)
        - Linear USDC: BTCUSDC.6
        - Inverse (NATIVE): BTCUSD.6 ✅ (confirmed working)
        """
        base = base_asset.upper()

        if market_type == "linear_perpetual":
            if settlement_currency == "USDT":
                return f"{base}USDT.6"
            elif settlement_currency == "USDC":
                return f"{base}USDC.6"
            else:
                # Default to USDT
                return f"{base}USDT.6"
        elif market_type == "inverse_perpetual":
            return f"{base}USD.6"
        else:
            raise ValueError(f"Unsupported market type for Bybit: {market_type}")

    def _format_gateio_symbol(
        self, base_asset: str, market_type: str, settlement_currency: str | None
    ) -> str:
        """
        Gate.io symbol formats (underscore separator):
        - Linear USDT: BTC_USDT.Y ✅ (confirmed working)
        - Linear USDC: BTC_USDC.Y (if available)
        - Inverse (NATIVE): BTC_USD.Y
        """
        base = base_asset.upper()

        if market_type == "linear_perpetual":
            if settlement_currency == "USDT":
                return f"{base}_USDT.Y"
            elif settlement_currency == "USDC":
                return f"{base}_USDC.Y"
            else:
                # Default to USDT
                return f"{base}_USDT.Y"
        elif market_type == "inverse_perpetual":
            return f"{base}_USD.Y"
        else:
            raise ValueError(f"Unsupported market type for Gate.io: {market_type}")

    def _format_huobi_symbol(
        self, base_asset: str, market_type: str, settlement_currency: str | None
    ) -> str:
        """
        Huobi symbol formats:
        - Linear USDT: BTCUSDT_PERP.4 ✅ (confirmed working)
        - Linear USDC: BTCUSDC_PERP.4 (if available)
        - Inverse (NATIVE): BTCUSD.4
        """
        base = base_asset.upper()

        if market_type == "linear_perpetual":
            if settlement_currency == "USDT":
                return f"{base}USDT_PERP.4"
            elif settlement_currency == "USDC":
                return f"{base}USDC_PERP.4"
            else:
                # Default to USDT
                return f"{base}USDT_PERP.4"
        elif market_type == "inverse_perpetual":
            return f"{base}USD.4"
        else:
            raise ValueError(f"Unsupported market type for Huobi: {market_type}")

    def _format_okx_symbol(
        self, base_asset: str, market_type: str, settlement_currency: str | None
    ) -> str:
        """
        OKX symbol formats (needs testing):
        - Linear USDT: BTCUSDT_PERP.3 (assumed)
        - Inverse: BTCUSD.3 (assumed)
        """
        base = base_asset.upper()

        if market_type == "linear_perpetual":
            if settlement_currency == "USDT":
                return f"{base}USDT_PERP.3"
            elif settlement_currency == "USDC":
                return f"{base}USDC_PERP.3"
            else:
                return f"{base}USDT_PERP.3"
        elif market_type == "inverse_perpetual":
            return f"{base}USD.3"
        else:
            raise ValueError(f"Unsupported market type for OKX: {market_type}")

    def _format_deribit_symbol(
        self, base_asset: str, market_type: str, settlement_currency: str | None
    ) -> str:
        """
        Deribit symbol formats (needs testing):
        - Inverse: BTC-USD.2 or BTC_USD.2 (assumed)
        """
        base = base_asset.upper()

        if market_type == "inverse_perpetual":
            return f"{base}-USD.2"
        else:
            raise ValueError(f"Unsupported market type for Deribit: {market_type}")

    def get_coinalyze_symbol(
        self,
        base_asset: str,
        exchange: str,
        market_type: str,
        settlement_currency: str | None = None,
    ) -> str:
        """
        Generate CoinAlyze symbol using exchange-specific formatters.

        Args:
            base_asset: Base asset (e.g., "BTC")
            exchange: Exchange name (e.g., "binance")
            market_type: "linear_perpetual" or "inverse_perpetual"
            settlement_currency: "USDT", "USDC", "BUSD", or "NATIVE" for inverse

        Returns:
            CoinAlyze symbol string (e.g., "BTCUSDT_PERP.A")
        """
        exchange_lower = exchange.lower()

        # Route to exchange-specific formatter
        if exchange_lower == "binance":
            return self._format_binance_symbol(
                base_asset, market_type, settlement_currency
            )
        elif exchange_lower == "bybit":
            return self._format_bybit_symbol(
                base_asset, market_type, settlement_currency
            )
        elif exchange_lower == "gateio":
            return self._format_gateio_symbol(
                base_asset, market_type, settlement_currency
            )
        elif exchange_lower == "huobi":
            return self._format_huobi_symbol(
                base_asset, market_type, settlement_currency
            )
        elif exchange_lower == "okx":
            return self._format_okx_symbol(base_asset, market_type, settlement_currency)
        elif exchange_lower == "deribit":
            return self._format_deribit_symbol(
                base_asset, market_type, settlement_currency
            )
        else:
            raise ValueError(f"No CoinAlyze symbol formatter for exchange: {exchange}")

    def get_coinalyze_symbols_for_asset(
        self,
        base_asset: str,
        include_exchanges: list[str] | None = None,
        include_market_types: list[str] | None = None,
    ) -> list[str]:
        """Get all possible CoinAlyze symbols for a base asset"""
        symbols = []

        if include_exchanges is None:
            include_exchanges = ["binance", "bybit", "gateio", "huobi"]

        if include_market_types is None:
            include_market_types = ["linear_perpetual", "inverse_perpetual"]

        for exchange in include_exchanges:
            for market_type in include_market_types:
                try:
                    if market_type == "linear_perpetual":
                        # Try USDT
                        symbol_usdt = self.get_coinalyze_symbol(
                            base_asset, exchange, market_type, "USDT"
                        )
                        symbols.append(symbol_usdt)

                        # USDC for supported exchanges
                        if exchange in ["binance", "bybit"]:
                            symbol_usdc = self.get_coinalyze_symbol(
                                base_asset, exchange, market_type, "USDC"
                            )
                            symbols.append(symbol_usdc)

                        # BUSD for Binance
                        if exchange == "binance":
                            symbol_busd = self.get_coinalyze_symbol(
                                base_asset, exchange, market_type, "BUSD"
                            )
                            symbols.append(symbol_busd)

                    elif market_type == "inverse_perpetual":
                        symbol = self.get_coinalyze_symbol(
                            base_asset, exchange, market_type
                        )
                        symbols.append(symbol)

                except (ValueError, KeyError) as e:
                    logger.debug(
                        f"Skipping {exchange}/{market_type} for {base_asset}: {e}"
                    )
                    continue

        logger.info(f"Generated {len(symbols)} CoinAlyze symbols for {base_asset}")
        return symbols


# Global instance
coin_alyze_registry = CoinAlyzeSymbolRegistry()
