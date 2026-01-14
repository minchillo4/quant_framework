"""
Composite CCXT adapter builder.

Creates OHLCV and OI adapters backed by exchange-specific CCXT clients.
"""

from __future__ import annotations

import logging
from typing import Any

from quant_framework.ingestion.adapters.ccxt_plugin.client_factory import (
    create_ccxt_client,
)
from quant_framework.ingestion.adapters.ccxt_plugin.ohlcv_adapter import (
    CCXTOHLCVAdapter,
)
from quant_framework.ingestion.adapters.ccxt_plugin.open_interest_adapter import (
    CCXTOpenInterestAdapter,
)
from quant_framework.shared.models.enums import (
    AssetClass,
    ClientType,
    ConnectionType,
    DataVenue,
    MarketType,
    WrapperImplementation,
)

logger = logging.getLogger(__name__)


def create_ccxt_adapters(
    venue: DataVenue,
    market_type: MarketType,
    api_key: str | None = None,
    api_secret: str | None = None,
    passphrase: str | None = None,
    testnet: bool = False,
    config: dict[str, Any] | None = None,
) -> tuple[CCXTOHLCVAdapter, CCXTOpenInterestAdapter]:
    """
    Create OHLCV and Open Interest adapters for a given exchange/market type.

    Args:
        venue: Data venue (e.g., DataVenue.HUOBI)
        market_type: Market type (e.g., MarketType.LINEAR_PERPETUAL)
        api_key: API key for authenticated endpoints
        api_secret: API secret
        passphrase: Passphrase (for exchanges like OKX)
        testnet: Whether to use testnet/sandbox mode
        config: Additional adapter config (rate limits, params, etc.)

    Returns:
        Tuple of (ohlcv_adapter, open_interest_adapter)

    Example:
        >>> ohlcv, oi = create_ccxt_adapters(
        ...     venue=DataVenue.HUOBI,
        ...     market_type=MarketType.LINEAR_PERPETUAL,
        ... )
        >>> await ohlcv.connect()
        >>> candles = await ohlcv.fetch_ohlcv(instrument, "1h")
    """
    config = config or {}

    # Build CCXT client with exchange-specific options
    client = create_ccxt_client(
        venue=venue,
        market_type=market_type,
        api_key=api_key,
        api_secret=api_secret,
        testnet=testnet,
        extra_options=config.get("ccxt_options"),
    )

    # Create OHLCV adapter
    ohlcv_adapter = CCXTOHLCVAdapter(
        client=client,
        api_key=api_key,
        api_secret=api_secret,
        passphrase=passphrase,
        testnet=testnet,
        config=config,
    )
    ohlcv_adapter.venue = venue
    ohlcv_adapter.wrapper = WrapperImplementation.CCXT
    ohlcv_adapter.client_type = ClientType.WRAPPER
    ohlcv_adapter.connection_type = ConnectionType.REST
    ohlcv_adapter.supported_asset_classes = {AssetClass.CRYPTO}

    # Create Open Interest adapter (shares client)
    oi_adapter = CCXTOpenInterestAdapter(
        client=client,
        api_key=api_key,
        api_secret=api_secret,
        passphrase=passphrase,
        testnet=testnet,
        config=config,
    )
    oi_adapter.venue = venue
    oi_adapter.wrapper = WrapperImplementation.CCXT
    oi_adapter.client_type = ClientType.WRAPPER
    oi_adapter.connection_type = ConnectionType.REST
    oi_adapter.supported_asset_classes = {AssetClass.CRYPTO}

    logger.info(
        f"Created CCXT adapters for {venue.value} ({market_type.value}) "
        f"with wrapper={WrapperImplementation.CCXT.value}"
    )

    return ohlcv_adapter, oi_adapter


def create_huobi_adapters(
    market_type: MarketType = MarketType.LINEAR_PERPETUAL,
    api_key: str | None = None,
    api_secret: str | None = None,
    testnet: bool = False,
    config: dict[str, Any] | None = None,
) -> tuple[CCXTOHLCVAdapter, CCXTOpenInterestAdapter]:
    """
    Convenience builder for Huobi/HTX adapters.

    Args:
        market_type: Market type (LINEAR_PERPETUAL, INVERSE_PERPETUAL, SPOT)
        api_key: API key
        api_secret: API secret
        testnet: Testnet mode
        config: Additional config

    Returns:
        Tuple of (ohlcv_adapter, open_interest_adapter)

    Example:
        >>> ohlcv, oi = create_huobi_adapters(MarketType.LINEAR_PERPETUAL)
    """
    config = config or {}

    # Merge Huobi-specific defaults
    huobi_defaults = {
        "open_interest_params": {"business_type": "swap"},
        "ccxt_options": {
            "verify": False,  # SSL fix
            "timeout": 30000,
        },
    }

    # User config takes precedence
    merged_config = {**huobi_defaults, **config}
    if "open_interest_params" in config:
        merged_config["open_interest_params"] = {
            **huobi_defaults["open_interest_params"],
            **config["open_interest_params"],
        }

    return create_ccxt_adapters(
        venue=DataVenue.HUOBI,
        market_type=market_type,
        api_key=api_key,
        api_secret=api_secret,
        testnet=testnet,
        config=merged_config,
    )


def create_binance_adapters(
    market_type: MarketType = MarketType.LINEAR_PERPETUAL,
    api_key: str | None = None,
    api_secret: str | None = None,
    testnet: bool = False,
    config: dict[str, Any] | None = None,
) -> tuple[CCXTOHLCVAdapter, CCXTOpenInterestAdapter]:
    """
    Convenience builder for Binance adapters.

    Supports three venues based on market type:
    - SPOT → DataVenue.BINANCE
    - LINEAR_PERPETUAL → DataVenue.BINANCE_USDM (USD-M futures)
    - INVERSE_PERPETUAL → DataVenue.BINANCE_COINM (COIN-M futures)

    Args:
        market_type: Market type (SPOT, LINEAR_PERPETUAL, INVERSE_PERPETUAL)
        api_key: API key
        api_secret: API secret
        testnet: Testnet mode
        config: Additional config

    Returns:
        Tuple of (ohlcv_adapter, open_interest_adapter)

    Example:
        >>> # USD-M futures
        >>> ohlcv, oi = create_binance_adapters(MarketType.LINEAR_PERPETUAL)
        >>> # Spot
        >>> spot_ohlcv, _ = create_binance_adapters(MarketType.SPOT)
    """
    # Map market type to venue
    if market_type == MarketType.SPOT:
        venue = DataVenue.BINANCE
    elif market_type == MarketType.LINEAR_PERPETUAL:
        venue = DataVenue.BINANCE_USDM
    elif market_type == MarketType.INVERSE_PERPETUAL:
        venue = DataVenue.BINANCE_COINM
    else:
        raise ValueError(
            f"Unsupported market type for Binance: {market_type}. "
            f"Supported: SPOT, LINEAR_PERPETUAL, INVERSE_PERPETUAL"
        )

    config = config or {}

    # Binance-specific defaults
    binance_defaults = {
        "ccxt_options": {
            "timeout": 30000,
        },
    }

    # Merge configs
    merged_config = {**binance_defaults, **config}

    return create_ccxt_adapters(
        venue=venue,
        market_type=market_type,
        api_key=api_key,
        api_secret=api_secret,
        testnet=testnet,
        config=merged_config,
    )


def create_bybit_adapters(
    market_type: MarketType = MarketType.LINEAR_PERPETUAL,
    api_key: str | None = None,
    api_secret: str | None = None,
    testnet: bool = False,
    config: dict[str, Any] | None = None,
) -> tuple[CCXTOHLCVAdapter, CCXTOpenInterestAdapter]:
    """
    Convenience builder for Bybit adapters.

    Args:
        market_type: Market type (LINEAR_PERPETUAL, INVERSE_PERPETUAL, SPOT)
        api_key: API key
        api_secret: API secret
        testnet: Testnet mode
        config: Additional config

    Returns:
        Tuple of (ohlcv_adapter, open_interest_adapter)

    Example:
        >>> ohlcv, oi = create_bybit_adapters(MarketType.LINEAR_PERPETUAL)
    """
    config = config or {}

    # Bybit-specific defaults
    bybit_defaults = {
        "ccxt_options": {
            "timeout": 30000,
        },
    }

    # Merge configs
    merged_config = {**bybit_defaults, **config}

    return create_ccxt_adapters(
        venue=DataVenue.BYBIT,
        market_type=market_type,
        api_key=api_key,
        api_secret=api_secret,
        testnet=testnet,
        config=merged_config,
    )


def create_gateio_adapters(
    market_type: MarketType = MarketType.LINEAR_PERPETUAL,
    api_key: str | None = None,
    api_secret: str | None = None,
    testnet: bool = False,
    config: dict[str, Any] | None = None,
) -> tuple[CCXTOHLCVAdapter, CCXTOpenInterestAdapter]:
    """
    Convenience builder for Gate.io adapters.

    Args:
        market_type: Market type (LINEAR_PERPETUAL, INVERSE_PERPETUAL, SPOT)
        api_key: API key
        api_secret: API secret
        testnet: Testnet mode
        config: Additional config

    Returns:
        Tuple of (ohlcv_adapter, open_interest_adapter)

    Example:
        >>> ohlcv, oi = create_gateio_adapters(MarketType.LINEAR_PERPETUAL)
    """
    config = config or {}

    # Gate.io-specific defaults
    gateio_defaults = {
        "ccxt_options": {
            "timeout": 30000,
        },
    }

    # Merge configs
    merged_config = {**gateio_defaults, **config}

    return create_ccxt_adapters(
        venue=DataVenue.GATEIO,
        market_type=market_type,
        api_key=api_key,
        api_secret=api_secret,
        testnet=testnet,
        config=merged_config,
    )
