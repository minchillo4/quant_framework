"""
Configuration management using the new SharedState-inspired ConfigState pattern.

This module provides backward-compatible access to application configuration
while using the clean, validated ConfigState from quant_framework.config.state.

Import this module to access:
  - settings: The global ConfigState instance
  - config_loader: The ConfigLoader for reloading configs
"""

import logging

from quant_framework.config.state import (
    ConfigLoader,
    ConfigState,
    ExchangeMarketConfig,
    get_config,
)

logger = logging.getLogger(__name__)
# =============================================================================
# GLOBAL CONFIGURATION STATE - Initialized on module load
# =============================================================================


def _initialize_config() -> ConfigState:
    """Initialize global configuration state with proper error handling."""
    try:
        return get_config()
    except Exception as e:
        logger.error(f"Failed to initialize configuration: {e}")
        logger.warning("Using minimal default configuration")
        # Return minimal config with defaults for graceful degradation
        return ConfigState()


settings: ConfigState = _initialize_config()
config_loader: ConfigLoader = ConfigLoader(config_dir=settings.config_dir)


# =============================================================================
# BACKWARD COMPATIBILITY - ConfigRegistry (if needed)
# =============================================================================


class ConfigRegistry:
    """
    Singleton registry for exchange configurations.
    Maintains backward compatibility with existing code that uses ConfigRegistry.
    """

    _instance = None
    _configs: dict[str, ExchangeMarketConfig] = {}

    def __new__(cls):
        if cls._instance is None:
            cls._instance = super().__new__(cls)
        return cls._instance

    def load_all(self):
        """Load all exchange configs from settings.exchanges."""
        if self._configs:
            return  # Already loaded

        logger.info("Loading exchange configurations from settings...")

        for key, config in settings.exchanges.items():
            self._configs[key] = config
            # Also store by ccxt_class for backward compatibility
            if hasattr(config, "ccxt_class"):
                self._configs[config.ccxt_class] = config

        logger.info(f"✅ Loaded {len(self._configs)} exchange configs")

    def get(self, exchange: str, market_type: str = None) -> ExchangeMarketConfig:
        """
        Get validated config for an exchange and optional market type.

        Args:
            exchange: Exchange name (e.g., 'binance')
            market_type: Market type (e.g., 'spot', 'linear_perpetual')

        Returns:
            ExchangeMarketConfig: Configuration for the exchange/market pair

        Raises:
            ValueError: If no config found for the exchange/market pair
        """
        if not self._configs:
            self.load_all()

        # Try different key formats for backward compatibility
        keys_to_try = [
            f"{exchange}_{market_type}" if market_type else None,
            exchange,
        ]

        for key in keys_to_try:
            if key and key in self._configs:
                return self._configs[key]

        # Try to load it on-demand
        if market_type:
            try:
                config = config_loader.load_exchange_config(exchange, market_type)
                self._configs[f"{exchange}_{market_type}"] = config
                return config
            except Exception as e:
                logger.error(f"Failed to load config for {exchange}/{market_type}: {e}")

        raise ValueError(f"No config found for {exchange}/{market_type}")


# Initialize global registry
config_registry = ConfigRegistry()
