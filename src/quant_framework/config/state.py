"""
Unified configuration state management inspired by SharedState pattern.

This module provides a single source of truth for all application configuration,
combining hierarchical YAML files with environment overrides, type validation,
and sensible defaults.
"""

import logging
import os
from pathlib import Path
from typing import Any

import yaml
from pydantic import BaseModel, Field, field_validator

logger = logging.getLogger(__name__)


# =============================================================================
# PYDANTIC MODELS - Type-Safe Configuration
# =============================================================================


class AssetConfig(BaseModel):
    """Asset universe configuration."""

    full_universe: list[str] = Field(default_factory=lambda: ["BTC", "ETH"])
    timeframes: list[str] = Field(
        default_factory=lambda: ["5m", "15m", "1h", "4h", "1d"]
    )

    class Config:
        extra = "allow"


class AssetsConfig(BaseModel):
    """Assets section configuration."""

    crypto: AssetConfig = Field(default_factory=AssetConfig)

    class Config:
        extra = "allow"


class DatabaseConfig(BaseModel):
    """Database connection and tuning configuration."""

    url: str = Field(default="postgresql://quant_app:password@timescaledb:5432/quant")
    pool_size: int = Field(default=20, ge=1, le=500)
    max_overflow: int = Field(default=10, ge=0, le=200)
    pool_timeout: int = Field(default=30, ge=1, le=300)
    statement_timeout: int = Field(default=30000, ge=1000)
    batch_size: int = Field(default=500, ge=1)

    @field_validator("url")
    @classmethod
    def validate_url(cls, v: str) -> str:
        """Allow environment variable override."""
        if not v or v.startswith("postgresql://"):
            return v
        raise ValueError("Database URL must start with postgresql://")

    class Config:
        extra = "allow"


class CoinalyzeConfig(BaseModel):
    """CoinAlyze API configuration."""

    base_url: str = Field(default="https://api.coinalyze.net/v1")
    api_keys: list[str] = Field(default_factory=list)
    rate_limit: int = Field(default=60, ge=1)
    request_interval: float = Field(default=1.0, ge=0.1)

    class Config:
        extra = "allow"


class RedisConfig(BaseModel):
    """Redis connection configuration."""

    redis_url: str = Field(default="redis://redis:6379/0")

    class Config:
        extra = "allow"


class EndpointSupportConfig(BaseModel):
    """Which data endpoints are supported."""

    ohlc: bool = Field(default=True)
    open_interest: bool = Field(default=True)
    funding_rate: bool = Field(default=False)
    liquidation: bool = Field(default=False)

    class Config:
        extra = "allow"


class BatchLimitConfig(BaseModel):
    """Batch size limits per endpoint."""

    ohlc: int = Field(default=1000, ge=1)
    open_interest: int = Field(default=500, ge=1)

    class Config:
        extra = "allow"


class ValidationConfig(BaseModel):
    """Data validation settings."""

    require_open_interest: bool = Field(default=False)
    require_funding_rate: bool = Field(default=False)
    price_precision: int = Field(default=8, ge=0)
    volume_precision: int = Field(default=8, ge=0)

    class Config:
        extra = "allow"


class MarketTypeConfig(BaseModel):
    """Configuration for a specific market type (spot, linear, inverse, etc)."""

    market_type: str
    settlement_currency: str | None = Field(default=None)
    instrument_type: str = Field(default="spot")
    margin_type: str | None = Field(default=None)
    contract_size: float = Field(default=1.0)

    supported_data_types: list[str] = Field(default_factory=list)
    validation: ValidationConfig = Field(default_factory=ValidationConfig)
    endpoint_support: EndpointSupportConfig = Field(
        default_factory=EndpointSupportConfig
    )

    class Config:
        extra = "allow"


class ExchangeMarketConfig(BaseModel):
    """Combined configuration for exchange + market type."""

    exchange: str
    market_type: str
    api_base_url: str
    ws_base_url: str | None = Field(default=None)
    ccxt_class: str

    rest_rate_limit: int = Field(default=1200, ge=1)
    batch_limits: BatchLimitConfig = Field(default_factory=BatchLimitConfig)
    supported_timeframes: list[str] = Field(
        default_factory=lambda: ["1m", "5m", "15m", "1h", "4h", "1d"]
    )
    supported_oi_timeframes: list[str] | None = Field(default_factory=list)

    retry_max: int = Field(default=5, ge=1, le=50)
    retry_delay: float = Field(default=2.0, ge=0.1)

    ws_max_connections: int | None = Field(default=None)
    ws_max_streams_per_connection: int | None = Field(default=None)

    class Config:
        extra = "allow"


class LoggingConfig(BaseModel):
    """Logging configuration."""

    level: str = Field(default="INFO")
    format: str = Field(default="%(asctime)s - %(name)s - %(levelname)s - %(message)s")

    class Config:
        extra = "allow"


class ConfigState(BaseModel):
    """
    Root configuration state - single source of truth for all app config.

    Inspired by SharedState pattern: provides unified access to all settings
    with type safety, validation, and sensible defaults.
    """

    assets: AssetsConfig = Field(default_factory=AssetsConfig)
    database: DatabaseConfig = Field(default_factory=DatabaseConfig)
    coinalyze: CoinalyzeConfig = Field(default_factory=CoinalyzeConfig)
    redis: RedisConfig = Field(default_factory=RedisConfig)
    logging: LoggingConfig = Field(default_factory=LoggingConfig)

    # Exchange configurations - loaded dynamically
    exchanges: dict[str, ExchangeMarketConfig] = Field(default_factory=dict)

    # Environment metadata
    env: str = Field(default="dev")
    config_dir: str = Field(default="/app/config")

    class Config:
        extra = "allow"  # Allow additional fields from YAML


# =============================================================================
# CONFIG LOADER - Clean, Validated Loading
# =============================================================================


class ConfigLoader:
    """
    Load and validate configuration from hierarchical YAML files.

    Merges:
      1. Global defaults (hardcoded)
      2. YAML files from config_dir
      3. Environment variable overrides
      4. Per-exchange/market-type configs
    """

    def __init__(self, config_dir: str = "/app/config"):
        self.config_dir = Path(config_dir)
        self._yaml_cache: dict[Path, Any] = {}
        self.env = os.getenv("MNEMO_ENV", "dev")

    def _load_yaml(self, path: Path) -> dict[str, Any]:
        """Load YAML file with caching."""
        if path in self._yaml_cache:
            return self._yaml_cache[path]

        if not path.exists():
            logger.debug(f"Config file not found (using defaults): {path}")
            return {}

        try:
            with open(path, encoding="utf-8") as f:
                data = yaml.safe_load(f) or {}
                self._yaml_cache[path] = data
                logger.debug(f"Loaded config: {path.relative_to(self.config_dir)}")
                return data
        except Exception as e:
            logger.error(f"Failed to load {path}: {e}")
            return {}

    def _apply_env_overrides(self, config: dict[str, Any]) -> dict[str, Any]:
        """Apply environment variable overrides to config."""
        # Database URL
        if db_url := os.getenv("TIMESCALE_URL"):
            config.setdefault("database", {})["url"] = db_url

        # Redis URL
        if redis_url := os.getenv("REDIS_URL"):
            config.setdefault("redis", {})["redis_url"] = redis_url

        # CoinAlyze API keys (support both singular and plural)
        api_keys_str = os.getenv("COINALYZE_API_KEYS") or os.getenv("COINALYZE_API_KEY")
        if api_keys_str:
            # Split by comma and strip whitespace
            api_keys = [key.strip() for key in api_keys_str.split(",") if key.strip()]
            if api_keys:
                config.setdefault("coinalyze", {})["api_keys"] = api_keys

        # Log level
        if log_level := os.getenv("LOG_LEVEL"):
            config.setdefault("logging", {})["level"] = log_level

        return config

    def _merge_dicts(self, base: dict, override: dict) -> dict:
        """Deep merge override into base dict."""
        result = base.copy()
        for key, value in override.items():
            if (
                key in result
                and isinstance(result[key], dict)
                and isinstance(value, dict)
            ):
                result[key] = self._merge_dicts(result[key], value)
            else:
                result[key] = value
        return result

    def load(self) -> ConfigState:
        """
        Load complete configuration state.

        Returns:
            ConfigState: Validated configuration object

        Raises:
            ValidationError: If configuration is invalid
        """
        logger.info(f"Loading configuration from {self.config_dir} (env: {self.env})")

        # Load config files in order of specificity
        config: dict[str, Any] = {}

        # 1. Load top-level YAML files
        for config_file in [
            "assets.yaml",
            "database.yaml",
            "coinalyze.yaml",
            "redis.yaml",
        ]:
            file_config = self._load_yaml(self.config_dir / config_file)
            config = self._merge_dicts(config, file_config)

        # 2. Load environment-specific overrides
        env_config = self._load_yaml(self.config_dir / "env" / f"{self.env}.yaml")
        config = self._merge_dicts(config, env_config)

        # 3. Apply environment variable overrides
        config = self._apply_env_overrides(config)

        # 4. Create ConfigState with validation
        try:
            state = ConfigState(env=self.env, config_dir=str(self.config_dir), **config)
            logger.info(
                f"✅ Configuration loaded: assets={len(state.assets.crypto.full_universe)} "
                f"currencies, db={state.database.pool_size} pool, "
                f"coinalyze_keys={len(state.coinalyze.api_keys)}"
            )
            return state
        except Exception as e:
            logger.error(f"Configuration validation failed: {e}")
            raise

    def load_exchange_config(
        self,
        exchange: str,
        market_type: str,
        defaults: ExchangeMarketConfig | None = None,
    ) -> ExchangeMarketConfig:
        """
        Load configuration for specific exchange and market type.

        Merges:
          1. Global CCXT defaults
          2. Market type template
          3. Exchange-specific config
          4. Exchange+market type specific config
        """
        exchange_lower = exchange.lower()
        market_type_lower = market_type.lower()

        result_config: dict[str, Any] = {}

        # 1. Global defaults
        if defaults:
            result_config = defaults.model_dump(exclude_unset=True)

        # 2. Market type template
        market_type_config = self._load_yaml(
            self.config_dir / "market_types" / f"{market_type_lower}.yaml"
        )
        if market_type_config:
            result_config = self._merge_dicts(result_config, market_type_config)

        # 3. Exchange-specific config
        exchange_config_path = (
            self.config_dir
            / "data_sources"
            / "ccxt"
            / "exchanges"
            / f"{exchange_lower}.yaml"
        )
        if exchange_config_path.exists():
            exchange_file_config = self._load_yaml(exchange_config_path)
            # Extract market type specific section if present
            if "market_types" in exchange_file_config:
                market_config = exchange_file_config["market_types"].get(
                    market_type_lower, {}
                )
                # Merge base exchange config with market-specific
                base_exchange_config = {
                    k: v for k, v in exchange_file_config.items() if k != "market_types"
                }
                result_config = self._merge_dicts(result_config, base_exchange_config)
                result_config = self._merge_dicts(result_config, market_config)
            else:
                result_config = self._merge_dicts(result_config, exchange_file_config)

        # Ensure required fields
        result_config.setdefault("exchange", exchange_lower)
        result_config.setdefault("market_type", market_type_lower)
        if "ccxt_class" not in result_config:
            result_config["ccxt_class"] = f"{exchange_lower}_{market_type_lower}"
        if "api_base_url" not in result_config:
            result_config["api_base_url"] = f"https://api.{exchange_lower}.com"

        try:
            return ExchangeMarketConfig(**result_config)
        except Exception as e:
            logger.error(f"Failed to load config for {exchange}/{market_type}: {e}")
            raise


# =============================================================================
# GLOBAL INSTANCE
# =============================================================================


def get_config(config_dir: str | None = None) -> ConfigState:
    """
    Load and return the global configuration state.

    Args:
        config_dir: Override config directory. Defaults to /app/config or ./config

    Returns:
        ConfigState: Validated configuration object
    """
    if config_dir is None:
        config_dir = os.getenv("MNEMO_CONFIG_DIR", "/app/config")
        if not Path(config_dir).exists():
            if Path("./config").exists():
                config_dir = "./config"
            else:
                logger.warning(
                    f"Config directory not found at {config_dir}, using defaults"
                )

    loader = ConfigLoader(config_dir=config_dir)
    return loader.load()


__all__ = [
    "AssetConfig",
    "AssetsConfig",
    "BatchLimitConfig",
    "ConfigLoader",
    "ConfigState",
    "CoinalyzeConfig",
    "DatabaseConfig",
    "EndpointSupportConfig",
    "ExchangeMarketConfig",
    "LoggingConfig",
    "MarketTypeConfig",
    "RedisConfig",
    "ValidationConfig",
    "get_config",
]
