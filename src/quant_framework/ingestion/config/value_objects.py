"""Configuration value objects for dependency injection.

Instead of injecting global settings object, inject specific configuration
dataclasses into each component. Enables:
- Easy testing with different configurations
- Clear constructor contracts
- Validation at composition root
"""

from dataclasses import dataclass


@dataclass(frozen=True)
class HttpClientConfig:
    """Configuration for HTTP client."""

    timeout: float = 30.0
    max_retries: int = 3
    backoff_factor: float = 1.5
    connect_timeout: float = 10.0


@dataclass(frozen=True)
class RetryConfig:
    """Configuration for retry behavior."""

    max_attempts: int = 3
    base_delay: float = 1.0
    max_delay: float = 60.0
    backoff_multiplier: float = 2.0
    retryable_status_codes: tuple[int, ...] = (429, 500, 502, 503, 504)


@dataclass(frozen=True)
class ValidationConfig:
    """Configuration for response validation."""

    strict_mode: bool = True  # Fail on unknown fields
    validate_nulls: bool = False  # Require non-null fields


@dataclass(frozen=True)
class CoinalyzeConfig:
    """Configuration for CoinAlyze API client."""

    base_url: str
    api_keys: list[str]
    request_interval: float = 0.1  # Minimum seconds between requests
    rate_limit: int = 100  # Requests per period
    rate_limit_period: float = 60.0  # Seconds
    http_config: HttpClientConfig = None
    retry_config: RetryConfig = None
    validation_config: ValidationConfig = None

    def __post_init__(self):
        """Set defaults for nested configs."""
        if self.http_config is None:
            object.__setattr__(self, "http_config", HttpClientConfig())
        if self.retry_config is None:
            object.__setattr__(self, "retry_config", RetryConfig())
        if self.validation_config is None:
            object.__setattr__(self, "validation_config", ValidationConfig())


@dataclass(frozen=True)
class BackfillConfig:
    """Configuration for backfill operations."""

    chunk_size_days: int = 30
    max_concurrent_chunks: int = 3
    batch_write_size: int = 1000
    enable_checkpointing: bool = True
    checkpoint_interval_chunks: int = 5


@dataclass(frozen=True)
class SymbolFormattingConfig:
    """Configuration for symbol formatting."""

    exchange: str
    use_normalized_symbols: bool = True
    contract_separator: str = ":"
