"""
Comprehensive dependency injection container for the entire ingestion layer.

Wires together:
- HTTP client (aiohttp wrapper)
- API key provider (key rotation)
- Response validator (schema validation)
- Error mapper (error classification)
- Retry handler (exponential backoff)
- Chunk generator (time-range splitting)
- Chunk processor (fetch + write)
- Reporter (progress/logging)
- Rate limiter
- Checkpoint manager
- Data adapter (unified backfill adapter)
"""

import logging
from typing import Any

from quant_framework.infrastructure.config.settings import settings
from quant_framework.ingestion.adapters.coinalyze_plugin.backfill_adapter import (
    CoinalyzeBackfillAdapter,
)
from quant_framework.ingestion.adapters.coinalyze_plugin.client import CoinalyzeClient
from quant_framework.ingestion.adapters.coinalyze_plugin.dependency_container import (
    CoinalyzeDependencyContainer,
)
from quant_framework.ingestion.adapters.coinalyze_plugin.error_handlers import (
    create_error_handler_chain,
)
from quant_framework.ingestion.adapters.coinalyze_plugin.impls import (
    CoinAlyzeResponseValidator,
    CoinAlyzeRetryHandler,
)
from quant_framework.ingestion.backfill.checkpoint_manager import (
    CheckpointManager,
)
from quant_framework.ingestion.backfill.chunk_generator import TimeRangeChunkGenerator
from quant_framework.ingestion.backfill.chunk_processor import BackfillChunkProcessor
from quant_framework.ingestion.backfill.dependency_container import (
    BackfillDependencyContainer,
)
from quant_framework.ingestion.backfill.rate_limiter import IRateLimiter
from quant_framework.ingestion.backfill.reporter import BackfillReporter
from quant_framework.ingestion.config.value_objects import (
    BackfillConfig,
    CoinalyzeConfig,
    HttpClientConfig,
    RetryConfig,
    ValidationConfig,
)
from quant_framework.ingestion.connectors.aiohttp_client import AiohttpClient
from quant_framework.ingestion.ports.backfill import (
    IBackfillReporter,
    IChunkGenerator,
    IChunkProcessor,
)
from quant_framework.ingestion.ports.http import IApiKeyProvider, IHttpClient
from quant_framework.ingestion.ports.validators import (
    IErrorMapper,
    IResponseValidator,
    IRetryHandler,
)

logger = logging.getLogger(__name__)


class IngestionDependencyContainer:
    """
    Master dependency injection container for entire ingestion layer.

    Single place where all concrete implementations are chosen.
    Enables complete testing/extension without modifying orchestration code.

    Usage:
        container = IngestionDependencyContainer(
            rate_limiter=rate_limiter,
            checkpoint_manager=checkpoint_manager,
            bronze_writer=bronze_writer,
        )

        # Get fully wired components
        http_client = container.create_http_client()
        coinalyze_client = container.create_coinalyze_client()
        data_adapter = container.create_data_adapter()
        backfill_coordinator = container.create_backfill_coordinator()
    """

    def __init__(
        self,
        rate_limiter: IRateLimiter,
        checkpoint_manager: CheckpointManager,
        bronze_writer: Any,
        http_config: HttpClientConfig | None = None,
        validation_config: ValidationConfig | None = None,
        retry_config: RetryConfig | None = None,
        backfill_config: BackfillConfig | None = None,
        verbose: bool = False,
    ):
        """
        Initialize ingestion dependency container.

        Args:
            rate_limiter: Rate limiting strategy
            checkpoint_manager: Checkpoint tracking manager
            bronze_writer: Writer for bronze layer
            http_config: HTTP client configuration (auto-loaded from settings if None)
            validation_config: Response validation config (auto-loaded if None)
            retry_config: Retry strategy config (auto-loaded if None)
            backfill_config: Backfill execution config (auto-loaded if None)
            verbose: Enable verbose logging
        """
        self.rate_limiter = rate_limiter
        self.checkpoint_manager = checkpoint_manager
        self.bronze_writer = bronze_writer
        self.verbose = verbose

        # Load configs from settings if not provided
        self.http_config = http_config or self._load_http_config()
        self.coinalyze_config = self._load_coinalyze_config()
        self.validation_config = validation_config or self._load_validation_config()
        self.retry_config = retry_config or self._load_retry_config()
        self.backfill_config = backfill_config or self._load_backfill_config()

        logger.info("IngestionDependencyContainer initialized")

    # ==================== HTTP Layer ====================

    def create_http_client(self) -> IHttpClient:
        """Create HTTP client implementation."""
        return AiohttpClient(config=self.http_config)

    def create_api_key_provider(self) -> IApiKeyProvider:
        """Create API key provider (delegates to Coinalyze container)."""
        coinalyze_container = CoinalyzeDependencyContainer(
            base_url=self.coinalyze_config.base_url,
            api_keys=self.coinalyze_config.api_keys,
        )
        return coinalyze_container.create_api_key_provider()

    def create_response_validator(self) -> IResponseValidator:
        """Create response validator implementation."""
        return CoinAlyzeResponseValidator(config=self.validation_config)

    def create_error_mapper(self) -> IErrorMapper:
        """Create error handler chain."""
        return create_error_handler_chain()

    def create_retry_handler(self) -> IRetryHandler:
        """Create retry handler implementation."""
        return CoinAlyzeRetryHandler(config=self.retry_config)

    # ==================== Coinalyze Client ====================

    def create_coinalyze_client(self) -> CoinalyzeClient:
        """Create fully wired Coinalyze API client."""
        return CoinalyzeClient(
            config=self.coinalyze_config,
            http_client=self.create_http_client(),
            api_key_provider=self.create_api_key_provider(),
            response_validator=self.create_response_validator(),
            retry_handler=self.create_retry_handler(),
        )

    # ==================== Data Adapter ====================

    def create_data_adapter(self) -> CoinalyzeBackfillAdapter:
        """Create unified backfill adapter with all dependencies."""
        return CoinalyzeBackfillAdapter(
            client=self.create_coinalyze_client(),
            rate_limiter=self.rate_limiter,
            verbose=self.verbose,
        )

    # ==================== Backfill Orchestration ====================

    def create_chunk_generator(self) -> IChunkGenerator:
        """Create chunk generator implementation."""
        return TimeRangeChunkGenerator(rate_limiter=self.rate_limiter)

    def create_chunk_processor(self) -> IChunkProcessor:
        """Create chunk processor implementation."""
        return BackfillChunkProcessor(
            data_adapter=self.create_data_adapter(),
            bronze_writer=self.bronze_writer,
        )

    def create_reporter(self) -> IBackfillReporter:
        """Create reporter implementation."""
        return BackfillReporter(verbose=self.verbose)

    def create_backfill_container(self) -> BackfillDependencyContainer:
        """Create backfill sub-container with all components."""
        return BackfillDependencyContainer(
            data_adapter=self.create_data_adapter(),
            bronze_writer=self.bronze_writer,
            rate_limiter=self.rate_limiter,
            checkpoint_manager=self.checkpoint_manager,
            verbose=self.verbose,
        )

    # ==================== Backfill Coordinator ====================

    def create_backfill_coordinator(self) -> Any:
        """
        Create fully wired BackfillCoordinator.

        Returns:
            BackfillCoordinator with all dependencies injected
        """
        from quant_framework.ingestion.backfill.coordinator import (
            BackfillCoordinator,
        )

        return BackfillCoordinator(
            data_adapter=self.create_data_adapter(),
            bronze_writer=self.bronze_writer,
            rate_limiter=self.rate_limiter,
            checkpoint_manager=self.checkpoint_manager,
            chunk_generator=self.create_chunk_generator(),
            chunk_processor=self.create_chunk_processor(),
            reporter=self.create_reporter(),
        )

    # ==================== Configuration Loading ====================

    def _load_http_config(self) -> HttpClientConfig:
        """Load HTTP client config from settings."""
        return HttpClientConfig(
            base_url=settings.coinalyze.base_url,
            timeout_seconds=settings.http.timeout_seconds or 30,
            max_retries=settings.http.max_retries or 3,
        )

    def _load_coinalyze_config(self) -> CoinalyzeConfig:
        """Load Coinalyze config from settings."""
        return CoinalyzeConfig(
            base_url=settings.coinalyze.base_url,
            api_keys=settings.coinalyze.api_keys,
        )

    def _load_validation_config(self) -> ValidationConfig:
        """Load validation config from settings."""
        return ValidationConfig(
            require_all_fields=True,
            strict_mode=False,
        )

    def _load_retry_config(self) -> RetryConfig:
        """Load retry config from settings."""
        return RetryConfig(
            max_retries=settings.backfill.max_retries or 3,
            initial_delay_seconds=settings.backfill.retry_delay_seconds or 1,
            max_delay_seconds=60,
            exponential_base=2.0,
        )

    def _load_backfill_config(self) -> BackfillConfig:
        """Load backfill config from settings."""
        return BackfillConfig(
            max_retries=settings.backfill.max_retries or 3,
            checkpoint_interval_chunks=settings.backfill.checkpoint_interval_chunks
            or 10,
        )


def create_ingestion_container(
    rate_limiter: IRateLimiter,
    checkpoint_manager: CheckpointManager,
    bronze_writer: Any,
    verbose: bool = False,
) -> IngestionDependencyContainer:
    """
    Factory function for creating fully configured ingestion container.

    Convenience wrapper; loads configs from settings automatically.

    Args:
        rate_limiter: Rate limiting strategy
        checkpoint_manager: Checkpoint tracking manager
        bronze_writer: Writer for bronze layer
        verbose: Enable verbose logging

    Returns:
        Configured IngestionDependencyContainer
    """
    return IngestionDependencyContainer(
        rate_limiter=rate_limiter,
        checkpoint_manager=checkpoint_manager,
        bronze_writer=bronze_writer,
        verbose=verbose,
    )
