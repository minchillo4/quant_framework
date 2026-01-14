"""Dependency injection container for CoinAlyze ingestion layer.

Wires all Phase 1 abstractions and their implementations together.
This is the single place where concrete implementations are chosen.

Usage:
    container = CoinalyzeDependencyContainer(config)
    client = container.create_coinalyze_client()
"""

from quant_framework.ingestion.adapters.coinalyze_plugin.client import (
    CoinalyzeClient,
)
from quant_framework.ingestion.adapters.coinalyze_plugin.impls import (
    CoinAlyzeResponseValidator,
    CoinAlyzeRetryHandler,
)
from quant_framework.ingestion.adapters.coinalyze_plugin.key_rotator import (
    KeyRotator,
)
from quant_framework.ingestion.config.value_objects import (
    CoinalyzeConfig,
    HttpClientConfig,
    RetryConfig,
    ValidationConfig,
)
from quant_framework.ingestion.connectors.aiohttp_client import AiohttpClient
from quant_framework.ingestion.ports import (
    IHttpClient,
    IResponseValidator,
    IRetryHandler,
)
from quant_framework.ingestion.ports.http import IApiKeyProvider


class CoinalyzeDependencyContainer:
    """Dependency injection container for CoinAlyze client and adapters.

    Responsible for:
    1. Choosing concrete implementations for each protocol
    2. Creating configuration value objects
    3. Wiring dependencies together
    4. Providing factory methods for components

    This is the single place where the abstraction/implementation choice
    is made. Tests can subclass this and override methods to inject mocks.
    """

    def __init__(
        self,
        base_url: str,
        api_keys: list[str],
        http_config: HttpClientConfig | None = None,
        retry_config: RetryConfig | None = None,
        validation_config: ValidationConfig | None = None,
    ):
        """Initialize container with configuration.

        Args:
            base_url: CoinAlyze API base URL
            api_keys: List of API keys for rotation
            http_config: HTTP client configuration (optional, uses defaults)
            retry_config: Retry behavior configuration (optional, uses defaults)
            validation_config: Response validation configuration (optional, uses defaults)
        """
        self.http_config = http_config or HttpClientConfig()
        self.retry_config = retry_config or RetryConfig()
        self.validation_config = validation_config or ValidationConfig()

        self.config = CoinalyzeConfig(
            base_url=base_url,
            api_keys=api_keys,
            http_config=self.http_config,
            retry_config=self.retry_config,
            validation_config=self.validation_config,
        )

    def create_http_client(self) -> IHttpClient:
        """Create HTTP client implementation.

        Override this in tests to inject a mock HTTP client.

        Returns:
            IHttpClient implementation (currently AiohttpClient)
        """
        return AiohttpClient(self.http_config)

    def create_api_key_provider(self) -> IApiKeyProvider:
        """Create API key provider implementation.

        Override this in tests to inject a mock key provider.

        Returns:
            IApiKeyProvider implementation (currently KeyRotator)
        """
        return KeyRotator(self.config.api_keys)

    def create_response_validator(self) -> IResponseValidator:
        """Create response validator implementation.

        Override this in tests to inject a mock validator.

        Returns:
            IResponseValidator implementation (currently CoinAlyzeResponseValidator)
        """
        return CoinAlyzeResponseValidator(self.validation_config)

    def create_retry_handler(self) -> IRetryHandler:
        """Create retry handler implementation.

        Override this in tests to inject a mock retry handler.

        Returns:
            IRetryHandler implementation (currently CoinAlyzeRetryHandler)
        """
        return CoinAlyzeRetryHandler(self.retry_config)

    def create_coinalyze_client(self) -> CoinalyzeClient:
        """Create fully-wired CoinalyzeClient.

        Assembles all injected dependencies:
        - Configuration
        - HTTP client
        - API key provider
        - Response validator
        - Retry handler

        Returns:
            CoinalyzeClient with all dependencies injected
        """
        return CoinalyzeClient(
            config=self.config,
            http_client=self.create_http_client(),
            api_key_provider=self.create_api_key_provider(),
            response_validator=self.create_response_validator(),
            retry_handler=self.create_retry_handler(),
        )


def create_coinalyze_client_from_settings(
    settings,
) -> CoinalyzeClient:
    """Factory function to create CoinalyzeClient from settings object.

    This bridges the old settings-based approach with the new
    dependency injection container approach.

    Args:
        settings: Settings object with coinalyze configuration

    Returns:
        Fully configured CoinalyzeClient
    """
    container = CoinalyzeDependencyContainer(
        base_url=settings.coinalyze.base_url,
        api_keys=settings.coinalyze.api_keys,
    )
    return container.create_coinalyze_client()
