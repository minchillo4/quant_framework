import asyncio
import logging

from quant_framework.ingestion.config.value_objects import CoinalyzeConfig
from quant_framework.ingestion.ports import (
    IHttpClient,
    IResponseValidator,
    IRetryHandler,
)
from quant_framework.ingestion.ports.http import IApiKeyProvider

logger = logging.getLogger(__name__)


class CoinalyzeClient:
    """Async client for coinalyze API.

    Single Responsibility: Coordinate HTTP requests with proper headers,
    validation, error handling, and retry logic.

    Dependencies injected (not instantiated):
    - http_client: Executes HTTP requests
    - api_key_provider: Provides headers with API keys
    - response_validator: Validates response structures
    - retry_handler: Decides retry eligibility and delays
    """

    def __init__(
        self,
        config: CoinalyzeConfig,
        http_client: IHttpClient,
        api_key_provider: IApiKeyProvider,
        response_validator: IResponseValidator,
        retry_handler: IRetryHandler,
    ):
        """Initialize CoinalyzeClient with injected dependencies.

        Args:
            config: Configuration including base_url, api_keys, rate limits
            http_client: HTTP client implementation (e.g., AiohttpClient)
            api_key_provider: Provides headers with valid API keys (e.g., KeyRotator)
            response_validator: Validates response structures
            retry_handler: Handles retry decisions and delays
        """
        self.config = config
        self.http_client = http_client
        self.api_key_provider = api_key_provider
        self.response_validator = response_validator
        self.retry_handler = retry_handler
        self.base_url = config.base_url

    async def fetch_data_async(self, endpoint: str, params: dict) -> dict:
        """Fetch data from coinalyze API with retry logic.

        Coordinates HTTP requests, validation, error handling, and retries.
        Does NOT implement these concerns - delegates to injected dependencies.

        Args:
            endpoint: API endpoint to call (e.g., "ohlcv-history")
            params: Query parameters for the request

        Returns:
            Parsed JSON response data

        Raises:
            Various CoinAlyzeAPIError subclasses based on error type
        """
        url = f"{self.base_url}/{endpoint}"
        max_attempts = self.config.retry_config.max_attempts

        logger.debug(f"🔍 Request: {endpoint}")
        logger.debug(f"🔍 URL: {url}")
        logger.debug(f"🔍 Params: {params}")

        for attempt_number in range(1, max_attempts + 1):
            try:
                # Get headers with API key from provider
                headers = await self.api_key_provider.get_headers()

                # Execute HTTP request via injected client
                response = await self.http_client.get(
                    url,
                    params=params,
                    headers=headers,
                    timeout=self.config.http_config.timeout,
                )

                # Success response
                if response.status_code == 200:
                    # Validate response structure via injected validator
                    validation_result = self.response_validator.validate(
                        endpoint, response.body
                    )

                    if not validation_result.is_valid:
                        logger.warning(
                            f"⚠️ Response validation failed for {endpoint}: "
                            f"{validation_result.error_message}"
                        )
                        from .exceptions import ValidationError

                        raise ValidationError(
                            f"Invalid response structure for {endpoint}: "
                            f"{validation_result.error_message}",
                            status_code=200,
                            endpoint=endpoint,
                        )

                    logger.info(
                        f"✅ Successfully fetched {endpoint} for "
                        f"{params.get('symbols')}"
                    )
                    return response.body

                # Error response - check if retryable
                if not self.retry_handler.should_retry(response.status_code):
                    # Non-retryable error - fail immediately
                    # Import here to avoid circular imports
                    from .error_handlers import create_error_mapper_chain

                    chain = create_error_mapper_chain()
                    error = chain.map_error(
                        response.status_code, response.body, endpoint
                    )
                    logger.error(f"❌ Non-retryable error for {endpoint}: {error}")
                    raise error

                # Retryable error - check if we have attempts left
                if attempt_number < max_attempts:
                    sleep_time = self.retry_handler.get_retry_delay(
                        attempt_number,
                        response.status_code,
                        response.headers,
                    )
                    logger.warning(
                        f"⚠️ Retryable error (attempt {attempt_number}/{max_attempts}), "
                        f"sleeping {sleep_time}s before retry"
                    )
                    await asyncio.sleep(sleep_time)
                    continue
                else:
                    # Out of retries
                    from .error_handlers import create_error_mapper_chain

                    chain = create_error_mapper_chain()
                    error = chain.map_error(
                        response.status_code, response.body, endpoint
                    )
                    logger.error(f"❌ Max retries reached for {endpoint}: {error}")
                    raise error

            except Exception as e:
                # Network or other errors
                if attempt_number < max_attempts:
                    sleep_time = self.retry_handler.get_retry_delay(
                        attempt_number, 500, None
                    )
                    logger.warning(
                        f"⚠️ Error (attempt {attempt_number}/{max_attempts}), "
                        f"sleeping {sleep_time}s: {e}"
                    )
                    await asyncio.sleep(sleep_time)
                    continue
                else:
                    logger.error(f"❌ Failed after {max_attempts} attempts: {e}")
                    raise RuntimeError(
                        f"Failed to fetch {endpoint} after {max_attempts} attempts: {e}"
                    )

        # Should not reach here
        raise RuntimeError(f"Failed to fetch {endpoint} after {max_attempts} attempts")
