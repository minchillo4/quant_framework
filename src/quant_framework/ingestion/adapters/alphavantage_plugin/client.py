"""
AlphaVantage HTTP client with rate limiting and API key rotation.

Handles AlphaVantage-specific error formats and provides a clean interface
for making API requests with proper rate limiting and error handling.
"""

from __future__ import annotations

import asyncio
import logging
import time
from typing import Any, Dict, List

from quant_framework.config.state import AlphaVantageConfig

logger = logging.getLogger(__name__)


class AlphaVantageError(Exception):
    """Base exception for AlphaVantage API errors."""

    pass


class RateLimitError(AlphaVantageError):
    """Raised when AlphaVantage rate limit is exceeded."""

    pass


class InvalidRequestError(AlphaVantageError):
    """Raised when AlphaVantage request is invalid."""

    pass


class AlphaVantageClient:
    """HTTP client for AlphaVantage API with rate limiting and key rotation."""

    def __init__(self, config: AlphaVantageConfig):
        self.config = config
        self.api_keys = config.api_keys.copy()
        if not self.api_keys:
            raise ValueError("No API keys provided for AlphaVantage client")

        self._current_key_index = 0
        self._last_request_time = 0.0
        self._semaphore = asyncio.Semaphore(1)  # Ensure sequential requests

        logger.info(
            f"AlphaVantage client initialized with {len(self.api_keys)} API keys"
        )

    @property
    def current_api_key(self) -> str:
        """Get the current API key."""
        return self.api_keys[self._current_key_index]

    def _rotate_api_key(self) -> None:
        """Rotate to the next API key."""
        self._current_key_index = (self._current_key_index + 1) % len(self.api_keys)
        logger.debug(f"Rotated to API key index {self._current_key_index}")

    async def _wait_for_rate_limit(self) -> None:
        """Wait if necessary to respect rate limits."""
        current_time = time.time()
        time_since_last_request = current_time - self._last_request_time

        if time_since_last_request < self.config.request_interval:
            wait_time = self.config.request_interval - time_since_last_request
            logger.debug(f"Rate limiting: waiting {wait_time:.2f} seconds")
            await asyncio.sleep(wait_time)

        self._last_request_time = time.time()

    def _parse_response(self, response_data: Dict[str, Any]) -> Dict[str, Any]:
        """Parse and validate AlphaVantage response."""
        # Check for rate limit message
        if "Note" in response_data:
            error_msg = response_data["Note"]
            if "API call frequency" in error_msg or "rate limit" in error_msg.lower():
                raise RateLimitError(f"AlphaVantage rate limit: {error_msg}")
            else:
                raise AlphaVantageError(f"AlphaVantage notice: {error_msg}")

        # Check for error message
        if "Error Message" in response_data:
            error_msg = response_data["Error Message"]
            raise InvalidRequestError(f"AlphaVantage error: {error_msg}")

        return response_data

    async def fetch_data(self, params: Dict[str, Any]) -> Dict[str, Any]:
        """
        Fetch data from AlphaVantage API with rate limiting and error handling.

        Args:
            params: Query parameters for the AlphaVantage API

        Returns:
            Parsed response data from AlphaVantage

        Raises:
            RateLimitError: If rate limit is exceeded
            InvalidRequestError: If request is invalid
            AlphaVantageError: For other API errors
        """
        async with self._semaphore:
            await self._wait_for_rate_limit()

            # Prepare request
            request_params = params.copy()
            request_params["apikey"] = self.current_api_key
            request_params["datatype"] = "json"

            # Build URL
            url = f"{self.config.base_url}"

            logger.debug(f"Making AlphaVantage request: {params}")

            try:
                import aiohttp

                async with aiohttp.ClientSession() as session:
                    async with session.get(url, params=request_params) as response:
                        if response.status != 200:
                            error_text = await response.text()
                            raise AlphaVantageError(
                                f"HTTP {response.status}: {error_text}"
                            )

                        response_data = await response.json()

                        # Parse and validate response
                        parsed_data = self._parse_response(response_data)

                        logger.debug(f"AlphaVantage request successful")
                        return parsed_data

            except aiohttp.ClientError as e:
                raise AlphaVantageError(f"Network error: {e}")
            except Exception as e:
                if isinstance(e, AlphaVantageError):
                    raise
                raise AlphaVantageError(f"Unexpected error: {e}")

    async def fetch_data_with_retry(
        self, params: Dict[str, Any], max_retries: int = 3
    ) -> Dict[str, Any]:
        """
        Fetch data with automatic retry on rate limit errors.

        Args:
            params: Query parameters for the AlphaVantage API
            max_retries: Maximum number of retry attempts

        Returns:
            Parsed response data from AlphaVantage
        """
        last_exception = None

        for attempt in range(max_retries + 1):
            try:
                return await self.fetch_data(params)
            except RateLimitError as e:
                last_exception = e

                if attempt < max_retries:
                    # Rotate API key and wait longer
                    self._rotate_api_key()
                    wait_time = self.config.request_interval * (attempt + 2)
                    logger.warning(
                        f"Rate limit hit, rotating API key and waiting {wait_time}s "
                        f"(attempt {attempt + 1}/{max_retries + 1})"
                    )
                    await asyncio.sleep(wait_time)
                else:
                    logger.error("Rate limit exceeded after all retries")
                    break
            except AlphaVantageError:
                # Don't retry on other errors (invalid request, etc.)
                raise

        # This should only be reached if we exhausted rate limit retries
        raise last_exception

    def get_api_keys_info(self) -> Dict[str, Any]:
        """Get information about current API keys."""
        return {
            "total_keys": len(self.api_keys),
            "current_key_index": self._current_key_index,
            "request_interval": self.config.request_interval,
            "rate_limit": self.config.rate_limit,
        }
