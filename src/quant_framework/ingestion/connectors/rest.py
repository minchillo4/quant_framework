"""
REST connectors for market data ingestion.
Shared CCXT utilities and HTTP client wrappers.
"""

import logging

import aiohttp

logger = logging.getLogger(__name__)


class RestConnector:
    """
    REST connector for HTTP-based data fetching.
    Currently a placeholder for future abstraction of CCXT HTTP client.
    """

    def __init__(self, timeout: int = 30):
        """Initialize REST connector."""
        self.timeout = timeout
        self.session: aiohttp.ClientSession | None = None

    async def __aenter__(self):
        """Async context manager entry."""
        self.session = aiohttp.ClientSession()
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        """Async context manager exit."""
        if self.session:
            await self.session.close()

    async def get(
        self,
        url: str,
        params: dict | None = None,
        headers: dict | None = None,
    ) -> dict:
        """Perform GET request."""
        if not self.session:
            raise RuntimeError("Connector not initialized (use async context manager)")

        async with self.session.get(
            url, params=params, headers=headers, timeout=self.timeout
        ) as response:
            if response.status == 200:
                return await response.json()
            else:
                raise Exception(f"HTTP {response.status}: {await response.text()}")
