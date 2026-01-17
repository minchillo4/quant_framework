import asyncio
import logging
from typing import Any

import aiohttp

logger = logging.getLogger(__name__)


class CoinMetricsClient:
    """Async client for Coin Metrics Community API."""

    def __init__(self, base_url: str = "https://community-api.coinmetrics.io/v4"):
        self.base_url = base_url
        self.session: aiohttp.ClientSession | None = None
        self.rate_limit_delay = 0.6  # 10 requests per 6 seconds = 0.6s delay

    async def __aenter__(self):
        self.session = aiohttp.ClientSession()
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        if self.session:
            await self.session.close()

    async def _make_request(
        self, endpoint: str, params: dict[str, Any] = None
    ) -> dict[str, Any]:
        """Make rate-limited request to Coin Metrics API."""
        if not self.session:
            raise RuntimeError("Client not initialized")

        url = f"{self.base_url}/{endpoint}"

        # Rate limiting
        await asyncio.sleep(self.rate_limit_delay)

        async with self.session.get(url, params=params) as response:
            if response.status == 200:
                return await response.json()
            elif response.status == 429:
                # Handle rate limit
                retry_after = int(response.headers.get("Retry-After", 6))
                logger.warning(f"Rate limited, waiting {retry_after}s")
                await asyncio.sleep(retry_after)
                return await self._make_request(endpoint, params)
            else:
                text = await response.text()
                raise Exception(f"HTTP {response.status}: {text}")

    async def get_assets(self) -> list[str]:
        """Get list of available assets."""
        data = await self._make_request("catalog/assets")
        return [asset["asset"] for asset in data.get("data", [])]


# CSV-based client for GitHub archives
import csv
import io


class CoinMetricsCSVClient:
    """Client for Coin Metrics CSV archives from GitHub."""

    def __init__(
        self,
        base_url: str = "https://raw.githubusercontent.com/coinmetrics/data/master/csv",
    ):
        self.base_url = base_url
        self.session = None

    async def __aenter__(self):
        self.session = aiohttp.ClientSession()
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        if self.session:
            await self.session.close()

    async def get_csv_data(self, symbol: str) -> list[dict]:
        """Get CSV data without using pandas."""
        if not self.session:
            raise RuntimeError("Session not initialized")

        url = f"{self.base_url}/{symbol}.csv"

        async with self.session.get(url) as response:
            if response.status != 200:
                raise Exception(f"Failed to fetch CSV: {response.status}")

            text = await response.text()

            # Parse CSV
            csv_reader = csv.DictReader(io.StringIO(text))
            data = list(csv_reader)

            return data
