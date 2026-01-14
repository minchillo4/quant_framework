import asyncio
import logging
from datetime import datetime
from typing import Any

import aiohttp
import pandas as pd

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

    async def get_metric_timeseries(
        self,
        asset: str,
        metrics: list[str],
        start_time: datetime | None = None,
        end_time: datetime | None = None,
        limit: int = 100,
    ) -> pd.DataFrame:
        """Get timeseries metrics for an asset."""
        params = {
            "assets": asset,
            "metrics": ",".join(metrics),
            "limit": min(limit, 10000),  # API max
        }

        if start_time:
            params["start_time"] = start_time.isoformat()
        if end_time:
            params["end_time"] = end_time.isoformat()

        data = await self._make_request("timeseries/asset-metrics", params)

        # Convert to DataFrame
        df = pd.DataFrame(data.get("data", []))
        if not df.empty:
            df["time"] = pd.to_datetime(df["time"])
            df = df.set_index("time")

        return df

    async def get_available_metrics(self, asset: str) -> list[str]:
        """Get available metrics for an asset."""
        data = await self._make_request(f"catalog/asset-metrics/{asset}")
        return [metric["metric"] for metric in data.get("data", [])]


# CSV-based client for GitHub archives
class CoinMetricsCSVClient:
    """Client for Coin Metrics CSV archives from GitHub."""

    def __init__(
        self,
        base_url: str = "https://raw.githubusercontent.com/coinmetrics/data/master/csv",
    ):
        self.base_url = base_url

    async def get_csv_data(self, symbol: str) -> pd.DataFrame:
        """Get CSV data for a symbol."""
        url = f"{self.base_url}/{symbol}.csv"

        # Use pandas to read CSV directly from URL
        df = pd.read_csv(url)

        # Convert time column to datetime if present
        if "time" in df.columns:
            df["time"] = pd.to_datetime(df["time"])
            df = df.set_index("time")

        return df
