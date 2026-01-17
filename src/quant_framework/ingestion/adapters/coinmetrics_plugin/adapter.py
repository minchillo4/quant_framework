# ingestion/adapters/coinmetrics_plugin/adapter.py
import logging

import aiohttp

from quant_framework.ingestion.adapters.base import BaseAdapter
from quant_framework.ingestion.ports.data_ports import OnChainDataPort
from quant_framework.shared.models.enums import (
    ClientType,
    ConnectionType,
    DataVenue,
    WrapperImplementation,
)

logger = logging.getLogger(__name__)


class CoinMetricsCsvAdapter(BaseAdapter, OnChainDataPort):
    """CoinMetrics CSV adapter - ONLY downloads CSV data."""

    # Required by BaseAdapter
    venue: DataVenue = DataVenue.COINMETRICS
    wrapper: WrapperImplementation = WrapperImplementation.COINMETRICS
    client_type: ClientType = ClientType.FILE_PARSER
    connection_type: ConnectionType = ConnectionType.FILE
    capabilities = {OnChainDataPort}

    def __init__(self, github_repo: str = "coinmetrics/data"):
        """Initialize adapter.

        Args:
            github_repo: GitHub repository path
        """
        self.github_repo = github_repo
        self.session: aiohttp.ClientSession | None = None

    async def connect(self) -> None:
        """Connect to GitHub (create HTTP session)."""
        self.session = aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=30))

    async def close(self) -> None:
        """Close HTTP session."""
        if self.session:
            await self.session.close()
            self.session = None

    async def fetch_onchain_history(
        self,
        asset: str,
        **kwargs,  # Accept but ignore start/end for now
    ) -> bytes:
        """
        Download CSV from GitHub.

        Returns:
            CSV content as bytes

        Raises:
            aiohttp.ClientError: If download fails
            ValueError: If asset not found
        """
        if not self.session:
            await self.connect()

        csv_url = f"https://raw.githubusercontent.com/{self.github_repo}/master/csv/{asset}.csv"

        logger.info(f"Downloading CSV from {csv_url}")

        try:
            async with self.session.get(csv_url) as response:
                if response.status == 404:
                    raise ValueError(
                        f"Asset '{asset}' not found in CoinMetrics CSV repository"
                    )
                response.raise_for_status()

                csv_content = await response.read()
                logger.info(f"Downloaded {len(csv_content)} bytes for {asset}")

                return csv_content

        except aiohttp.ClientError as e:
            logger.error(f"Failed to download CSV for {asset}: {e}")
            raise

    # Optional: Helper method for discovery
    async def list_available_assets(self) -> list[str]:
        """List assets available in CSV repository.

        Note: This might be heavy - GitHub doesn't have easy directory listing.
        Alternative: Hardcode known assets or fetch from another source.
        """
        # For simplicity, return common assets
        return ["btc", "eth", "sol", "ada", "dot", "matic", "avax", "link"]

    async def __aenter__(self):
        await self.connect()
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        await self.close()
