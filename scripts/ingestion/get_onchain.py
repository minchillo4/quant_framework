import asyncio
import logging

from quant_framework.ingestion.adapters.coinmetrics_plugin.client import (
    CoinMetricsCSVClient,
)

logging.basicConfig(level=logging.INFO)


async def main():
    # Use async context manager para a versão com aiohttp
    async with CoinMetricsCSVClient() as client:
        # Get Bitcoin metrics
        btc_metrics = await client.get_csv_data("btc")  # ou get_csv_data_raw
        print(f"Found {len(btc_metrics)} metrics for Bitcoin")
        print(btc_metrics[:5])  # Print first 5 records


if __name__ == "__main__":
    asyncio.run(main())
