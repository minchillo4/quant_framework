"""
Minimalist Coinalyze DAG - Fetch and Log Only
==============================================

Simple DAG that:
✅ Fetches OHLCV data from Coinalyze
✅ Logs the results with clear output
✅ Returns basic metrics
❌ No MinIO storage (for now)
❌ No checkpointing (for now)
"""

import asyncio
import logging
from datetime import datetime, timedelta
from typing import Any

from airflow import DAG
from airflow.decorators import task

from quant_framework.ingestion.adapters.coinalyze_plugin.ohlcv_adapter import (
    CoinalyzeOHLCVAdapter,
)
from quant_framework.shared.models.enums import (
    AssetClass,
    MarketType,
    WrapperImplementation,
)
from quant_framework.shared.models.instruments import Instrument

logger = logging.getLogger(__name__)

# ========================================
# DAG Configuration
# ========================================

default_args = {
    "owner": "mnemo-quant",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

# ========================================
# Helper Functions
# ========================================


def create_test_instrument() -> Instrument:
    """Create a test instrument for BTC USDT perpetual on Binance."""
    return Instrument(
        instrument_id="BTCUSDT_PERP_BINANCE",
        base_asset="BTC",
        quote_asset="USDT",
        asset_class=AssetClass.CRYPTO,
        market_type=MarketType.LINEAR_PERPETUAL,
        venue="binance",
        wrapper=WrapperImplementation.COINALYZE,
        symbol="BTCUSDT_PERP.A",  # Binance perpetual format
        is_active=True,
    )


# ========================================
# Tasks
# ========================================


@task
def fetch_coinalyze_data(timeframe: str = "1h", hours_back: int = 24) -> dict[str, Any]:
    """
    Fetch OHLCV data from Coinalyze API.

    Args:
        timeframe: Timeframe to fetch (1m, 5m, 15m, 1h, 4h, 1d)
        hours_back: How many hours of data to fetch

    Returns:
        Dictionary with raw data and metadata
    """

    async def async_fetch():
        # Calculate time range
        end_time = datetime.utcnow()
        start_time = end_time - timedelta(hours=hours_back)

        logger.info("🚀 Starting Coinalyze fetch")
        logger.info(f"📊 Timeframe: {timeframe}")
        logger.info(f"🕐 Time range: {start_time} to {end_time}")

        # Create instrument
        instrument = create_test_instrument()
        logger.info(f"🎯 Instrument: {instrument.instrument_id} ({instrument.symbol})")

        # Fetch data with injected client from dependency container
        from quant_framework.infrastructure.config.settings import settings
        from quant_framework.ingestion.adapters.coinalyze_plugin.dependency_container import (
            CoinalyzeDependencyContainer,
        )

        container = CoinalyzeDependencyContainer(
            base_url=settings.coinalyze.base_url,
            api_keys=settings.coinalyze.api_keys,
        )
        client = container.create_coinalyze_client()

        adapter = CoinalyzeOHLCVAdapter(client=client)

        logger.info("⬇️ Fetching data from Coinalyze...")
        records = await adapter.fetch_ohlcv(
            instrument=instrument,
            timeframe=timeframe,
            start=start_time,
            end=end_time,
        )

        # Convert generator to list
        records_list = list(records)

        logger.info(f"✅ Fetch complete! Retrieved {len(records_list)} records")

        return {
            "records": records_list,
            "instrument_id": instrument.instrument_id,
            "symbol": instrument.symbol,
            "timeframe": timeframe,
            "start_time": start_time.isoformat(),
            "end_time": end_time.isoformat(),
            "record_count": len(records_list),
        }

    # Run async function
    return asyncio.run(async_fetch())


@task
def log_and_analyze(data: dict[str, Any]) -> dict[str, Any]:
    """
    Log fetched data and return metrics.

    Args:
        data: Data dictionary from fetch task

    Returns:
        Dictionary with metrics and summary
    """
    records = data["records"]

    logger.info("=" * 60)
    logger.info("📈 COINALYZE DATA SUMMARY")
    logger.info("=" * 60)
    logger.info(f"🎯 Symbol: {data['symbol']}")
    logger.info(f"📊 Timeframe: {data['timeframe']}")
    logger.info(f"📝 Total Records: {data['record_count']}")
    logger.info(f"🕐 Time Range: {data['start_time']} → {data['end_time']}")
    logger.info("-" * 60)

    if not records:
        logger.warning("⚠️ No records returned!")
        return {"status": "empty", "record_count": 0, "metrics": {}}

    # Calculate metrics
    timestamps = [r["timestamp"] for r in records]
    closes = [r["close"] for r in records]
    volumes = [r["volume"] for r in records]

    metrics = {
        "record_count": len(records),
        "first_timestamp": min(timestamps),
        "last_timestamp": max(timestamps),
        "price_range": {
            "min": min(closes),
            "max": max(closes),
            "first": records[0]["close"],
            "last": records[-1]["close"],
        },
        "volume": {
            "total": sum(volumes),
            "average": sum(volumes) / len(volumes),
            "max": max(volumes),
        },
    }

    # Log sample data (first 3 records)
    logger.info("📊 Sample Data (first 3 records):")
    for i, record in enumerate(records[:3], 1):
        logger.info(f"  {i}. Timestamp: {record['timestamp']}")
        logger.info(
            f"     OHLC: {record['open']:.2f} / {record['high']:.2f} / "
            f"{record['low']:.2f} / {record['close']:.2f}"
        )
        logger.info(f"     Volume: {record['volume']:.2f}")

    logger.info("-" * 60)
    logger.info("📊 METRICS:")
    logger.info(
        f"  💰 Price Range: ${metrics['price_range']['min']:.2f} - "
        f"${metrics['price_range']['max']:.2f}"
    )
    logger.info(
        f"  📈 Price Change: ${metrics['price_range']['first']:.2f} → "
        f"${metrics['price_range']['last']:.2f} "
        f"({((metrics['price_range']['last'] / metrics['price_range']['first'] - 1) * 100):.2f}%)"
    )
    logger.info(f"  📦 Total Volume: {metrics['volume']['total']:.2f}")
    logger.info(f"  📊 Avg Volume: {metrics['volume']['average']:.2f}")
    logger.info("=" * 60)

    return {
        "status": "success",
        "record_count": len(records),
        "metrics": metrics,
        "symbol": data["symbol"],
        "timeframe": data["timeframe"],
    }


# ========================================
# DAG Definition
# ========================================

with DAG(
    dag_id="coinalyze_minimal",
    default_args=default_args,
    description="Minimalist DAG to fetch and log Coinalyze data",
    schedule_interval="@hourly",  # Run every hour
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["coinalyze", "minimal", "fetch", "ohlcv"],
) as dag:
    # Fetch data from Coinalyze
    data = fetch_coinalyze_data(timeframe="1h", hours_back=24)

    # Log and analyze the data
    metrics = log_and_analyze(data)

    # Task dependencies (automatic with TaskFlow API)
    data >> metrics
