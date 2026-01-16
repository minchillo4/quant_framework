"""
Example demonstrating CCXT adapter usage with Huobi.

This script shows how to:
1. Create CCXT adapters using the builder pattern
2. Build Instrument models with proper venue/wrapper metadata
3. Fetch OHLCV and Open Interest data
4. Use preprocessing and normalization pipeline

Run with:
    python -m quant_framework.examples.ccxt_huobi_demo
"""

import asyncio
import logging
from datetime import UTC, datetime, timedelta

from quant_framework.ingestion.adapters.ccxt_plugin.builders import (
    create_huobi_adapters,
)
from quant_framework.ingestion.preprocessing.providers import HuobiPreprocessor
from quant_framework.shared.models.enums import (
    AssetClass,
    DataVenue,
    MarketType,
    WrapperImplementation,
)
from quant_framework.shared.models.instruments import Instrument

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)

logger = logging.getLogger(__name__)


async def demo_huobi_ohlcv():
    """Fetch OHLCV data from Huobi using CCXT adapter."""
    logger.info("=" * 60)
    logger.info("DEMO: Huobi OHLCV via CCXT")
    logger.info("=" * 60)

    # Create adapters
    ohlcv_adapter, oi_adapter = create_huobi_adapters(
        market_type=MarketType.LINEAR_PERPETUAL,
    )

    # Build instrument
    instrument = Instrument(
        instrument_id="BTCUSDT",
        asset_class=AssetClass.CRYPTO,
        market_type=MarketType.LINEAR_PERPETUAL,
        venue=DataVenue.HUOBI,
        wrapper=WrapperImplementation.CCXT,
        base_asset="BTC",
        quote_asset="USDT",
        raw_symbol="BTC-USDT",  # Huobi futures use dash separator
        is_inverse=False,
        is_active=True,
    )

    logger.info(f"Instrument: {instrument.instrument_id}")
    logger.info(f"Data lineage: {instrument.data_lineage}")
    logger.info(f"Settlement currency: {instrument.settlement_currency}")

    # Fetch OHLCV
    start = datetime.now(UTC) - timedelta(hours=2)
    end = datetime.now(UTC)

    try:
        await ohlcv_adapter.connect()
        logger.info(f"Fetching OHLCV from {start} to {end}")

        raw_ohlcv = await ohlcv_adapter.fetch_ohlcv(
            instrument=instrument,
            timeframe="1h",
            start=start,
            end=end,
            limit=10,
        )

        logger.info(f"Received {len(list(raw_ohlcv))} raw OHLCV records")

        # Preprocess
        preprocessor = HuobiPreprocessor()
        preprocessed = list(preprocessor.preprocess_ohlcv(instrument, raw_ohlcv))

        logger.info(f"Preprocessed {len(preprocessed)} records")
        if preprocessed:
            logger.info(f"Sample record: {preprocessed[0]}")

    except Exception as e:
        logger.error(f"Error fetching OHLCV: {e}", exc_info=True)
    finally:
        await ohlcv_adapter.close()

    logger.info("OHLCV demo complete\n")


async def demo_huobi_open_interest():
    """Fetch Open Interest data from Huobi using CCXT adapter."""
    logger.info("=" * 60)
    logger.info("DEMO: Huobi Open Interest via CCXT")
    logger.info("=" * 60)

    # Create adapters
    ohlcv_adapter, oi_adapter = create_huobi_adapters(
        market_type=MarketType.LINEAR_PERPETUAL,
    )

    # Build instrument
    instrument = Instrument(
        instrument_id="BTCUSDT",
        asset_class=AssetClass.CRYPTO,
        market_type=MarketType.LINEAR_PERPETUAL,
        venue=DataVenue.HUOBI,
        wrapper=WrapperImplementation.CCXT,
        base_asset="BTC",
        quote_asset="USDT",
        raw_symbol="BTC-USDT",
        is_inverse=False,
        is_active=True,
    )

    logger.info(f"Instrument: {instrument.instrument_id}")

    # Fetch Open Interest
    start = datetime.now(UTC) - timedelta(hours=2)
    end = datetime.now(UTC)

    try:
        await oi_adapter.connect()
        logger.info(f"Fetching Open Interest from {start} to {end}")

        raw_oi = await oi_adapter.fetch_open_interest(
            instrument=instrument,
            timeframe="5m",
            start=start,
            end=end,
            limit=20,
        )

        logger.info(f"Received {len(list(raw_oi))} raw OI records")

        # Preprocess
        preprocessor = HuobiPreprocessor()
        preprocessed = list(preprocessor.preprocess_open_interest(instrument, raw_oi))

        logger.info(f"Preprocessed {len(preprocessed)} records")
        if preprocessed:
            logger.info(f"Sample record: {preprocessed[0]}")

    except Exception as e:
        logger.error(f"Error fetching Open Interest: {e}", exc_info=True)
    finally:
        await oi_adapter.close()

    logger.info("Open Interest demo complete\n")


async def main():
    """Run all demos."""
    logger.info("Starting Huobi CCXT adapter demos")

    await demo_huobi_ohlcv()
    await demo_huobi_open_interest()

    logger.info("All demos complete!")


if __name__ == "__main__":
    asyncio.run(main())
