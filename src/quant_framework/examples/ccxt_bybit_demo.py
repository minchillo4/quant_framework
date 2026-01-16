"""
Example demonstrating CCXT adapter usage with Bybit.

This script shows how to:
1. Create Bybit adapters for linear and inverse perpetuals
2. Build Instrument models
3. Fetch OHLCV and Open Interest data
4. Use preprocessing pipeline

Run with:
    python -m quant_framework.examples.ccxt_bybit_demo
"""

import asyncio
import logging
from datetime import UTC, datetime, timedelta

from quant_framework.ingestion.adapters.ccxt_plugin.builders import (
    create_bybit_adapters,
)
from quant_framework.ingestion.preprocessing.providers import BybitPreprocessor
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


async def demo_bybit_linear_ohlcv():
    """Fetch OHLCV data from Bybit linear perpetuals using CCXT adapter."""
    logger.info("=" * 60)
    logger.info("DEMO: Bybit Linear Perpetual OHLCV via CCXT")
    logger.info("=" * 60)

    # Create adapters
    ohlcv_adapter, oi_adapter = create_bybit_adapters(
        market_type=MarketType.LINEAR_PERPETUAL,
    )

    # Build instrument
    instrument = Instrument(
        instrument_id="BTCUSDT",
        asset_class=AssetClass.CRYPTO,
        market_type=MarketType.LINEAR_PERPETUAL,
        venue=DataVenue.BYBIT,
        wrapper=WrapperImplementation.CCXT,
        base_asset="BTC",
        quote_asset="USDT",
        raw_symbol="BTC/USDT:USDT",  # Bybit linear format
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
        preprocessor = BybitPreprocessor()
        preprocessed = list(preprocessor.preprocess_ohlcv(instrument, raw_ohlcv))

        logger.info(f"Preprocessed {len(preprocessed)} records")
        if preprocessed:
            logger.info(f"Sample record: {preprocessed[0]}")

    except Exception as e:
        logger.error(f"Error fetching OHLCV: {e}", exc_info=True)
    finally:
        await ohlcv_adapter.close()

    logger.info("Linear OHLCV demo complete\n")


async def demo_bybit_open_interest():
    """Fetch Open Interest data from Bybit using CCXT adapter."""
    logger.info("=" * 60)
    logger.info("DEMO: Bybit Open Interest via CCXT")
    logger.info("=" * 60)

    # Create adapters
    ohlcv_adapter, oi_adapter = create_bybit_adapters(
        market_type=MarketType.LINEAR_PERPETUAL,
    )

    # Build instrument
    instrument = Instrument(
        instrument_id="BTCUSDT",
        asset_class=AssetClass.CRYPTO,
        market_type=MarketType.LINEAR_PERPETUAL,
        venue=DataVenue.BYBIT,
        wrapper=WrapperImplementation.CCXT,
        base_asset="BTC",
        quote_asset="USDT",
        raw_symbol="BTC/USDT:USDT",
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
        preprocessor = BybitPreprocessor()
        preprocessed = list(preprocessor.preprocess_open_interest(instrument, raw_oi))

        logger.info(f"Preprocessed {len(preprocessed)} records")
        if preprocessed:
            logger.info(f"Sample record: {preprocessed[0]}")

    except Exception as e:
        logger.error(f"Error fetching Open Interest: {e}", exc_info=True)
    finally:
        await oi_adapter.close()

    logger.info("Open Interest demo complete\n")


async def demo_bybit_inverse():
    """Fetch OHLCV data from Bybit inverse perpetuals using CCXT adapter."""
    logger.info("=" * 60)
    logger.info("DEMO: Bybit Inverse Perpetual OHLCV via CCXT")
    logger.info("=" * 60)

    # Create adapters
    ohlcv_adapter, oi_adapter = create_bybit_adapters(
        market_type=MarketType.INVERSE_PERPETUAL,
    )

    # Build instrument
    instrument = Instrument(
        instrument_id="BTCUSD",
        asset_class=AssetClass.CRYPTO,
        market_type=MarketType.INVERSE_PERPETUAL,
        venue=DataVenue.BYBIT,
        wrapper=WrapperImplementation.CCXT,
        base_asset="BTC",
        quote_asset="USD",
        raw_symbol="BTC/USD:BTC",  # Bybit inverse format
        is_inverse=True,
        is_active=True,
    )

    logger.info(f"Instrument: {instrument.instrument_id}")
    logger.info(f"Settlement currency: {instrument.settlement_currency}")

    # Fetch OHLCV
    start = datetime.now(UTC) - timedelta(hours=2)
    end = datetime.now(UTC)

    try:
        await ohlcv_adapter.connect()
        logger.info(f"Fetching Inverse OHLCV from {start} to {end}")

        raw_ohlcv = await ohlcv_adapter.fetch_ohlcv(
            instrument=instrument,
            timeframe="1h",
            start=start,
            end=end,
            limit=10,
        )

        logger.info(f"Received {len(list(raw_ohlcv))} raw OHLCV records")

        # Preprocess
        preprocessor = BybitPreprocessor()
        preprocessed = list(preprocessor.preprocess_ohlcv(instrument, raw_ohlcv))

        logger.info(f"Preprocessed {len(preprocessed)} records")
        if preprocessed:
            logger.info(f"Sample record: {preprocessed[0]}")

    except Exception as e:
        logger.error(f"Error fetching Inverse OHLCV: {e}", exc_info=True)
    finally:
        await ohlcv_adapter.close()

    logger.info("Inverse OHLCV demo complete\n")


async def main():
    """Run all Bybit demos."""
    logger.info("Starting Bybit CCXT adapter demos")

    await demo_bybit_linear_ohlcv()
    await demo_bybit_open_interest()
    await demo_bybit_inverse()

    logger.info("All Bybit demos complete!")


if __name__ == "__main__":
    asyncio.run(main())
