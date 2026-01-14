"""
Example demonstrating CCXT adapter usage with Binance (all market types).

This script shows how to:
1. Create Binance adapters for spot, USD-M futures, and COIN-M futures
2. Build Instrument models for different market types
3. Fetch OHLCV and Open Interest data
4. Use preprocessing pipeline

Run with:
    python -m quant_framework.examples.ccxt_binance_demo
"""

import asyncio
import logging
from datetime import UTC, datetime, timedelta

from quant_framework.ingestion.adapters.ccxt_plugin.builders import (
    create_binance_adapters,
)
from quant_framework.ingestion.preprocessing.providers import BinancePreprocessor
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


async def demo_binance_usdm_ohlcv():
    """Fetch OHLCV data from Binance USD-M Futures using CCXT adapter."""
    logger.info("=" * 60)
    logger.info("DEMO: Binance USD-M Futures OHLCV via CCXT")
    logger.info("=" * 60)

    # Create adapters for USD-M futures
    ohlcv_adapter, oi_adapter = create_binance_adapters(
        market_type=MarketType.LINEAR_PERPETUAL,
    )

    # Build instrument
    instrument = Instrument(
        instrument_id="BTCUSDT",
        asset_class=AssetClass.CRYPTO,
        market_type=MarketType.LINEAR_PERPETUAL,
        venue=DataVenue.BINANCE_USDM,
        wrapper=WrapperImplementation.CCXT,
        base_asset="BTC",
        quote_asset="USDT",
        raw_symbol="BTC/USDT:USDT",  # Binance USD-M format
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
        preprocessor = BinancePreprocessor()
        preprocessed = list(preprocessor.preprocess_ohlcv(instrument, raw_ohlcv))

        logger.info(f"Preprocessed {len(preprocessed)} records")
        if preprocessed:
            logger.info(f"Sample record: {preprocessed[0]}")

    except Exception as e:
        logger.error(f"Error fetching OHLCV: {e}", exc_info=True)
    finally:
        await ohlcv_adapter.close()

    logger.info("USD-M OHLCV demo complete\n")


async def demo_binance_spot():
    """Fetch OHLCV data from Binance Spot using CCXT adapter."""
    logger.info("=" * 60)
    logger.info("DEMO: Binance Spot OHLCV via CCXT")
    logger.info("=" * 60)

    # Create adapters for Spot
    ohlcv_adapter, _ = create_binance_adapters(
        market_type=MarketType.SPOT,
    )

    # Build instrument
    instrument = Instrument(
        instrument_id="BTCUSDT",
        asset_class=AssetClass.CRYPTO,
        market_type=MarketType.SPOT,
        venue=DataVenue.BINANCE,
        wrapper=WrapperImplementation.CCXT,
        base_asset="BTC",
        quote_asset="USDT",
        raw_symbol="BTC/USDT",  # Binance spot format
        is_inverse=False,
        is_active=True,
    )

    logger.info(f"Instrument: {instrument.instrument_id}")
    logger.info(f"Data lineage: {instrument.data_lineage}")

    # Fetch OHLCV
    start = datetime.now(UTC) - timedelta(hours=2)
    end = datetime.now(UTC)

    try:
        await ohlcv_adapter.connect()
        logger.info(f"Fetching Spot OHLCV from {start} to {end}")

        raw_ohlcv = await ohlcv_adapter.fetch_ohlcv(
            instrument=instrument,
            timeframe="1h",
            start=start,
            end=end,
            limit=10,
        )

        logger.info(f"Received {len(list(raw_ohlcv))} raw OHLCV records")

        # Preprocess
        preprocessor = BinancePreprocessor()
        preprocessed = list(preprocessor.preprocess_ohlcv(instrument, raw_ohlcv))

        logger.info(f"Preprocessed {len(preprocessed)} records")
        if preprocessed:
            logger.info(f"Sample record: {preprocessed[0]}")

    except Exception as e:
        logger.error(f"Error fetching Spot OHLCV: {e}", exc_info=True)
    finally:
        await ohlcv_adapter.close()

    logger.info("Spot OHLCV demo complete\n")


async def demo_binance_coinm_open_interest():
    """Fetch Open Interest data from Binance COIN-M Futures using CCXT adapter."""
    logger.info("=" * 60)
    logger.info("DEMO: Binance COIN-M Futures Open Interest via CCXT")
    logger.info("=" * 60)

    # Create adapters for COIN-M futures
    ohlcv_adapter, oi_adapter = create_binance_adapters(
        market_type=MarketType.INVERSE_PERPETUAL,
    )

    # Build instrument
    instrument = Instrument(
        instrument_id="BTCUSD_PERP",
        asset_class=AssetClass.CRYPTO,
        market_type=MarketType.INVERSE_PERPETUAL,
        venue=DataVenue.BINANCE_COINM,
        wrapper=WrapperImplementation.CCXT,
        base_asset="BTC",
        quote_asset="USD",
        raw_symbol="BTC/USD:BTC",  # Binance COIN-M format
        is_inverse=True,
        is_active=True,
    )

    logger.info(f"Instrument: {instrument.instrument_id}")
    logger.info(f"Data lineage: {instrument.data_lineage}")
    logger.info(f"Settlement currency: {instrument.settlement_currency}")

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
        preprocessor = BinancePreprocessor()
        preprocessed = list(preprocessor.preprocess_open_interest(instrument, raw_oi))

        logger.info(f"Preprocessed {len(preprocessed)} records")
        if preprocessed:
            logger.info(f"Sample record: {preprocessed[0]}")

    except Exception as e:
        logger.error(f"Error fetching Open Interest: {e}", exc_info=True)
    finally:
        await oi_adapter.close()

    logger.info("COIN-M Open Interest demo complete\n")


async def main():
    """Run all Binance demos."""
    logger.info("Starting Binance CCXT adapter demos")

    await demo_binance_usdm_ohlcv()
    await demo_binance_spot()
    await demo_binance_coinm_open_interest()

    logger.info("All Binance demos complete!")


if __name__ == "__main__":
    asyncio.run(main())
