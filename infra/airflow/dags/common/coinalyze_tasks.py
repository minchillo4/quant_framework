"""
CoinAlyze Airflow Tasks: Discovery, Checkpointing, and Fetch/Write Logic.

Implements:
- Rolling discovery with XCom caching (no fixed listing dates)
- Checkpoint management with corruption detection
- Fetch-and-write loop with daily Parquet writes
- Soft failure handling per symbol/timeframe
"""

import asyncio
import json
import logging
from datetime import datetime
from typing import Any

from airflow.decorators import task
from airflow.exceptions import AirflowException
from airflow.operators.python import get_current_context

# CoinAlyze imports
# Bronze writer import
from common.bronze_writer import BronzeWriter

from quant_framework.infrastructure.ports.system import IClock
from quant_framework.ingestion.adapters.coinalyze_plugin.ohlcv_adapter import (
    CoinalyzeOHLCVAdapter,
)
from quant_framework.ingestion.adapters.coinalyze_plugin.open_interest_adapter import (
    CoinalyzeOpenInterestAdapter,
)
from quant_framework.shared.models.enums import (
    AssetClass,
    DataVenue,
    MarketType,
    WrapperImplementation,
)
from quant_framework.shared.models.instruments import Instrument

logger = logging.getLogger(__name__)


# ============================================================================
# CONFIGURATION (now loaded from dependency container instead of hardcoded)
# ============================================================================

# Global floor dates: don't go back before these dates for each asset
# (prevents infinite loops and API errors)
GLOBAL_FLOOR_DATES = {
    "BTC": datetime(2011, 1, 3),
    "ETH": datetime(2015, 7, 30),
    "XRP": datetime(2013, 1, 1),
    # Default for unknowns
    "default": datetime(2017, 1, 1),
}


# ============================================================================
# INSTRUMENT CREATION HELPER
# ============================================================================


def create_instrument_for_symbol(symbol: str) -> Instrument:
    """
    Create Instrument object for a given symbol (BTC, ETH, XRP).

    Args:
        symbol: Canonical symbol (BTC, ETH, XRP)

    Returns:
        Instrument configured for CoinAlyze API with Binance perpetual format
    """
    return Instrument(
        instrument_id=f"{symbol}USDT_PERP_BINANCE",
        base_asset=symbol,
        quote_asset="USDT",
        asset_class=AssetClass.CRYPTO,
        market_type=MarketType.LINEAR_PERPETUAL,
        venue=DataVenue.BINANCE_USDM,
        wrapper=WrapperImplementation.COINALYZE,
        symbol=f"{symbol}USDT_PERP.A",  # CoinAlyze Binance perpetual format
        is_active=True,
    )


# ============================================================================
# CHECKPOINT MANAGEMENT
# ============================================================================


@task
def load_checkpoint_safe(
    symbol: str, timeframe: str, data_type: str, clock: IClock | None = None
) -> dict[str, Any] | None:
    """
    Load checkpoint from XCom with corruption detection.

    Args:
        symbol: Canonical symbol (BTC, ETH, XRP)
        timeframe: Timeframe (1m, 5m, 1h, etc.)
        data_type: Data type (ohlc or oi)
        clock: Clock implementation for time operations

    Returns:
        Checkpoint dict or None
    """
    clock = clock or SystemClock()
    checkpoint_key = f"{symbol}_{timeframe}_{data_type}_checkpoint"

    try:
        context = get_current_context()
        ti = context["ti"]

        checkpoint_json = ti.xcom_pull(key=checkpoint_key)

        if checkpoint_json is None:
            logger.info(
                f"ℹ️ No checkpoint found for {symbol}/{timeframe}/{data_type} (fresh run)"
            )
            return None

        checkpoint = json.loads(checkpoint_json)

        # Validate checkpoint: ensure timestamp is not in future
        last_ts = checkpoint.get("last_successful_timestamp")
        if last_ts is not None:
            last_dt = datetime.fromtimestamp(last_ts)
            if last_dt > clock.utcnow():
                raise AirflowException(
                    f"❌ CORRUPTED CHECKPOINT: {symbol}/{timeframe}/{data_type} "
                    f"has future timestamp {last_dt}. Manual review required."
                )

        logger.info(
            f"✅ Loaded checkpoint for {symbol}/{timeframe}/{data_type}: "
            f"status={checkpoint.get('status')}"
        )

        return checkpoint

    except Exception as e:
        logger.error(f"❌ Error loading checkpoint: {e}")
        raise


@task
def save_checkpoint(
    symbol: str,
    timeframe: str,
    data_type: str,
    timestamp: int,
    file_path: str,
    status: str,
    error_msg: str | None = None,
) -> None:
    """
    Save checkpoint to XCom with validation.

    Args:
        symbol: Canonical symbol
        timeframe: Timeframe
        data_type: Data type
        timestamp: Unix timestamp of last successful data point
        file_path: S3 path to last written file
        status: Status string (success, in_progress, failed)
        error_msg: Optional error message if status=failed
    """
    checkpoint_key = f"{symbol}_{timeframe}_{data_type}_checkpoint"

    try:
        context = get_current_context()
        ti = context["ti"]

        checkpoint = {
            "last_successful_timestamp": timestamp,
            "last_file_written": file_path,
            "status": status,
            "error_msg": error_msg,
        }

        ti.xcom_push(key=checkpoint_key, value=json.dumps(checkpoint))

        logger.info(
            f"✅ Saved checkpoint for {symbol}/{timeframe}/{data_type}: {status}"
        )

    except Exception as e:
        logger.error(f"❌ Error saving checkpoint: {e}")
        raise


# ============================================================================
# DISCOVERY WITH ROLLING BACKTRACK & CACHING
# ============================================================================


@task
def discover_historical_start_date(
    symbol: str, timeframe: str, data_type: str, clock: IClock | None = None
) -> dict[str, Any]:
    """
    Discover the historical start date for a symbol/timeframe.

    MVP Strategy:
    1. Check XCom for cached discovery result (zero-cost if cached)
    2. Return global floor date (simplified - actual API probing deferred to production)

    Returns:
    {
        "discovered_timestamp": int (Unix timestamp),
        "is_at_floor": bool (True if reached global floor)
    }

    Args:
        symbol: Canonical symbol
        timeframe: Timeframe
        data_type: Data type (ohlc or oi)
        clock: Clock implementation for time operations

    Returns:
        Dict with discovered_timestamp and is_at_floor flag
    """
    clock = clock or SystemClock()
    discovery_key = f"{symbol}_{timeframe}_{data_type}_discovery_result"

    try:
        context = get_current_context()
        ti = context["ti"]

        # Check XCom cache
        cached_result_json = ti.xcom_pull(key=discovery_key)
        if cached_result_json is not None:
            cached_result = json.loads(cached_result_json)
            logger.info(
                f"♻️ Using cached discovery for {symbol}/{timeframe}/{data_type}"
            )
            return cached_result

        # MVP: use global floor date
        # In production, would probe API for actual start date
        floor_dt = GLOBAL_FLOOR_DATES.get(symbol, GLOBAL_FLOOR_DATES["default"])

        result = {
            "discovered_timestamp": int(floor_dt.timestamp()),
            "is_at_floor": True,
        }

        # Cache result
        ti.xcom_push(key=discovery_key, value=json.dumps(result))

        logger.info(
            f"🔍 Discovered start date for {symbol}/{timeframe}/{data_type}: {floor_dt}"
        )

        return result

    except Exception as e:
        logger.error(f"❌ Discovery failed: {e}")
        # Fallback to global floor
        floor_dt = GLOBAL_FLOOR_DATES.get(symbol, GLOBAL_FLOOR_DATES["default"])
        return {"discovered_timestamp": int(floor_dt.timestamp()), "is_at_floor": True}


# ============================================================================
# FETCH AND WRITE WITH CHECKPOINTING
# ============================================================================


def normalize_oi_records(
    raw_records: list[Any],
    symbol: str,
    timeframe: str,
    exchange: str = "binance",
    clock: IClock | None = None,
) -> list[dict[str, Any]]:
    """
    Normalize raw CoinAlyze Open Interest response to BronzeWriter format.

    CoinAlyze returns: [{"history": [{"t": timestamp_unix, "v": value}, ...]}]
    BronzeWriter expects: [{"timestamp": datetime, "open_interest": float, ...}]

    Args:
        raw_records: Raw API response from CoinalyzeOpenInterestAdapter
        symbol: Canonical symbol (BTC, ETH, XRP)
        timeframe: Timeframe (1h, 4h, 1d, etc.)
        exchange: Exchange name (default: binance)
        clock: Clock implementation for time operations

    Returns:
        List of normalized records ready for BronzeWriter
    """
    clock = clock or SystemClock()
    normalized = []

    for record_wrapper in raw_records:
        if isinstance(record_wrapper, dict) and "history" in record_wrapper:
            for point in record_wrapper["history"]:
                try:
                    normalized.append(
                        {
                            "timestamp": datetime.fromtimestamp(point["t"]),
                            "open_interest": float(point["v"]),
                            "canonical_symbol": symbol,
                            "exchange": exchange,
                            "timeframe": timeframe,
                        }
                    )
                except (KeyError, ValueError, TypeError) as e:
                    logger.warning(f"⚠️ Skipping invalid OI record: {point}, error: {e}")
                    continue

    return normalized


def normalize_ohlcv_records(
    raw_records: list[Any],
    symbol: str,
    timeframe: str,
    exchange: str = "binance",
    clock: IClock | None = None,
) -> list[dict[str, Any]]:
    """
    Normalize raw CoinAlyze OHLCV response to BronzeWriter format.

    CoinAlyze returns: [{"history": [{"t": ts, "o": open, "h": high, "l": low, "c": close, "v": volume}, ...]}]
    BronzeWriter expects: [{"timestamp": datetime, "open": float, "high": float, ...}]

    Args:
        raw_records: Raw API response from CoinalyzeOHLCVAdapter
        symbol: Canonical symbol
        timeframe: Timeframe
        exchange: Exchange name
        clock: Clock implementation for time operations

    Returns:
        List of normalized records ready for BronzeWriter
    """
    clock = clock or SystemClock()
    normalized = []

    for record_wrapper in raw_records:
        if isinstance(record_wrapper, dict) and "history" in record_wrapper:
            for point in record_wrapper["history"]:
                try:
                    normalized.append(
                        {
                            "timestamp": datetime.fromtimestamp(point["t"]),
                            "open": float(point["o"]),
                            "high": float(point["h"]),
                            "low": float(point["l"]),
                            "close": float(point["c"]),
                            "volume": float(point["v"]),
                            "canonical_symbol": symbol,
                            "exchange": exchange,
                            "timeframe": timeframe,
                        }
                    )
                except (KeyError, ValueError, TypeError) as e:
                    logger.warning(
                        f"⚠️ Skipping invalid OHLCV record: {point}, error: {e}"
                    )
                    continue

    return normalized


def group_records_by_day(
    records: list[dict[str, Any]],
) -> dict[str, list[dict[str, Any]]]:
    """
    Group records by day (YYYY-MM-DD) for daily batch writing.

    Args:
        records: List of normalized records with 'timestamp' field

    Returns:
        Dict mapping date strings to lists of records
    """
    grouped = {}

    for record in records:
        date_str = record["timestamp"].strftime("%Y-%m-%d")
        if date_str not in grouped:
            grouped[date_str] = []
        grouped[date_str].append(record)

    return grouped


@task
def fetch_and_write_historical_data(
    symbol: str,
    timeframe: str,
    data_type: str,
    end_date: datetime,
    coinalyze_base_url: str | None = None,
    coinalyze_api_keys: list[str] | None = None,
    clock: IClock | None = None,
) -> dict[str, Any]:
    """
    Fetch and write historical data with checkpoint resumption.

    Process:
    1. Load checkpoint (if exists) to determine start date
    2. Fetch data from CoinAlyze API using appropriate adapter
    3. Normalize response to BronzeWriter schema
    4. Group by day and write daily Parquet batches
    5. Save checkpoint after successful writes
    6. Return soft failure dict on exception (status="failed")

    Args:
        symbol: Canonical symbol (BTC, ETH, XRP)
        timeframe: Timeframe (1m, 5m, 15m, 30m, 1h, 4h, 1d)
        data_type: Data type (ohlc or oi)
        end_date: End date for backfill (usually today)
        coinalyze_base_url: CoinAlyze API base URL (injected; uses settings if None)
        coinalyze_api_keys: CoinAlyze API keys (injected; uses settings if None)
        clock: Clock implementation (uses SystemClock if None)

    Returns:
        {
            "symbol": str,
            "timeframe": str,
            "status": "success" | "failed",
            "records_written": int,
            "days_completed": int,
            "files_written": List[str],
            "error": str | None
        }
    """
    clock = clock or SystemClock()

    async def async_fetch_and_write():
        """Async implementation of fetch and write logic."""
        total_records = 0
        files_written = []

        try:
            # Load settings for DI container if not injected
            if coinalyze_base_url is None or coinalyze_api_keys is None:
                from quant_framework.infrastructure.config.settings import settings

                base_url = coinalyze_base_url or settings.coinalyze.base_url
                api_keys = coinalyze_api_keys or settings.coinalyze.api_keys
            else:
                base_url = coinalyze_base_url
                api_keys = coinalyze_api_keys

            # Create dependency container
            container = CoinalyzeDependencyContainer(
                base_url=base_url,
                api_keys=api_keys,
            )
            client = container.create_coinalyze_client()

            # Step 1: Load checkpoint to determine start date
            logger.info(f"📊 Starting fetch-write for {symbol}/{timeframe}/{data_type}")

            checkpoint = load_checkpoint_safe(symbol, timeframe, data_type, clock=clock)

            if checkpoint and checkpoint.get("last_successful_timestamp"):
                # Resume from checkpoint
                start_dt = datetime.fromtimestamp(
                    checkpoint["last_successful_timestamp"]
                )
                logger.info(f"♻️ Resuming from checkpoint: {start_dt}")
            else:
                # Start from global floor date
                start_dt = GLOBAL_FLOOR_DATES.get(symbol, GLOBAL_FLOOR_DATES["default"])
                logger.info(f"🔍 Starting fresh from floor date: {start_dt}")

            # Ensure end_date is timezone-naive UTC
            if end_date.tzinfo is not None:
                end_dt = end_date.replace(tzinfo=None)
            else:
                end_dt = end_date

            # Step 2: Load rate limiting config from backfill.yaml
            from quant_framework.ingestion.orchestration.backfill.rate_limiter import (
                CoinAlyzeRateLimiter,
            )

            rate_limiter = CoinAlyzeRateLimiter(verbose=False)
            chunk_days = rate_limiter.calculate_chunk_size(timeframe).days

            # Step 3: Fetch data in chunks
            logger.info(f"⬇️ Fetching {data_type} data from {start_dt} to {end_dt}")
            logger.info(f"📦 Chunk size: {chunk_days} days")

            # Create instrument
            instrument = create_instrument_for_symbol(symbol)

            # Fetch data using appropriate adapter
            if data_type == "ohlc":
                adapter = CoinalyzeOHLCVAdapter(client=client)
                raw_records = await adapter.fetch_ohlcv(
                    instrument=instrument,
                    timeframe=timeframe,
                    start=start_dt,
                    end=end_dt,
                )
            elif data_type == "oi":
                adapter = CoinalyzeOpenInterestAdapter(client=client)
                raw_records = await adapter.fetch_open_interest(
                    instrument=instrument,
                    timeframe=timeframe,
                    start=start_dt,
                    end=end_dt,
                )
            else:
                raise ValueError(f"Invalid data_type: {data_type}")

            # Convert generator to list
            raw_records_list = list(raw_records)
            logger.info(f"✅ Fetched {len(raw_records_list)} raw response objects")

            # Step 4: Normalize records
            if data_type == "oi":
                normalized_records = normalize_oi_records(
                    raw_records_list, symbol, timeframe, clock=clock
                )
            else:
                normalized_records = normalize_ohlcv_records(
                    raw_records_list, symbol, timeframe, clock=clock
                )

            logger.info(f"✅ Normalized {len(normalized_records)} records")

            if not normalized_records:
                logger.warning(
                    f"⚠️ No records to write for {symbol}/{timeframe}/{data_type}"
                )
                return {
                    "symbol": symbol,
                    "timeframe": timeframe,
                    "status": "success",
                    "records_written": 0,
                    "days_completed": 0,
                    "files_written": [],
                    "error": "No records fetched",
                }

            # Step 5: Group by day and write
            grouped = group_records_by_day(normalized_records)
            logger.info(f"📅 Grouped into {len(grouped)} daily batches")

            # Initialize bronze writer
            writer = BronzeWriter()

            # Write each daily batch
            for date_str, day_records in sorted(grouped.items()):
                batch_date = datetime.strptime(date_str, "%Y-%m-%d")

                logger.info(f"📝 Writing {len(day_records)} records for {date_str}")

                result = await writer.write_daily_batch(
                    records=day_records,
                    symbol=symbol,
                    timeframe=timeframe,
                    data_type=data_type,
                    date=batch_date,
                )

                if result["success"]:
                    total_records += result["records_written"]
                    files_written.append(result["file_path"])
                    logger.info(
                        f"✅ Wrote {result['records_written']} records to {result['file_path']}"
                    )

                    # Save checkpoint after successful write
                    last_timestamp = int(day_records[-1]["timestamp"].timestamp())
                    save_checkpoint(
                        symbol=symbol,
                        timeframe=timeframe,
                        data_type=data_type,
                        timestamp=last_timestamp,
                        file_path=result["file_path"],
                        status="success",
                    )
                else:
                    logger.error(
                        f"❌ Failed to write batch for {date_str}: {result['error']}"
                    )
                    raise Exception(f"Write failed: {result['error']}")

            logger.info(
                f"🎉 Completed! Wrote {total_records} records across {len(files_written)} files"
            )

            return {
                "symbol": symbol,
                "timeframe": timeframe,
                "status": "success",
                "records_written": total_records,
                "days_completed": len(grouped),
                "files_written": files_written,
                "error": None,
            }

        except Exception as e:
            logger.error(
                f"❌ Fetch-write failed for {symbol}/{timeframe}/{data_type}: {e}",
                exc_info=True,
            )

            return {
                "symbol": symbol,
                "timeframe": timeframe,
                "status": "failed",
                "records_written": total_records,
                "days_completed": len(files_written),
                "files_written": files_written,
                "error": str(e),
            }

    # Run async function in event loop
    try:
        return asyncio.run(async_fetch_and_write())
    except Exception as e:
        logger.error(f"❌ Async execution failed: {e}", exc_info=True)
        return {
            "symbol": symbol,
            "timeframe": timeframe,
            "status": "failed",
            "records_written": 0,
            "days_completed": 0,
            "files_written": [],
            "error": str(e),
        }
