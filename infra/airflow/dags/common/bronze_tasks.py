"""
Generic bronze ingestion tasks for Airflow.

Focus: CoinAlyze OHLC backfill with file-based checkpoints in MinIO.
No Kafka publishing; raw bronze-only.

Uses CheckpointStore/CheckpointCoordinator for unified checkpoint paths
that work with both backfill and incremental DAGs.
"""

# Ensure DAG root is on sys.path for absolute imports - MUST BE FIRST
import os
import sys

try:
    _dags_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    if _dags_root not in sys.path:
        sys.path.insert(0, _dags_root)
except Exception:
    pass

import asyncio
import logging
from collections import defaultdict
from datetime import UTC, datetime, timedelta
from typing import Any

from airflow.decorators import task

# Legacy bronze writer - will be migrated to storage.bronze.registry
from common.bronze_writer import BronzeWriter

from monitoring.bronze_metrics import (
    BRONZE_FILES_WRITTEN,
    BRONZE_FRESHNESS,
)
from quant_framework.infrastructure.checkpoint.store import (
    CheckpointDocument,
    CheckpointStore,
)
from quant_framework.infrastructure.impls.system import SystemClock
from quant_framework.infrastructure.ports.system import IClock
from quant_framework.ingestion.adapters.coinalyze_plugin.backfill_adapter import (
    CoinalyzeBackfillAdapter,
)

# ✅ NOVO IMPORT ADICIONADO
from quant_framework.ingestion.adapters.coinalyze_plugin.dependency_container import (
    CoinalyzeDependencyContainer,
)

# CoinAlyze-specific rate limiter (per-candle limiting)
from quant_framework.ingestion.adapters.coinalyze_plugin.rate_limiter import (
    AdaptiveRateLimiter,
)
from quant_framework.shared.models.enums import (
    AssetClass,
    DataVenue,
    MarketType,
    WrapperImplementation,
)
from quant_framework.shared.models.instruments import Instrument

logger = logging.getLogger(__name__)

SOURCE = "coinalyze"


def _default_start_date(timeframe: str, clock: IClock) -> datetime:
    # Conservative cold-start defaults per timeframe
    # Return timezone-aware datetime in UTC for consistency
    now = clock.utcnow().replace(hour=0, minute=0, second=0, microsecond=0)
    if timeframe == "1m":
        return now - timedelta(days=30)
    if timeframe == "5m":
        return now - timedelta(days=90)
    if timeframe == "15m":
        return now - timedelta(days=270)
    if timeframe == "30m":
        return now - timedelta(days=540)
    if timeframe == "1h":
        return now - timedelta(days=730)  # ~2 years
    if timeframe == "4h":
        return now - timedelta(days=1460)  # ~4 years
    return now - timedelta(days=3650)  # 10 years for 1d


@task
def discover_historical_start_date(
    symbol: str, timeframe: str, data_type: str = "ohlc", clock: IClock | None = None
) -> dict[str, Any]:
    """
    Cold-start discovery: use checkpoint if present, else conservative default.

    Uses unified orchestration checkpoint interface.
    """

    clock = clock or SystemClock()
    checkpoint_store = CheckpointStore()

    async def _load_checkpoint():
        return await checkpoint_store.load_checkpoint(
            symbol, timeframe, data_type, "coinalyze"
        )

    cp = asyncio.run(_load_checkpoint())

    if cp and cp.get("last_timestamp"):
        # Convert milliseconds to ISO string
        ts_sec = cp["last_timestamp"] / 1000
        start_dt = datetime.fromtimestamp(ts_sec, tz=UTC)
        return {
            "symbol": symbol,
            "timeframe": timeframe,
            "start_date": start_dt.isoformat().replace("+00:00", "Z"),
        }
    start = _default_start_date(timeframe, clock)
    return {
        "symbol": symbol,
        "timeframe": timeframe,
        "start_date": start.isoformat() + "Z",
    }


async def _fetch_chunk(
    symbol: str,
    timeframe: str,
    start_dt: datetime,
    end_dt: datetime,
    data_type: str,
    adapter: CoinalyzeBackfillAdapter,
) -> list[dict[str, Any]]:
    """Fetch a chunk of records via CoinAlyze adapter; returns list of dicts."""
    # Create instrument for adapter
    instrument = Instrument(
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

    if data_type == "ohlc":
        models = await adapter.fetch_ohlcv_history(
            symbol, timeframe, start_dt, end_dt, instrument
        )
    else:
        models = await adapter.fetch_oi_history(
            symbol, timeframe, start_dt, end_dt, instrument
        )

    # Models already come as dicts from adapter
    records: list[dict[str, Any]] = models if isinstance(models, list) else list(models)
    return records


@task
def fetch_and_write_historical_data(
    symbol: str,
    timeframe: str,
    data_type: str = "ohlc",
    end_date: datetime | None = None,
    clock: IClock | None = None,
) -> dict[str, Any]:
    """
    Fetch historical data and write to bronze with daily Parquet files.
    Uses unified orchestration checkpoint interface for resumable backfill.
    Checkpoint saved after each daily write for resumable backfill.
    """

    clock = clock or SystemClock()
    checkpoint_store = CheckpointStore()

    # Initialize checkpoint variables for low-level operations
    store = checkpoint_store
    key = f"_checkpoints/coinalyze/{data_type}/{symbol}_{timeframe}.json"

    async def _load_checkpoint():
        doc, etag = store.read(key)
        if not doc:
            return None, None
        # Convert CheckpointDocument to dict format expected by code
        return {
            "last_timestamp": doc.last_successful_timestamp,
            "total_records": doc.total_records,
            "consecutive_failures": doc.metadata.get("consecutive_failures", 0),
            "last_error": doc.metadata.get("last_error"),
            "status": doc.metadata.get("status", "in_progress"),
            "metadata": doc.metadata,
        }, etag

    cp, cp_etag = asyncio.run(_load_checkpoint())

    # Determine start/end
    if cp and cp.get("last_timestamp"):
        try:
            ts_sec = cp["last_timestamp"] / 1000
            current_start = datetime.fromtimestamp(ts_sec, tz=UTC)
        except Exception:
            current_start = _default_start_date(timeframe, clock)
    else:
        current_start = _default_start_date(timeframe, clock)

    # Ensure end_dt is timezone-aware UTC for comparison with current_start
    end_dt = end_date or clock.utcnow().replace(
        hour=0, minute=0, second=0, microsecond=0
    )

    # Create dependency container and adapter
    from quant_framework.infrastructure.config.settings import settings

    coinalyze_container = CoinalyzeDependencyContainer(
        base_url=settings.coinalyze.base_url,
        api_keys=settings.coinalyze.api_keys,
    )
    coinalyze_client = coinalyze_container.create_coinalyze_client()

    rl = AdaptiveRateLimiter()
    adapter = CoinalyzeBackfillAdapter(
        client=coinalyze_client, rate_limiter=rl, verbose=False
    )
    writer = BronzeWriter()

    files_written = cp.get("total_records", 0) if cp else 0
    total_records_written = 0

    # Initialize checkpoint metadata if not exists
    metadata = cp.get("metadata", {}) if cp else {}

    try:
        while current_start < end_dt:
            chunk_size = rl.get_chunk_size(timeframe)
            chunk_end = min(current_start + chunk_size, end_dt)

            # Fetch chunk async
            records = asyncio.run(
                _fetch_chunk(
                    symbol, timeframe, current_start, chunk_end, data_type, adapter
                )
            )
            rl.record_result(timeframe, len(records), current_start, chunk_end)

            if not records:
                # Advance to next chunk to avoid infinite loop
                current_start = chunk_end
                continue

            # Group by day and write
            by_day: dict[str, list[dict[str, Any]]] = defaultdict(list)
            for r in records:
                ts = r.get("ts") or r.get("timestamp")
                if isinstance(ts, str):
                    ts_dt = datetime.fromisoformat(ts.replace("Z", ""))
                else:
                    ts_dt = ts
                day_key = ts_dt.strftime("%Y-%m-%d")
                # Normalize required fields for writer schema
                if data_type == "oi":
                    # Open Interest: use open_interest (from pydantic model_dump)
                    # or fallback to open_interest_amount/open_interest_value (from preprocessing)
                    oi_value = (
                        r.get("open_interest")
                        or r.get("open_interest_amount")
                        or r.get("open_interest_value")
                        or 0.0
                    )
                    normalized = {
                        "timestamp": ts_dt,
                        "open_interest": float(oi_value),
                        "canonical_symbol": r.get("canonical_symbol") or symbol,
                        "exchange": str(
                            getattr(
                                r.get("exchange"), "value", r.get("exchange", "binance")
                            )
                        ),
                        "timeframe": timeframe,
                    }
                else:
                    # OHLCV data
                    normalized = {
                        "timestamp": ts_dt,
                        "open": r.get("open"),
                        "high": r.get("high"),
                        "low": r.get("low"),
                        "close": r.get("close"),
                        "volume": r.get("volume") or 0.0,
                        "canonical_symbol": r.get("canonical_symbol") or symbol,
                        "exchange": str(
                            getattr(
                                r.get("exchange"), "value", r.get("exchange", "binance")
                            )
                        ),
                        "timeframe": timeframe,
                    }
                by_day[day_key].append(normalized)

            last_written_file = None
            last_successful_ts = None

            for day_str, day_records in sorted(by_day.items()):
                day_dt = datetime.strptime(day_str, "%Y-%m-%d")
                res = asyncio.run(
                    writer.write_daily_batch(
                        day_records, symbol, timeframe, data_type, day_dt
                    )
                )
                if not res.get("success"):
                    # Update failure counters and continue (tolerant)
                    cp["consecutive_failures"] = cp.get("consecutive_failures", 0) + 1
                    cp["last_error"] = res.get("error")
                    cp["status"] = "failed"
                    # Update via unified path
                    checkpoint_doc = CheckpointDocument(
                        source="coinalyze",
                        exchange="coinalyze",
                        symbol=symbol,
                        timeframe=timeframe,
                        market_type=None,
                        last_successful_timestamp=cp.get(
                            "last_successful_timestamp", 0
                        ),
                        last_updated=clock.utcnow().isoformat(),
                        total_records=total_records_written,
                        metadata=cp.get("metadata", {}),
                    )
                    try:
                        store.write_atomic(key, checkpoint_doc, if_match=cp_etag)
                    except RuntimeError:
                        logger.warning(
                            f"Checkpoint write conflict for {symbol}/{timeframe}"
                        )
                    continue

                files_written += 1
                total_records_written += res.get("records_written", 0)
                last_written_file = res.get("file_path")

                # Last successful timestamp = end of that day (in milliseconds)
                last_dt = day_dt + timedelta(days=1) - timedelta(seconds=1)
                last_successful_ts_ms = int(last_dt.timestamp() * 1000)

                # Update checkpoint after each daily write (resumable)
                # Use CheckpointCoordinator to write with unified paths
                metadata = cp.get("metadata", {})
                stats = rl.performance_stats.get(timeframe, [])
                avg_points = (
                    int(
                        sum(s.get("points_received", 0) for s in stats)
                        / max(len(stats), 1)
                    )
                    if stats
                    else 0
                )
                metadata["avg_points_per_request"] = avg_points
                metadata["chunk_size_days"] = rl.SAFE_CHUNK_DAYS.get(
                    timeframe, rl.SAFE_CHUNK_DAYS["1h"]
                )
                metadata["last_written_file"] = last_written_file
                metadata["files_written"] = files_written

                # Create checkpoint document using unified paths
                checkpoint_doc = CheckpointDocument(
                    source="coinalyze",
                    exchange="coinalyze",
                    symbol=symbol,
                    timeframe=timeframe,
                    market_type=None,
                    last_successful_timestamp=last_successful_ts_ms,
                    last_updated=clock.utcnow().isoformat(),
                    total_records=total_records_written,
                    metadata=metadata,
                )

                # Write atomically to unified path
                try:
                    store.write_atomic(key, checkpoint_doc, if_match=cp_etag)
                    cp_etag = None  # Reset etag after write
                except RuntimeError as e:
                    logger.warning(
                        f"Checkpoint write conflict (concurrent modification): {e}"
                    )

                # Basic monitoring updates (file count and freshness)
                try:
                    BRONZE_FILES_WRITTEN.labels(
                        "coinalyze", data_type, symbol, timeframe
                    ).inc()
                    # Freshness = now - latest timestamp
                    freshness = (clock.utcnow() - last_dt).total_seconds()
                    BRONZE_FRESHNESS.labels(
                        "coinalyze", data_type, symbol, timeframe
                    ).set(freshness)
                except Exception:
                    # Metrics are optional; don't fail task if unavailable
                    pass

            # Move to next chunk
            current_start = chunk_end

        return {
            "status": "success",
            "symbol": symbol,
            "timeframe": timeframe,
            "records_written": total_records_written,
            "files_written": files_written,
        }

    except Exception as e:
        # Update checkpoint with failure info using unified paths
        metadata = cp.get("metadata", {})
        metadata["last_error"] = str(e)
        checkpoint_doc = CheckpointDocument(
            source="coinalyze",
            exchange="coinalyze",
            symbol=symbol,
            timeframe=timeframe,
            market_type=None,
            last_successful_timestamp=cp.get("last_successful_timestamp", 0),
            last_updated=clock.utcnow().isoformat(),
            total_records=total_records_written,
            metadata=metadata,
        )
        try:
            store.write_atomic(key, checkpoint_doc, if_match=cp_etag)
        except RuntimeError:
            logger.warning(
                f"Failed to update checkpoint for {symbol}/{timeframe} with error info"
            )

        logger.error(f"Backfill failed for {symbol}/{timeframe}: {e}", exc_info=True)
        return {
            "status": "failed",
            "symbol": symbol,
            "timeframe": timeframe,
            "error": str(e),
            "records_written": total_records_written,
            "files_written": files_written,
        }


# =============================================================================
# CCXT INCREMENTAL HELPERS
# =============================================================================


def _group_by_day(records: list[dict[str, Any]]) -> dict[str, list[dict[str, Any]]]:
    """Group records by day (UTC date).

    Args:
        records: List of records with 'timestamp' field (datetime or ms int)

    Returns:
        Dict mapping date (YYYYMMDD) to list of records for that day
    """
    grouped = defaultdict(list)

    for record in records:
        ts = record.get("timestamp")

        # Convert to datetime if needed
        if isinstance(ts, int):
            # Assume milliseconds
            dt = datetime.fromtimestamp(ts / 1000, tz=UTC)
        elif isinstance(ts, datetime):
            if ts.tzinfo is None:
                dt = ts.replace(tzinfo=UTC)
            else:
                dt = ts.astimezone(UTC)
        else:
            logger.warning(f"Skipping record with invalid timestamp: {ts}")
            continue

        # Group by date
        date_key = dt.strftime("%Y%m%d")
        grouped[date_key].append(record)

    return dict(grouped)
