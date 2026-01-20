"""
CCXT 1h Open Interest Incremental DAG - Inverse Perpetuals.

Continuously updates Open Interest data every 1 hour by fetching from CCXT exchanges
and writing directly to MinIO bronze layer as Parquet files.
Supports all major exchanges including Huobi (which only supports 1h and longer timeframes).

Bronze-First Architecture:
- Reads checkpoints from MinIO to determine fetch range
- Fetches only new data since last checkpoint (gap-free continuation)
- Writes directly to MinIO bronze layer (same format as backfill)
- Updates checkpoint after successful write
- NO Kafka, NO TimescaleDB - pure bronze storage

Workflow:
1. Backfill DAG runs first (manual trigger) → Historical data to MinIO bronze
2. This DAG auto-triggers every 1 hour → Incremental updates to MinIO bronze
3. Checkpoint coordination ensures no gaps or duplicates

Schedule: Every 1 hour (0 * * * *)
Exchanges: binance, bybit, huobi (inverse perpetuals only)
Symbols: BTC, ETH (from config, as BTCUSD, ETHUSD)
Market Type: inverse_perpetual
Data Type: open_interest at 1h
Storage: MinIO bronze layer (Parquet)
"""

import asyncio
import logging
from datetime import datetime, timedelta

from airflow.decorators import dag, task
from airflow.sensors.external_task import ExternalTaskSensor
from airflow.utils.state import DagRunState

from quant_framework.infrastructure.checkpoint import (
    CheckpointCoordinator,
    CheckpointStore,
)
from quant_framework.ingestion.adapters import (
    CCXTOpenInterestAdapter,
)
from quant_framework.shared.models import (
    Instrument,
)
from quant_framework.shared.models.enums import (
    AssetClass,
    DataVenue,
    MarketType,
    WrapperImplementation,
)

logger = logging.getLogger(__name__)


def _get_venue_for_exchange(exchange: str, market_type: str) -> DataVenue:
    """Map exchange name and market type to DataVenue enum."""
    if exchange == "binance":
        if market_type == "inverse_perpetual":
            return DataVenue.BINANCE_COINM
        elif market_type == "linear_perpetual":
            return DataVenue.BINANCE_USDM
        else:
            return DataVenue.BINANCE
    elif exchange == "bybit":
        return DataVenue.BYBIT
    elif exchange == "huobi":
        return DataVenue.HUOBI
    elif exchange == "gateio":
        return DataVenue.GATEIO
    else:
        # Default fallback - use exchange name directly
        return DataVenue(exchange)


# =============================================================================
# DAG CONFIGURATION
# =============================================================================

DEFAULT_ARGS = {
    "owner": "data-eng",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=2),
}

EXCHANGE_CLASSES = {
    "binance": "binance",
    "bybit": "bybit",
    "huobi": "huobi",
}

DEFAULT_CONFIG = {
    "fetching": {
        "exchanges": ["binance", "bybit", "huobi"],
        "symbols": ["BTC", "ETH"],
        "market_types": {
            "binance": "inverse_perpetual",
            "bybit": "inverse_perpetual",
            "huobi": "inverse_perpetual",
        },
        "fetch_all_settlements": False,
    },
    "concurrency": {
        "max_concurrent_exchanges": 2,
        "rate_limit_delay_ms": 200,
    },
    "checkpoint": {
        "warn_threshold_ms": 2 * 60 * 60 * 1000,  # 2h
        "alert_threshold_ms": 4 * 60 * 60 * 1000,  # 4h
    },
}


# =============================================================================
# HELPER TASKS
# =============================================================================


@task
def load_config() -> dict:
    """Load service configuration from YAML or use defaults."""
    from pathlib import Path

    import yaml

    config_path = Path("config/services/oi_incremental_5m_inverse.yaml")
    try:
        with open(config_path) as fp:
            loaded = yaml.safe_load(fp) or {}
        merged = {**DEFAULT_CONFIG, **loaded}
        logger.info(f"✅ Loaded config from {config_path}")
        return merged
    except FileNotFoundError:
        logger.warning(f"Config not found at {config_path}; using defaults")
        return DEFAULT_CONFIG


@task
def build_fetch_combinations(config: dict) -> list[dict]:
    """Build all symbol/exchange/market_type combinations to fetch (inverse only)."""
    combos: list[dict] = []
    exchanges = config["fetching"]["exchanges"]
    symbols = config["fetching"]["symbols"]

    for exchange in exchanges:
        market_type = "inverse_perpetual"  # enforce inverse for this DAG
        for symbol in symbols:
            # For inverse perpetuals: BTC/USD:BTC, ETH/USD:ETH
            # Quote is always USD, settlement is the base asset
            combos.append(
                {
                    "exchange": exchange,
                    "market_type": market_type,
                    "canonical_symbol": symbol,
                    "quote_asset": "USD",
                    "settlement": symbol,
                }
            )

    logger.info(f"📋 Built {len(combos)} inverse fetch combinations")
    return combos


@task
def process_combo(combo: dict, config: dict) -> dict:
    """Process a single exchange/symbol combination (inverse).

    Fetches 1h OI incremental data from CCXT and writes to MinIO Bronze layer.
    Uses checkpoints (from MinIO) to track progress, no database dependency.
    """

    async def _run():
        try:
            from datetime import UTC
            from datetime import datetime as dt

            import ccxt

            from quant_framework.shared.models.enums import MarketDataType
            from quant_framework.storage.bronze.registry import (
                BronzeIngestionRequest,
                BronzeRegistry,
            )
            from quant_framework.storage.bronze.tasks import group_by_day

            exchange_name = combo["exchange"]
            market_type = combo["market_type"]
            canonical_symbol = combo["canonical_symbol"]
            quote_asset = combo["quote_asset"]
            settlement = combo["settlement"]
            # For inverse perpetuals: BTC/USD:BTC
            ccxt_symbol = f"{canonical_symbol}/{quote_asset}:{settlement}"

            logger.info(
                f"🔄 Processing {exchange_name}/{market_type}/{ccxt_symbol} (1h OI)"
            )

            # Initialize checkpoint system
            checkpoints = CheckpointCoordinator(CheckpointStore())

            # Load or bootstrap CCXT checkpoint from Coinalyze
            try:
                bootstrap_cp = checkpoints.load_or_bootstrap_ccxt(
                    exchange=exchange_name,
                    market_type=market_type,
                    ccxt_symbol=ccxt_symbol,
                    canonical_symbol=canonical_symbol,
                    timeframe="1h",
                )
                start_ms = bootstrap_cp.last_successful_timestamp
                start_dt = dt.fromtimestamp(start_ms / 1000, tz=UTC)
                logger.info(
                    f"✅ Checkpoint for {ccxt_symbol}: resuming from {start_dt.isoformat()}"
                )
            except RuntimeError as exc:
                logger.error(f"Checkpoint bootstrap failed: {exc}")
                return {
                    "success": False,
                    "error": f"Checkpoint bootstrap failed: {exc}",
                    "combo": combo,
                }

            # Fetch recent OI data from CCXT using modern adapter
            end_dt = dt.now(UTC)

            try:
                # Use quant_framework CCXT adapter for OI data
                exchange_class = getattr(ccxt, exchange_name)
                ccxt_client = exchange_class()

                adapter = CCXTOpenInterestAdapter(
                    client=ccxt_client, config={"market_type": market_type}
                )

                # Map exchange to proper venue
                venue = _get_venue_for_exchange(exchange_name, market_type)

                # Set venue on adapter (CCXT adapters support multiple venues)
                adapter.venue = venue
                adapter.wrapper = WrapperImplementation.CCXT
                adapter.supports_multiple_venues = True
                adapter.supported_asset_classes = {AssetClass.CRYPTO}

                # Create proper instrument_id: BTCUSD_PERP_BINANCE_COINM
                instrument_id = (
                    f"{canonical_symbol}{quote_asset}_PERP_{venue.value.upper()}"
                )

                # Create instrument with all required fields
                instrument = Instrument(
                    instrument_id=instrument_id,
                    asset_class=AssetClass.CRYPTO,
                    market_type=MarketType(market_type),
                    venue=venue,
                    wrapper=WrapperImplementation.CCXT,
                    base_asset=canonical_symbol,
                    quote_asset=quote_asset,
                    raw_symbol=ccxt_symbol,
                    is_inverse=True,
                    is_active=True,
                )

                # Fetch OI data
                records = await adapter.fetch_open_interest(
                    instrument=instrument,
                    timeframe="1h",
                    start=start_dt,
                    end=end_dt,
                    limit=200,  # 1h * 200 = ~8 days of data
                )

                if not records:
                    logger.info(f"⏭️ No new OI data for {ccxt_symbol}")
                    return {
                        "success": True,
                        "records": 0,
                        "exchange": exchange_name,
                        "symbol": ccxt_symbol,
                        "message": "no new data",
                    }

                # Normalize records to schema format and enrich with metadata
                data_dicts = []
                for r in records:
                    if isinstance(r, dict):
                        record_dict = r.copy()
                    elif hasattr(r, "model_dump"):
                        record_dict = r.model_dump()
                    elif hasattr(r, "dict"):
                        record_dict = r.dict()
                    else:
                        record_dict = dict(r)

                    # Transform CCXT format to schema format
                    # CCXT returns: {'timestamp': ms, 'symbol': 'BTC/USDT:USDT', 'openInterestValue': float}
                    # Schema expects: {'timestamp': datetime, 'open_interest': float, 'canonical_symbol': str, 'exchange': str, 'timeframe': str}

                    # Convert timestamp from milliseconds to datetime
                    if "timestamp" in record_dict and isinstance(
                        record_dict["timestamp"], (int, float)
                    ):
                        record_dict["timestamp"] = dt.fromtimestamp(
                            record_dict["timestamp"] / 1000, UTC
                        )

                    # Rename openInterestValue to open_interest
                    if "openInterestValue" in record_dict:
                        record_dict["open_interest"] = record_dict.pop(
                            "openInterestValue"
                        )

                    # Skip records with None open_interest
                    if record_dict.get("open_interest") is None:
                        continue

                    # Remove fields not in schema
                    record_dict.pop("symbol", None)
                    record_dict.pop("info", None)

                    # Enrich with required metadata fields
                    record_dict["exchange"] = exchange_name
                    record_dict["canonical_symbol"] = canonical_symbol
                    record_dict["timeframe"] = "1h"
                    data_dicts.append(record_dict)

                logger.info(
                    f"📥 Fetched {len(data_dicts)} records from {exchange_name}"
                )

                # Group by day and write to MinIO via shared BronzeRegistry
                registry = BronzeRegistry.get_default()
                daily_batches = group_by_day(data_dicts)
                total_written = 0

                for day_date, day_records in daily_batches.items():
                    try:
                        date_obj = dt.strptime(day_date, "%Y%m%d").replace(tzinfo=UTC)
                        request = BronzeIngestionRequest(
                            source=venue,
                            data_type=MarketDataType.OPEN_INTEREST,
                            instrument=instrument,
                            raw_data=day_records,
                            file_format="raw_json",
                            compression="none",
                            custom_metadata={
                                "exchange": exchange_name,
                                "timeframe": "1h",
                                "date": day_date,
                            },
                        )
                        await registry.ingest_raw_data(request)
                        total_written += len(day_records)
                        logger.info(
                            f"✅ Wrote {len(day_records)} records for {day_date}"
                        )
                    except Exception as e:
                        logger.error(f"Failed to write batch for {day_date}: {e}")
                        raise

                # Update checkpoint with latest timestamp
                if data_dicts:
                    latest_ts = max(
                        r.get("timestamp") for r in data_dicts if r.get("timestamp")
                    )
                    if isinstance(latest_ts, dt):
                        latest_ms = int(latest_ts.timestamp() * 1000)
                    else:
                        latest_ms = (
                            latest_ts if isinstance(latest_ts, int) else int(latest_ts)
                        )

                    updated_cp = checkpoints.update_ccxt_checkpoint(
                        exchange=exchange_name,
                        market_type=market_type,
                        ccxt_symbol=ccxt_symbol,
                        timeframe="1h",
                        last_successful_timestamp=latest_ms,
                        total_records=total_written,
                        metadata={
                            "data_source": "ccxt",
                            "data_type": "oi",
                            "batch_size": total_written,
                        },
                    )
                    logger.info(
                        f"✅ Updated checkpoint: {total_written} records written"
                    )

                return {
                    "success": True,
                    "records": total_written,
                    "exchange": exchange_name,
                    "symbol": ccxt_symbol,
                    "combo": combo,
                }

            except Exception as e:
                logger.error(
                    f"Failed to fetch OI data for {ccxt_symbol}: {e}", exc_info=True
                )
                raise

        except Exception as exc:
            logger.error(
                f"❌ {combo['exchange']}/{combo['canonical_symbol']}: {exc}",
                exc_info=True,
            )
            return {
                "success": False,
                "error": str(exc),
                "combo": combo,
            }

    loop = asyncio.get_event_loop()
    return loop.run_until_complete(_run())


@task
def aggregate_results(results: list[dict]) -> dict:
    """Aggregate results and summarize."""
    successful = sum(1 for r in results if r.get("success"))
    failed = len(results) - successful
    total_records = sum(r.get("records", 0) for r in results if r.get("success"))

    summary = {
        "total_combos": len(results),
        "successful": successful,
        "failed": failed,
        "total_records_published": total_records,
        "success_rate": (successful / len(results) * 100) if results else 0,
    }

    logger.info(f"📊 Cycle Summary: {summary}")

    if failed > 0:
        logger.warning(f"⚠️ {failed} combos failed")

    return summary


# =============================================================================
# DAG DEFINITION
# =============================================================================


@dag(
    dag_id="ccxt_oi_1h_inverse_incremental",
    description="CCXT 1h Open Interest incremental fetcher for inverse perpetuals",
    schedule="0 * * * *",
    start_date=datetime(2026, 1, 1),
    catchup=False,
    max_active_runs=1,
    max_active_tasks=4,
    default_args=DEFAULT_ARGS,
    tags=["ccxt", "oi", "1h", "inverse"],
    doc_md=__doc__,
    is_paused_upon_creation=False,
)
def ccxt_oi_1h_inverse_incremental():
    """CCXT 1h OI incremental DAG for inverse perpetuals."""

    wait_for_backfill = ExternalTaskSensor(
        task_id="wait_for_coinalyze_backfill",
        external_dag_id="coinalyze_oi_backfill",
        external_task_id=None,
        allowed_states=[DagRunState.SUCCESS],
        failed_states=[DagRunState.FAILED],
        mode="reschedule",
        poke_interval=300,
        timeout=60 * 60 * 12,
    )

    # Step 1: Load config
    cfg = load_config()

    # Step 2: Build combinations (inverse only)
    combos = build_fetch_combinations(cfg)

    # Step 3: Process each combination in parallel via dynamic task mapping
    results = process_combo.expand(combo=combos, config=cfg)

    # Step 4: Aggregate and validate
    summary = aggregate_results(results)

    wait_for_backfill >> cfg >> combos >> results >> summary


# Instantiate the DAG
ccxt_oi_1h_inverse_dag = ccxt_oi_1h_inverse_incremental()
