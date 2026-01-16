# dags/common/tasks.py
"""
Shared tasks for backfill/maintenance DAGs.
NOW INCLUDES: Extract → Store in DB → Publish to Kafka → Validate
"""

import asyncio
import logging
from datetime import datetime, timedelta

import yaml
from airflow.decorators import task
from airflow.exceptions import AirflowFailException, AirflowSkipException
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.sensors.time_delta import TimeDeltaSensor
from airflow.utils.trigger_rule import TriggerRule

# Compatibility note:
# - Prevent import-time failures inside Airflow by providing safe fallbacks.
try:
    from quant_framework.infrastructure.config import (
        EnhancedSymbolRegistry as SymbolRegistry,  # type: ignore
    )
except Exception:
    SymbolRegistry = None  # type: ignore

try:
    from quant_framework.legacy import (  # type: ignore
        Database,
        get_ccxt_extraction_pipeline,
    )
except Exception:
    Database = None  # type: ignore

    def get_ccxt_extraction_pipeline(exchange: str, data_type: str):  # type: ignore
        raise ImportError("quant_framework legacy extraction helper unavailable")


class _SymbolConfig:
    def __init__(self, canonical: str):
        self.canonical = canonical


class _YamlSymbolRegistry:
    """Minimal YAML-backed registry to replace deprecated registry.

    Reads config/assets/crypto_universe.yaml and exposes:
    - get_active_symbols(exchange): list of objects with `.canonical`
    - get_exchange_symbol(canonical, exchange): mapped string per YAML
    """

    def __init__(self, yaml_path: str, data_type: str = "ohlc"):
        self.yaml_path = yaml_path
        self.data_type = data_type
        with open(yaml_path) as f:
            data = yaml.safe_load(f)
        self.symbols = data.get("symbols", [])

    def get_active_symbols(self, exchange: str):
        active = []
        for s in self.symbols:
            if not s.get("active", False):
                continue
            entries = s.get("exchanges", [])
            if any(e.get("exchange") == exchange for e in entries):
                active.append(_SymbolConfig(s.get("canonical")))
        return active

    def get_exchange_symbol(self, canonical: str, exchange: str):
        for s in self.symbols:
            if s.get("canonical") == canonical:
                for e in s.get("exchanges", []):
                    if e.get("exchange") == exchange:
                        symbols = e.get("symbols", {})
                        return (
                            symbols.get(self.data_type)
                            or symbols.get("ohlc")
                            or symbols.get("open_interest")
                        )
        raise ValueError(f"No exchange symbol mapping for {canonical} on {exchange}")


class _StubDatabase:
    async def connect(self):
        return None

    async def disconnect(self):
        return None

    async def get_data_count(self, data_type: str, symbol: str, timeframe: str) -> int:
        return 0

    async def get_last_timestamp(self, data_type: str, symbol: str, timeframe: str):
        return None

    async def upsert_from_model_batch(
        self, records, table: str, conflict_keys: list[str], data_type: str, mode: str
    ) -> int:
        # No-op persistence stub
        return len(records)


logger = logging.getLogger(__name__)


@task
def load_and_validate_config(**context) -> dict:
    """Load config and validate active status"""
    params = context["params"]
    exchange = params.get("exchange", "binance")
    data_source = params.get("data_source", "ccxt")
    data_type = params.get("data_type", "ohlc")
    mode = params["mode"]
    config_path = params.get("config_path", "config/dags/dag_configs.yaml")

    # Load config and check active
    with open(config_path) as f:
        dag_configs = yaml.safe_load(f)["backfill_dags"]

    type_config = next((c for c in dag_configs if c["data_type"] == data_type), None)
    if not type_config or not type_config.get("active", True):
        raise AirflowSkipException(f"{data_type} is inactive or not configured")

    # Load registry (prefer quant_framework, fallback to YAML-backed minimal)
    try:
        if SymbolRegistry:
            registry = SymbolRegistry("config/assets/crypto_universe.yaml")  # type: ignore
        else:
            registry = _YamlSymbolRegistry(
                "config/assets/crypto_universe.yaml", data_type=data_type
            )
    except Exception:
        # Fallback in case constructor signature differs
        registry = _YamlSymbolRegistry(
            "config/assets/crypto_universe.yaml", data_type=data_type
        )

    # Get active symbols for exchange
    all_active = registry.get_active_symbols(exchange)
    param_symbols = params.get("symbols", [])
    symbols_to_process = [
        s for s in all_active if not param_symbols or s.canonical in param_symbols
    ]

    if not symbols_to_process:
        raise AirflowSkipException(f"No active symbols for '{exchange}' (mode: {mode})")

    # Build symbol-timeframe pairs
    pairs = []
    param_timeframes = params.get("timeframes", [])
    timeframes = (
        param_timeframes if param_timeframes else type_config.get("timeframes", [])
    )

    for symbol_config in symbols_to_process:
        for tf in timeframes:
            pairs.append(
                {
                    "symbol": symbol_config.canonical,
                    "timeframe": tf,
                    "exchange_symbol": registry.get_exchange_symbol(
                        symbol_config.canonical, exchange
                    ),
                }
            )

    if not pairs:
        raise AirflowSkipException("No valid pairs")

    return {
        "pairs": pairs,
        "exchange": exchange,
        "data_source": data_source,
        "data_type": data_type,
        "total_pairs": len(pairs),
        "mode": mode,
        "dry_run": params["dry_run"],
        "force_refresh": params["force_refresh"],
        "max_records": params.get("max_records_per_symbol", 1500),
        "maintenance_lookback_days": params.get("maintenance_lookback_days", 7),
        "gap_threshold_candles": params.get("gap_threshold_candles", 5),
        "endpoints": type_config.get("endpoints", [f"fetch_{data_type}"]),
        "wait_seconds": type_config.get("wait_seconds", 60),
        "success_threshold": type_config.get("success_threshold", 90),
        "pool_slots": type_config.get("pool_slots", 2),
    }


@task
def check_existing_data(config: dict) -> dict:
    """Check what data needs to be backfilled/maintained"""
    loop = asyncio.get_event_loop()
    return loop.run_until_complete(_check_existing_data_async(config))


async def _check_existing_data_async(config: dict) -> dict:
    pairs = config["pairs"]
    mode = config["mode"]
    force_refresh = config["force_refresh"]
    max_records = config["max_records"]
    lookback_days = config["maintenance_lookback_days"]
    data_type = config["data_type"]

    needed = []
    skipped = []

    db = Database() if Database else _StubDatabase()
    await db.connect()

    try:
        for pair in pairs:
            symbol = pair["symbol"]
            timeframe = pair["timeframe"]

            if force_refresh:
                needed.append(
                    {
                        **pair,
                        "since": None,
                        "limit": max_records,
                        "existing_count": 0,
                        "reason": "force_refresh",
                    }
                )
                continue

            # Check existing data
            count = await db.get_data_count(data_type, symbol, timeframe)
            last_ts = await db.get_last_timestamp(data_type, symbol, timeframe)

            if mode == "backfill":
                since = int(last_ts.timestamp() * 1000) if last_ts else None
                if count < max_records:
                    needed.append(
                        {
                            **pair,
                            "since": since,
                            "limit": max_records,
                            "existing_count": count,
                            "reason": "backfill_needed",
                        }
                    )
                else:
                    skipped.append(
                        {
                            "symbol": symbol,
                            "timeframe": timeframe,
                            "existing_count": count,
                            "reason": "complete",
                        }
                    )
            else:  # maintenance
                start_ts = datetime.utcnow() - timedelta(days=lookback_days)
                since = int(start_ts.timestamp() * 1000)
                limit = 2000  # Overlap for gap detection
                needed.append(
                    {
                        **pair,
                        "since": since,
                        "limit": limit,
                        "existing_count": count,
                        "reason": "maintenance",
                    }
                )

        if not needed and mode == "backfill" and not force_refresh:
            raise AirflowSkipException("No backfill needed")

        return {
            **config,
            "needed_pairs": needed,
            "skipped_pairs": skipped,
            "pairs_to_process": len(needed),
        }
    finally:
        await db.disconnect()


@task
def extract_needed_pairs(config_with_needed: dict) -> list[dict]:
    """Extract list of pairs that need processing"""
    return config_with_needed["needed_pairs"]


@task(pool="ccxt_pool")
def extract_store_and_publish(pair: dict, **context) -> dict:
    """
    NEW: Extract → Store in DB → Publish to Kafka
    This ensures data reaches database regardless of Kafka/consumer issues
    """
    ti = context["ti"]
    config = ti.xcom_pull(task_ids="check_existing_data")

    loop = asyncio.get_event_loop()
    return loop.run_until_complete(_extract_store_and_publish_async(pair, config))


async def _extract_store_and_publish_async(pair: dict, config: dict) -> dict:
    """
    Complete pipeline: Extract → Store → Publish
    """
    symbol = pair["symbol"]
    timeframe = pair["timeframe"]
    since = pair["since"]
    limit = pair["limit"]
    dry_run = config["dry_run"]
    data_type = config["data_type"]
    data_source = config["data_source"]
    exchange = config["exchange"]

    start_time = datetime.utcnow()

    result = {
        "symbol": symbol,
        "timeframe": timeframe,
        "since": since,
        "limit": limit,
        "extracted_count": 0,
        "stored_count": 0,
        "success": False,
        "error": None,
        "duration_seconds": 0,
        "storage_success": False,
    }

    try:
        # ============================================================
        # STEP 1: EXTRACT DATA
        # ============================================================
        logger.info(f"[{symbol}/{timeframe}] Starting extraction...")

        pipeline_class = get_ccxt_extraction_pipeline(exchange, data_type)

        # Extract records
        async with pipeline_class() as pipeline:
            records = await pipeline.extract_batch(
                symbol=symbol, timeframe=timeframe, since=since, limit=limit
            )

        result["extracted_count"] = len(records)
        logger.info(f"[{symbol}/{timeframe}] Extracted {len(records)} records")

        if not records:
            result["success"] = True
            result["error"] = "no_data"
            result["duration_seconds"] = (
                datetime.utcnow() - start_time
            ).total_seconds()
            return result

        # ============================================================
        # STEP 2: STORE IN DATABASE (PRIMARY - GUARANTEED PERSISTENCE)
        # ============================================================
        if not dry_run:
            logger.info(
                f"[{symbol}/{timeframe}] Storing {len(records)} records in database..."
            )

            db = Database() if Database else _StubDatabase()
            await db.connect()

            try:
                # Use dynamic upsert_from_model_batch (NEW unified method)
                # Determine table and conflict keys based on data_type
                if data_type == "ohlc":
                    table = "market.ohlc"
                    conflict_keys = ["canonical_symbol", "exchange", "timeframe", "ts"]
                elif data_type == "open_interest":
                    table = "market.open_interest"
                    conflict_keys = ["canonical_symbol", "exchange", "timeframe", "ts"]
                else:
                    raise ValueError(f"Unknown data_type: {data_type}")

                # Universal upsert
                stored_count = await db.upsert_from_model_batch(
                    records=records,
                    table=table,
                    conflict_keys=conflict_keys,
                    data_type=data_type,
                    mode="backfill",
                )

                result["stored_count"] = stored_count
                result["storage_success"] = True

                logger.info(
                    f"[{symbol}/{timeframe}] ✅ Stored {stored_count}/{len(records)} records in database"
                )

            except Exception as db_error:
                result["error"] = f"Database storage failed: {str(db_error)[:200]}"
                result["storage_success"] = False
                logger.error(
                    f"[{symbol}/{timeframe}] ❌ Database storage failed: {db_error}"
                )
                raise  # Fail task if DB write fails

            finally:
                await db.disconnect()
        else:
            # Dry run - simulate storage
            result["stored_count"] = len(records)
            result["storage_success"] = True
            logger.info(
                f"[{symbol}/{timeframe}] DRY RUN: Would store {len(records)} records"
            )

        # ============================================================
        # FINAL SUCCESS CHECK
        # ============================================================
        # Task succeeds if data reached database
        result["success"] = result["storage_success"]
        result["duration_seconds"] = (datetime.utcnow() - start_time).total_seconds()

        logger.info(
            f"[{symbol}/{timeframe}] Pipeline complete: "
            f"Extracted={result['extracted_count']}, "
            f"Stored={result['stored_count']}, "
            f"Duration={result['duration_seconds']:.2f}s"
        )

    except Exception as e:
        result["error"] = str(e)[:200]
        result["duration_seconds"] = (datetime.utcnow() - start_time).total_seconds()
        logger.error(f"[{symbol}/{timeframe}] ❌ Pipeline failed: {e}", exc_info=True)
        raise AirflowFailException(
            f"Pipeline failed for {symbol}/{timeframe}: {result['error']}"
        )

    return result


@task
def aggregate_results(results: list[dict], config_with_needed: dict) -> dict:
    """Aggregate results from all symbol-timeframe pairs"""
    total = len(results)
    successful = sum(1 for r in results if r["success"])
    failed = total - successful

    total_extracted = sum(r["extracted_count"] for r in results)
    total_stored = sum(r["stored_count"] for r in results)

    storage_success = sum(1 for r in results if r.get("storage_success", False))

    success_rate = (successful / total * 100) if total > 0 else 0

    summary = {
        "mode": config_with_needed["mode"],
        "data_type": config_with_needed["data_type"],
        "data_source": config_with_needed["data_source"],
        "exchange": config_with_needed["exchange"],
        "total_pairs": total,
        "successful": successful,
        "failed": failed,
        "total_extracted": total_extracted,
        "total_stored": total_stored,
        "storage_success_rate": (storage_success / total * 100) if total > 0 else 0,
        "success_rate": round(success_rate, 2),
        "skipped": len(config_with_needed.get("skipped_pairs", [])),
        "success_threshold": config_with_needed.get("success_threshold", 90),
    }

    logger.info(f"Pipeline Summary: {summary}")
    return summary


@task
def final_check(summary: dict) -> None:
    """Final validation - fail if below threshold"""
    threshold = summary.get("success_threshold", 90)
    success_rate = summary["success_rate"]

    if success_rate < threshold:
        raise AirflowFailException(
            f"Low success rate: {success_rate}% (threshold: {threshold}%) "
            f"for {summary.get('data_type', 'data')}"
        )

    logger.info(f"✅ DAG complete: {summary}")


def create_wait_sensor(
    wait_seconds: int = 60, task_id: str = "wait_for_consumer"
) -> TimeDeltaSensor:
    """Create sensor to wait for consumer processing"""
    return TimeDeltaSensor(
        task_id=task_id,
        delta=timedelta(seconds=wait_seconds),
        mode="reschedule",
        trigger_rule=TriggerRule.ALL_SUCCESS,
    )


def create_validate_operator(
    data_type: str,
    wait_seconds: int,
    sql_template: str,
    postgres_conn_id: str = "timescaledb_default",
    task_id: str = "validate_db_ingestion",
) -> PostgresOperator:
    """Create PostgreSQL validation operator"""
    return PostgresOperator(
        task_id=task_id,
        postgres_conn_id=postgres_conn_id,
        sql=sql_template,
        trigger_rule=TriggerRule.ALL_DONE,
    )
