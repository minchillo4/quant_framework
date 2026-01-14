"""
CoinAlyze Open Interest Backfill DAG

Medallion Architecture: Writes daily Open Interest Parquet files to MinIO bronze layer.

Identical to coinalyze_ohlc_backfill.py but for data_type='oi' instead of 'ohlc'.

Architecture:
- Incremental-first checkpointing at day granularity
- Rolling discovery with cached probe results
- Hybrid parallelism: sequential symbols + parallel timeframes (5-slot pool)
- Soft failure per symbol/timeframe with centralized validation
- Trigger rule 'all_done' ensures validation runs even if some tasks fail

DAG Structure:
  load_symbols()
    ↓
  chain(BTC, ETH, XRP)  ← sequential
    ↓
  expand(timeframes)  ← parallel in pool='coinalyze_api' (5 slots)
    ↓
  [discover, fetch_and_write, validate_timeframe]
    ↓
  validate_backfill_completion (trigger_rule='all_done')
"""

from datetime import UTC, datetime, timedelta
from typing import Any

from airflow.decorators import dag, task
from common.bronze_tasks import (
    discover_historical_start_date,
    fetch_and_write_historical_data,
)

# ============================================================================
# DAG CONFIGURATION
# ============================================================================

default_args = {
    "owner": "data-eng",
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "on_failure_callback": None,  # Could add alerting here
}

dag_config = {
    "dag_id": "coinalyze_oi_backfill",
    "description": "Backfill CoinAlyze Open Interest data to bronze layer (incremental + rolling discovery)",
    "schedule": "@once",  # Run once on initialization to bootstrap CCXT incremental DAGs
    "catchup": False,
    "max_active_runs": 1,  # Prevent concurrent runs
    "tags": ["coinalyze", "bronze", "open_interest", "initialization"],
}

# Timeframes to backfill
TIMEFRAMES = ["5m", "1h"]

# Target symbols (from dag_configs.yaml or hardcoded)
# Only BTC and ETH for consistency across all DAGs
TARGET_SYMBOLS = ["BTC", "ETH"]

# End date for backfill (today) - use timezone-aware UTC
END_DATE = datetime.now(tz=UTC).replace(hour=0, minute=0, second=0, microsecond=0)


# ============================================================================
# TASK DEFINITIONS
# ============================================================================


@task
def load_symbols() -> list[str]:
    """Load target symbols from configuration."""
    import logging

    logger = logging.getLogger(__name__)
    logger.info(f"📦 Loading symbols: {TARGET_SYMBOLS}")
    return TARGET_SYMBOLS


@task
def validate_timeframe_completion(result: dict[str, Any]) -> None:
    """
    Validate that a single timeframe completed (or failed softly).

    Used to ensure we log status before moving to next timeframe.
    """
    import logging

    logger = logging.getLogger(__name__)

    if result.get("status") == "success":
        logger.info(
            f"✅ {result['symbol']}/{result['timeframe']}: "
            f"{result['records_written']} records written"
        )
    else:
        logger.warning(
            f"⚠️ {result['symbol']}/{result['timeframe']}: "
            f"Soft failure - {result.get('error')}"
        )


@task(trigger_rule="all_done")
def validate_backfill_completion(all_results: list[dict[str, Any]]) -> dict[str, Any]:
    """
    Final validation task with trigger_rule='all_done'.

    Runs even if some upstream tasks failed (soft failures).
    Aggregates status and reports:
    - Total records written
    - Failed symbols/timeframes
    - Whether to alert or auto-retry

    Returns:
    {
        "total_records": int,
        "symbols_completed": int,
        "symbols_failed": int,
        "failed_symbols": List[str],
        "status": "success" | "partial_failure",
        "should_retry": bool
    }
    """
    import logging

    logger = logging.getLogger(__name__)

    if not all_results:
        return {
            "total_records": 0,
            "symbols_completed": 0,
            "symbols_failed": 0,
            "failed_symbols": [],
            "status": "failure",
            "should_retry": False,
        }

    total_records = 0
    failed_symbols = []

    for result in all_results:
        if isinstance(result, dict):
            if result.get("status") == "success":
                total_records += result.get("records_written", 0)
            else:
                failed_symbols.append(
                    f"{result.get('symbol')}/{result.get('timeframe')}"
                )

    symbols_completed = len(
        [r for r in all_results if isinstance(r, dict) and r.get("status") == "success"]
    )
    symbols_failed = len(
        [r for r in all_results if isinstance(r, dict) and r.get("status") == "failed"]
    )

    status = "success" if symbols_failed == 0 else "partial_failure"

    logger.info("📊 Backfill Summary:")
    logger.info(f"  Total records: {total_records}")
    logger.info(f"  Symbols completed: {symbols_completed}")
    logger.info(f"  Symbols failed: {symbols_failed}")
    if failed_symbols:
        logger.warning(f"  Failed: {', '.join(failed_symbols)}")

    return {
        "total_records": total_records,
        "symbols_completed": symbols_completed,
        "symbols_failed": symbols_failed,
        "failed_symbols": failed_symbols,
        "status": status,
        "should_retry": symbols_failed > 0,
    }


# ----------------------------------------------------------------------------
# SAFETY CHECKS
# ----------------------------------------------------------------------------


@task
def ensure_bronze_bucket() -> str:
    """Ensure the MinIO bronze bucket exists before any reads/writes."""
    import logging
    import os

    import boto3
    from botocore.exceptions import ClientError

    logger = logging.getLogger(__name__)

    bucket = os.getenv("MINIO_BUCKET_NAME", "bronze")
    endpoint = os.getenv("MINIO_ENDPOINT", "http://minio:9000")
    access_key = os.getenv("MINIO_ROOT_USER", "admin")
    secret_key = os.getenv("MINIO_ROOT_PASSWORD", "password123")
    region = os.getenv("MINIO_REGION", "us-east-1")

    s3 = boto3.client(
        "s3",
        endpoint_url=endpoint,
        aws_access_key_id=access_key,
        aws_secret_access_key=secret_key,
        region_name=region,
    )

    try:
        s3.head_bucket(Bucket=bucket)
        logger.info(f"🟢 Bucket exists: {bucket}")
    except ClientError as exc:
        code = exc.response.get("Error", {}).get("Code")
        if code in {"404", "NoSuchBucket"}:
            logger.warning(f"🪣 Bucket missing, creating: {bucket}")
            s3.create_bucket(Bucket=bucket)
            logger.info(f"✅ Created bucket: {bucket}")
        else:
            raise

    return bucket


# ============================================================================
# DAG DEFINITION
# ============================================================================


@dag(
    dag_id=dag_config["dag_id"],
    description=dag_config["description"],
    schedule=dag_config["schedule"],
    start_date=datetime(2024, 1, 1),
    catchup=dag_config["catchup"],
    max_active_runs=dag_config["max_active_runs"],
    default_args=default_args,
    tags=dag_config["tags"],
    doc_md=__doc__,
    is_paused_upon_creation=False,
)
def coinalyze_oi_backfill():
    """
    DAG: CoinAlyze Open Interest Backfill to MinIO Bronze Layer

    Fetches historical Open Interest data from CoinAlyze API and writes daily
    Parquet files to MinIO bronze layer, using checkpointing for resumption.

    Hybrid Parallelism:
    - Sequential symbols: BTC → ETH → XRP (respects API rate limits)
    - Parallel timeframes: 1m, 5m, 15m, 30m, 1h, 4h, 1d (via pool)

    Incremental Backfill:
    - Loads checkpoint from XCom (or discovers start date on fresh run)
    - Fetches only new data since last successful run
    - Saves checkpoint after each daily Parquet write for resumption

    Soft Failures:
    - Each timeframe task catches exceptions and returns dict with status
    - Final validation runs with trigger_rule='all_done' to aggregate results
    """

    # Step 0: Ensure bronze bucket exists before any checkpoints or writes
    bucket_ready = ensure_bronze_bucket()

    # Step 1: Load symbols (returns list via XCom)
    symbols_list = load_symbols()
    bucket_ready >> symbols_list

    # Step 2: Process each symbol sequentially
    # For simplicity, we'll create the task chain statically with SYMBOLS
    results = []

    for symbol in TARGET_SYMBOLS:
        # For each symbol, process each timeframe
        for timeframe in TIMEFRAMES:
            # Discovery task (cached in XCom)
            discovery = discover_historical_start_date(
                symbol=symbol,
                timeframe=timeframe,
                data_type="oi",
            )
            bucket_ready >> discovery
            symbols_list >> discovery

            # Fetch and write task (with checkpoint resumption)
            fetch_result = fetch_and_write_historical_data(
                symbol=symbol,
                timeframe=timeframe,
                data_type="oi",
                end_date=END_DATE,
            )

            # Validation task
            validate = validate_timeframe_completion(fetch_result)

            # Chain: discovery → fetch → validate
            discovery >> fetch_result >> validate

            results.append(fetch_result)

    # Step 3: Final validation with trigger_rule='all_done'
    # Collects all results and validates
    final_validation = validate_backfill_completion(results)

    # Step 4: Create Coinalyze checkpoints for CCXT bootstrap
    checkpoints_created = create_coinalyze_checkpoints(final_validation)
    final_validation >> checkpoints_created


# ============================================================================
# CHECKPOINT CREATION TASK
# ============================================================================


@task
def create_coinalyze_checkpoints(validation_summary: dict[str, Any]) -> dict[str, Any]:
    """
    Verify Coinalyze OI checkpoints exist and are accessible for CCXT bootstrap.

    NOTE: Checkpoints are now created and updated during fetch_and_write_historical_data().
    This task verifies they exist and are readable by downstream CCXT incremental DAGs.
    """
    import logging
    from datetime import datetime

    logger = logging.getLogger(__name__)

    try:
        from quant_framework.infrastructure.checkpoint import (
            CheckpointPathBuilder,
            CheckpointStore,
        )

        checkpoint_store = CheckpointStore()
        verified = 0
        failed = 0

        for symbol in TARGET_SYMBOLS:
            for timeframe in TIMEFRAMES:
                try:
                    key = CheckpointPathBuilder.coinalyze_backfill(
                        "oi", symbol, timeframe
                    )
                    checkpoint, _ = checkpoint_store.read(key)

                    if checkpoint and checkpoint.last_successful_timestamp:
                        ts_sec = checkpoint.last_successful_timestamp / 1000
                        last_ts = datetime.fromtimestamp(ts_sec, tz=UTC)
                        logger.info(
                            f"✅ Verified checkpoint: {key} "
                            f"(last successful: {last_ts.isoformat()})"
                        )
                        verified += 1
                    else:
                        logger.warning(f"⚠️ Checkpoint exists but empty: {key}")
                        failed += 1

                except Exception as e:
                    logger.error(
                        f"❌ Failed to verify checkpoint for {symbol}/{timeframe}: {e}"
                    )
                    failed += 1

        logger.info(
            f"✅ Checkpoint verification complete: {verified} verified, {failed} failed"
        )
        return {
            "status": "success" if failed == 0 else "partial_failure",
            "checkpoints_verified": verified,
            "checkpoints_failed": failed,
            "symbols": TARGET_SYMBOLS,
            "timeframes": TIMEFRAMES,
        }

    except ImportError:
        logger.warning("Checkpoint verification skipped: imports unavailable")
        return {
            "status": "skipped",
            "reason": "checkpoint modules unavailable",
        }
    except Exception as e:
        logger.error(f"❌ Checkpoint verification failed: {e}", exc_info=True)
        return {
            "status": "failed",
            "error": str(e),
        }


# Instantiate DAG
coinalyze_oi_backfill_dag = coinalyze_oi_backfill()
