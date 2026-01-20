"""
Airflow tasks for historical data initialization.
Wires BackfillCoordinator with proper dependencies.
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

import logging
from datetime import datetime
from typing import Any

from airflow.decorators import task

logger = logging.getLogger(__name__)


@task
async def check_initialization_status(
    symbol: str, timeframe: str, data_type: str, **context
) -> dict[str, Any]:
    """
    Check if initialization is already complete for this combination.

    Args:
        symbol: Trading symbol
        timeframe: Data timeframe
        data_type: Type of data ('ohlc', 'oi')

    Returns:
        {
            'needs_initialization': bool,
            'last_completed_date': str | None,
            'total_records': int
        }
    """
    from quant_framework.infrastructure.database.ports import DatabaseAdapter
    from quant_framework.legacy import Database

    db = Database()
    await db.connect()

    try:
        db_adapter = DatabaseAdapter(db)

        # Check for existing checkpoint
        query = """
            SELECT 
                status,
                last_completed_date,
                total_records_written
            FROM system.backfill_checkpoints
            WHERE symbol = $1 
              AND timeframe = $2 
              AND data_type = $3
              AND status = 'completed'
        """

        result = await db_adapter.fetch_one(query, symbol, timeframe, data_type)

        if result:
            logger.info(
                f"✅ Initialization already completed for {symbol}/{timeframe}/{data_type}: "
                f"{result['total_records_written']} records"
            )
            return {
                "needs_initialization": False,
                "last_completed_date": result["last_completed_date"].isoformat(),
                "total_records": result["total_records_written"],
            }
        else:
            logger.info(
                f"🔄 Initialization needed for {symbol}/{timeframe}/{data_type}"
            )
            return {
                "needs_initialization": True,
                "last_completed_date": None,
                "total_records": 0,
            }

    finally:
        await db.disconnect()


@task
async def execute_backfill(
    symbols: list[str],
    timeframes: list[str],
    data_types: list[str],
    start_date: str,
    end_date: str,
    exchanges: list[str] = None,
    **context,
) -> dict[str, Any]:
    """
    Execute historical backfill using BackfillCoordinator.

    Args:
        symbols: List of symbols to backfill
        timeframes: List of timeframes
        data_types: List of data types ('ohlc', 'oi')
        start_date: Start date (ISO format)
        end_date: End date (ISO format)
        exchanges: List of exchanges for OI data

    Returns:
        Summary of backfill results
    """
    # Ensure DAG root is on sys.path for absolute imports
    try:
        _dags_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
        if _dags_root not in sys.path:
            sys.path.append(_dags_root)
    except Exception:
        pass

    from common.bronze_writer import BronzeWriter

    from quant_framework.ingestion.backfill.checkpoint_manager import (
        CheckpointManager,
        MinIOCheckpointStore,
    )
    from quant_framework.ingestion.backfill.coordinator import (
        BackfillRequest,
    )
    from quant_framework.ingestion.backfill.rate_limiter import (
        CoinAlyzeRateLimiter,
    )

    logger.info(f"🚀 Starting backfill: {symbols} x {timeframes} x {data_types}")

    # Wire up dependencies using master ingestion container
    rate_limiter = CoinAlyzeRateLimiter(verbose=True)
    checkpoint_store = MinIOCheckpointStore()
    checkpoint_manager = CheckpointManager(checkpoint_store)
    bronze_writer = BronzeWriter()

    # Create master ingestion container (wires everything)
    from quant_framework.ingestion.dependency_container import (
        IngestionDependencyContainer,
    )

    ingestion_container = IngestionDependencyContainer(
        rate_limiter=rate_limiter,
        checkpoint_manager=checkpoint_manager,
        bronze_writer=bronze_writer,
        verbose=True,
    )

    # Get fully wired coordinator
    coordinator = ingestion_container.create_backfill_coordinator()

    # Create backfill request
    request = BackfillRequest(
        symbols=symbols,
        timeframes=timeframes,
        data_types=data_types,
        start_date=datetime.fromisoformat(start_date),
        end_date=datetime.fromisoformat(end_date),
        exchanges=exchanges or ["binance", "bybit", "okx", "deribit", "bitget"],
    )

    # Execute backfill
    results = await coordinator.execute_backfill(request)

    # Prepare summary
    summary = {
        "total_combinations": len(results),
        "completed": sum(1 for r in results.values() if r.status == "completed"),
        "partial": sum(1 for r in results.values() if r.status == "partial"),
        "failed": sum(1 for r in results.values() if r.status == "failed"),
        "total_records": sum(r.total_records for r in results.values()),
        "total_duration_seconds": sum(r.duration_seconds for r in results.values()),
        "results": {
            key: {
                "status": result.status,
                "total_chunks": result.total_chunks,
                "successful_chunks": result.successful_chunks,
                "total_records": result.total_records,
                "duration_seconds": result.duration_seconds,
            }
            for key, result in results.items()
        },
    }

    logger.info(
        f"✅ Backfill completed: {summary['completed']} succeeded, "
        f"{summary['failed']} failed, {summary['total_records']:,} records"
    )

    return summary


@task
async def validate_bronze_data(
    symbol: str,
    timeframe: str,
    data_type: str,
    expected_start_date: str,
    expected_end_date: str,
    **context,
) -> dict[str, Any]:
    """
    Validate that bronze layer contains expected data.

    Args:
        symbol: Trading symbol
        timeframe: Data timeframe
        data_type: Type of data
        expected_start_date: Expected start date
        expected_end_date: Expected end date

    Returns:
        Validation result dictionary
    """
    from datetime import datetime

    from common.bronze_writer import BronzeWriter

    logger.info(f"🔍 Validating bronze data for {symbol}/{timeframe}/{data_type}")

    bronze_writer = BronzeWriter()

    # Calculate expected number of daily partitions
    start = datetime.fromisoformat(expected_start_date)
    end = datetime.fromisoformat(expected_end_date)
    expected_days = (end - start).days + 1

    # List objects in bronze layer
    prefix = f"bronze/source=coinalyze/data_type={data_type}/symbol={symbol}/"

    try:
        response = bronze_writer.s3_client.list_objects_v2(
            Bucket=bronze_writer.bucket, Prefix=prefix
        )

        if "Contents" not in response:
            logger.warning(
                f"⚠️  No data found in bronze layer for {symbol}/{timeframe}/{data_type}"
            )
            return {
                "valid": False,
                "found_files": 0,
                "expected_days": expected_days,
                "message": "No data found in bronze layer",
            }

        files_found = len(response["Contents"])

        validation_result = {
            "valid": files_found > 0,
            "found_files": files_found,
            "expected_days": expected_days,
            "coverage_percentage": (files_found / expected_days * 100)
            if expected_days > 0
            else 0,
            "message": f"Found {files_found} files for {expected_days} expected days",
        }

        if validation_result["coverage_percentage"] < 90:
            logger.warning(
                f"⚠️  Low coverage: {validation_result['coverage_percentage']:.1f}% "
                f"({files_found}/{expected_days} days)"
            )
        else:
            logger.info(
                f"✅ Good coverage: {validation_result['coverage_percentage']:.1f}% "
                f"({files_found}/{expected_days} days)"
            )

        return validation_result

    except Exception as e:
        logger.error(f"❌ Validation failed: {e}")
        return {
            "valid": False,
            "found_files": 0,
            "expected_days": expected_days,
            "message": f"Validation error: {str(e)}",
        }


@task
async def mark_initialization_complete(
    symbol: str, timeframe: str, data_type: str, total_records: int, **context
) -> dict[str, str]:
    """
    Mark initialization as complete in database.

    Args:
        symbol: Trading symbol
        timeframe: Data timeframe
        data_type: Type of data
        total_records: Total records written

    Returns:
        Completion status
    """
    from quant_framework.infrastructure.database.ports import DatabaseAdapter
    from quant_framework.legacy import Database

    logger.info(f"✅ Marking initialization complete: {symbol}/{timeframe}/{data_type}")

    db = Database()
    await db.connect()

    try:
        db_adapter = DatabaseAdapter(db)

        # Update or insert initialization state
        query = """
            INSERT INTO system.backfill_initialization_state (
                symbol, timeframe, data_type,
                initialization_completed_at,
                total_records_initialized,
                status
            )
            VALUES ($1, $2, $3, NOW(), $4, 'completed')
            ON CONFLICT (symbol, timeframe, data_type)
            DO UPDATE SET
                initialization_completed_at = NOW(),
                total_records_initialized = EXCLUDED.total_records_initialized,
                status = 'completed'
        """

        await db_adapter.execute_query(
            query, symbol, timeframe, data_type, total_records
        )

        logger.info(
            f"✅ Initialization marked complete: {symbol}/{timeframe}/{data_type} "
            f"with {total_records:,} records"
        )

        return {
            "status": "completed",
            "symbol": symbol,
            "timeframe": timeframe,
            "data_type": data_type,
            "total_records": total_records,
        }

    finally:
        await db.disconnect()


@task
def get_backfill_summary(**context) -> dict[str, Any]:
    """
    Get overall backfill progress summary from XCom.

    Returns:
        Summary of all backfill operations
    """
    ti = context["ti"]

    # Pull results from previous tasks
    backfill_results = ti.xcom_pull(task_ids="execute_backfill")
    validation_results = ti.xcom_pull(task_ids="validate_bronze_data")

    summary = {
        "backfill": backfill_results or {},
        "validation": validation_results or {},
        "timestamp": datetime.utcnow().isoformat(),
    }

    logger.info("=" * 80)
    logger.info("INITIALIZATION SUMMARY")
    logger.info("=" * 80)
    logger.info(f"Backfill results: {backfill_results}")
    logger.info(f"Validation results: {validation_results}")
    logger.info("=" * 80)

    return summary
