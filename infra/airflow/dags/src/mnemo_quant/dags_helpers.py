# airflow/dags/mnemo_quant/dag_helpers.py
import logging
from collections.abc import Callable
from datetime import datetime, timedelta
from typing import Any

from airflow import DAG
from airflow.decorators import task

logger = logging.getLogger(__name__)


def create_incremental_dag(
    data_type: str,
    timeframe: str,
    schedule_interval: str,
    ingestion_pipeline_factory: Callable,
    assets_getter: Callable[[], list],
    lookback_minutes: int = 60,
    default_args: dict | None = None,
    tags: list | None = None,
    description_template: str = "Incremental {data_type} updates for {timeframe} timeframe",
) -> DAG:
    """
    Factory function to create incremental DAGs for any data type.
    """
    import asyncio

    # Set default values
    if default_args is None:
        default_args = {
            "owner": "quant-team",
            "retries": 2,
            "retry_delay": timedelta(minutes=2),
            "sla": timedelta(minutes=10),
        }

    if tags is None:
        tags = ["incremental", data_type, timeframe]
    else:
        tags = tags + ["incremental", data_type, timeframe]

    dag_id = f"{data_type}_incremental_{timeframe}"
    description = description_template.format(data_type=data_type, timeframe=timeframe)

    dag = DAG(
        dag_id=dag_id,
        default_args=default_args,
        description=description,
        schedule_interval=schedule_interval,
        start_date=datetime(2025, 1, 1),
        catchup=False,
        max_active_runs=1,
        tags=tags,
    )

    @task(dag=dag)
    def ingest_incremental(**context) -> dict[str, Any]:
        """Generic incremental update for any data type using metadata."""
        from quant_framework.legacy import Database

        async def run_incremental() -> dict[str, Any]:
            db = Database()
            await db.connect()
            try:
                pipeline = ingestion_pipeline_factory()
                assets = assets_getter()

                successful_total = 0
                failed_total = 0
                failed_assets = []

                for asset in assets:
                    try:
                        # Generic incremental update call
                        records_count = await pipeline.incremental_update(
                            asset=asset,
                            timeframe=timeframe,
                            safety_lookback_minutes=lookback_minutes,
                            db=db,
                        )
                        successful_total += records_count
                        logger.info(
                            f"Incremental {data_type} update for {asset}/{timeframe}: {records_count} records"
                        )
                    except Exception as e:
                        failed_total += 1
                        failed_assets.append(asset)
                        logger.error(
                            f"Incremental {data_type} update failed for {asset}/{timeframe}: {e}"
                        )

                return {
                    "data_type": data_type,
                    "timeframe": timeframe,
                    "successful": successful_total,
                    "failed": failed_total,
                    "failed_assets": failed_assets,
                    "total_assets": len(assets),
                }

            finally:
                await db.disconnect()

        return asyncio.run(run_incremental())

    @task(dag=dag)
    def validate_freshness(ingest_metrics: dict[str, Any]) -> dict[str, Any]:
        """Validate data freshness using metadata for any data type."""
        from quant_framework.legacy import Database

        async def validate_async() -> dict[str, Any]:
            db = Database()
            await db.connect()
            try:
                async with db.pool.acquire() as conn:
                    # Generic freshness check using data_type
                    freshness_threshold = datetime.utcnow() - timedelta(
                        minutes=lookback_minutes * 2
                    )

                    stale_count = await conn.fetchval(
                        """
                        SELECT COUNT(*) FROM market.metadata 
                        WHERE data_type = $1 
                        AND interval_type = $2
                        AND latest_ts < $3
                    """,
                        data_type,
                        timeframe,
                        freshness_threshold,
                    )

                    # Also check last update time
                    outdated_threshold = datetime.utcnow() - timedelta(
                        minutes=lookback_minutes
                    )
                    outdated_count = await conn.fetchval(
                        """
                        SELECT COUNT(*) FROM market.metadata 
                        WHERE data_type = $1 
                        AND interval_type = $2
                        AND last_incremental_at < $3
                    """,
                        data_type,
                        timeframe,
                        outdated_threshold,
                    )

                    logger.info(
                        f"Freshness check for {data_type}/{timeframe}: {stale_count} stale symbols, {outdated_count} outdated updates"
                    )

                    return {
                        "stale_symbols": stale_count,
                        "outdated_updates": outdated_count,
                    }
            finally:
                await db.disconnect()

        validation_results = asyncio.run(validate_async())
        return {**ingest_metrics, **validation_results}

    @task(dag=dag)
    def handle_results(metrics: dict[str, Any]) -> dict[str, Any]:
        """Handle results and trigger alerts if needed."""
        if metrics.get("failed", 0) > 0:
            logger.warning(
                f"Incremental {metrics['data_type']} update had {metrics['failed']} failures: {metrics.get('failed_assets', [])}"
            )
            # TODO: Add alerting logic here (Slack, email, etc.)

        if metrics.get("stale_symbols", 0) > 0:
            logger.error(
                f"Found {metrics['stale_symbols']} stale symbols for {metrics['data_type']}/{metrics['timeframe']}"
            )
            # TODO: Trigger backfill for stale symbols

        logger.info(f"Incremental {metrics['data_type']} update completed: {metrics}")
        return metrics

    # Set up task dependencies
    ingest_metrics = ingest_incremental()
    validation_results = validate_freshness(ingest_metrics)
    final_results = handle_results(validation_results)

    return dag


# Pre-configured factory functions for common data types


def create_oi_incremental_dag(
    timeframe: str, schedule_interval: str, lookback_minutes: int = 60
) -> DAG:
    """Factory function specifically for Open Interest incremental DAGs."""
    from config.settings import settings
    from quant_framework.legacy import OIIngestionPipeline

    return create_incremental_dag(
        data_type="open_interest",
        timeframe=timeframe,
        schedule_interval=schedule_interval,
        ingestion_pipeline_factory=OIIngestionPipeline,
        assets_getter=lambda: settings.assets.crypto.full_universe,
        lookback_minutes=lookback_minutes,
        description_template="Incremental Open Interest updates for {timeframe} timeframe",
        tags=["oi"],
    )
