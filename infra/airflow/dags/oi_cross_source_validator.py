"""
Cross-Source Open Interest Validator DAG.

Validates lag and consistency between Coinalyze backfill and CCXT incremental checkpoints.
Runs every 6 hours to ensure CCXT stays within 2-4h of Coinalyze.

Schedule: Every 6 hours
Checks: All BTC/ETH combinations across Binance/Bybit/GateIO
Alerts: 2h warn, 4h critical
"""

import logging
from datetime import datetime, timedelta

from airflow.decorators import dag, task
from airflow.exceptions import AirflowFailException

from quant_framework.infrastructure.checkpoint import (
    CheckpointCoordinator,
    CheckpointStore,
)

logger = logging.getLogger(__name__)

DEFAULT_ARGS = {
    "owner": "data-eng",
    "depends_on_past": False,
    "email_on_failure": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

SYMBOLS = ["BTC", "ETH"]
EXCHANGES = ["binance", "bybit", "gateio"]
MARKET_TYPE = "linear_perpetual"
TIMEFRAME = "5m"


@task
def validate_all_gaps() -> dict:
    """Check all CCXT checkpoints vs Coinalyze for lag and consistency."""
    cp_coord = CheckpointCoordinator(CheckpointStore())
    results = []
    alerts = []
    warnings = []

    for symbol in SYMBOLS:
        coinalyze_cp = cp_coord.load_coinalyze_checkpoint(symbol, TIMEFRAME)
        if not coinalyze_cp:
            logger.warning(f"⚠️ No Coinalyze checkpoint for {symbol}/{TIMEFRAME}")
            continue

        for exchange in EXCHANGES:
            ccxt_cp = cp_coord.load_ccxt_checkpoint(
                market_type=MARKET_TYPE,
                exchange=exchange,
                symbol=f"{symbol}USDT",
                timeframe=TIMEFRAME,
            )
            if not ccxt_cp:
                logger.debug(f"No CCXT checkpoint for {symbol}/{exchange}/{TIMEFRAME}")
                continue

            gap = cp_coord.validate_gap(
                coinalyze_checkpoint=coinalyze_cp,
                ccxt_checkpoint=ccxt_cp,
                warn_threshold_ms=2 * 60 * 60 * 1000,
                alert_threshold_ms=4 * 60 * 60 * 1000,
            )

            result = {
                "symbol": symbol,
                "exchange": exchange,
                "lag_ms": gap.lag_ms,
                "lag_hours": gap.lag_ms / (60 * 60 * 1000),
                "alert": gap.alert,
                "warn": gap.warn,
                "message": gap.message,
            }
            results.append(result)

            if gap.alert:
                alerts.append(result)
                logger.error(
                    f"🚨 ALERT: {symbol}/{exchange} lag {gap.lag_ms}ms - {gap.message}"
                )
            elif gap.warn:
                warnings.append(result)
                logger.warning(
                    f"⚠️ WARN: {symbol}/{exchange} lag {gap.lag_ms}ms - {gap.message}"
                )
            else:
                logger.info(f"✅ {symbol}/{exchange}: {gap.message}")

    summary = {
        "total_pairs": len(results),
        "alerts": len(alerts),
        "warnings": len(warnings),
        "alert_details": alerts,
        "warning_details": warnings,
        "all_results": results,
    }

    logger.info(
        f"📊 Validation Summary: {len(results)} pairs, {len(alerts)} alerts, {len(warnings)} warnings"
    )

    if alerts:
        logger.error(
            f"Critical lag detected: {len(alerts)} pair(s) >4h behind Coinalyze"
        )

    return summary


@task
def check_thresholds(summary: dict) -> None:
    """Fail DAG if critical alerts detected."""
    if summary["alerts"]:
        raise AirflowFailException(
            f"CCXT lag threshold exceeded: {len(summary['alerts'])} "
            f"pair(s) more than 4h behind Coinalyze. "
            f"Details: {summary['alert_details']}"
        )


# =============================================================================
# DAG DEFINITION
# =============================================================================


@dag(
    dag_id="oi_cross_source_validator",
    description="Validate Open Interest lag between Coinalyze and CCXT",
    schedule="0 */6 * * *",
    start_date=datetime(2026, 1, 1),
    catchup=False,
    max_active_runs=1,
    default_args=DEFAULT_ARGS,
    tags=["validation", "oi", "cross_source"],
    doc_md=__doc__,
)
def oi_cross_source_validator():
    """OI cross-source validator DAG."""

    summary = validate_all_gaps()
    check_thresholds(summary)


# Instantiate the DAG
oi_cross_source_validator_dag = oi_cross_source_validator()
