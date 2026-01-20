"""
Historical Data Initialization DAG
Runs once on service initialization to populate bronze layer with historical data.
"""

# Ensure DAG root is on sys.path for absolute imports - MUST BE FIRST
import os
import sys

try:
    _dags_root = os.path.dirname(os.path.abspath(__file__))
    if _dags_root not in sys.path:
        sys.path.insert(0, _dags_root)
except Exception:
    pass

from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import BranchPythonOperator
from common.initialization_tasks import (
    execute_backfill,
    get_backfill_summary,
    mark_initialization_complete,
    validate_bronze_data,
)

# Default arguments
default_args = {
    "owner": "data-team",
    "depends_on_past": False,
    "email_on_failure": True,
    "email_on_retry": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}

# Configuration
SYMBOLS = ["BTC", "ETH"]  # Start with major symbols
TIMEFRAMES = ["1h", "4h", "1d"]
DATA_TYPES = ["ohlc", "oi"]
START_DATE = "2024-01-01"  # Adjust based on needs
END_DATE = datetime.utcnow().strftime("%Y-%m-%d")

# OI exchanges
OI_EXCHANGES = ["binance", "bybit", "okx", "deribit", "bitget"]


with DAG(
    dag_id="historical_data_initialization",
    default_args=default_args,
    description="One-time historical data backfill for service initialization",
    schedule=None,  # Manual trigger only
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["initialization", "backfill", "bronze", "coinalyze"],
    max_active_runs=1,
    doc_md=f"""
    ## Historical Data Initialization
    
    This DAG performs a one-time backfill of historical market data to initialize
    the bronze layer before starting real-time data ingestion.
    
    ### What it does:
    1. Checks if initialization is already complete
    2. Executes chunked backfill with rate limiting
    3. Writes data to MinIO/S3 bronze layer as Parquet
    4. Validates data completeness
    5. Marks initialization as complete in database
    
    ### Data Sources:
    - **OHLC**: CoinAlyze API (Binance)
    - **Open Interest**: CoinAlyze API (multiple exchanges)
    
    ### Run Configuration:
    - Symbols: {SYMBOLS}
    - Timeframes: {TIMEFRAMES}
    - Data Types: {DATA_TYPES}
    - Date Range: {START_DATE} to {END_DATE}
    
    ### Trigger:
    ```bash
    airflow dags trigger historical_data_initialization
    ```
    
    Or with custom config:
    ```bash
    airflow dags trigger historical_data_initialization \\
        --conf '{{"symbols": ["BTC", "ETH"], "start_date": "2024-01-01"}}'
    ```
    """,
) as dag:
    # Start marker
    start = EmptyOperator(task_id="start")

    # Check if already initialized
    # Note: This checks at DAG level. For per-symbol checks, use dynamic task mapping
    def check_if_needs_initialization(**context):
        """
        Branch based on whether initialization is needed.
        For MVP, always run backfill. In production, check database.
        """
        # TODO: Query database to check if ANY symbol needs initialization
        # For now, always proceed with backfill
        return "execute_backfill_task"

    check_branch = BranchPythonOperator(
        task_id="check_if_needs_initialization",
        python_callable=check_if_needs_initialization,
        provide_context=True,
    )

    # Skip marker if already initialized
    skip_initialization = EmptyOperator(task_id="skip_initialization")

    # Execute backfill
    backfill_task = execute_backfill.override(task_id="execute_backfill_task")(
        symbols="{{ dag_run.conf.get('symbols', %s) }}" % SYMBOLS,
        timeframes="{{ dag_run.conf.get('timeframes', %s) }}" % TIMEFRAMES,
        data_types="{{ dag_run.conf.get('data_types', %s) }}" % DATA_TYPES,
        start_date="{{ dag_run.conf.get('start_date', '%s') }}" % START_DATE,
        end_date="{{ dag_run.conf.get('end_date', '%s') }}" % END_DATE,
        exchanges=OI_EXCHANGES,
    )

    # Validate bronze data for each symbol/timeframe/data_type
    # For simplicity, validating one representative combination
    validate_task = validate_bronze_data.override(task_id="validate_bronze_data")(
        symbol="{{ dag_run.conf.get('symbols', %s)[0] }}" % SYMBOLS,  # First symbol
        timeframe="{{ dag_run.conf.get('timeframes', %s)[0] }}"
        % TIMEFRAMES,  # First timeframe
        data_type="{{ dag_run.conf.get('data_types', %s)[0] }}"
        % DATA_TYPES,  # First data type
        expected_start_date="{{ dag_run.conf.get('start_date', '%s') }}" % START_DATE,
        expected_end_date="{{ dag_run.conf.get('end_date', '%s') }}" % END_DATE,
    )

    # Mark as complete
    # In production, this should be dynamic per symbol/timeframe
    complete_task = mark_initialization_complete.override(task_id="mark_complete")(
        symbol="{{ dag_run.conf.get('symbols', %s)[0] }}" % SYMBOLS,
        timeframe="{{ dag_run.conf.get('timeframes', %s)[0] }}" % TIMEFRAMES,
        data_type="{{ dag_run.conf.get('data_types', %s)[0] }}" % DATA_TYPES,
        total_records="{{ ti.xcom_pull(task_ids='execute_backfill_task')['total_records'] }}",
    )

    # Get summary
    summary_task = get_backfill_summary.override(task_id="get_summary")()

    # End marker
    end = EmptyOperator(task_id="end", trigger_rule="none_failed_min_one_success")

    # Define dependencies
    start >> check_branch
    check_branch >> [backfill_task, skip_initialization]
    backfill_task >> validate_task >> complete_task >> summary_task >> end
    skip_initialization >> end
