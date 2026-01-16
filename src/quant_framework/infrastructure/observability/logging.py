"""
Structured logging infrastructure for mnemo-quant.
Provides consistent, machine-readable logs across all services.

Log Structure:
    {
        "app": "mnemo-quant",         # Application identifier
        "layer": "ingestion",          # Architectural layer
        "component": "ccxt-adapter",   # Specific component/service
        "module": "...",               # Python module (optional)
        "exchange": "binance",         # Domain context
        "event": "ohlcv_fetched",      # What happened
        ...
    }

Architectural Layers:
    - infrastructure: Cross-cutting (database, storage, config, checkpoints)
    - ingestion: Data acquisition (CCXT, Coinalyze, validators)
    - pipeline: Orchestration (Airflow DAGs, schedulers)
    - processing: Data transformation & validation
    - storage: Data persistence layer (if separate from infrastructure)
    - api: REST API services
    - strategy: Trading strategies (future)
    - backtest: Strategy backtesting (future)
"""

import logging
import sys
from typing import Any, Literal

import structlog
from structlog.types import EventDict

# Define valid architectural layers
Layer = Literal[
    "infrastructure", "ingestion", "pipeline", "processing", "storage", "api"
]


def add_app_context(logger: Any, method_name: str, event_dict: EventDict) -> EventDict:
    """
    Add application-wide context to every log entry.

    This ensures every log has the base 'app' identifier, useful when:
    - Running multiple applications in the same environment
    - Aggregating logs from different systems
    - Filtering logs in centralized logging (Loki, etc.)
    """
    event_dict["app"] = "mnemo-quant"
    return event_dict


def add_severity_level(
    logger: Any, method_name: str, event_dict: EventDict
) -> EventDict:
    """
    Add severity level for cloud logging compatibility.
    Maps Python log levels to standard severity levels (Cloud Logging, Stackdriver, etc.).
    """
    level = event_dict.get("level")
    if level:
        severity_map = {
            "debug": "DEBUG",
            "info": "INFO",
            "warning": "WARNING",
            "error": "ERROR",
            "critical": "CRITICAL",
        }
        event_dict["severity"] = severity_map.get(level, "INFO")
    return event_dict


def setup_logging(
    level: str = "INFO",
    json_logs: bool = True,
    include_timestamp: bool = True,
) -> None:
    """
    Configure structured logging for the application.

    Args:
        level: Logging level (DEBUG, INFO, WARNING, ERROR, CRITICAL)
        json_logs: If True, output JSON. If False, use human-readable format (dev mode).
        include_timestamp: Whether to include ISO timestamps in logs

    Usage:
        >>> from quant_framework.infrastructure.observability import setup_logging
        >>> setup_logging(level="DEBUG", json_logs=True)
    """
    # Convert string level to logging constant
    log_level = getattr(logging, level.upper(), logging.INFO)

    # Configure Python's standard logging
    logging.basicConfig(
        format="%(message)s",
        stream=sys.stdout,
        level=log_level,
    )

    # Build processor chain
    processors = [
        structlog.contextvars.merge_contextvars,  # Merge context variables
        add_app_context,  # Add app identifier
        structlog.stdlib.add_log_level,  # Add log level
        add_severity_level,  # Add severity (for cloud compatibility)
        structlog.stdlib.PositionalArgumentsFormatter(),  # Handle %s formatting
        structlog.processors.StackInfoRenderer(),  # Add stack traces when requested
        structlog.processors.format_exc_info,  # Format exceptions nicely
        structlog.processors.UnicodeDecoder(),  # Handle unicode
    ]

    # Add timestamp at the beginning if requested
    if include_timestamp:
        processors.insert(0, structlog.processors.TimeStamper(fmt="iso"))

    # Choose output format based on environment
    if json_logs:
        # Production: JSON for log aggregation
        processors.append(structlog.processors.JSONRenderer())
    else:
        # Development: Pretty console output with colors
        processors.append(
            structlog.dev.ConsoleRenderer(
                colors=True,
                exception_formatter=structlog.dev.plain_traceback,
            )
        )

    # Configure structlog
    structlog.configure(
        processors=processors,
        wrapper_class=structlog.stdlib.BoundLogger,
        context_class=dict,
        logger_factory=structlog.stdlib.LoggerFactory(),
        cache_logger_on_first_use=True,
    )


def get_logger(
    name: str | None = None,
    layer: Layer | None = None,
    component: str | None = None,
    **initial_context: Any,
) -> structlog.stdlib.BoundLogger:
    """
    Get a configured logger instance with architectural context.

    Args:
        name: Logger name (typically __name__ of the calling module)
        layer: Architectural layer (infrastructure, ingestion, pipeline, etc.)
        component: Specific component/service within the layer
        **initial_context: Additional context key-value pairs to bind to logger

    Returns:
        Configured structlog logger with bound context

    Usage:
        >>> log = get_logger(
        ...     __name__,
        ...     layer="ingestion",
        ...     component="ccxt-adapter",
        ...     exchange="binance"
        ... )
        >>> log.info("connection_established", latency_ms=45)
    """
    logger = structlog.get_logger(name)

    # Build context with layer hierarchy
    context = {}

    if layer:
        context["layer"] = layer

    if component:
        context["component"] = component

    # Add module path for debugging (if name provided)
    if name:
        context["module"] = name

    # Merge with additional context
    context.update(initial_context)

    # Bind all context
    if context:
        logger = logger.bind(**context)

    return logger


# ============================================================================
# Layer-Specific Logger Factories
# ============================================================================


def get_infrastructure_logger(
    component: str,
    **context: Any,
) -> structlog.stdlib.BoundLogger:
    """
    Get a logger for infrastructure layer (database, storage, config, checkpoints).

    Args:
        component: Component name (e.g., "database-adapter", "checkpoint-coordinator", "minio-client")
        **context: Additional context (table, bucket, checkpoint_id, etc.)

    Usage:
        >>> log = get_infrastructure_logger("database-adapter", table="ohlcv")
        >>> log.info("query_executed", rows_affected=1000)
    """
    return get_logger(
        "infrastructure",
        layer="infrastructure",
        component=component,
        **context,
    )


def get_ingestion_logger(
    component: str,
    exchange: str | None = None,
    **context: Any,
) -> structlog.stdlib.BoundLogger:
    """
    Get a logger for ingestion layer (data acquisition).

    Args:
        component: Component name (e.g., "ccxt-adapter", "coinalyze-adapter", "validator")
        exchange: Exchange name (e.g., "binance", "bybit") - optional
        **context: Additional context (symbol, market_type, timeframe, etc.)

    Usage:
        >>> log = get_ingestion_logger("ccxt-adapter", exchange="binance", symbol="BTC/USDT")
        >>> log.info("fetching_ohlcv", timeframe="5m")
    """
    ctx = {}
    if exchange:
        ctx["exchange"] = exchange
    ctx.update(context)

    return get_logger(
        "ingestion",
        layer="ingestion",
        component=component,
        **ctx,
    )


def get_pipeline_logger(
    component: str = "airflow-dag",
    dag_id: str | None = None,
    **context: Any,
) -> structlog.stdlib.BoundLogger:
    """
    Get a logger for pipeline/orchestration layer (Airflow DAGs, schedulers).

    Args:
        component: Component name (default: "airflow-dag")
        dag_id: Airflow DAG ID (optional)
        **context: Additional context (task_id, execution_date, etc.)

    Usage:
        >>> log = get_pipeline_logger(dag_id="ccxt_backfill", task_id="fetch_binance")
        >>> log.info("task_started")
    """
    ctx = {}
    if dag_id:
        ctx["dag_id"] = dag_id
    ctx.update(context)

    return get_logger(
        "pipeline",
        layer="pipeline",
        component=component,
        **ctx,
    )


def get_processing_logger(
    component: str,
    **context: Any,
) -> structlog.stdlib.BoundLogger:
    """
    Get a logger for processing layer (data transformation, validation, normalization).

    Args:
        component: Component name (e.g., "validator", "normalizer", "aggregator")
        **context: Additional context

    Usage:
        >>> log = get_processing_logger("validator", validation_type="cross_source")
        >>> log.info("validation_started")
    """
    return get_logger(
        "processing",
        layer="processing",
        component=component,
        **context,
    )


def get_storage_logger(
    component: str,
    **context: Any,
) -> structlog.stdlib.BoundLogger:
    """
    Get a logger for storage layer (if separate from infrastructure).

    Args:
        component: Component name (e.g., "timescale-writer", "minio-archiver")
        **context: Additional context

    Usage:
        >>> log = get_storage_logger("timescale-writer", table="ohlcv")
        >>> log.info("batch_inserted", records=1000)
    """
    return get_logger(
        "storage",
        layer="storage",
        component=component,
        **context,
    )


def get_api_logger(
    component: str = "fastapi",
    **context: Any,
) -> structlog.stdlib.BoundLogger:
    """
    Get a logger for API layer (REST API services).

    Args:
        component: Component name (default: "fastapi")
        **context: Additional context

    Usage:
        >>> log = get_api_logger()
        >>> log.info("request_received", method="GET", path="/health")
    """
    return get_logger(
        "api",
        layer="api",
        component=component,
        **context,
    )


# Alias for backward compatibility
def get_database_logger(**context: Any) -> structlog.stdlib.BoundLogger:
    """
    Convenience alias for database logging (maps to infrastructure layer).

    Usage:
        >>> log = get_database_logger(table="ohlcv")
        >>> log.info("query_executed", duration_ms=123)
    """
    return get_infrastructure_logger("database-adapter", **context)
