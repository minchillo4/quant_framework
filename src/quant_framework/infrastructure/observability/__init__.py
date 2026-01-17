"""
Observability infrastructure for mnemo-quant.

Provides structured logging across all architectural layers:
- infrastructure: Database, storage, config, checkpoints
- ingestion: Data acquisition from exchanges
- orchestration: Airflow DAGs and schedulers
- transformation: Data validation and normalization
- storage: Data persistence operations
"""

from .logging import (
    # Convenience aliases
    get_database_logger,
    # Layer-specific loggers
    get_infrastructure_logger,
    get_ingestion_logger,
    # Base logger
    get_logger,
    get_orchestration_logger,
    get_processing_logger,
    get_storage_logger,
    get_transformation_logger,
    # Setup
    setup_logging,
)

__all__ = [
    # Setup
    "setup_logging",
    # Base
    "get_logger",
    # Layer-specific
    "get_infrastructure_logger",
    "get_ingestion_logger",
    "get_orchestration_logger",
    "get_transformation_logger",
    "get_storage_logger",
    # Aliases
    "get_database_logger",
    "get_processing_logger",
]
