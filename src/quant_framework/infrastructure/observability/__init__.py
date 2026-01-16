"""
The monitoring layer provides deep visibility into the health and quality of the entire financial data ecosystem, going beyond simple infrastructure metrics to assess the data itself. It continuously tracks critical data quality dimensions such as timeliness, ensuring real-time feeds are not stale; completeness, detecting gaps in historical sequences; and accuracy, identifying statistical anomalies or outliers in market prices. This module integrates logging, metrics, and alerting systems to proactively notify engineers of pipeline latency, connection failures, or data validation breaches. By embedding observability into every stage of the lifecycle, it enables rapid root cause analysis and remediation, ensuring that the platform maintains the high-trust standards required for algorithmic trading
"""

from .logging import (
    get_api_logger,
    # Convenience aliases
    get_database_logger,
    # Layer-specific logger factories
    get_infrastructure_logger,
    get_ingestion_logger,
    # Base logger factory
    get_logger,
    get_pipeline_logger,
    get_processing_logger,
    get_storage_logger,
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
    "get_pipeline_logger",
    "get_processing_logger",
    "get_storage_logger",
    "get_api_logger",
    # Aliases
    "get_database_logger",
]
