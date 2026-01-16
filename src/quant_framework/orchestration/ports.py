"""
Orchestration Layer Protocol Definitions
=========================================

Defines protocol interfaces for workflow coordination, operators, and checkpoint management.
These protocols enable dependency injection and testing without concrete implementations.
"""

from datetime import datetime
from enum import Enum
from typing import Any, Protocol, TypeVar, runtime_checkable

# Type variable for generic protocols
T = TypeVar("T")


class WorkflowStatus(str, Enum):
    """Workflow execution status."""

    PENDING = "pending"
    RUNNING = "running"
    SUCCESS = "success"
    FAILED = "failed"
    CANCELLED = "cancelled"
    RETRYING = "retrying"


class CheckpointType(str, Enum):
    """Type of checkpoint storage backend."""

    MINIO = "minio"  # File-based in MinIO/S3
    DATABASE = "database"  # Database-backed (PostgreSQL)
    HYBRID = "hybrid"  # Both MinIO and database


@runtime_checkable
class IWorkflowContext(Protocol):
    """
    Abstraction over workflow execution context.

    Wraps Airflow context (ti, dag_run, etc.) to decouple workflow logic
    from Airflow-specific implementations.
    """

    @property
    def workflow_id(self) -> str:
        """Unique identifier for this workflow execution."""
        ...

    @property
    def execution_date(self) -> datetime:
        """Logical execution date/time."""
        ...

    @property
    def params(self) -> dict[str, Any]:
        """Workflow parameters passed at runtime."""
        ...

    def get_parameter(self, key: str, default: Any = None) -> Any:
        """Get a workflow parameter by key."""
        ...

    def set_state(self, key: str, value: Any) -> None:
        """Set workflow state (XCom push in Airflow)."""
        ...

    def get_state(self, key: str, task_id: str | None = None) -> Any:
        """Get workflow state (XCom pull in Airflow)."""
        ...

    def log_info(self, message: str) -> None:
        """Log informational message."""
        ...

    def log_warning(self, message: str) -> None:
        """Log warning message."""
        ...

    def log_error(self, message: str) -> None:
        """Log error message."""
        ...


@runtime_checkable
class IWorkflow(Protocol):
    """
    Protocol defining workflow execution interface.

    A workflow coordinates multiple operations (fetch, transform, validate, write)
    with error handling, retries, and progress tracking.
    """

    async def execute(self, context: IWorkflowContext) -> dict[str, Any]:
        """
        Execute the workflow.

        Args:
            context: Workflow execution context

        Returns:
            Execution result dictionary with status, metrics, errors

        Raises:
            WorkflowError: If workflow execution fails
        """
        ...

    async def validate(self, context: IWorkflowContext) -> tuple[bool, list[str]]:
        """
        Validate workflow preconditions.

        Args:
            context: Workflow execution context

        Returns:
            Tuple of (is_valid, error_messages)
        """
        ...

    def get_status(self) -> WorkflowStatus:
        """Get current workflow status."""
        ...


@runtime_checkable
class IOperator(Protocol):
    """
    Protocol for Airflow operator interface.

    Operators wrap specific operations (fetch, write, validate) into
    reusable Airflow tasks.
    """

    def execute(self, context: dict[str, Any]) -> Any:
        """
        Execute the operator logic.

        Args:
            context: Airflow context dictionary

        Returns:
            Operator result (pushed to XCom)
        """
        ...


@runtime_checkable
class ICheckpointStore(Protocol):
    """
    Unified checkpoint storage interface.

    Abstracts checkpoint persistence across MinIO (file-based) and
    PostgreSQL (database-backed) implementations.
    """

    async def save_checkpoint(
        self,
        symbol: str,
        timeframe: str,
        data_type: str,
        source: str,
        last_timestamp: int,
        metadata: dict[str, Any] | None = None,
    ) -> bool:
        """
        Save checkpoint for a data stream.

        Args:
            symbol: Symbol identifier (e.g., "BTC")
            timeframe: Timeframe (e.g., "1h")
            data_type: Type of data (e.g., "ohlcv", "open_interest")
            source: Data source (e.g., "ccxt", "coinalyze")
            last_timestamp: Last successfully processed timestamp (Unix ms)
            metadata: Additional checkpoint metadata

        Returns:
            True if save succeeded
        """
        ...

    async def load_checkpoint(
        self,
        symbol: str,
        timeframe: str,
        data_type: str,
        source: str,
    ) -> dict[str, Any] | None:
        """
        Load checkpoint for a data stream.

        Args:
            symbol: Symbol identifier
            timeframe: Timeframe
            data_type: Type of data
            source: Data source

        Returns:
            Checkpoint data dict or None if not found
            Dict contains: last_timestamp, metadata, updated_at
        """
        ...

    async def delete_checkpoint(
        self,
        symbol: str,
        timeframe: str,
        data_type: str,
        source: str,
    ) -> bool:
        """
        Delete checkpoint for a data stream.

        Args:
            symbol: Symbol identifier
            timeframe: Timeframe
            data_type: Type of data
            source: Data source

        Returns:
            True if delete succeeded
        """
        ...

    async def list_checkpoints(
        self,
        source: str | None = None,
        data_type: str | None = None,
    ) -> list[dict[str, Any]]:
        """
        List all checkpoints, optionally filtered.

        Args:
            source: Optional source filter
            data_type: Optional data type filter

        Returns:
            List of checkpoint dicts
        """
        ...


@runtime_checkable
class IChunkStrategy(Protocol):
    """
    Protocol for time-range chunking strategies.

    Splits large time ranges into manageable chunks for backfill operations.
    """

    def generate_chunks(
        self,
        start_date: datetime,
        end_date: datetime,
        timeframe: str,
    ) -> list[tuple[datetime, datetime]]:
        """
        Generate time range chunks.

        Args:
            start_date: Backfill start date
            end_date: Backfill end date
            timeframe: Data timeframe (affects chunk size)

        Returns:
            List of (chunk_start, chunk_end) tuples
        """
        ...


@runtime_checkable
class IRateLimiter(Protocol):
    """
    Protocol for rate limiting API requests.

    Manages request throttling based on API rate limits and error responses.
    """

    async def acquire(self) -> None:
        """
        Acquire permission to make a request.

        Blocks until rate limit allows the request.
        """
        ...

    async def record_success(self) -> None:
        """Record a successful request."""
        ...

    async def record_error(self, error_code: int | None = None) -> None:
        """
        Record a failed request.

        Args:
            error_code: HTTP error code (429 for rate limit)
        """
        ...

    def get_current_rate(self) -> float:
        """
        Get current request rate (requests per second).

        Returns:
            Current rate
        """
        ...


@runtime_checkable
class IProgressReporter(Protocol):
    """
    Protocol for reporting workflow progress.

    Tracks and reports execution progress for long-running workflows.
    """

    def report_start(self, total_items: int) -> None:
        """
        Report workflow start.

        Args:
            total_items: Total number of items to process
        """
        ...

    def report_progress(self, completed_items: int, message: str | None = None) -> None:
        """
        Report progress update.

        Args:
            completed_items: Number of completed items
            message: Optional progress message
        """
        ...

    def report_completion(self, success: bool, summary: dict[str, Any]) -> None:
        """
        Report workflow completion.

        Args:
            success: Whether workflow succeeded
            summary: Summary statistics
        """
        ...


@runtime_checkable
class IDagBuilder(Protocol):
    """
    Protocol for programmatic DAG construction.

    Builds Airflow DAG objects from configuration.
    """

    def build(self) -> Any:  # Returns airflow.models.DAG
        """
        Build and return an Airflow DAG object.

        Returns:
            Airflow DAG instance
        """
        ...

    def validate(self) -> tuple[bool, list[str]]:
        """
        Validate DAG configuration.

        Returns:
            Tuple of (is_valid, error_messages)
        """
        ...


# Exception classes


class WorkflowError(Exception):
    """Base exception for workflow errors."""

    pass


class CheckpointError(Exception):
    """Exception for checkpoint-related errors."""

    pass


class OperatorError(Exception):
    """Exception for operator execution errors."""

    pass


class ValidationError(Exception):
    """Exception for workflow validation errors."""

    pass


# Export all protocols and types

__all__ = [
    # Enums
    "WorkflowStatus",
    "CheckpointType",
    # Protocols
    "IWorkflowContext",
    "IWorkflow",
    "IOperator",
    "ICheckpointStore",
    "IChunkStrategy",
    "IRateLimiter",
    "IProgressReporter",
    "IDagBuilder",
    # Exceptions
    "WorkflowError",
    "CheckpointError",
    "OperatorError",
    "ValidationError",
]
