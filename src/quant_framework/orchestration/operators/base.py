"""
Base Operator Implementation
============================

Provides base class for all orchestration operators with common functionality.
"""

import logging
from dataclasses import dataclass, field
from typing import Any

logger = logging.getLogger(__name__)


@dataclass
class OperatorResult:
    """Result of operator execution."""

    success: bool
    records_processed: int = 0
    duration_seconds: float = 0.0
    error: str | None = None
    metadata: dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        """Convert to dictionary for XCom."""
        return {
            "success": self.success,
            "records_processed": self.records_processed,
            "duration_seconds": self.duration_seconds,
            "error": self.error,
            "metadata": self.metadata,
        }


class BaseDataOperator:
    """
    Base class for data operators.

    Provides common functionality:
    - Error handling with retries
    - Duration tracking
    - Result formatting
    - Logging

    Note: This is NOT an Airflow BaseOperator subclass.
    It's a utility class for operator logic. Use with TaskFlow API or
    wrap in actual Airflow operators.
    """

    def __init__(
        self,
        task_id: str,
        max_retries: int = 3,
        retry_delay_seconds: int = 5,
    ):
        """
        Initialize base operator.

        Args:
            task_id: Task identifier
            max_retries: Maximum retry attempts
            retry_delay_seconds: Delay between retries
        """
        self.task_id = task_id
        self.max_retries = max_retries
        self.retry_delay_seconds = retry_delay_seconds
        self._logger = logging.getLogger(f"{__name__}.{task_id}")

    async def execute_with_retry(
        self,
        operation: callable,
        *args,
        **kwargs,
    ) -> Any:
        """
        Execute operation with retry logic.

        Args:
            operation: Async callable to execute
            *args: Positional arguments for operation
            **kwargs: Keyword arguments for operation

        Returns:
            Operation result

        Raises:
            Exception: If all retries exhausted
        """
        last_error = None

        for attempt in range(self.max_retries):
            try:
                result = await operation(*args, **kwargs)
                return result
            except Exception as e:
                last_error = e
                self._logger.warning(
                    f"Attempt {attempt + 1}/{self.max_retries} failed: {str(e)}"
                )

                if attempt < self.max_retries - 1:
                    import asyncio

                    await asyncio.sleep(self.retry_delay_seconds)

        raise last_error

    def create_result(
        self,
        success: bool,
        records_processed: int = 0,
        duration: float = 0.0,
        error: str | None = None,
        **metadata,
    ) -> OperatorResult:
        """
        Create operator result.

        Args:
            success: Whether operation succeeded
            records_processed: Number of records processed
            duration: Duration in seconds
            error: Error message if failed
            **metadata: Additional metadata

        Returns:
            OperatorResult instance
        """
        return OperatorResult(
            success=success,
            records_processed=records_processed,
            duration_seconds=duration,
            error=error,
            metadata=metadata,
        )

    def log_info(self, message: str) -> None:
        """Log informational message."""
        self._logger.info(message)

    def log_warning(self, message: str) -> None:
        """Log warning message."""
        self._logger.warning(message)

    def log_error(self, message: str) -> None:
        """Log error message."""
        self._logger.error(message)
