"""
Base Workflow Implementation
============================

Provides base workflow class and workflow context implementations.
"""

import logging
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any

from quant_framework.orchestration.ports import (
    IWorkflowContext,
    WorkflowStatus,
)

logger = logging.getLogger(__name__)


@dataclass
class WorkflowResult:
    """Result of workflow execution."""

    status: WorkflowStatus
    duration_seconds: float
    records_processed: int = 0
    records_written: int = 0
    errors: list[str] = field(default_factory=list)
    metadata: dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        """Convert to dictionary for serialization."""
        return {
            "status": self.status.value,
            "duration_seconds": self.duration_seconds,
            "records_processed": self.records_processed,
            "records_written": self.records_written,
            "errors": self.errors,
            "metadata": self.metadata,
        }


class AirflowWorkflowContext:
    """
    Workflow context implementation for Airflow.

    Wraps Airflow task instance context to provide a consistent interface
    for workflow execution regardless of orchestration platform.
    """

    def __init__(self, ti: Any, **kwargs):
        """
        Initialize Airflow context wrapper.

        Args:
            ti: Airflow TaskInstance
            **kwargs: Additional context from Airflow (dag_run, params, etc.)
        """
        self._ti = ti
        self._context = kwargs
        self._logger = logging.getLogger(f"{__name__}.{ti.task_id}" if ti else __name__)

    @property
    def workflow_id(self) -> str:
        """Unique identifier for this workflow execution."""
        if self._ti:
            return f"{self._ti.dag_id}_{self._ti.task_id}_{self._ti.execution_date}"
        return "unknown_workflow"

    @property
    def execution_date(self) -> datetime:
        """Logical execution date/time."""
        if self._ti:
            return self._ti.execution_date
        return datetime.utcnow()

    @property
    def params(self) -> dict[str, Any]:
        """Workflow parameters passed at runtime."""
        if "params" in self._context:
            return self._context["params"]
        if self._ti and hasattr(self._ti, "dag_run") and self._ti.dag_run:
            return self._ti.dag_run.conf or {}
        return {}

    def get_parameter(self, key: str, default: Any = None) -> Any:
        """Get a workflow parameter by key."""
        return self.params.get(key, default)

    def set_state(self, key: str, value: Any) -> None:
        """Set workflow state (XCom push in Airflow)."""
        if self._ti:
            self._ti.xcom_push(key=key, value=value)
        else:
            self._logger.warning(f"No TaskInstance available to push XCom: {key}")

    def get_state(self, key: str, task_id: str | None = None) -> Any:
        """Get workflow state (XCom pull in Airflow)."""
        if self._ti:
            return self._ti.xcom_pull(key=key, task_ids=task_id)
        else:
            self._logger.warning(f"No TaskInstance available to pull XCom: {key}")
            return None

    def log_info(self, message: str) -> None:
        """Log informational message."""
        self._logger.info(message)

    def log_warning(self, message: str) -> None:
        """Log warning message."""
        self._logger.warning(message)

    def log_error(self, message: str) -> None:
        """Log error message."""
        self._logger.error(message)


class BaseWorkflow(ABC):
    """
    Base class for workflow implementations.

    Provides template for workflow execution with standard lifecycle:
    1. Validate preconditions
    2. Execute workflow logic
    3. Handle errors and cleanup
    4. Report results

    Subclasses implement _execute_impl() with specific business logic.
    """

    def __init__(self):
        """Initialize base workflow."""
        self._status = WorkflowStatus.PENDING
        self._start_time: datetime | None = None
        self._end_time: datetime | None = None
        self._logger = logging.getLogger(f"{__name__}.{self.__class__.__name__}")

    async def execute(self, context: IWorkflowContext) -> dict[str, Any]:
        """
        Execute the workflow with standard lifecycle.

        Args:
            context: Workflow execution context

        Returns:
            Execution result dictionary

        Raises:
            WorkflowError: If workflow execution fails critically
        """
        self._start_time = datetime.utcnow()
        self._status = WorkflowStatus.RUNNING

        context.log_info(f"Starting workflow: {self.__class__.__name__}")

        try:
            # 1. Validate preconditions
            is_valid, errors = await self.validate(context)
            if not is_valid:
                self._status = WorkflowStatus.FAILED
                error_msg = f"Workflow validation failed: {errors}"
                context.log_error(error_msg)
                return self._create_failure_result(errors)

            # 2. Execute workflow logic
            context.log_info("Executing workflow logic")
            result = await self._execute_impl(context)

            # 3. Mark success
            self._status = WorkflowStatus.SUCCESS
            self._end_time = datetime.utcnow()

            # 4. Report results
            duration = (self._end_time - self._start_time).total_seconds()
            context.log_info(f"Workflow completed successfully in {duration:.2f}s")

            # Add duration to result
            if isinstance(result, dict):
                result["duration_seconds"] = duration

            return result

        except Exception as e:
            self._status = WorkflowStatus.FAILED
            self._end_time = datetime.utcnow()
            duration = (self._end_time - self._start_time).total_seconds()

            error_msg = f"Workflow execution failed: {str(e)}"
            context.log_error(error_msg)

            return self._create_failure_result([error_msg], duration=duration)

    async def validate(self, context: IWorkflowContext) -> tuple[bool, list[str]]:
        """
        Validate workflow preconditions.

        Default implementation always returns valid.
        Override in subclasses for custom validation.

        Args:
            context: Workflow execution context

        Returns:
            Tuple of (is_valid, error_messages)
        """
        return True, []

    @abstractmethod
    async def _execute_impl(self, context: IWorkflowContext) -> dict[str, Any]:
        """
        Execute workflow-specific logic.

        Subclasses must implement this method with their business logic.

        Args:
            context: Workflow execution context

        Returns:
            Execution result dictionary

        Raises:
            WorkflowError: If execution fails
        """
        pass

    def get_status(self) -> WorkflowStatus:
        """Get current workflow status."""
        return self._status

    def _create_failure_result(
        self,
        errors: list[str],
        duration: float | None = None,
    ) -> dict[str, Any]:
        """Create a failure result dictionary."""
        if duration is None and self._start_time and self._end_time:
            duration = (self._end_time - self._start_time).total_seconds()

        return {
            "status": WorkflowStatus.FAILED.value,
            "errors": errors,
            "duration_seconds": duration or 0,
            "records_processed": 0,
            "records_written": 0,
        }
