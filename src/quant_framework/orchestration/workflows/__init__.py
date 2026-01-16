"""
Workflows Module - Business Logic Coordination
==============================================

Provides workflow abstractions for coordinating multi-step operations:
- Backfill workflows: Historical data ingestion
- Incremental workflows: Real-time streaming updates
- Validation workflows: Cross-source data validation

Workflows coordinate between adapters, processors, and storage while
remaining independent of Airflow-specific implementations.
"""

from quant_framework.orchestration.workflows.backfill_workflow import (
    BackfillChunk,
    BackfillRequest,
    BackfillResult,
    BackfillWorkflow,
)
from quant_framework.orchestration.workflows.base import (
    AirflowWorkflowContext,
    BaseWorkflow,
    WorkflowResult,
)
from quant_framework.orchestration.workflows.incremental_workflow import (
    IncrementalRequest,
    IncrementalWorkflow,
)

__all__ = [
    # Base
    "BaseWorkflow",
    "AirflowWorkflowContext",
    "WorkflowResult",
    # Backfill
    "BackfillWorkflow",
    "BackfillRequest",
    "BackfillChunk",
    "BackfillResult",
    # Incremental
    "IncrementalWorkflow",
    "IncrementalRequest",
]
