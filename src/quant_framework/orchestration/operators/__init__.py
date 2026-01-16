"""
Operators Module - Airflow Task Wrappers
========================================

Provides reusable Airflow operators for common data operations:
- Fetch operators: Data retrieval from adapters
- Write operators: Persistence to bronze/silver layers
- Validation operators: Data quality checks
- Checkpoint operators: State management
"""

from quant_framework.orchestration.operators.base import (
    BaseDataOperator,
    OperatorResult,
)

__all__ = [
    "BaseDataOperator",
    "OperatorResult",
]
