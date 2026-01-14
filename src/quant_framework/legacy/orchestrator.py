"""Compatibility wrapper for the legacy CoreOrchestrator."""

from mnemo_quant.pipelines.orchestration.core.orchestrator import (
    CoreOrchestrator,
    OrchestrationConfig,
)

__all__ = ["CoreOrchestrator", "OrchestrationConfig"]
