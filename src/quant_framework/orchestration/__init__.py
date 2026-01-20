"""
Orchestration Layer - Workflow Coordination and DAG Management
==============================================================

The orchestration layer provides abstractions for coordinating data pipelines,
managing workflow execution, and building Airflow DAGs programmatically.

Architecture
------------

    ┌─────────────────────────────────────────────────────────────┐
    │                    Airflow DAG Files                        │
    │              (infra/airflow/dags/*.py)                      │
    └───────────────────────────┬─────────────────────────────────┘
                                │ uses
                                ▼
    ┌─────────────────────────────────────────────────────────────┐
    │              Orchestration Layer (this module)              │
    │                                                             │
    │  • DAG Builders: Programmatic DAG construction             │
    │  • Workflows: Business logic coordination                   │
    │  • Operators: Reusable Airflow operators                   │
    │  • Tasks: Common task functions                             │
    └───────────────────────────┬─────────────────────────────────┘
                                │ coordinates
                                ▼
    ┌─────────────────────────────────────────────────────────────┐
    │                    Ingestion Layer                          │
    │           (adapters, processors, normalizers)               │
    └───────────────────────────┬─────────────────────────────────┘
                                │ uses
                                ▼
    ┌─────────────────────────────────────────────────────────────┐
    │                  Infrastructure Layer                       │
    │           (database, checkpoint, config, logging)           │
    └─────────────────────────────────────────────────────────────┘

Module Structure
----------------

orchestration/
├── ports.py                    # Protocol definitions
│   ├── IWorkflow              # Workflow execution interface
│   ├── IWorkflowContext       # Context abstraction
│   ├── IOperator              # Operator interface
│   └── ICheckpointStore       # Unified checkpoint interface
│
├── workflows/                  # Workflow coordination
│   ├── base.py                # BaseWorkflow, WorkflowContext
│   ├── backfill_workflow.py   # Historical backfill coordination
│   ├── incremental_workflow.py # Incremental update coordination
│   └── validation_workflow.py  # Cross-source validation
│
├── operators/                  # Airflow operator implementations
│   ├── base.py                # BaseDataOperator
│   ├── fetch_operators.py     # FetchOHLCVOperator, FetchOIOperator
│   ├── write_operators.py     # WriteBronzeOperator, WriteSilverOperator
│   ├── validation_operators.py # ValidationOperator, GapDetector
│   └── checkpoint_operators.py # CheckpointReader, CheckpointWriter
│
├── dag_builders/               # Programmatic DAG construction
│   ├── base.py                # BaseDagBuilder
│   ├── backfill_dag_builder.py # Generate backfill DAGs
│   ├── incremental_dag_builder.py # Generate incremental DAGs
│   └── factory.py             # DagFactory (config-driven)
│
└── tasks/                      # Reusable task functions
    ├── fetch_tasks.py         # Generic fetch operations
    ├── write_tasks.py         # Generic write operations
    ├── validation_tasks.py    # Validation operations
    └── checkpoint_tasks.py    # Checkpoint management

Design Principles
-----------------

1. **Separation of Concerns**
   - Orchestration coordinates, does not execute
   - Business logic stays in ingestion/transformation layers
   - Airflow-specific code isolated in operators/builders

2. **Protocol-Based Interfaces**
   - IWorkflow, IOperator, IWorkflowContext for dependency injection
   - Enables testing without Airflow scheduler
   - Structural typing via typing.Protocol

3. **Checkpoint Unification**
   - Single ICheckpointStore protocol
   - Multiple implementations (MinIO file-based, PostgreSQL database-backed)
   - Consistent API across streaming and backfill workflows

4. **Airflow Decoupling**
   - WorkflowContext abstraction wraps Airflow context
   - Core workflow logic doesn't depend on Airflow
   - Operators handle Airflow-specific features

5. **Type Safety**
   - Full type hints throughout
   - Pydantic models for configuration
   - Mypy-compatible protocols

Usage Examples
--------------

Example 1: Using Workflow Abstraction
```python
from quant_framework.orchestration.workflows import BackfillWorkflow
from quant_framework.orchestration.ports import ICheckpointStore

# Create workflow
workflow = BackfillWorkflow(
    adapter=coinalyze_adapter,
    checkpoint_store=postgres_checkpoint_store,
    writer=bronze_writer,
)

# Execute backfill
result = await workflow.execute(
    symbol="BTC",
    timeframe="1h",
    start_date=datetime(2024, 1, 1),
    end_date=datetime(2024, 12, 31),
)
```

Example 2: Building DAG Programmatically
```python
from quant_framework.orchestration.dag_builders import BackfillDagBuilder

builder = BackfillDagBuilder(
    dag_id="coinalyze_btc_backfill",
    schedule=None,  # Manual trigger
    symbols=["BTC", "ETH"],
    timeframes=["1h", "4h"],
)

dag = builder.build()
```

Example 3: Custom Operator
```python
from quant_framework.orchestration.operators import FetchOHLCVOperator

fetch_task = FetchOHLCVOperator(
    task_id="fetch_btc_ohlcv",
    adapter=ccxt_adapter,
    symbol="BTC/USDT",
    timeframe="1h",
)
```

Integration Points
------------------

### With Ingestion Layer
Orchestration layer imports from ingestion:
- Adapters: CCXTAdapter, CoinalyzeAdapter
- Processors: DataProcessor, Normalizer
- Pipelines: IngestionPipeline

Ingestion layer does NOT import orchestration (one-way dependency).

### With Infrastructure Layer
Orchestration layer uses infrastructure:
- Checkpoint stores: CheckpointCoordinator, CheckpointStore
- Database: IDatabaseAdapter
- Config: settings
- Logging: get_logger

### With Airflow
DAG files import from orchestration:
- Builders: BackfillDagBuilder, IncrementalDagBuilder
- Operators: FetchOHLCVOperator, WriteBronzeOperator
- Tasks: fetch_and_validate, write_bronze_batch

Orchestration layer does NOT import airflow internals (except in operators/).

Migration Guide
---------------

### From Old Structure

Before refactoring:
```python
# In DAG file
from infra.airflow.dags.common.bronze_tasks import discover_and_fetch

# Backfill orchestration
results = await coordinator.execute_backfill(request)
```

### New Structure

After refactoring with MinIO checkpoints:
```python
# In DAG file
from quant_framework.orchestration.workflows import BackfillWorkflow
from quant_framework.ingestion.backfill import CheckpointManager, MinIOCheckpointStore

checkpoint_store = MinIOCheckpointStore()
checkpoint_manager = CheckpointManager(checkpoint_store)

# Backfill orchestration
results = await coordinator.execute_backfill(request)
```

### Migration Checklist

✅ Replace old imports with new paths
✅ Use MinIOCheckpointStore instead of DatabaseCheckpointStore
✅ Update checkpoint initialization to use ingestion.backfill module

Testing Strategy
----------------

1. **Unit Tests**
   - Mock IWorkflowContext for workflow testing
   - Mock adapters, writers, checkpoint stores
   - Test workflow logic independently from Airflow

2. **Integration Tests**
   - Test operators with Airflow test mode
   - Test DAG builders generate valid DAG objects
   - Test end-to-end workflow execution

3. **Airflow Tests**
   - Use airflow.models.DagBag to validate DAGs
   - Test task dependencies and scheduling
   - Test XCom passing between tasks

Exports
-------
"""

from quant_framework.orchestration import operators, ports, workflows

__all__ = [
    "ports",
    "workflows",
    "operators",
]

__version__ = "1.0.0"
