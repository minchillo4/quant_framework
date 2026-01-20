# Phase 4: Orchestration Layer - README

## Overview

The orchestration layer provides high-level coordination of data pipelines, abstracting workflow execution, operator implementations, and DAG construction from business logic in the ingestion and transformation layers.

## Architecture Philosophy

### Layer Responsibilities

```
┌─────────────────────────────────────────────────────────────┐
│                    Airflow Scheduler                        │
│                  (Execution Engine)                         │
└───────────────────────────┬─────────────────────────────────┘
                            │ executes
                            ▼
┌─────────────────────────────────────────────────────────────┐
│                Orchestration Layer (HERE)                   │
│                                                             │
│  Coordinates: What to run, when, and how                   │
│  • Workflows: Business process coordination                │
│  • Operators: Airflow task wrappers                        │
│  • DAG Builders: Programmatic DAG construction             │
│  • Tasks: Reusable task functions                          │
└───────────────────────────┬─────────────────────────────────┘
                            │ uses
                            ▼
┌─────────────────────────────────────────────────────────────┐
│                   Ingestion Layer                           │
│                                                             │
│  Executes: Data fetching and initial processing            │
│  • Adapters: API clients                                   │
│  • Processors: Data transformation                          │
│  • Normalizers: Format conversion                           │
└───────────────────────────┬─────────────────────────────────┘
                            │ uses
                            ▼
┌─────────────────────────────────────────────────────────────┐
│                Infrastructure Layer                         │
│                                                             │
│  Provides: Shared utilities and resources                  │
│  • Database: Connection pooling                            │
│  • Checkpoint: State management                             │
│  • Config: Settings management                              │
│  • Logging: Structured logging                              │
└─────────────────────────────────────────────────────────────┘
```

### Key Principles

1. **Orchestration Coordinates, Doesn't Execute**
   - Orchestration layer defines *what* and *when*
   - Ingestion/transformation layers define *how*
   - Infrastructure provides *resources*

2. **Protocol-Based Interfaces**
   - All dependencies expressed as protocols
   - Enables testing without Airflow scheduler
   - Decouples from concrete implementations

3. **Airflow Independence**
   - Core workflow logic doesn't depend on Airflow
   - Operators and DAG builders handle Airflow-specific features
   - Can run workflows outside Airflow for testing

## Directory Structure

```
orchestration/
├── __init__.py                  # Layer documentation and exports
├── ports.py                     # Protocol definitions (392 lines)
├── README.md                    # This file
│
├── workflows/                   # Workflow coordination
│   ├── __init__.py
│   ├── base.py                 # BaseWorkflow, AirflowWorkflowContext
│   ├── backfill_workflow.py    # Historical data backfill
│   ├── incremental_workflow.py # Real-time incremental updates
│   └── validation_workflow.py  # Cross-source validation
│
├── operators/                   # Airflow operators
│   ├── __init__.py
│   ├── base.py                 # BaseDataOperator
│   ├── fetch_operators.py      # FetchOHLCVOperator, FetchOIOperator
│   ├── write_operators.py      # WriteBronzeOperator, WriteSilverOperator
│   ├── validation_operators.py # ValidationOperator, GapDetector
│   └── checkpoint_operators.py # CheckpointReader, CheckpointWriter
│
├── dag_builders/                # DAG construction
│   ├── __init__.py
│   ├── base.py                 # BaseDagBuilder
│   ├── backfill_dag_builder.py # Backfill DAG generation
│   ├── incremental_dag_builder.py # Incremental DAG generation
│   └── factory.py              # Config-driven DAG factory
│
├── tasks/                       # Task function library
│   ├── __init__.py
│   ├── fetch_tasks.py          # Generic fetch operations
│   ├── write_tasks.py          # Generic write operations
│   ├── validation_tasks.py     # Validation operations
│   └── checkpoint_tasks.py     # Checkpoint management
│
└── checkpoint/                  # Unified checkpoint abstraction
    ├── __init__.py
    ├── base.py                 # Base checkpoint implementations
    ├── minio_store.py          # MinIO-backed checkpoint store
    ├── database_store.py       # PostgreSQL-backed checkpoint store
    └── hybrid_store.py         # Hybrid MinIO + Database store
```

## Module Purposes

### ports.py - Protocol Definitions

Defines contracts for all orchestration abstractions:

- **IWorkflow**: Workflow execution interface
- **IWorkflowContext**: Execution context abstraction (wraps Airflow context)
- **IOperator**: Operator execution interface
- **ICheckpointStore**: Unified checkpoint storage
- **IChunkStrategy**: Time-range chunking for backfills
- **IRateLimiter**: API rate limiting
- **IProgressReporter**: Progress tracking and reporting
- **IDagBuilder**: Programmatic DAG construction

### workflows/ - Business Logic Coordination

Coordinates multi-step business processes:

- **BaseWorkflow**: Template for workflow execution (validate → execute → report)
- **BackfillWorkflow**: Historical data backfill with chunking and checkpoints
- **IncrementalWorkflow**: Real-time incremental updates with state management
- **ValidationWorkflow**: Cross-source data validation and reconciliation

### operators/ - Airflow Task Wrappers

Reusable Airflow operators for common operations:

- **BaseDataOperator**: Base class with error handling and logging
- **FetchOHLCVOperator**: Fetch OHLCV data from adapter
- **FetchOIOperator**: Fetch open interest data
- **WriteBronzeOperator**: Write to MinIO bronze layer
- **ValidationOperator**: Validate data quality
- **CheckpointReader/Writer**: Checkpoint state management

### dag_builders/ - DAG Construction

Programmatic DAG generation:

- **BaseDagBuilder**: Template for DAG construction
- **BackfillDagBuilder**: Generate backfill DAGs from config
- **IncrementalDagBuilder**: Generate incremental update DAGs
- **DagFactory**: Config-driven DAG factory (replaces universal_dag_factory)

### tasks/ - Task Function Library

Reusable task functions (TaskFlow API compatible):

- **fetch_tasks**: Generic fetch operations with retry logic
- **write_tasks**: Generic write operations with validation
- **validation_tasks**: Data quality validation
- **checkpoint_tasks**: Checkpoint read/write/delete operations

### checkpoint/ - Unified Checkpoint Abstraction

Checkpoint storage implementations:

- **MinIOCheckpointStore**: File-based checkpoints in MinIO/S3 (recommended for all use cases)
- Supports both streaming and backfill checkpoint tracking

## Design Patterns

### 1. Template Method (Workflows)

Base workflow provides algorithm structure:

```python
class BaseWorkflow:
    async def execute(self, context: IWorkflowContext):
        # 1. Validate
        is_valid, errors = await self.validate(context)
        if not is_valid:
            return {"status": "failed", "errors": errors}
        
        # 2. Execute (subclass implements)
        result = await self._execute_impl(context)
        
        # 3. Report
        await self._report_results(result, context)
        
        return result
    
    async def _execute_impl(self, context):
        raise NotImplementedError
```

### 2. Dependency Injection (Protocols)

All dependencies expressed as protocols:

```python
class BackfillWorkflow:
    def __init__(
        self,
        adapter: IDataAdapter,  # Protocol
        writer: IDataWriter,    # Protocol
        checkpoint_store: ICheckpointStore,  # Protocol
        rate_limiter: IRateLimiter | None = None,
    ):
        self.adapter = adapter
        self.writer = writer
        self.checkpoint_store = checkpoint_store
        self.rate_limiter = rate_limiter
```

### 3. Strategy Pattern (Chunk Strategy)

Pluggable chunking strategies:

```python
class AdaptiveChunkStrategy:
    def generate_chunks(self, start, end, timeframe):
        # Small chunks for 1m data, large for 1d
        ...

class FixedChunkStrategy:
    def generate_chunks(self, start, end, timeframe):
        # Fixed-size chunks regardless of timeframe
        ...
```

### 4. Builder Pattern (DAG Construction)

Fluent DAG construction:

```python
dag = (BackfillDagBuilder("backfill_btc")
    .with_symbols(["BTC", "ETH"])
    .with_timeframes(["1h", "4h"])
    .with_source("coinalyze")
    .with_schedule(None)  # Manual trigger
    .build())
```

## Usage Examples

### Example 1: Backfill Workflow

```python
from quant_framework.orchestration.workflows import BackfillWorkflow
from quant_framework.ingestion.backfill import CheckpointManager, MinIOCheckpointStore

# Create checkpoint manager with MinIO storage
checkpoint_store = MinIOCheckpointStore()
checkpoint_manager = CheckpointManager(checkpoint_store)

# Create workflow
workflow = BackfillWorkflow(
    adapter=coinalyze_adapter,
    writer=bronze_writer,
    checkpoint_store=checkpoint_manager,
    chunk_strategy=AdaptiveChunkStrategy(),
    rate_limiter=CoinalyzeRateLimiter(),
)

# Execute
context = AirflowWorkflowContext(ti=task_instance)
result = await workflow.execute(context)

print(f"Status: {result['status']}")
print(f"Records: {result['records_written']}")
print(f"Duration: {result['duration_seconds']}s")
```

### Example 2: Custom Operator

```python
from quant_framework.orchestration.operators import FetchOHLCVOperator

class FetchBTCOHLCV(FetchOHLCVOperator):
    def __init__(self, **kwargs):
        super().__init__(
            task_id="fetch_btc_ohlcv",
            adapter=ccxt_adapter,
            symbol="BTC/USDT",
            timeframe="1h",
            **kwargs
        )

# Use in DAG
with DAG("btc_ingestion") as dag:
    fetch = FetchBTCOHLCV()
    write = WriteBronzeOperator(task_id="write_bronze")
    fetch >> write
```

### Example 3: DAG Builder

```python
from quant_framework.orchestration.dag_builders import BackfillDagBuilder

builder = BackfillDagBuilder(
    dag_id="coinalyze_backfill",
    description="Historical OHLCV backfill from Coinalyze",
    schedule=None,
    symbols=["BTC", "ETH", "SOL"],
    timeframes=["1h", "4h", "1d"],
    source="coinalyze",
    data_types=["ohlcv", "open_interest"],
)

# Validate configuration
is_valid, errors = builder.validate()
if not is_valid:
    print(f"Invalid config: {errors}")
else:
    dag = builder.build()
```

## Migration from Phase 3

### New Structure (Orchestration Layer)

```python
# Orchestration logic in orchestration layer
from quant_framework.orchestration.workflows import BackfillWorkflow
from quant_framework.ingestion.backfill import CheckpointManager, MinIOCheckpointStore

checkpoint_store = MinIOCheckpointStore()
checkpoint_manager = CheckpointManager(checkpoint_store)

workflow = BackfillWorkflow(
    adapter=adapter,
    checkpoint_store=checkpoint_manager,
    # ...
)
```

## Testing Strategy

### Unit Tests

Test workflows without Airflow:

```python
# Mock context
class MockWorkflowContext:
    def __init__(self):
        self.state = {}
    
    def set_state(self, key, value):
        self.state[key] = value
    
    def get_state(self, key, task_id=None):
        return self.state.get(key)

# Test workflow
context = MockWorkflowContext()
workflow = BackfillWorkflow(...)
result = await workflow.execute(context)
assert result["status"] == "success"
```

### Integration Tests

Test operators with Airflow:

```python
from airflow.utils.state import State
from airflow.models import TaskInstance

def test_fetch_operator():
    op = FetchOHLCVOperator(...)
    ti = TaskInstance(task=op.task, execution_date=datetime.now())
    op.execute({"ti": ti})
    assert ti.state == State.SUCCESS
```

### DAG Validation Tests

Validate generated DAGs:

```python
from airflow.models import DagBag

def test_dag_builder():
    builder = BackfillDagBuilder(...)
    dag = builder.build()
    
    # Validate with Airflow
    dag_bag = DagBag()
    dag_bag.bag_dag(dag, root_dag=dag)
    assert len(dag_bag.import_errors) == 0
```

## Performance Considerations

### Checkpoint Strategy Selection

- **MinIO (file-based)**: Fast writes, eventual consistency, good for streaming
- **Database (PostgreSQL)**: ACID guarantees, immediate consistency, good for backfills
- **Hybrid**: Write to both, read from database, best for critical workflows

### Rate Limiting

Adaptive rate limiting adjusts chunk size based on API responses:

```python
rate_limiter = CoinalyzeRateLimiter(
    base_rate=10,  # 10 req/sec baseline
    max_rate=50,   # 50 req/sec max
    min_rate=1,    # 1 req/sec under rate limit
    backoff_factor=2,  # Exponential backoff
)
```

### Chunking Strategy

Chunk sizes impact memory and failure recovery:

- **1m timeframe**: Small chunks (1 day) for fast recovery
- **1h timeframe**: Medium chunks (7 days)
- **1d timeframe**: Large chunks (30 days)

## Next Steps

1. **Implement workflows/** - Create base workflow and concrete implementations
2. **Implement operators/** - Create Airflow operators
3. **Implement dag_builders/** - Create DAG builders
4. **Implement tasks/** - Extract task functions from DAG common files
5. **Implement checkpoint/** - Create unified checkpoint stores
6. **Migrate DAGs** - Refactor existing DAGs to use new orchestration layer
7. **Write tests** - Comprehensive unit and integration tests
8. **Documentation** - Update DAG documentation and migration guide

---

**Phase 4 Status**: Foundation created, implementation in progress
