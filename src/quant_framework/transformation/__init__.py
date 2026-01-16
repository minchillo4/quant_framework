"""Transformation Layer: Normalize & validate data before persistence.

Phase 3 Architecture:

```
Raw API Response
    ↓
Normalizer (normalizers.py)
    - Extract fields from source-specific API format
    - Convert to storage schema objects (OHLCVRecord, OpenInterestRecord, etc.)
    - No validation, just data conversion
    ↓
Validator (validators.py)
    - Check data quality (price bounds, volume > 0, valid timestamps)
    - Collect all validation errors
    - Return validation report (is_valid, error_list)
    ↓
Pipeline (pipelines.py)
    - Orchestrate: normalize → validate → store
    - Handle retry logic and error recovery
    - Log transformation progress
    ↓
Storage Repository
    - Persist validated records to database
```

## Module Structure

### ports.py
Protocol/interface definitions for dependency injection:
- `IDataNormalizer`: Abstract normalizer interface
- `IOHLCVNormalizer`: OHLCV-specific normalization contract
- `IOpenInterestNormalizer`: OI-specific normalization contract
- `ITradeNormalizer`: Trade-specific normalization contract
- `IDataValidator`: Abstract validator interface
- `IOHLCVValidator`: OHLCV validation contract
- `IOpenInterestValidator`: OI validation contract
- `ITransformationPipeline`: End-to-end pipeline orchestration

### normalizers.py
Base normalizer implementations for common patterns:
- `BaseOHLCVNormalizer`: Template for OHLCV conversion
  - Override `_extract_timestamp()`, `_extract_prices()`, `_extract_volume()`
  - Provides `normalize_single()` and `normalize_batch()`
- `BaseOpenInterestNormalizer`: Template for OI conversion
  - Override `_extract_timestamp()`, `_extract_open_interest_usd()`, `_extract_optional_fields()`
- `BaseTradeNormalizer`: Template for trade conversion
  - Override `_extract_timestamp()`, `_extract_price()`, `_extract_quantity()`, `_extract_side()`
- `NormalizationError`: Exception for normalization failures

### validators.py
Base validator implementations for data quality:
- `BaseOHLCVValidator`: Validates OHLCV records
  - Checks: price relationships (high >= low), volume, valid timestamps
  - Returns: (is_valid: bool, errors: list[str])
- `BaseOpenInterestValidator`: Validates OI records
  - Checks: positive amounts, valid settlement currencies, valid timestamps
  - Configurable: strict_settlement_currency mode
- `BaseTradeValidator`: Validates trade records
  - Checks: positive prices, valid side (buy/sell), valid timestamps
- `ValidationError`: Exception for validation failures

### adapters/ (Source-specific implementations)
Each data source creates adapter with source-specific logic:
- `adapters/coinalyze.py`: CoinalyzeOHLCVNormalizer, CoinalyzeOINormalizer
- `adapters/ccxt.py`: CCXTOHLCVNormalizer, CCXTTradeNormalizer
- `adapters/coinmetrics.py`: CoinMetricsOHLCVNormalizer
- Each adapter extends base normalizer and overrides extraction methods

### pipelines/ (Orchestration)
End-to-end transformation workflows:
- `OHLCVTransformationPipeline`: Fetch → normalize → validate → store OHLCV
- `OpenInterestPipeline`: Fetch → normalize → validate → store OI
- `TradeTransformationPipeline`: Fetch → normalize → validate → store trades
- Error handling, retry logic, progress logging

## Design Decisions

### 1. Normalizer Template Method Pattern
Each source-specific adapter inherits from base normalizer and overrides extraction methods.
This enables:
- Reuse of common normalization logic
- Per-source API format handling
- Easy testing (mock extraction methods)

Example (CCXT):
```python
class CCXTOHLCVNormalizer(BaseOHLCVNormalizer):
    def _extract_timestamp(self, raw_ohlcv: dict) -> int:
        # CCXT returns [timestamp, open, high, low, close, volume]
        return raw_ohlcv[0]

    def _extract_prices(self, raw_ohlcv) -> tuple[Decimal, ...]:
        return (Decimal(str(raw_ohlcv[1])), ...)
```

### 2. Validation Without Modification
Validators are pure functions: they check data quality but never modify records.
This enables:
- Clear separation of concerns
- Easy testing (validator outputs don't affect storage)
- Flexible error handling (log vs retry vs discard)

### 3. Async Throughout
All normalizers and validators are async-first to enable:
- Concurrent processing of multiple records
- Integration with async storage repositories
- Compatibility with Airflow DAGs (async operators)

### 4. Error Collection (Not Early Exit)
Validators collect ALL errors before returning, allowing:
- Better error reporting (all issues visible, not just first)
- Flexible recovery (e.g., discard record or retry with metadata)
- Analytics on data quality issues

### 5. Decimal for Financial Data
All monetary values (prices, OI amounts, volumes) use Decimal:
- Matches PostgreSQL NUMERIC type
- Prevents floating-point rounding errors
- Required for regulatory compliance (audit trail)

### 6. Protocol-Based Dependency Injection
Uses typing.Protocol for interfaces (not ABCs):
- Enables structural typing (duck typing with type hints)
- Makes testing easier (no inheritance required)
- Decouples adapter implementations from storage layer

## Usage Examples

### Example 1: CCXT OHLCV Normalization and Validation
```python
from transformation.adapters.ccxt import CCXTOHLCVNormalizer
from transformation.validators import BaseOHLCVValidator

normalizer = CCXTOHLCVNormalizer(symbol="BTC/USDT", exchange="binance")
validator = BaseOHLCVValidator()

# CCXT returns: [[timestamp, o, h, l, c, v], ...]
raw_data = [[1704067200000, 42000, 42500, 41500, 42200, 1000], ...]

# Normalize
records = await normalizer.normalize_batch(raw_data)

# Validate
validities, error_lists = await validator.validate_batch(records)

# Store valid records
for record, is_valid in zip(records, validities):
    if is_valid:
        await repository.save(record)
```

### Example 2: Coinalyze OI Normalization with Settlement Currency
```python
from transformation.adapters.coinalyze import CoinalyzeOINormalizer
from transformation.validators import BaseOpenInterestValidator

normalizer = CoinalyzeOINormalizer(symbol="BTC/USDT", exchange="binance")
validator = BaseOpenInterestValidator(strict_settlement_currency=True)

# Coinalyze returns: {"time": 1704067200000, "open_interest": 1234567890, "currency": "USDT"}
raw_data = {...}

# Normalize with Coinalyze-specific field extraction
record = await normalizer.normalize_single(raw_data)

# Validate (checks settlement_currency is in known list)
is_valid, errors = await validator.validate_single(record)
```

### Example 3: End-to-End Pipeline
```python
from transformation.pipelines import OHLCVTransformationPipeline
from ingestion.adapters import CCXTAdapter
from storage.repositories import OHLCVRepository

pipeline = OHLCVTransformationPipeline(
    normalizer=CCXTOHLCVNormalizer(symbol="BTC/USDT", exchange="binance"),
    validator=BaseOHLCVValidator(),
    repository=OHLCVRepository(db_pool),
)

# Execute: fetch from CCXT → normalize → validate → store
result = await pipeline.transform_and_store(
    raw_data=ccxt_response,
    data_type="ohlcv",
)

print(f"Stored {result['records_stored']} of {result['records_processed']} records")
if result['errors']:
    print(f"Errors: {result['errors']}")
```

## Integration with Storage Layer

The transformation layer bridges ingestion and storage:

```
Ingestion Layer
    ↓
    fetch_ohlcv_from_ccxt() → raw API response
    ↓
Transformation Layer
    normalize_ccxt_ohlcv() → OHLCVRecord
    validate_ohlcv_quality() → errors: list[str]
    ↓
Storage Layer
    repository.save(record) → inserted to market.ohlc
```

Each layer is independently testable:
- Test normalizer with mock API responses
- Test validator with OHLCVRecord fixtures
- Test repository with mock database adapter

No direct dependencies:
- Storage doesn't depend on transformation (can persist raw data)
- Transformation doesn't depend on storage (can output to logs)
- Ingestion doesn't depend on either (can fetch without storing)

## Testing Strategy

### Unit Tests
```python
# test_normalizers.py
async def test_ccxt_ohlcv_normalization():
    normalizer = CCXTOHLCVNormalizer(...)
    raw = [1704067200000, 42000, 42500, 41500, 42200, 1000]
    record = await normalizer.normalize_single(raw)
    assert record.open == Decimal("42000")

# test_validators.py
async def test_ohlcv_validator_price_relationships():
    validator = BaseOHLCVValidator()
    record = OHLCVRecord(..., high=100, low=200)  # Invalid!
    is_valid, errors = await validator.validate_single(record)
    assert not is_valid
    assert "high < low" in errors[0]
```

### Integration Tests
```python
# test_pipelines.py
async def test_ohlcv_pipeline_end_to_end(db_pool, ccxt_mock):
    pipeline = OHLCVTransformationPipeline(...)
    result = await pipeline.transform_and_store(ccxt_response)

    # Verify records stored
    stored_records = await repository.find_range(...)
    assert len(stored_records) == len(result['records_stored'])
```

## Phase Roadmap

**Phase 3 (Current):**
✅ Create directory structure
✅ Define port interfaces (ports.py)
✅ Create base normalizers (normalizers.py)
✅ Create base validators (validators.py)
⏳ Create source-specific adapters (adapters/{coinalyze,ccxt,coinmetrics}.py)
⏳ Create transformation pipelines (pipelines/{ohlcv,oi,trades}.py)
⏳ Integration tests with real/mock API responses

**Phase 4 (Future):**
- Monitoring layer (transformation metrics, validation rates)
- Preprocessing layer (outlier detection, anomaly handling)
- Orchestration layer (Airflow DAGs for scheduled transformations)
- Features layer (ML feature engineering, derived metrics)
"""

# Port definitions (interfaces)
# Source-specific adapters
from quant_framework.transformation.adapters import (
    CCXTOHLCVNormalizer,
    CCXTTradeNormalizer,
    CoinalyzeOHLCVNormalizer,
    CoinalyzeOINormalizer,
    CoinMetricsOHLCVNormalizer,
)

# Normalizer implementations
from quant_framework.transformation.normalizers import (
    BaseOHLCVNormalizer,
    BaseOpenInterestNormalizer,
    BaseTradeNormalizer,
    NormalizationError,
)

# Transformation pipelines
from quant_framework.transformation.pipelines import (
    BaseTransformationPipeline,
    OHLCVTransformationPipeline,
    OpenInterestPipeline,
    PipelineError,
    TradeTransformationPipeline,
)
from quant_framework.transformation.ports import (
    IDataNormalizer,
    IDataValidator,
    IOHLCVNormalizer,
    IOpenInterestNormalizer,
    ITradeNormalizer,
    ITransformationPipeline,
)

# Validator implementations
from quant_framework.transformation.validators import (
    BaseOHLCVValidator,
    BaseOpenInterestValidator,
    BaseTradeValidator,
    ValidationError,
)

__all__ = [
    # Ports
    "IDataNormalizer",
    "IOHLCVNormalizer",
    "IOpenInterestNormalizer",
    "ITradeNormalizer",
    "IDataValidator",
    "ITransformationPipeline",
    # Normalizers
    "BaseOHLCVNormalizer",
    "BaseOpenInterestNormalizer",
    "BaseTradeNormalizer",
    "NormalizationError",
    # Validators
    "BaseOHLCVValidator",
    "BaseOpenInterestValidator",
    "BaseTradeValidator",
    "ValidationError",
    # Adapters
    "CCXTOHLCVNormalizer",
    "CCXTTradeNormalizer",
    "CoinalyzeOHLCVNormalizer",
    "CoinalyzeOINormalizer",
    "CoinMetricsOHLCVNormalizer",
    # Pipelines
    "BaseTransformationPipeline",
    "OHLCVTransformationPipeline",
    "OpenInterestPipeline",
    "TradeTransformationPipeline",
    "PipelineError",
]
