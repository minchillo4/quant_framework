# Common Layer - Shared Utilities and Factories

## Overview

The common layer provides **reusable components** used across multiple layers (ingestion, storage, transformation, orchestration):

- **factories/**: Adapter and preprocessor factories
- **domain/**: Re-exported shared models and enums
- **utils/**: Utility functions for dates, logging, etc.

## Directory Structure

```
common/
├── __init__.py              # Layer exports
├── README.md                # This file
│
├── factories/               # Moved from ingestion/factories/
│   ├── __init__.py
│   ├── adapter_factory.py   # AdapterFactory: creates adapters
│   └── preprocessor_factory.py  # PreprocessorFactory: creates preprocessors
│
├── domain/                  # Re-exports from shared/
│   └── __init__.py          # Exports models and enums
│
└── utils/                   # Shared utility functions
    ├── __init__.py
    ├── date_utils.py        # Unix timestamp, timezone handling
    └── logging_wrappers.py  # Logger setup and configuration
```

## Module Purposes

### factories/

**AdapterFactory** - Registry-driven factory for creating data adapters:

```python
from quant_framework.common.factories import AdapterFactory
from quant_framework.ingestion.models.enums import DataProvider

factory = AdapterFactory()
factory.register(DataProvider.CCXT, CCXTAdapter)

adapter = factory.create(DataProvider.CCXT, **kwargs)
```

**PreprocessorFactory** - Registry-driven factory for creating preprocessors:

```python
from quant_framework.common.factories import PreprocessorFactory

factory = PreprocessorFactory()
factory.register(DataProvider.COINALYZE, CoinalyzePreprocessor)

preprocessor = factory.create(DataProvider.COINALYZE)
```

### domain/

Re-exports core domain models:

```python
from quant_framework.common.domain import (
    Instrument,
    MarketType,
    AssetClass,
    DataVenue,
)
```

### utils/

**date_utils.py** - Timestamp and timezone utilities:

```python
from quant_framework.common.utils import (
    to_unix_ms,        # datetime → Unix ms
    from_unix_ms,      # Unix ms → datetime
    utc_now,           # Current UTC time
    utc_today,         # Today at midnight UTC
    truncate_to_day,   # Truncate to start of day
    add_days,          # Add days to datetime
    date_range,        # Generate date sequence
)
```

**logging_wrappers.py** - Logging setup utilities:

```python
from quant_framework.common.utils import (
    get_logger,       # Get configured logger
    setup_logging,    # Initialize application logging
)

logger = get_logger(__name__)
```

## Migration Notes

### Factories

**Old imports (deprecated but still work):**
```python
from quant_framework.ingestion.factories import AdapterFactory
```

**New imports (preferred):**
```python
from quant_framework.common.factories import AdapterFactory
```

The old location still works via backward compatibility shims with deprecation warnings.

### Domain

No migration needed - domain/ is new and complements shared/:

```python
# These are equivalent
from quant_framework.shared.models.enums import Instrument
from quant_framework.common.domain import Instrument
```

Use whichever is more convenient.

## Design Principles

1. **Single Responsibility**: Each module has one clear purpose
2. **No Circular Dependencies**: common/ only imports from shared/ and ingestion/models/
3. **Backward Compatibility**: Old imports still work with deprecation warnings
4. **Type Safety**: 100% type-hinted functions
5. **Async-Ready**: All utilities support async operations

## Testing

All utilities are tested in `tests/common/`:

```bash
pytest tests/common/factories/  # Factory tests
pytest tests/common/utils/      # Utility tests
```

## Future Additions

Potential utilities to add:
- `validation/`: Data validation helpers
- `constants/`: Shared constants
- `exceptions/`: Domain exceptions
- `decorators/`: Common decorators (retry, cache, etc.)

---

**Phase 6**: Orchestration Layer Cleanup - Common utilities consolidated here.
