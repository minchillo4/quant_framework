"""
Common Layer - Shared Utilities and Factories
==============================================

Consolidates shared utilities, factories, and domain entities that are used
across multiple layers (ingestion, storage, transformation, orchestration).

Structure:
    common/
    ├── factories/      # Adapter and preprocessor factories
    ├── domain/         # Re-exported shared models
    └── utils/          # Utility functions
"""

from quant_framework.common import factories

__all__ = [
    "factories",
]

__version__ = "1.0.0"
