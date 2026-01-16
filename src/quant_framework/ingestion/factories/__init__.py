"""
DEPRECATED: Factories have moved to quant_framework.common.factories

This module provides backward compatibility only.
Please update imports to use quant_framework.common.factories instead.
"""

import warnings

# Issue deprecation warning
warnings.warn(
    "quant_framework.ingestion.factories is deprecated. "
    "Import from quant_framework.common.factories instead.",
    DeprecationWarning,
    stacklevel=2,
)

# Re-export from new location for backward compatibility
from quant_framework.common.factories.adapter_factory import AdapterFactory
from quant_framework.common.factories.preprocessor_factory import PreprocessorFactory

__all__ = ["AdapterFactory", "PreprocessorFactory"]
