"""
Factories Module - Adapter and Preprocessor Creation
====================================================

Provides registry-driven factories for creating adapters and preprocessors
based on data provider and desired capabilities.

Moved from: quant_framework.ingestion.factories
"""

from quant_framework.common.factories.adapter_factory import AdapterFactory
from quant_framework.common.factories.preprocessor_factory import PreprocessorFactory

__all__ = [
    "AdapterFactory",
    "PreprocessorFactory",
]
