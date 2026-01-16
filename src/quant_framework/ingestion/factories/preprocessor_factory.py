"""
Preprocessor factory for provider-specific quirks.
"""

from __future__ import annotations

from collections.abc import Callable
from typing import Any

from quant_framework.ingestion.models.enums import DataProvider
from quant_framework.ingestion.preprocessing.base import DataPreprocessor

PreprocessorBuilder = Callable[..., DataPreprocessor]


class PreprocessorFactory:
    """Registry-driven factory for preprocessors."""

    def __init__(self) -> None:
        self._registry: dict[DataProvider, PreprocessorBuilder] = {}

    def register(self, provider: DataProvider, builder: PreprocessorBuilder) -> None:
        self._registry[provider] = builder

    def create(
        self, provider: DataProvider, *args: Any, **kwargs: Any
    ) -> DataPreprocessor:
        if provider not in self._registry:
            raise ValueError(f"No preprocessor registered for provider {provider}")
        return self._registry[provider](*args, **kwargs)

    def available_providers(self) -> list[DataProvider]:
        return list(self._registry.keys())
