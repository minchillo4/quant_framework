"""
Adapter Factory
===============

Creates adapters based on data provider and desired class.

Moved from: quant_framework.ingestion.factories.adapter_factory
"""

from __future__ import annotations

from collections.abc import Callable
from typing import Any

from quant_framework.ingestion.adapters.base import BaseAdapter
from quant_framework.ingestion.models.enums import DataProvider

AdapterBuilder = Callable[..., BaseAdapter]


class AdapterFactory:
    """Registry-driven factory for adapters."""

    def __init__(self) -> None:
        self._registry: dict[DataProvider, AdapterBuilder] = {}

    def register(self, provider: DataProvider, builder: AdapterBuilder) -> None:
        self._registry[provider] = builder

    def create(self, provider: DataProvider, *args: Any, **kwargs: Any) -> BaseAdapter:
        if provider not in self._registry:
            raise ValueError(f"No adapter registered for provider {provider}")
        return self._registry[provider](*args, **kwargs)

    def available_providers(self) -> list[DataProvider]:
        return list(self._registry.keys())
