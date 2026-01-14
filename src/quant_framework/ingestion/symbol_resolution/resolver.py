"""
SymbolResolver bridges legacy symbol registry to the Instrument domain model.
"""

from __future__ import annotations

from typing import Protocol

from quant_framework.shared.models.instruments import Instrument


class SymbolRegistry(Protocol):
    def resolve(self, symbol: str) -> dict: ...


class SymbolResolver:
    """Converts legacy registry output into Instrument instances."""

    def __init__(self, registry: SymbolRegistry):
        self.registry = registry

    def resolve(self, symbol: str) -> Instrument:
        data = self.registry.resolve(symbol)
        return Instrument(**data)
