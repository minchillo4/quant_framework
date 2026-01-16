"""
Preprocessing layer interface for provider-specific quirks.

Preprocessors convert raw adapter payloads into normalized intermediary
records before domain normalization. They handle:
- Parameter adjustments (e.g., timeframe mapping, pagination hints)
- Field renaming/conversions (e.g., timestamps in seconds vs ms)
- Provider quirks (e.g., COIN-M openInterestValue vs openInterestAmount)
"""

from __future__ import annotations

from abc import ABC, abstractmethod
from collections.abc import Iterable
from typing import Any

from quant_framework.shared.models.instruments import Instrument


class DataPreprocessor(ABC):
    """Abstract preprocessor for provider-specific quirks."""

    @abstractmethod
    def preprocess_ohlcv(
        self,
        instrument: Instrument,
        raw: Iterable[dict[str, Any]],
    ) -> Iterable[dict[str, Any]]:
        """Transform raw OHLCV payloads into normalized intermediary rows."""

    @abstractmethod
    def preprocess_open_interest(
        self,
        instrument: Instrument,
        raw: Iterable[dict[str, Any]],
    ) -> Iterable[dict[str, Any]]:
        """Transform raw open interest payloads into normalized intermediary rows."""
