"""
DataFetchOrchestrator coordinates adapter → preprocessor → normalizer pipeline.
"""

from __future__ import annotations

from collections.abc import Iterable
from typing import Protocol

from quant_framework.ingestion.adapters.base import BaseAdapter
from quant_framework.ingestion.preprocessing.base import DataPreprocessor
from quant_framework.shared.models.instruments import Instrument


class Normalizer(Protocol):
    def __call__(
        self, instrument: Instrument, row: dict, timeframe: str
    ):  # pragma: no cover - structural
        ...


class DataFetchOrchestrator:
    """Coordinates fetching raw data, preprocessing quirks, and normalizing to domain models."""

    def __init__(
        self,
        adapter: BaseAdapter,
        preprocessor: DataPreprocessor,
        ohlcv_normalizer: Normalizer,
        open_interest_normalizer: Normalizer,
    ) -> None:
        self.adapter = adapter
        self.preprocessor = preprocessor
        self.ohlcv_normalizer = ohlcv_normalizer
        self.open_interest_normalizer = open_interest_normalizer

    async def fetch_ohlcv(
        self,
        instrument: Instrument,
        timeframe: str,
        start=None,
        end=None,
        limit: int | None = None,
    ) -> Iterable[object]:
        raw = await self.adapter.fetch_ohlcv(instrument, timeframe, start, end, limit)
        preprocessed = self.preprocessor.preprocess_ohlcv(instrument, raw)
        return [
            self.ohlcv_normalizer(instrument, row, timeframe) for row in preprocessed
        ]

    async def fetch_open_interest(
        self,
        instrument: Instrument,
        timeframe: str,
        start=None,
        end=None,
        limit: int | None = None,
    ) -> Iterable[object]:
        raw = await self.adapter.fetch_open_interest(
            instrument, timeframe, start, end, limit
        )
        preprocessed = self.preprocessor.preprocess_open_interest(instrument, raw)
        return [
            self.open_interest_normalizer(instrument, row, timeframe)
            for row in preprocessed
        ]
