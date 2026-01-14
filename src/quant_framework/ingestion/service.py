"""
IngestionService: public entrypoint for capability-based ingestion.
"""

from __future__ import annotations

from typing import Any

from quant_framework.ingestion.factories.adapter_factory import AdapterFactory
from quant_framework.ingestion.factories.preprocessor_factory import PreprocessorFactory
from quant_framework.ingestion.models.enums import DataProvider
from quant_framework.ingestion.orchestration.data_fetcher import DataFetchOrchestrator
from quant_framework.shared.models.instruments import Instrument


class IngestionService:
    """Composes adapter, preprocessor, and normalizers to serve ingestion requests."""

    def __init__(
        self,
        adapter_factory: AdapterFactory,
        preprocessor_factory: PreprocessorFactory,
        ohlcv_normalizer,
        open_interest_normalizer,
    ) -> None:
        self.adapter_factory = adapter_factory
        self.preprocessor_factory = preprocessor_factory
        self.ohlcv_normalizer = ohlcv_normalizer
        self.open_interest_normalizer = open_interest_normalizer

    def _build_orchestrator(
        self, provider: DataProvider, *args: Any, **kwargs: Any
    ) -> DataFetchOrchestrator:
        adapter = self.adapter_factory.create(provider, *args, **kwargs)
        preprocessor = self.preprocessor_factory.create(provider)
        return DataFetchOrchestrator(
            adapter, preprocessor, self.ohlcv_normalizer, self.open_interest_normalizer
        )

    async def fetch_ohlcv(
        self,
        provider: DataProvider,
        instrument: Instrument,
        timeframe: str,
        start=None,
        end=None,
        limit: int | None = None,
        **adapter_kwargs: Any,
    ):
        orchestrator = self._build_orchestrator(provider, **adapter_kwargs)
        return await orchestrator.fetch_ohlcv(instrument, timeframe, start, end, limit)

    async def fetch_open_interest(
        self,
        provider: DataProvider,
        instrument: Instrument,
        timeframe: str,
        start=None,
        end=None,
        limit: int | None = None,
        **adapter_kwargs: Any,
    ):
        orchestrator = self._build_orchestrator(provider, **adapter_kwargs)
        return await orchestrator.fetch_open_interest(
            instrument, timeframe, start, end, limit
        )
