"""
Exchange-specific preprocessors extracting common fields and harmonizing payloads.
"""

from __future__ import annotations

import logging
from collections.abc import Iterable
from typing import Any

from quant_framework.ingestion.preprocessing.base import DataPreprocessor
from quant_framework.shared.models.instruments import Instrument


def _to_ms(value: Any) -> Any:
    try:
        # Some exchanges return seconds, others ms
        return int(value) if int(value) > 10_000_000_000 else int(value) * 1000
    except Exception:
        return value


class BinancePreprocessor(DataPreprocessor):
    def preprocess_ohlcv(
        self,
        instrument: Instrument,
        raw: Iterable[dict[str, Any]],
    ) -> Iterable[dict[str, Any]]:
        for row in raw:
            ts = row[0] if isinstance(row, (list, tuple)) else row.get("timestamp")
            yield {
                "timestamp": _to_ms(ts),
                "open": row[1] if isinstance(row, (list, tuple)) else row.get("open"),
                "high": row[2] if isinstance(row, (list, tuple)) else row.get("high"),
                "low": row[3] if isinstance(row, (list, tuple)) else row.get("low"),
                "close": row[4] if isinstance(row, (list, tuple)) else row.get("close"),
                "volume": row[5]
                if isinstance(row, (list, tuple))
                else row.get("volume"),
            }

    def preprocess_open_interest(
        self,
        instrument: Instrument,
        raw: Iterable[dict[str, Any]],
    ) -> Iterable[dict[str, Any]]:
        for row in raw:
            amount = row.get("openInterestAmount") or row.get("sumOpenInterest")
            value = row.get("openInterestValue")
            yield {
                "timestamp": _to_ms(row.get("timestamp") or row.get("ts")),
                "open_interest_amount": amount,
                "open_interest_value": value,
                "settlement_currency": row.get("settlementCurrency"),
            }


class BybitPreprocessor(DataPreprocessor):
    def preprocess_ohlcv(
        self, instrument: Instrument, raw: Iterable[dict[str, Any]]
    ) -> Iterable[dict[str, Any]]:
        for row in raw:
            ts = row[0] if isinstance(row, (list, tuple)) else row.get("timestamp")
            yield {
                "timestamp": _to_ms(ts),
                "open": row[1] if isinstance(row, (list, tuple)) else row.get("open"),
                "high": row[2] if isinstance(row, (list, tuple)) else row.get("high"),
                "low": row[3] if isinstance(row, (list, tuple)) else row.get("low"),
                "close": row[4] if isinstance(row, (list, tuple)) else row.get("close"),
                "volume": row[5]
                if isinstance(row, (list, tuple))
                else row.get("volume"),
            }

    def preprocess_open_interest(
        self, instrument: Instrument, raw: Iterable[dict[str, Any]]
    ) -> Iterable[dict[str, Any]]:
        for row in raw:
            amount = row.get("openInterest")
            if amount is None:
                continue
            yield {
                "timestamp": _to_ms(row.get("timestamp") or row.get("ts")),
                "open_interest_amount": amount,
                "open_interest_value": row.get("openInterestValue"),
                "settlement_currency": row.get("settlementCurrency"),
            }


class GateIOPreprocessor(DataPreprocessor):
    def preprocess_ohlcv(
        self, instrument: Instrument, raw: Iterable[dict[str, Any]]
    ) -> Iterable[dict[str, Any]]:
        for row in raw:
            ts = row[0] if isinstance(row, (list, tuple)) else row.get("timestamp")
            yield {
                "timestamp": _to_ms(ts),
                "open": row[1] if isinstance(row, (list, tuple)) else row.get("open"),
                "high": row[2] if isinstance(row, (list, tuple)) else row.get("high"),
                "low": row[3] if isinstance(row, (list, tuple)) else row.get("low"),
                "close": row[4] if isinstance(row, (list, tuple)) else row.get("close"),
                "volume": row[5]
                if isinstance(row, (list, tuple))
                else row.get("volume"),
            }

    def preprocess_open_interest(
        self, instrument: Instrument, raw: Iterable[dict[str, Any]]
    ) -> Iterable[dict[str, Any]]:
        for row in raw:
            yield {
                "timestamp": _to_ms(row.get("t") or row.get("timestamp")),
                "open_interest_amount": row.get("v") or row.get("openInterest"),
                "open_interest_value": row.get("openInterestValue"),
                "settlement_currency": row.get("settlementCurrency"),
            }


class HuobiPreprocessor(DataPreprocessor):
    def preprocess_ohlcv(
        self, instrument: Instrument, raw: Iterable[dict[str, Any]]
    ) -> Iterable[dict[str, Any]]:
        for row in raw:
            ts = (
                row[0]
                if isinstance(row, (list, tuple))
                else row.get("id") or row.get("timestamp")
            )
            yield {
                "timestamp": _to_ms(ts),
                "open": row[1] if isinstance(row, (list, tuple)) else row.get("open"),
                "high": row[2] if isinstance(row, (list, tuple)) else row.get("high"),
                "low": row[3] if isinstance(row, (list, tuple)) else row.get("low"),
                "close": row[4] if isinstance(row, (list, tuple)) else row.get("close"),
                "volume": row[5] if isinstance(row, (list, tuple)) else row.get("vol"),
            }

    def preprocess_open_interest(
        self, instrument: Instrument, raw: Iterable[dict[str, Any]]
    ) -> Iterable[dict[str, Any]]:
        """
        Huobi OI preprocessing.

        CCXT returns: [{'timestamp': ..., 'openInterestAmount': ..., 'openInterestValue': ...}, ...]
        For inverse perpetuals, openInterestValue may be None.
        """
        for row in raw:
            amount = (
                row.get("openInterestAmount")
                or row.get("volume")
                or row.get("openInterest")
            )
            value = row.get("openInterestValue")

            # For inverse, use amount as fallback for value if not provided
            if value is None and instrument.is_inverse:
                value = amount

            yield {
                "timestamp": _to_ms(row.get("timestamp") or row.get("ts")),
                "open_interest_amount": amount,
                "open_interest_value": value,
                "settlement_currency": row.get("settlementCurrency")
                or instrument.settlement_currency,
            }


class BitgetPreprocessor(DataPreprocessor):
    def preprocess_ohlcv(
        self, instrument: Instrument, raw: Iterable[dict[str, Any]]
    ) -> Iterable[dict[str, Any]]:
        for row in raw:
            ts = row[0] if isinstance(row, (list, tuple)) else row.get("timestamp")
            yield {
                "timestamp": _to_ms(ts),
                "open": row[1] if isinstance(row, (list, tuple)) else row.get("open"),
                "high": row[2] if isinstance(row, (list, tuple)) else row.get("high"),
                "low": row[3] if isinstance(row, (list, tuple)) else row.get("low"),
                "close": row[4] if isinstance(row, (list, tuple)) else row.get("close"),
                "volume": row[5]
                if isinstance(row, (list, tuple))
                else row.get("volume"),
            }

    def preprocess_open_interest(
        self, instrument: Instrument, raw: Iterable[dict[str, Any]]
    ) -> Iterable[dict[str, Any]]:
        for row in raw:
            yield {
                "timestamp": _to_ms(row.get("timestamp") or row.get("ts")),
                "open_interest_amount": row.get("size") or row.get("openInterest"),
                "open_interest_value": row.get("openInterestValue"),
                "settlement_currency": row.get("marginCoin")
                or row.get("settlementCurrency"),
            }


"""CoinAlyze preprocessor for transforming raw API responses."""


logger = logging.getLogger(__name__)


class CoinalyzePreprocessor(DataPreprocessor):
    """
    Transform CoinAlyze API responses into normalized format.

    Handles exchange-specific response structures and field extraction.
    """

    def preprocess_ohlcv(
        self,
        instrument: Instrument,
        raw: list[dict[str, Any]],
    ) -> Iterable[dict[str, Any]]:
        """
        Transform CoinAlyze OHLCV response structure:

        Input format:
        [
            {
                "symbol": "BTCUSDT_PERP.A",
                "history": [
                    {"t": timestamp, "o": open, "h": high, "l": low, "c": close, "v": volume},
                    ...
                ]
            }
        ]

        Output format:
        Iterator of: {
            "timestamp": ms,
            "open": float,
            "high": float,
            "low": float,
            "close": float,
            "volume": float
        }
        """
        for symbol_data in raw:
            history = symbol_data.get("history", [])
            for item in history:
                yield {
                    "timestamp": self._to_ms(item["t"]),
                    "open": float(item["o"]),
                    "high": float(item["h"]),
                    "low": float(item["l"]),
                    "close": float(item["c"]),
                    "volume": float(item.get("v", 0)),
                }

    def preprocess_open_interest(
        self,
        instrument: Instrument,
        raw: list[dict[str, Any]],
    ) -> Iterable[dict[str, Any]]:
        """
        Transform CoinAlyze Open Interest response.

        CoinAlyze returns OI in OHLC format - we use 'c' (close) as the OI value.
        """
        for symbol_data in raw:
            history = symbol_data.get("history", [])
            for item in history:
                yield {
                    "timestamp": self._to_ms(item["t"]),
                    "open_interest_amount": float(item["c"]),
                    "open_interest_value": float(item["c"]),
                    "settlement_currency": instrument.settlement_currency,
                }

    def _to_ms(self, timestamp: int) -> int:
        """Convert timestamp to milliseconds if needed"""
        return timestamp * 1000 if timestamp < 10_000_000_000 else timestamp
