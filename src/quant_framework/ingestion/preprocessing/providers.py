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


class AlphaVantagePreprocessor(DataPreprocessor):
    """
    Transform AlphaVantage API responses into normalized format.

    Handles AlphaVantage-specific response structures and field extraction.
    Preserves all raw fields while adding normalized fields for consistency.
    """

    def preprocess_treasury_yields(
        self,
        instrument: Instrument,
        raw: Iterable[dict[str, Any]],
    ) -> Iterable[dict[str, Any]]:
        """
        Transform AlphaVantage TREASURY_YIELD response.

        Input format:
        [
            {"date": "2024-01-15", "value": "4.28", "maturity": "10year"},
            {"date": "2024-01-12", "value": "4.26", "maturity": "10year"},
            ...
        ]

        Output format:
        Iterator of: {
            "timestamp": ms,
            "date": str,  # Raw field preserved
            "rate": float,
            "maturity": str,
            "instrument_id": str
        }
        """
        for record in raw:
            # Extract raw fields
            date_str = record.get("date")
            raw_value = record.get("value")
            maturity = record.get("maturity")
            instrument_id = record.get("instrument_id")

            if not date_str or raw_value is None:
                continue

            try:
                rate = float(raw_value)
            except (ValueError, TypeError):
                continue

            yield {
                "timestamp": self._to_ms(date_str),
                "date": date_str,  # Preserve raw field
                "rate": rate,  # Normalized field
                "raw_value": raw_value,  # Preserve raw field
                "maturity": maturity,
                "instrument_id": instrument_id,
                "symbol": record.get("symbol"),
            }

    def preprocess_equity_ohlcv(
        self,
        instrument: Instrument,
        raw: Iterable[dict[str, Any]],
    ) -> Iterable[dict[str, Any]]:
        """
        Transform AlphaVantage TIME_SERIES_DAILY response.

        Input format:
        [
            {
                "date": "2024-01-15",
                "1. open": "173.4300",
                "2. high": "174.4800",
                "3. low": "172.2000",
                "4. close": "173.8000",
                "5. volume": "4321500",
                "timestamp": ms,
                "instrument_id": str
            },
            ...
        ]

        Output format:
        Iterator of: {
            "timestamp": ms,
            "date": str,  # Raw field preserved
            "open": float,
            "high": float,
            "low": float,
            "close": float,
            "volume": float,
            "instrument_id": str
        }
        """
        for record in raw:
            # Extract raw fields
            date_str = record.get("date")
            raw_open = record.get("1. open")
            raw_high = record.get("2. high")
            raw_low = record.get("3. low")
            raw_close = record.get("4. close")
            raw_volume = record.get("5. volume")

            if not all([date_str, raw_open, raw_high, raw_low, raw_close, raw_volume]):
                continue

            try:
                open_price = float(raw_open)
                high_price = float(raw_high)
                low_price = float(raw_low)
                close_price = float(raw_close)
                volume = float(raw_volume)
            except (ValueError, TypeError):
                continue

            yield {
                "timestamp": self._to_ms(date_str),
                "date": date_str,  # Preserve raw field
                "open": open_price,
                "high": high_price,
                "low": low_price,
                "close": close_price,
                "volume": volume,
                # Preserve raw AlphaVantage fields
                "1. open": raw_open,
                "2. high": raw_high,
                "3. low": raw_low,
                "4. close": raw_close,
                "5. volume": raw_volume,
                "instrument_id": record.get("instrument_id"),
                "symbol": record.get("symbol"),
            }

    def _to_ms(self, date_str: str) -> int:
        """Convert AlphaVantage date string to milliseconds timestamp."""
        from datetime import datetime

        try:
            dt = datetime.strptime(date_str, "%Y-%m-%d")
            return int(dt.timestamp() * 1000)
        except ValueError:
            # Fallback to current time
            import time

            return int(time.time() * 1000)
