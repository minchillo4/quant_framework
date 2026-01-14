"""
Test fixtures package for ingestion tests.

Provides mock exchange responses, test data, and helper utilities.
"""

import json
from datetime import datetime
from pathlib import Path
from typing import Any


def load_fixture(fixture_name: str) -> Any:
    """
    Load fixture data from exchange_responses.json.

    Args:
        fixture_name: Name of the fixture to load

    Returns:
        Fixture data (dict, list, etc.)

    Example:
        >>> ohlcv = load_fixture("binance_linear_ohlcv")
    """
    fixtures_path = Path(__file__).parent / "exchange_responses.json"

    with open(fixtures_path) as f:
        all_fixtures = json.load(f)

    if fixture_name not in all_fixtures:
        raise KeyError(f"Fixture '{fixture_name}' not found")

    return all_fixtures[fixture_name]


def get_mock_ohlcv_response(exchange: str, market_type: str = "linear") -> list:
    """Get mock OHLCV response for an exchange."""
    fixture_map = {
        ("binance", "linear"): "binance_linear_ohlcv",
        ("binance", "inverse"): "binance_linear_ohlcv",  # Same format
        ("bybit", "linear"): "bybit_linear_ohlcv",
        ("gateio", "linear"): "gateio_ohlcv_seconds",
        ("huobi", "linear"): "huobi_ohlcv",
    }

    fixture_name = fixture_map.get((exchange.lower(), market_type.lower()))
    if not fixture_name:
        raise ValueError(f"No fixture for {exchange} {market_type}")

    return load_fixture(fixture_name)


def get_mock_oi_response(exchange: str, market_type: str = "linear") -> list | dict:
    """Get mock OI response for an exchange."""
    fixture_map = {
        ("binance", "linear"): "binance_linear_oi",
        ("binance", "inverse"): "binance_inverse_coinm_oi",
        ("bybit", "linear"): "bybit_oi",
        ("gateio", "linear"): "gateio_oi_seconds",
    }

    fixture_name = fixture_map.get((exchange.lower(), market_type.lower()))
    if not fixture_name:
        raise ValueError(f"No fixture for {exchange} {market_type}")

    return load_fixture(fixture_name)


def get_mock_error_response(error_type: str) -> dict:
    """Get mock error response."""
    errors = load_fixture("error_responses")

    if error_type not in errors:
        raise KeyError(f"Error type '{error_type}' not found")

    return errors[error_type]


def get_edge_case_data(case_name: str) -> list:
    """Get edge case test data."""
    edge_cases = load_fixture("edge_cases")

    if case_name not in edge_cases:
        raise KeyError(f"Edge case '{case_name}' not found")

    return edge_cases[case_name]


# Test helper functions


def create_mock_ohlc_record(
    timestamp_ms: int = 1704110400000,
    open_price: float = 50000.0,
    high: float = 50100.0,
    low: float = 49900.0,
    close: float = 50050.0,
    volume: float = 100.0,
) -> list:
    """Create a single mock OHLC record."""
    return [timestamp_ms, open_price, high, low, close, volume]


def create_mock_oi_record(
    timestamp_ms: int = 1704110400000,
    open_interest: float = 10000.0,
    symbol: str = "BTCUSDT",
) -> dict:
    """Create a single mock OI record."""
    return {
        "symbol": symbol,
        "sumOpenInterest": str(open_interest),
        "timestamp": timestamp_ms,
    }


def assert_ohlc_validity(record: dict) -> None:
    """
    Assert OHLC record passes basic validity checks.

    Args:
        record: OHLC record to validate

    Raises:
        AssertionError: If validation fails
    """
    assert record["open"] > 0, "Open price must be positive"
    assert record["high"] > 0, "High price must be positive"
    assert record["low"] > 0, "Low price must be positive"
    assert record["close"] > 0, "Close price must be positive"
    assert record["volume"] >= 0, "Volume must be non-negative"

    assert record["high"] >= record["low"], "High must be >= Low"
    assert record["high"] >= record["open"], "High must be >= Open"
    assert record["high"] >= record["close"], "High must be >= Close"
    assert record["low"] <= record["open"], "Low must be <= Open"
    assert record["low"] <= record["close"], "Low must be <= Close"


def assert_oi_validity(record: dict) -> None:
    """
    Assert OI record passes basic validity checks.

    Args:
        record: OI record to validate

    Raises:
        AssertionError: If validation fails
    """
    assert record["open_interest"] >= 0, "OI must be non-negative"
    assert record["settlement_currency"], "Settlement currency required"
    assert isinstance(record["ts"], datetime), "Timestamp must be datetime"


__all__ = [
    "load_fixture",
    "get_mock_ohlcv_response",
    "get_mock_oi_response",
    "get_mock_error_response",
    "get_edge_case_data",
    "create_mock_ohlc_record",
    "create_mock_oi_record",
    "assert_ohlc_validity",
    "assert_oi_validity",
]


def get_coinalyze_ohlcv_response() -> list:
    """Get mock OHLCV response for CoinAlyze."""
    return load_fixture("coinalyze_ohlcv")


def get_coinalyze_oi_response() -> list:
    """Get mock Open Interest response for CoinAlyze."""
    return load_fixture("coinalyze_oi")


def get_coinalyze_error_responses() -> dict:
    """Get mock error responses for CoinAlyze."""
    return load_fixture("coinalyze_error_responses")
