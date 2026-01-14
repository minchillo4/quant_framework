"""Utilities to build consistent bronze MinIO paths for data and checkpoints.

Centralizes path conventions so DAGs, coordinators, and writers do not drift.
"""

from __future__ import annotations

from datetime import date


class CheckpointPathBuilder:
    """Builds consistent checkpoint and data paths across the system."""

    CHECKPOINTS_ROOT = "checkpoints"
    DATA_ROOT = "data/open_interest"

    @classmethod
    def coinalyze_backfill(cls, data_type: str, symbol: str, timeframe: str) -> str:
        """Path for Coinalyze backfill checkpoints."""
        return f"{cls.CHECKPOINTS_ROOT}/coinalyze_backfill/{data_type}/{symbol}/{timeframe}.json"

    @classmethod
    def ccxt_incremental(
        cls,
        market_type: str,
        exchange: str,
        symbol: str,
        timeframe: str = "5m",
    ) -> str:
        """Path for CCXT incremental checkpoints."""
        return (
            f"{cls.CHECKPOINTS_ROOT}/ccxt_incremental/"
            f"{market_type}/{exchange}/{symbol}.json"
        )

    @classmethod
    def bronze_data_partition(
        cls,
        source: str,
        symbol: str,
        timeframe: str,
        dt: date,
        exchange: str | None = None,
        market_type: str | None = None,
    ) -> str:
        """Path for bronze data partitions (directory prefix ending with a slash)."""
        if source == "coinalyze":
            return (
                f"{cls.DATA_ROOT}/source=coinalyze/"
                f"symbol={symbol}/timeframe={timeframe}/"
                f"dt={dt.isoformat()}/"
            )

        if exchange is None or market_type is None:
            raise ValueError("exchange and market_type are required for ccxt paths")

        return (
            f"{cls.DATA_ROOT}/source=ccxt/"
            f"exchange={exchange}/market_type={market_type}/"
            f"symbol={symbol}/timeframe={timeframe}/"
            f"dt={dt.isoformat()}/"
        )
