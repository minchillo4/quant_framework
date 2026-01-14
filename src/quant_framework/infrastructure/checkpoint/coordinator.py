"""CheckpointCoordinator for OI pipelines.

Responsibilities:
- Load checkpoints from MinIO using consistent paths
- Bootstrap CCXT incremental checkpoints from Coinalyze backfill with a safe overlap
- Validate lag thresholds between sources
- Atomically update checkpoints after writes
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime

from .path_builder import CheckpointPathBuilder
from .store import CheckpointDocument, CheckpointStore


@dataclass
class GapStatus:
    """Result of cross-source gap validation."""

    lag_ms: int
    warn: bool
    alert: bool
    message: str


class CheckpointCoordinator:
    """High-level checkpoint orchestration for OI pipelines."""

    def __init__(
        self,
        store: CheckpointStore,
        overlap_ms: int = 60 * 60 * 1000,
    ) -> None:
        self.store = store
        self.overlap_ms = overlap_ms

    # ------------------------------------------------------------------
    # Load helpers
    # ------------------------------------------------------------------
    def load_coinalyze_checkpoint(
        self, symbol: str, timeframe: str
    ) -> CheckpointDocument | None:
        key = CheckpointPathBuilder.coinalyze_backfill("oi", symbol, timeframe)
        doc, _ = self.store.read(key)
        return doc

    def load_ccxt_checkpoint(
        self,
        market_type: str,
        exchange: str,
        symbol: str,
        timeframe: str = "5m",
    ) -> CheckpointDocument | None:
        key = CheckpointPathBuilder.ccxt_incremental(
            market_type, exchange, symbol, timeframe
        )
        doc, _ = self.store.read(key)
        return doc

    # ------------------------------------------------------------------
    # Bootstrap CCXT from Coinalyze
    # ------------------------------------------------------------------
    def load_or_bootstrap_ccxt(
        self,
        exchange: str,
        market_type: str,
        ccxt_symbol: str,
        canonical_symbol: str,
        timeframe: str = "5m",
    ) -> CheckpointDocument:
        key = CheckpointPathBuilder.ccxt_incremental(
            market_type, exchange, ccxt_symbol, timeframe
        )
        existing, etag = self.store.read(key)
        if existing:
            return existing

        coinalyze = self.load_coinalyze_checkpoint(canonical_symbol, timeframe)
        if coinalyze is None:
            raise RuntimeError(
                f"Missing Coinalyze checkpoint for {canonical_symbol}/{timeframe}; cannot bootstrap"
            )

        start_ts = max(0, coinalyze.last_successful_timestamp - self.overlap_ms)
        now_iso = datetime.now(UTC).isoformat()

        bootstrap = CheckpointDocument(
            source="ccxt",
            exchange=exchange,
            symbol=ccxt_symbol,
            timeframe=timeframe,
            market_type=market_type,
            last_successful_timestamp=start_ts,
            last_updated=now_iso,
            total_records=0,
            metadata={
                "source_checkpoint": CheckpointPathBuilder.coinalyze_backfill(
                    "oi", canonical_symbol, timeframe
                ),
                "bootstrap_overlap_ms": self.overlap_ms,
            },
        )

        self.store.write_atomic(key, bootstrap, if_match=etag)
        return bootstrap

    # ------------------------------------------------------------------
    # Updates
    # ------------------------------------------------------------------
    def update_ccxt_checkpoint(
        self,
        exchange: str,
        market_type: str,
        ccxt_symbol: str,
        timeframe: str,
        last_successful_timestamp: int,
        total_records: int,
        metadata: dict | None = None,
    ) -> CheckpointDocument:
        key = CheckpointPathBuilder.ccxt_incremental(
            market_type, exchange, ccxt_symbol, timeframe
        )
        existing, etag = self.store.read(key)
        now_iso = datetime.now(UTC).isoformat()

        doc = existing or CheckpointDocument(
            source="ccxt",
            exchange=exchange,
            symbol=ccxt_symbol,
            timeframe=timeframe,
            market_type=market_type,
            last_successful_timestamp=last_successful_timestamp,
            last_updated=now_iso,
            total_records=0,
            metadata={},
        )

        doc.last_successful_timestamp = last_successful_timestamp
        doc.last_updated = now_iso
        doc.total_records = total_records
        doc.metadata.update(metadata or {})

        new_etag = self.store.write_atomic(key, doc, if_match=etag)
        doc.metadata["etag"] = new_etag
        return doc

    # ------------------------------------------------------------------
    # Gap validation
    # ------------------------------------------------------------------
    @staticmethod
    def validate_gap(
        coinalyze_checkpoint: CheckpointDocument,
        ccxt_checkpoint: CheckpointDocument,
        warn_threshold_ms: int = 2 * 60 * 60 * 1000,
        alert_threshold_ms: int = 4 * 60 * 60 * 1000,
    ) -> GapStatus:
        lag = (
            coinalyze_checkpoint.last_successful_timestamp
            - ccxt_checkpoint.last_successful_timestamp
        )
        warn = lag > warn_threshold_ms
        alert = lag > alert_threshold_ms
        message = "ok"
        if alert:
            message = "ccxt more than 4h behind coinalyze"
        elif warn:
            message = "ccxt more than 2h behind coinalyze"
        return GapStatus(lag_ms=lag, warn=warn, alert=alert, message=message)
