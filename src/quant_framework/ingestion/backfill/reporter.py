"""
Backfill reporter for progress logging and summary reporting.

Responsibility: Format and log progress/results.
Does NOT make backfill decisions or execute chunks.
"""

import logging

from quant_framework.ingestion.ports.backfill import BackfillSummary

logger = logging.getLogger(__name__)


class BackfillReporter:
    """
    Reports backfill progress and results.

    Implements IBackfillReporter protocol.

    Single Responsibility: Format and log progress metrics without making backfill decisions.
    """

    def __init__(self, verbose: bool = False):
        """
        Initialize reporter.

        Args:
            verbose: Enable verbose logging
        """
        self.verbose = verbose
        logger.info("BackfillReporter initialized")

    def log_progress(
        self,
        symbol: str,
        timeframe: str,
        data_type: str,
        chunks_completed: int,
        total_chunks: int,
        records_written: int,
    ) -> None:
        """
        Log progress after chunk completion.

        Args:
            symbol: Trading symbol
            timeframe: Candle timeframe
            data_type: Data type being backfilled
            chunks_completed: Number of completed chunks
            total_chunks: Total chunks to process
            records_written: Records written by last chunk
        """
        progress_pct = (
            (chunks_completed / total_chunks * 100) if total_chunks > 0 else 0
        )

        if self.verbose:
            logger.debug(
                f"📊 {symbol}/{timeframe}/{data_type}: "
                f"{chunks_completed}/{total_chunks} chunks ({progress_pct:.1f}%), "
                f"{records_written} records"
            )
        else:
            if (
                chunks_completed == 1
                or chunks_completed % 5 == 0
                or chunks_completed == total_chunks
            ):
                logger.info(
                    f"📊 {symbol}/{timeframe}/{data_type}: "
                    f"{chunks_completed}/{total_chunks} chunks ({progress_pct:.1f}%), "
                    f"{records_written} records"
                )

    def log_summary(self, summary: BackfillSummary) -> None:
        """
        Log final backfill summary.

        Args:
            summary: Backfill completion summary with statistics
        """
        logger.info("\n" + "=" * 80)
        logger.info("BACKFILL SUMMARY")
        logger.info("=" * 80)

        logger.info(f"Total combinations: {summary.total_chunks}")
        logger.info(f"  ✅ Successful: {summary.successful_chunks}")
        logger.info(f"  ❌ Failed: {summary.failed_chunks}")
        logger.info(f"Success rate: {self._success_rate_pct(summary):.1f}%")
        logger.info(f"Total records written: {summary.total_records_written:,}")
        logger.info(f"Total duration: {summary.total_duration_seconds:.1f}s")

        if summary.total_records_written > 0:
            rate = summary.total_records_written / max(
                summary.total_duration_seconds, 0.1
            )
            logger.info(f"Write rate: {rate:.0f} records/sec")

        if summary.errors:
            logger.info("\nErrors encountered:")
            for symbol, error_msg in summary.errors:
                logger.info(f"  - {symbol}: {error_msg}")

        logger.info("=" * 80 + "\n")

    def _success_rate_pct(self, summary: BackfillSummary) -> float:
        """Calculate success rate as percentage."""
        total = summary.successful_chunks + summary.failed_chunks
        if total == 0:
            return 0.0
        return (summary.successful_chunks / total) * 100
