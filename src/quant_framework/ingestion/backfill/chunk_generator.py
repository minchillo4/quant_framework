"""
Time-range chunk generator for backfill orchestration.

Responsibility: Generate optimal chunks based on rate limiting strategy.
Does NOT execute chunks or track progress.
"""

import logging
from datetime import datetime

from quant_framework.ingestion.backfill.rate_limiter import IRateLimiter
from quant_framework.ingestion.ports.backfill import BackfillChunk

logger = logging.getLogger(__name__)


class TimeRangeChunkGenerator:
    """
    Generates time-range chunks for backfill processing.

    Implements IChunkGenerator protocol.

    Single Responsibility: Calculate chunk boundaries based on rate limiting.
    """

    def __init__(self, rate_limiter: IRateLimiter):
        """
        Initialize chunk generator.

        Args:
            rate_limiter: Rate limiting strategy for chunk sizing
        """
        self.rate_limiter = rate_limiter
        logger.info("TimeRangeChunkGenerator initialized")

    def generate_chunks(
        self,
        symbol: str,
        timeframe: str,
        data_type: str,
        start_date: datetime,
        end_date: datetime,
    ) -> list[BackfillChunk]:
        """
        Generate optimal chunks based on rate limiting strategy.

        Args:
            symbol: Trading symbol
            timeframe: Candle timeframe (e.g., "1h", "1d")
            data_type: Type of data (e.g., "ohlcv", "open_interest")
            start_date: Backfill start date
            end_date: Backfill end date

        Returns:
            List of chunks covering the date range
        """
        chunks = []
        chunk_size = self.rate_limiter.calculate_chunk_size(timeframe)

        current_start = start_date
        while current_start < end_date:
            chunk_end = min(current_start + chunk_size, end_date)

            chunks.append(
                BackfillChunk(
                    symbol=symbol,
                    timeframe=timeframe,
                    data_type=data_type,
                    start_date=current_start,
                    end_date=chunk_end,
                )
            )

            current_start = chunk_end

        logger.debug(
            f"Generated {len(chunks)} chunks for {symbol}/{timeframe}/{data_type}"
        )
        return chunks
