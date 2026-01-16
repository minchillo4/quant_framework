"""
Dependency container for backfill orchestration layer.

Wires together:
- ChunkGenerator (time-range splitting)
- ChunkProcessor (fetch + write)
- Reporter (progress/logging)
- Rate limiter and checkpoint manager
"""

import logging
from typing import Any

from quant_framework.ingestion.orchestration.backfill.checkpoint_manager import (
    CheckpointManager,
)
from quant_framework.ingestion.orchestration.backfill.chunk_generator import (
    TimeRangeChunkGenerator,
)
from quant_framework.ingestion.orchestration.backfill.chunk_processor import (
    BackfillChunkProcessor,
)
from quant_framework.ingestion.orchestration.backfill.rate_limiter import IRateLimiter
from quant_framework.ingestion.orchestration.backfill.reporter import BackfillReporter

logger = logging.getLogger(__name__)


class BackfillDependencyContainer:
    """
    Dependency injection container for backfill orchestration.

    Wires together all backfill components in one place.
    Single responsibility: Assemble dependencies.

    Usage:
        container = BackfillDependencyContainer(
            data_adapter=coinalyze_adapter,
            bronze_writer=bronze_writer,
            rate_limiter=rate_limiter,
            checkpoint_manager=checkpoint_manager
        )

        generator = container.create_chunk_generator()
        processor = container.create_chunk_processor()
        reporter = container.create_reporter()
    """

    def __init__(
        self,
        data_adapter: Any,  # CoinalyzeBackfillAdapter
        bronze_writer: Any,  # BronzeWriter
        rate_limiter: IRateLimiter,
        checkpoint_manager: CheckpointManager,
        verbose: bool = False,
    ):
        """
        Initialize backfill dependency container.

        Args:
            data_adapter: Adapter for fetching data (CoinalyzeBackfillAdapter)
            bronze_writer: Writer for bronze layer
            rate_limiter: Rate limiting strategy
            checkpoint_manager: Checkpoint tracking manager
            verbose: Enable verbose logging for reporter
        """
        self.data_adapter = data_adapter
        self.bronze_writer = bronze_writer
        self.rate_limiter = rate_limiter
        self.checkpoint_manager = checkpoint_manager
        self.verbose = verbose

        logger.info("BackfillDependencyContainer initialized")

    def create_chunk_generator(self) -> TimeRangeChunkGenerator:
        """
        Create chunk generator implementation.

        Returns:
            TimeRangeChunkGenerator instance
        """
        return TimeRangeChunkGenerator(rate_limiter=self.rate_limiter)

    def create_chunk_processor(self) -> BackfillChunkProcessor:
        """
        Create chunk processor implementation.

        Returns:
            BackfillChunkProcessor instance
        """
        return BackfillChunkProcessor(
            data_adapter=self.data_adapter, bronze_writer=self.bronze_writer
        )

    def create_reporter(self) -> BackfillReporter:
        """
        Create reporter implementation.

        Returns:
            BackfillReporter instance
        """
        return BackfillReporter(verbose=self.verbose)


def create_backfill_container_from_components(
    data_adapter: Any,
    bronze_writer: Any,
    rate_limiter: IRateLimiter,
    checkpoint_manager: CheckpointManager,
    verbose: bool = False,
) -> BackfillDependencyContainer:
    """
    Factory function for creating backfill container.

    Convenience wrapper around BackfillDependencyContainer constructor.

    Args:
        data_adapter: Adapter for fetching data
        bronze_writer: Writer for bronze layer
        rate_limiter: Rate limiting strategy
        checkpoint_manager: Checkpoint tracking manager
        verbose: Enable verbose logging

    Returns:
        Configured BackfillDependencyContainer
    """
    return BackfillDependencyContainer(
        data_adapter=data_adapter,
        bronze_writer=bronze_writer,
        rate_limiter=rate_limiter,
        checkpoint_manager=checkpoint_manager,
        verbose=verbose,
    )
