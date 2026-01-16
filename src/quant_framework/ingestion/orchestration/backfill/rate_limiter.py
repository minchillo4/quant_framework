"""
Rate limiting strategies for external data sources.
Prevents API throttling and data truncation.
"""

import logging
from datetime import datetime, timedelta
from typing import Protocol

import yaml

logger = logging.getLogger(__name__)


class IRateLimiter(Protocol):
    """
    Protocol for rate limiting strategies.
    Enables different implementations per data source.
    """

    def normalize_timeframe(self, timeframe: str) -> str:
        """
        Normalize timeframe to standard format.

        Args:
            timeframe: Input timeframe (e.g., '1hour', '5min', '1h')

        Returns:
            Normalized timeframe in standard format (e.g., '1h', '5m')
        """
        ...

    def calculate_chunk_size(self, timeframe: str) -> timedelta:
        """
        Calculate optimal chunk size for timeframe.

        Args:
            timeframe: Standard timeframe format

        Returns:
            Time duration for safe chunk size
        """
        ...

    def should_reduce_chunk(self, points_received: int, timeframe: str) -> bool:
        """
        Determine if chunk size should be reduced based on response.

        Args:
            points_received: Number of data points in response
            timeframe: Timeframe being fetched

        Returns:
            True if approaching rate limit
        """
        ...

    def should_increase_chunk(self, points_received: int, timeframe: str) -> bool:
        """
        Determine if chunk size can be increased.

        Args:
            points_received: Number of data points in response
            timeframe: Timeframe being fetched

        Returns:
            True if safe to increase chunk size
        """
        ...

    def record_performance(
        self,
        timeframe: str,
        chunk_size_days: float,
        points_received: int,
        success: bool,
    ) -> None:
        """
        Record performance metrics for adaptive chunking.

        Args:
            timeframe: Timeframe fetched
            chunk_size_days: Chunk size used (in days)
            points_received: Data points received
            success: Whether fetch succeeded
        """
        ...


class CoinAlyzeRateLimiter:
    """
    Rate limiter for CoinAlyze API with adaptive chunking.
    Prevents silent data truncation from API point limits.
    """

    def __init__(self, config_path: str | None = None, verbose: bool = False):
        """
        Initialize rate limiter.

        Args:
            config_path: Path to backfill.yaml config file
            verbose: Enable verbose logging
        """
        self.verbose = verbose
        self.performance_stats: dict[str, list[dict]] = {}

        # Load configuration
        if config_path is None:
            from quant_framework.infrastructure.impls import ProjectConfigPathResolver

            resolver = ProjectConfigPathResolver()
            config_path = resolver.resolve_config_file("backfill.yaml")

        with open(config_path) as f:
            config = yaml.safe_load(f)

        rate_config = config["rate_limiting"]["coinalyze"]
        self.max_points = rate_config["max_points_per_request"]
        self.timeframe_aliases = rate_config["timeframe_aliases"]
        self.timeframe_minutes = rate_config["timeframe_minutes"]
        self.safe_chunk_days = rate_config["safe_chunk_days"]

        adaptive_config = rate_config.get("adaptive_chunking", {})
        self.adaptive_enabled = adaptive_config.get("enabled", True)
        self.reduce_threshold = adaptive_config.get("reduce_threshold", 0.95)
        self.increase_threshold = adaptive_config.get("increase_threshold", 0.70)
        self.min_samples = adaptive_config.get("min_samples_for_adjustment", 3)

        logger.info(
            f"CoinAlyze rate limiter initialized: "
            f"max_points={self.max_points}, adaptive={self.adaptive_enabled}"
        )

    def normalize_timeframe(self, timeframe: str) -> str:
        """
        Normalize timeframe to standard CCXT format.

        Examples:
            '1hour' -> '1h'
            '5min' -> '5m'
            '1day' -> '1d'
            '1h' -> '1h' (already normalized)
        """
        normalized = self.timeframe_aliases.get(timeframe, timeframe)

        if normalized not in self.timeframe_minutes:
            logger.warning(
                f"Unknown timeframe '{timeframe}' (normalized to '{normalized}'). "
                f"Valid timeframes: {list(self.timeframe_minutes.keys())}"
            )
            # Default to 1h for unknown timeframes
            normalized = "1h"

        if self.verbose and normalized != timeframe:
            logger.debug(f"Normalized timeframe: {timeframe} -> {normalized}")

        return normalized

    def calculate_chunk_size(self, timeframe: str) -> timedelta:
        """
        Calculate optimal chunk size for timeframe.
        Uses safe defaults with optional adaptive adjustment.
        """
        normalized_tf = self.normalize_timeframe(timeframe)
        base_days = self.safe_chunk_days.get(normalized_tf, 36.0)

        # Apply adaptive adjustment if enabled and we have performance data
        if self.adaptive_enabled and normalized_tf in self.performance_stats:
            stats = self.performance_stats[normalized_tf]
            if len(stats) >= self.min_samples:
                avg_points = sum(s["points"] for s in stats) / len(stats)
                points_ratio = avg_points / self.max_points

                # Adjust chunk size based on historical performance
                if points_ratio > self.reduce_threshold:
                    # Getting too close to limit, reduce by 10%
                    adjusted_days = base_days * 0.9
                    logger.info(
                        f"Reducing chunk size for {normalized_tf}: "
                        f"{base_days:.1f} -> {adjusted_days:.1f} days "
                        f"(avg points: {avg_points:.0f})"
                    )
                    base_days = adjusted_days
                elif points_ratio < self.increase_threshold:
                    # Safe to increase, grow by 10%
                    adjusted_days = min(
                        base_days * 1.1, self.safe_chunk_days[normalized_tf] * 1.5
                    )
                    logger.info(
                        f"Increasing chunk size for {normalized_tf}: "
                        f"{base_days:.1f} -> {adjusted_days:.1f} days "
                        f"(avg points: {avg_points:.0f})"
                    )
                    base_days = adjusted_days

        return timedelta(days=base_days)

    def should_reduce_chunk(self, points_received: int, timeframe: str) -> bool:
        """
        Check if we're approaching API limits.
        Returns True if >= 95% of max points received.
        """
        threshold = self.max_points * self.reduce_threshold
        approaching_limit = points_received >= threshold

        if approaching_limit:
            logger.warning(
                f"Approaching rate limit for {timeframe}: "
                f"{points_received}/{self.max_points} points "
                f"({points_received/self.max_points*100:.1f}%)"
            )

        return approaching_limit

    def should_increase_chunk(self, points_received: int, timeframe: str) -> bool:
        """
        Check if safe to increase chunk size.
        Returns True if < 70% of max points received.
        """
        threshold = self.max_points * self.increase_threshold
        safe_to_increase = points_received < threshold

        if safe_to_increase and self.verbose:
            logger.debug(
                f"Safe to increase chunk for {timeframe}: "
                f"{points_received}/{self.max_points} points "
                f"({points_received/self.max_points*100:.1f}%)"
            )

        return safe_to_increase

    def record_performance(
        self,
        timeframe: str,
        chunk_size_days: float,
        points_received: int,
        success: bool,
    ) -> None:
        """
        Record performance metrics for adaptive learning.
        """
        normalized_tf = self.normalize_timeframe(timeframe)

        if normalized_tf not in self.performance_stats:
            self.performance_stats[normalized_tf] = []

        self.performance_stats[normalized_tf].append(
            {
                "chunk_size_days": chunk_size_days,
                "points": points_received,
                "success": success,
                "timestamp": datetime.utcnow(),
            }
        )

        # Keep only recent history (last 100 samples)
        if len(self.performance_stats[normalized_tf]) > 100:
            self.performance_stats[normalized_tf] = self.performance_stats[
                normalized_tf
            ][-100:]

        if self.verbose:
            logger.debug(
                f"Recorded performance: {normalized_tf}, "
                f"chunk={chunk_size_days:.1f}d, points={points_received}, "
                f"success={success}"
            )

    def validate_chunk_response(
        self, points_received: int, timeframe: str, expected_range: tuple | None = None
    ) -> dict[str, any]:
        """
        Validate API response for truncation or issues.

        Args:
            points_received: Number of points in response
            timeframe: Timeframe of request
            expected_range: Optional (min, max) expected point range

        Returns:
            Validation result dictionary with warnings/errors
        """
        result = {"valid": True, "warnings": [], "truncated": False}

        # Check for API truncation
        if points_received >= self.max_points:
            result["valid"] = False
            result["truncated"] = True
            result["warnings"].append(
                f"TRUNCATION DETECTED: Received {points_received} points "
                f"(>= max {self.max_points}). Data may be incomplete!"
            )

        # Check if approaching limit
        elif self.should_reduce_chunk(points_received, timeframe):
            result["warnings"].append(
                f"Approaching limit: {points_received}/{self.max_points} points. "
                f"Consider reducing chunk size."
            )

        # Check expected range if provided
        if expected_range:
            min_expected, max_expected = expected_range
            if points_received < min_expected:
                result["warnings"].append(
                    f"Fewer points than expected: {points_received} < {min_expected}"
                )
            elif points_received > max_expected:
                result["warnings"].append(
                    f"More points than expected: {points_received} > {max_expected}"
                )

        return result

    def get_performance_summary(self) -> dict[str, dict]:
        """
        Get performance statistics summary for all timeframes.
        """
        summary = {}

        for tf, stats in self.performance_stats.items():
            if not stats:
                continue

            successful = [s for s in stats if s["success"]]
            summary[tf] = {
                "total_fetches": len(stats),
                "successful_fetches": len(successful),
                "avg_points": sum(s["points"] for s in successful) / len(successful)
                if successful
                else 0,
                "max_points": max(s["points"] for s in successful) if successful else 0,
                "avg_chunk_size": sum(s["chunk_size_days"] for s in successful)
                / len(successful)
                if successful
                else 0,
                "success_rate": len(successful) / len(stats) * 100 if stats else 0,
            }

        return summary
