"""
AdaptiveRateLimiter for CoinAlyze: tracks points per request and adjusts chunk sizes.

CoinAlyze limits by points (candles) per request, not by date range like other sources.
This limiter adapts chunk size based on observed API efficiency to stay within limits.

Persists adaptive stats into checkpoint metadata via caller.
"""

from datetime import timedelta


class AdaptiveRateLimiter:
    """
    Adaptive rate limiter for CoinAlyze API with per-candle limiting.

    Adjusts date-range chunk sizes based on API efficiency to avoid exceeding
    the 950 points (candles) per request limit.
    """

    MAX_POINTS_PER_REQUEST = 950

    TIMEFRAME_MINUTES = {
        "1m": 1,
        "5m": 5,
        "15m": 15,
        "30m": 30,
        "1h": 60,
        "4h": 240,
        "1d": 1440,
    }

    SAFE_CHUNK_DAYS = {
        "1m": 0.6,
        "5m": 3.0,
        "15m": 9.0,
        "30m": 18.0,
        "1h": 36.0,
        "4h": 144.0,
        "1d": 855.0,
    }

    def __init__(self) -> None:
        self.performance_stats: dict[str, list[dict]] = {}

    def get_chunk_size(
        self, timeframe: str, default_days: float | None = None
    ) -> timedelta:
        """Get safe chunk size (days) for a timeframe based on observed efficiency."""
        tf = timeframe if timeframe in self.SAFE_CHUNK_DAYS else "1h"
        base_days = (
            default_days if default_days is not None else self.SAFE_CHUNK_DAYS[tf]
        )

        stats = self.performance_stats.get(tf, [])
        if not stats:
            return timedelta(days=base_days)

        avg_eff = sum(s.get("efficiency", 1.0) for s in stats) / len(stats)
        if avg_eff >= 0.95:
            adj = 1.1  # 10% increase if very efficient
        elif avg_eff <= 0.8:
            adj = 0.8  # 20% decrease if inefficient
        else:
            adj = 1.0  # Keep same

        new_days = base_days * adj
        max_days = (self.MAX_POINTS_PER_REQUEST * self.TIMEFRAME_MINUTES[tf]) / (
            24 * 60
        )
        new_days = min(new_days, max_days * 0.95)
        return timedelta(days=new_days)

    def record_result(
        self, timeframe: str, points_received: int, start_ts, end_ts
    ) -> None:
        """Record fetch result to compute efficiency and adjust future chunk sizes."""
        tf = timeframe if timeframe in self.TIMEFRAME_MINUTES else "1h"
        tf_minutes = self.TIMEFRAME_MINUTES[tf]
        expected = ((end_ts - start_ts).total_seconds() / 60) / tf_minutes
        eff = (points_received / expected) if expected > 0 else 1.0
        self.performance_stats.setdefault(tf, []).append(
            {
                "points_received": points_received,
                "points_expected": expected,
                "efficiency": eff,
                "date_range_days": (end_ts - start_ts).days,
            }
        )
        # Keep last 10 results for averaging
        if len(self.performance_stats[tf]) > 10:
            self.performance_stats[tf].pop(0)
