"""
AdaptiveRateLimiter for chunk sizing across timeframes.

Tracks points received vs expected and suggests chunk size adjustments.
Persists adaptive stats into checkpoint metadata via caller.
"""

from datetime import timedelta


class AdaptiveRateLimiter:
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
        tf = timeframe if timeframe in self.SAFE_CHUNK_DAYS else "1h"
        base_days = (
            default_days if default_days is not None else self.SAFE_CHUNK_DAYS[tf]
        )

        stats = self.performance_stats.get(tf, [])
        if not stats:
            return timedelta(days=base_days)

        avg_eff = sum(s.get("efficiency", 1.0) for s in stats) / len(stats)
        if avg_eff >= 0.95:
            adj = 1.1
        elif avg_eff <= 0.8:
            adj = 0.8
        else:
            adj = 1.0

        new_days = base_days * adj
        max_days = (self.MAX_POINTS_PER_REQUEST * self.TIMEFRAME_MINUTES[tf]) / (
            24 * 60
        )
        new_days = min(new_days, max_days * 0.95)
        return timedelta(days=new_days)

    def record_result(
        self, timeframe: str, points_received: int, start_ts, end_ts
    ) -> None:
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
        # keep last 10
        if len(self.performance_stats[tf]) > 10:
            self.performance_stats[tf].pop(0)
