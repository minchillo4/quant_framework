from datetime import UTC, datetime, timedelta


class TimezoneEnforcer:
    @staticmethod
    def ensure_utc(dt: datetime) -> datetime:
        if dt.tzinfo is None:
            return dt.replace(tzinfo=UTC)
        return dt.astimezone(UTC)

    @staticmethod
    def align_to_timeframe(dt: datetime, timeframe: str) -> datetime:
        dt = TimezoneEnforcer.ensure_utc(dt)
        seconds = TimeframeUtils.timeframe_to_seconds(timeframe)
        aligned = dt - timedelta(seconds=dt.timestamp() % seconds)
        return aligned


class TimeframeUtils:
    @staticmethod
    def timeframe_to_seconds(timeframe: str) -> int:
        unit = timeframe[-1]
        value = int(timeframe[:-1])
        if unit == "m":
            return value * 60
        if unit == "h":
            return value * 3600
        if unit == "d":
            return value * 86400
        raise ValueError(f"Unsupported timeframe: {timeframe}")
