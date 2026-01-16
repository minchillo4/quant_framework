"""
Date Utilities
==============

Common date/time handling utilities for timestamps, conversions, and timezone-aware operations.
"""

from datetime import UTC, datetime, timedelta


def to_unix_ms(dt: datetime) -> int:
    """
    Convert datetime to Unix timestamp in milliseconds.

    Args:
        dt: Datetime object (timezone-aware or naive assumed UTC)

    Returns:
        Unix timestamp in milliseconds
    """
    if dt.tzinfo is None:
        # Assume UTC for naive datetimes
        dt = dt.replace(tzinfo=UTC)

    return int(dt.timestamp() * 1000)


def from_unix_ms(timestamp_ms: int) -> datetime:
    """
    Convert Unix timestamp in milliseconds to datetime (UTC).

    Args:
        timestamp_ms: Unix timestamp in milliseconds

    Returns:
        Timezone-aware datetime in UTC
    """
    return datetime.fromtimestamp(timestamp_ms / 1000, tz=UTC)


def utc_now() -> datetime:
    """
    Get current time in UTC.

    Returns:
        Current UTC datetime
    """
    return datetime.now(UTC)


def utc_today() -> datetime:
    """
    Get today's date at midnight UTC.

    Returns:
        Today's date at 00:00:00 UTC
    """
    now = utc_now()
    return now.replace(hour=0, minute=0, second=0, microsecond=0)


def truncate_to_day(dt: datetime) -> datetime:
    """
    Truncate datetime to start of day (00:00:00).

    Args:
        dt: Datetime to truncate

    Returns:
        Datetime at 00:00:00 in same timezone
    """
    return dt.replace(hour=0, minute=0, second=0, microsecond=0)


def truncate_to_hour(dt: datetime) -> datetime:
    """
    Truncate datetime to start of hour (00:00).

    Args:
        dt: Datetime to truncate

    Returns:
        Datetime at start of hour
    """
    return dt.replace(minute=0, second=0, microsecond=0)


def add_days(dt: datetime, days: int) -> datetime:
    """
    Add days to datetime, preserving timezone.

    Args:
        dt: Base datetime
        days: Number of days to add

    Returns:
        New datetime
    """
    return dt + timedelta(days=days)


def add_hours(dt: datetime, hours: int) -> datetime:
    """
    Add hours to datetime, preserving timezone.

    Args:
        dt: Base datetime
        hours: Number of hours to add

    Returns:
        New datetime
    """
    return dt + timedelta(hours=hours)


def date_range(start: datetime, end: datetime, days: int = 1) -> list[datetime]:
    """
    Generate a list of dates between start and end with given interval.

    Args:
        start: Start date
        end: End date (inclusive)
        days: Interval in days

    Returns:
        List of dates
    """
    result = []
    current = truncate_to_day(start)
    end = truncate_to_day(end)

    while current <= end:
        result.append(current)
        current = add_days(current, days)

    return result
