from prometheus_client import REGISTRY, Counter, Gauge

# Metrics used by bronze ingestion tasks.
# These are deliberately lightweight; if Prometheus is not configured, using these
# should still be safe within try/except blocks in callers.

# Safe metric registration: only register if not already registered
try:
    BRONZE_FILES_WRITTEN = REGISTRY._names_to_collectors.get(
        "bronze_files_written_total"
    )
    if BRONZE_FILES_WRITTEN is None:
        BRONZE_FILES_WRITTEN = Counter(
            "bronze_files_written_total",
            "Total number of bronze Parquet files written",
            ["source", "data_type", "symbol", "timeframe"],
        )
except Exception:
    # Fallback if registry access fails
    BRONZE_FILES_WRITTEN = Counter(
        "bronze_files_written_total",
        "Total number of bronze Parquet files written",
        ["source", "data_type", "symbol", "timeframe"],
    )

try:
    BRONZE_FRESHNESS = REGISTRY._names_to_collectors.get("bronze_freshness_seconds")
    if BRONZE_FRESHNESS is None:
        BRONZE_FRESHNESS = Gauge(
            "bronze_freshness_seconds",
            "Freshness in seconds (now - latest timestamp written)",
            ["source", "data_type", "symbol", "timeframe"],
        )
except Exception:
    # Fallback if registry access fails
    BRONZE_FRESHNESS = Gauge(
        "bronze_freshness_seconds",
        "Freshness in seconds (now - latest timestamp written)",
        ["source", "data_type", "symbol", "timeframe"],
    )
