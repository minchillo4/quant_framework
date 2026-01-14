#!/usr/bin/env bash
# infra/minio/init-bronze-layer.sh
set -euo pipefail

echo "🏗️  Initializing Bronze Layer (MinIO)..."

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/../../../.." && pwd)"
VALIDATE_ONLY="${BRONZE_VALIDATE_ONLY:-0}"

# Discover config file (env override -> /app -> repo)
CONFIG_CANDIDATES=()
[[ -n "${BRONZE_SOURCES_CONFIG:-}" ]] && CONFIG_CANDIDATES+=("${BRONZE_SOURCES_CONFIG}")
CONFIG_CANDIDATES+=("/app/config/bronze/sources.yaml" "$REPO_ROOT/config/bronze/sources.yaml")

CONFIG_FILE=""
for candidate in "${CONFIG_CANDIDATES[@]}"; do
    if [[ -f "$candidate" ]]; then
        CONFIG_FILE="$candidate"
        break
    fi
done

# Fallback defaults (kept for compatibility)
FALLBACK_SOURCES=("coinalyze" "binance_exchange" "yahoo_finance")
declare -A FALLBACK_DATA_TYPES
FALLBACK_DATA_TYPES[coinalyze]="ohlc open_interest funding_rate"
FALLBACK_DATA_TYPES[binance_exchange]="trades orderbook ohlc funding_rate"
FALLBACK_DATA_TYPES[yahoo_finance]="ohlcv dividends splits"

declare -A SOURCE_DATA_TYPES
declare -A SEEN_SOURCES
SOURCES=()

find_python() {
    if command -v python3 >/dev/null 2>&1; then
        PYTHON_BIN=python3
    elif command -v python >/dev/null 2>&1; then
        PYTHON_BIN=python
    else
        PYTHON_BIN=""
    fi
}

find_yq() {
    if command -v yq >/dev/null 2>&1; then
        YQ_BIN=yq
    else
        YQ_BIN=""
    fi
}

add_source_datatype() {
    local source="$1" data_type="$2"
    if [[ -z "${SEEN_SOURCES[$source]:-}" ]]; then
        SOURCES+=("$source")
        SEEN_SOURCES[$source]=1
    fi

    if [[ -n "$data_type" ]]; then
        local existing="${SOURCE_DATA_TYPES[$source]:-}"
        for dt in $existing; do
            [[ "$dt" == "$data_type" ]] && return
        done
        SOURCE_DATA_TYPES[$source]="${existing:+$existing }$data_type"
    fi
}

load_config_sources_py() {
    local cfg="$1"
    "$PYTHON_BIN" - "$cfg" <<'PY'
import pathlib
import sys

try:
    import yaml
except ImportError as exc:
    sys.stderr.write("ERROR: PyYAML is required to parse sources config.\n")
    sys.exit(2)

cfg_path = pathlib.Path(sys.argv[1])

try:
    data = yaml.safe_load(cfg_path.read_text()) or {}
except Exception as exc:  # noqa: BLE001
    sys.stderr.write(f"ERROR: Failed to parse YAML: {exc}\n")
    sys.exit(3)

sources = data.get("sources")
errors = []

if not isinstance(sources, list) or len(sources) == 0:
    errors.append("Config 'sources' must be a non-empty list.")
    sources = []

seen = set()
for idx, src in enumerate(sources):
    if not isinstance(src, dict):
        errors.append(f"Item {idx} must be a mapping.")
        continue
    source_id = src.get("id")
    if not source_id or not isinstance(source_id, str):
        errors.append(f"Item {idx} missing string 'id'.")
        continue
    if source_id in seen:
        errors.append(f"Duplicate source id: {source_id}")
        continue
    seen.add(source_id)

    data_types = src.get("data_types")
    if data_types is None:
        data_types = []
    if not isinstance(data_types, (list, tuple)):
        errors.append(f"source {source_id}: 'data_types' must be a list.")
        data_types = []

    for dt in data_types:
        if not dt:
            errors.append(f"source {source_id}: empty data_type entry.")

    # Emit pairs (allow empty list to just create source dir)
    if data_types:
        for dt in data_types:
            if dt:
                print(f"{source_id}|{dt}")
    else:
        print(f"{source_id}|")

if errors:
    for err in errors:
        sys.stderr.write(f"ERROR: {err}\n")
    sys.exit(4)
PY
}

load_config_sources_yq() {
    local cfg="$1"

    # Ensure sources list exists and non-empty
    local count
    count=$("$YQ_BIN" -r '.sources | length // 0' "$cfg" 2>/dev/null || echo 0)
    if [[ "$count" -lt 1 ]]; then
        echo "ERROR: Config 'sources' must be a non-empty list." >&2
        return 4
    fi

    local missing_ids
    missing_ids=$("$YQ_BIN" -r '[.sources[] | select(.id == null or .id == "")] | length' "$cfg" 2>/dev/null || echo 0)
    if [[ "$missing_ids" -gt 0 ]]; then
        echo "ERROR: One or more sources are missing id." >&2
        return 4
    fi

    "$YQ_BIN" -r '.sources[] | .id as $id | ( ( .data_types // [] ) | (if length==0 then [""] else . end))[] | "\($id)|\(.)"' "$cfg"
}

if [[ -n "$CONFIG_FILE" ]]; then
    echo "📂 Loading source configuration from $CONFIG_FILE"
    find_python
    find_yq

    PARSED=0

    if [[ -n "${PYTHON_BIN:-}" ]]; then
        if mapfile -t SOURCE_DT_PAIRS < <(load_config_sources_py "$CONFIG_FILE"); then
            PARSED=1
        else
            echo "ℹ️  Python parser failed; attempting yq fallback..."
        fi
    fi

    if [[ $PARSED -eq 0 && -n "${YQ_BIN:-}" ]]; then
        if mapfile -t SOURCE_DT_PAIRS < <(load_config_sources_yq "$CONFIG_FILE"); then
            PARSED=1
        else
            echo "❌ Failed to parse config with yq; aborting"
            exit 1
        fi
    fi

    if [[ $PARSED -eq 0 ]]; then
        echo "❌ No parser available (python+PyYAML or yq). Install one of them."
        exit 1
    fi

    if ((${#SOURCE_DT_PAIRS[@]} > 0)); then
        for pair in "${SOURCE_DT_PAIRS[@]}"; do
            source="${pair%%|*}"
            data_type="${pair#*|}"
            add_source_datatype "$source" "$data_type"
        done
    else
        echo "❌ Config is empty; aborting"
        exit 1
    fi
fi

if ((${#SOURCES[@]} == 0)); then
    echo "ℹ️  Using fallback source list"
    for src in "${FALLBACK_SOURCES[@]}"; do
        for dt in ${FALLBACK_DATA_TYPES[$src]}; do
            add_source_datatype "$src" "$dt"
        done
        # Ensure source exists even if no data types
        add_source_datatype "$src" ""
    done
fi

BUCKET="${MINIO_BUCKET_NAME:-bronze}"
ENDPOINT="${MINIO_ENDPOINT:-http://minio:9000}"

if [[ "$VALIDATE_ONLY" == "1" ]]; then
    echo "✅ Validation-only mode: configuration parsed successfully."
    echo "Sources: ${SOURCES[*]}"
    for source in "${SOURCES[@]}"; do
        echo "  - ${source}: ${SOURCE_DATA_TYPES[$source]:-}"
    done
    exit 0
fi

# Validate required env
MISSING_ENV=()
[[ -z "${MINIO_ROOT_USER:-}" ]] && MISSING_ENV+=(MINIO_ROOT_USER)
[[ -z "${MINIO_ROOT_PASSWORD:-}" ]] && MISSING_ENV+=(MINIO_ROOT_PASSWORD)
if ((${#MISSING_ENV[@]} > 0)); then
    echo "❌ Missing required env vars: ${MISSING_ENV[*]}"
    exit 1
fi

# Wait for MinIO to be ready
echo "⏳ Waiting for MinIO..."
MAX_RETRIES=30
RETRY_COUNT=0

until curl -s -f "${ENDPOINT}/minio/health/live" > /dev/null 2>&1; do
    RETRY_COUNT=$((RETRY_COUNT + 1))
    if [ $RETRY_COUNT -ge $MAX_RETRIES ]; then
        echo "❌ MinIO not ready after $MAX_RETRIES retries"
        exit 1
    fi
    echo "   Retry $RETRY_COUNT/$MAX_RETRIES..."
    sleep 2
done
echo "✅ MinIO is ready"

# Configure mc client
echo "🔧 Configuring MinIO client..."
mc alias set bronze "$ENDPOINT" "${MINIO_ROOT_USER}" "${MINIO_ROOT_PASSWORD}" > /dev/null

# Create main bronze bucket
echo "📦 Creating bronze bucket..."
if ! mc ls "bronze/${BUCKET}" > /dev/null 2>&1; then
    mc mb "bronze/${BUCKET}" --region="${MINIO_REGION:-us-east-1}"
    echo "✅ Created bucket: ${BUCKET}"
else
    echo "ℹ️ Bucket '${BUCKET}' already exists"
fi

echo "📁 Creating source directory structure..."
for source in "${SOURCES[@]}"; do
    echo "  Creating: source=${source}/"
    echo -n "" | mc pipe "bronze/${BUCKET}/source=${source}/_directory_marker" || true
done

echo "📊 Creating data type directories..."
for source in "${SOURCES[@]}"; do
    for data_type in ${SOURCE_DATA_TYPES[$source]:-}; do
        echo "  Creating: source=${source}/data_type=${data_type}/"
        echo -n "" | mc pipe "bronze/${BUCKET}/source=${source}/data_type=${data_type}/_directory_marker" || true
    done
done

# Set lifecycle policy (keep raw data for 1 year)
echo "📅 Setting lifecycle policy (1 year retention)..."
mc ilm add "bronze/${BUCKET}" --expire-days "365" || echo "⚠️  Could not set lifecycle policy"

# Create a README file in the bucket
echo "📝 Creating bucket documentation..."
{
    echo "# Bronze Layer - Raw Data Storage"
    echo ""
    echo "## Sources and data types"
    for source in "${SOURCES[@]}"; do
        echo "- source=${source}"
        for data_type in ${SOURCE_DATA_TYPES[$source]:-}; do
            echo "  - data_type=${data_type}"
        done
    done
    echo ""
    echo "## File naming"
    echo "{source}-{data_type}-{exchange?}-{YYYYMMDD}-{HHMM}.parquet"
    echo "Example: coinalyze-ohlc-binance-20240115-1400.parquet"
    echo ""
    echo "## Retention"
    echo "- Raw data kept for 365 days"
    echo "- Parquet format with Snappy compression"
    echo "- Partitioned by source and data_type"
    echo ""
    echo "## Processing"
    echo "Files in this bucket are immutable raw data."
    echo "Processed data moves to Silver layer (TimescaleDB)."
} > /tmp/bronze_readme.md

mc cp /tmp/bronze_readme.md "bronze/${BUCKET}/README.md" || echo "⚠️  Could not upload README"

echo "🎉 Bronze layer initialization complete!"
echo ""
echo "Bucket: ${BUCKET}"
echo "Sources: ${SOURCES[*]}"
echo "Console: http://localhost:9001 (user: ${MINIO_ROOT_USER})"
echo "API: http://localhost:9000"