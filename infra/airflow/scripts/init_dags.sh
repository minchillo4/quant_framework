#!/usr/bin/env bash
set -euo pipefail

echo "🔧 Initializing DAG generation..."

# Ensure generated directory exists with correct permissions
GENERATED_DIR="/opt/airflow/dags/generated"

echo "📁 Ensuring generated directory structure..."
mkdir -p "$GENERATED_DIR/ccxt/ohlc"
mkdir -p "$GENERATED_DIR/ccxt/open_interest"

# Generate DAGs
echo "🏗️  Generating DAGs from configuration..."
cd /opt/airflow
python dags/loader.py

# Verify generation
DAG_COUNT=$(find "$GENERATED_DIR" -name "*.py" -type f | wc -l)
echo "✅ Generated $DAG_COUNT DAG files"

if [ "$DAG_COUNT" -eq 0 ]; then
    echo "⚠️  Warning: No DAGs were generated!"
    # Don't exit 1 here - let Airflow start anyway
fi

# List generated DAGs for verification
echo "📋 Generated DAGs:"
find "$GENERATED_DIR" -name "*.py" -type f | sort

echo "🎉 DAG initialization complete"