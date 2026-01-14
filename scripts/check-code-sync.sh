#!/bin/bash
# scripts/check-code-sync.sh - Verify code synchronization in containers

set -e

echo "🔍 Checking Code Synchronization..."
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"

SCHEDULER_CONTAINER="quant-airflow-scheduler"
MNEMO_CONTAINER="quant-mnemo"

# Check if containers are running
if ! docker ps --format '{{.Names}}' | grep -q "$SCHEDULER_CONTAINER"; then
    echo "❌ Error: $SCHEDULER_CONTAINER is not running"
    echo "   Start the development environment first: ./scripts/dev-up.sh"
    exit 1
fi

echo ""
echo "1️⃣  Checking Airflow Scheduler source code mount..."
echo "   Expected: /opt/airflow/src should be mounted"
docker exec "$SCHEDULER_CONTAINER" ls -la /opt/airflow/src/ 2>/dev/null | head -5 || echo "   ⚠️  Directory not found"

echo ""
echo "2️⃣  Checking DAGs directory..."
docker exec "$SCHEDULER_CONTAINER" ls -la /opt/airflow/dags/ 2>/dev/null | head -5 || echo "   ⚠️  Directory not found"

echo ""
echo "3️⃣  Checking Python paths..."
docker exec "$SCHEDULER_CONTAINER" python -c "import sys; print('Python paths:'); [print(f'  - {p}') for p in sys.path[:7]]" 2>/dev/null

echo ""
echo "4️⃣  Testing import from quant_framework..."
docker exec "$SCHEDULER_CONTAINER" python -c "
try:
    from quant_framework.ingestion.adapters.ccxt_plugin.base import CCXTBaseAdapter
    print('✅ Successfully imported CCXTBaseAdapter')
except ImportError as e:
    print(f'❌ Import failed: {e}')
" 2>/dev/null

echo ""
echo "5️⃣  Checking mnemo_quant container..."
if docker ps --format '{{.Names}}' | grep -q "$MNEMO_CONTAINER"; then
    docker exec "$MNEMO_CONTAINER" ls -la /app/src/ 2>/dev/null | head -5 || echo "   ⚠️  Directory not found"
else
    echo "   ⚠️  $MNEMO_CONTAINER is not running"
fi

echo ""
echo "6️⃣  Testing file change detection..."
TEST_FILE="./infra/airflow/dags/_test_sync_$(date +%s).py"
echo "# Test file created at $(date)" > "$TEST_FILE"
sleep 2
if docker exec "$SCHEDULER_CONTAINER" cat "/opt/airflow/dags/_test_sync_"* 2>/dev/null | grep -q "Test file"; then
    echo "✅ File changes are syncing correctly!"
else
    echo "❌ File changes are NOT syncing"
fi
rm -f "$TEST_FILE"

echo ""
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo "✅ Code synchronization check complete!"
echo ""
