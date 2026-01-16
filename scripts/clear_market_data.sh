#!/bin/bash
# Clear all market data from TimescaleDB
# This will delete all OHLC and Open Interest data, allowing a fresh backfill

echo "⚠️  WARNING: This will DELETE ALL MARKET DATA from the database!"
echo "   - All OHLC data (market.ohlc and timeframe-specific tables)"
echo "   - All Open Interest data (market.open_interest and timeframe-specific tables)"
echo "   - Metadata tables"
echo ""
read -p "Are you sure you want to continue? (yes/no): " confirm

if [ "$confirm" != "yes" ]; then
    echo "❌ Aborted. No data was deleted."
    exit 0
fi

echo ""
echo "🗑️  Deleting all market data..."

# SQL to truncate all market data tables
docker exec quant-timescaledb psql -U postgres -d quant_dev <<EOF
-- Truncate main OHLC table
TRUNCATE TABLE market.ohlc CASCADE;

-- Truncate main Open Interest table
TRUNCATE TABLE market.open_interest CASCADE;

-- Truncate timeframe-specific tables
TRUNCATE TABLE market.ohlc_5m CASCADE;
TRUNCATE TABLE market.ohlc_15m CASCADE;
TRUNCATE TABLE market.ohlc_30m CASCADE;
TRUNCATE TABLE market.ohlc_1h CASCADE;
TRUNCATE TABLE market.ohlc_4h CASCADE;
TRUNCATE TABLE market.ohlc_1d CASCADE;

TRUNCATE TABLE market.open_interest_5m CASCADE;
TRUNCATE TABLE market.open_interest_15m CASCADE;
TRUNCATE TABLE market.open_interest_30m CASCADE;
TRUNCATE TABLE market.open_interest_1h CASCADE;
TRUNCATE TABLE market.open_interest_4h CASCADE;
TRUNCATE TABLE market.open_interest_1d CASCADE;

-- Truncate metadata tables
TRUNCATE TABLE market.metadata CASCADE;
TRUNCATE TABLE market.aggregation_metadata CASCADE;
TRUNCATE TABLE market.aggregation_state CASCADE;

-- Show counts to verify deletion
SELECT 
    'OHLC' as table_name, 
    COUNT(*) as record_count 
FROM market.ohlc
UNION ALL
SELECT 
    'Open Interest' as table_name, 
    COUNT(*) as record_count 
FROM market.open_interest;
EOF

echo ""
echo "✅ All market data has been deleted!"
echo ""
echo "📊 Next steps:"
echo "   1. Run backfill: ./scripts/backfill_inverse_contracts.sh"
echo "   2. Or use the backfill_runner directly with your desired parameters"
echo "   3. Verify data: docker exec quant-mnemo python scripts/verify_backfill_coverage.py"
