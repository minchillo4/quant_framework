#!/bin/bash
# Backfill inverse contracts with correct settlement currency (BTC/ETH instead of NATIVE)
# This will populate missing inverse contract data for all supported exchanges

echo "🚀 Starting inverse contract backfill..."
echo "This will fetch OI data for inverse perpetuals (coin-margined) with proper settlement currency"
echo ""

# Run backfill for last 2 years
START_DATE=$(date -d '2 years ago' +%Y-%m-%d)
END_DATE=$(date +%Y-%m-%d)

echo "Date range: $START_DATE to $END_DATE"
echo "Symbols: BTC, ETH"
echo "Timeframes: 1h, 1d"
echo "Data types: OI (open interest only for inverse contracts)"
echo ""

docker exec quant-mnemo python -m mnemo_quant.pipelines.orchestration.scripts.backfill_runner \
  --symbols BTC ETH \
  --timeframes 1h 1d \
  --data-types oi \
  --start-date "$START_DATE" \
  --end-date "$END_DATE" \
  --verbose

echo ""
echo "✅ Inverse contract backfill complete!"
echo "Run verification: docker exec quant-mnemo python scripts/verify_backfill_coverage.py"
