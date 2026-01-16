#!/usr/bin/env bash
set -euo pipefail

DB_USER="${POSTGRES_USER:-postgres}"
DB_PASSWORD="${POSTGRES_PASSWORD}"

if [ -z "$DB_PASSWORD" ]; then
  echo "❌ POSTGRES_PASSWORD is not set"
  exit 1
fi

if [ -z "${POSTGRES_MULTIPLE_DATABASES:-}" ]; then
  echo "❌ POSTGRES_MULTIPLE_DATABASES is not set (e.g. 'quant_dev,quant_prod')"
  exit 1
fi

if [ -z "${QUANT_APP_PASSWORD:-}" ]; then
  echo "❌ QUANT_APP_PASSWORD is not set (used for quant_app role)"
  exit 1
fi

# All timeframe tables you want to generate
TIMEFRAMES=(
  "5m"
  "15m"
  "30m"
  "1h"
  "4h"
  "1d"
)

for db in $(echo $POSTGRES_MULTIPLE_DATABASES | tr ',' ' '); do
  echo "🔧 Setting up database: $db"

  # Create DB if missing
  DB_EXISTS=$(PGPASSWORD="$DB_PASSWORD" psql -U "$DB_USER" -d postgres -tAc "SELECT 1 FROM pg_database WHERE datname='$db'")
  if [ "$DB_EXISTS" != "1" ]; then
    PGPASSWORD="$DB_PASSWORD" psql -U "$DB_USER" -d postgres -v ON_ERROR_STOP=1 \
      -c "CREATE DATABASE \"$db\";"
    echo "✅ Created database $db"
  else
    echo "ℹ️ Database $db already exists"
  fi

  # Apply schema
  PGPASSWORD="$DB_PASSWORD" psql -U "$DB_USER" -d "$db" -v ON_ERROR_STOP=1 <<EOF
CREATE EXTENSION IF NOT EXISTS timescaledb CASCADE;

-- Quant role
DO \$\$
BEGIN
   IF NOT EXISTS (SELECT FROM pg_roles WHERE rolname = 'quant_app') THEN
      CREATE ROLE quant_app WITH LOGIN PASSWORD '${QUANT_APP_PASSWORD}';
   ELSE
      ALTER ROLE quant_app WITH PASSWORD '${QUANT_APP_PASSWORD}';
   END IF;
END
\$\$;

GRANT CONNECT ON DATABASE "$db" TO quant_app;
CREATE SCHEMA IF NOT EXISTS market AUTHORIZATION quant_app;

-- Base OHLC table
CREATE TABLE IF NOT EXISTS market.ohlc (
    canonical_symbol TEXT NOT NULL,
    exchange TEXT NOT NULL,
    asset_class TEXT NOT NULL DEFAULT 'crypto',
    market_type TEXT NOT NULL,
    data_type TEXT NOT NULL DEFAULT 'ohlc',
    timeframe TEXT NOT NULL,
    ts TIMESTAMPTZ NOT NULL,
    open NUMERIC NOT NULL,
    high NUMERIC NOT NULL,
    low NUMERIC NOT NULL,
    close NUMERIC NOT NULL,
    volume NUMERIC,
    quote_volume NUMERIC,
    trades INTEGER,
    ingest_ts TIMESTAMPTZ DEFAULT NOW(),
    data_source TEXT NOT NULL,
    producer_version TEXT DEFAULT '1.0',
    PRIMARY KEY (canonical_symbol, exchange, timeframe, ts)
);
SELECT create_hypertable('market.ohlc', 'ts', if_not_exists => TRUE);

-- Base Open Interest table
CREATE TABLE IF NOT EXISTS market.open_interest (
    canonical_symbol TEXT NOT NULL,
    exchange TEXT NOT NULL,
    asset_class TEXT NOT NULL DEFAULT 'crypto',
    market_type TEXT NOT NULL,
    data_type TEXT NOT NULL DEFAULT 'open_interest',
    timeframe TEXT NOT NULL,
    ts TIMESTAMPTZ NOT NULL,
    open_interest NUMERIC NOT NULL,
    open_interest_value NUMERIC,
    settlement_currency TEXT,  -- NEW: Track USDT vs USDC vs BUSD etc.
    ingest_ts TIMESTAMPTZ DEFAULT NOW(),
    data_source TEXT NOT NULL,
    producer_version TEXT DEFAULT '1.0',
    PRIMARY KEY (canonical_symbol, exchange, settlement_currency, timeframe, ts)
);
CREATE INDEX IF NOT EXISTS idx_oi_settlement ON market.open_interest(canonical_symbol, exchange, settlement_currency, timeframe);
SELECT create_hypertable('market.open_interest', 'ts', if_not_exists => TRUE, chunk_time_interval => INTERVAL '7 days');

-- Metadata table
CREATE TABLE IF NOT EXISTS market.metadata (
    data_type TEXT NOT NULL,
    symbol TEXT NOT NULL,
    interval_type TEXT NOT NULL,
    exchange TEXT NOT NULL,
    market_type TEXT NOT NULL,
    settlement_currency TEXT DEFAULT 'USDT',
    earliest_ts TIMESTAMPTZ,
    latest_ts TIMESTAMPTZ,
    last_incremental_at TIMESTAMPTZ DEFAULT NOW(),
    last_backfill_at TIMESTAMPTZ,
    updated_at TIMESTAMPTZ DEFAULT NOW(),
    backfill_status TEXT,
    backfill_progress JSONB,
    records_count INTEGER,
    PRIMARY KEY (data_type, symbol, interval_type, exchange, market_type)
);

-- ============================================================
-- Timeframe-specific cloned tables (FIXED)
-- ============================================================

EOF

  # generate tables in the loop
  for tf in "${TIMEFRAMES[@]}"; do
    psql_cmd="
      CREATE TABLE IF NOT EXISTS market.ohlc_${tf} (LIKE market.ohlc INCLUDING ALL);
      CREATE TABLE IF NOT EXISTS market.open_interest_${tf} (LIKE market.open_interest INCLUDING ALL);
      SELECT create_hypertable('market.ohlc_${tf}', 'ts', if_not_exists => TRUE);
      SELECT create_hypertable('market.open_interest_${tf}', 'ts', if_not_exists => TRUE);
    "

    PGPASSWORD="$DB_PASSWORD" psql -U "$DB_USER" -d "$db" -v ON_ERROR_STOP=1 -c "$psql_cmd"
  done

  # Remaining schema
  PGPASSWORD="$DB_PASSWORD" psql -U "$DB_USER" -d "$db" -v ON_ERROR_STOP=1 <<EOF

-- Aggregation metadata
CREATE TABLE IF NOT EXISTS market.aggregation_metadata (
    id SERIAL PRIMARY KEY,
    data_type TEXT NOT NULL,
    symbol TEXT NOT NULL,
    interval_type TEXT NOT NULL,
    exchange TEXT NOT NULL,
    market_type TEXT,
    settlement_currency TEXT DEFAULT 'USDT',
    earliest_ts TIMESTAMPTZ,
    latest_ts TIMESTAMPTZ,
    last_incremental_at TIMESTAMPTZ,
    last_backfill_at TIMESTAMPTZ,
    backfill_status TEXT,
    backfill_progress JSONB,
    records_count BIGINT,
    created_at TIMESTAMPTZ DEFAULT NOW(),
    updated_at TIMESTAMPTZ DEFAULT NOW(),
    UNIQUE(data_type, symbol, interval_type, exchange, market_type)
);

CREATE INDEX IF NOT EXISTS idx_aggregation_metadata_lookup 
ON market.aggregation_metadata(data_type, symbol, interval_type, exchange);

-- Aggregation progress tracking
CREATE TABLE IF NOT EXISTS market.aggregation_state (
    id SERIAL PRIMARY KEY,
    canonical_symbol VARCHAR(20) NOT NULL,
    exchange VARCHAR(20) NOT NULL,
    source_timeframe VARCHAR(10) NOT NULL,     -- '5m'
    target_timeframe VARCHAR(10) NOT NULL,     -- '1h', '4h', '1d'
    data_type VARCHAR(20) NOT NULL,            -- 'ohlc' or 'oi'
    
    last_window_start TIMESTAMPTZ NOT NULL,    -- Last aggregated window start
    last_window_end TIMESTAMPTZ NOT NULL,      -- Last aggregated window end
    candles_in_buffer INT DEFAULT 0,           -- Current buffer size
    total_windows_aggregated BIGINT DEFAULT 0,
    
    created_at TIMESTAMPTZ DEFAULT NOW(),
    updated_at TIMESTAMPTZ DEFAULT NOW(),
    
    UNIQUE(canonical_symbol, exchange, source_timeframe, target_timeframe, data_type)
);
CREATE INDEX idx_agg_state_symbol_exchange 
    ON market.aggregation_state(canonical_symbol, exchange);

-- Permissions
GRANT USAGE ON SCHEMA market TO quant_app;
GRANT SELECT, INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA market TO quant_app;
GRANT USAGE ON ALL SEQUENCES IN SCHEMA market TO quant_app;
ALTER DEFAULT PRIVILEGES IN SCHEMA market GRANT SELECT, INSERT, UPDATE, DELETE ON TABLES TO quant_app;
ALTER DEFAULT PRIVILEGES IN SCHEMA market GRANT USAGE ON SEQUENCES TO quant_app;

EOF

  echo "✅ Updated database schema for $db"
done

echo "🎉 Database schema migration complete"
