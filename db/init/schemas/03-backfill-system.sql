-- System schema for backfill management
-- Creates tables for tracking initialization state and checkpoints

-- Create system schema if not exists
CREATE SCHEMA IF NOT EXISTS system;

-- ============================================================================
-- Backfill Checkpoints Table
-- Tracks progress of historical backfill operations for resumability
-- ============================================================================

CREATE TABLE IF NOT EXISTS system.backfill_checkpoints (
    -- Primary identifiers
    symbol VARCHAR(20) NOT NULL,
    timeframe VARCHAR(10) NOT NULL,
    data_type VARCHAR(20) NOT NULL,  -- 'ohlc', 'oi'
    source VARCHAR(50) NOT NULL DEFAULT 'coinalyze',
    
    -- Progress tracking
    last_completed_date TIMESTAMP NOT NULL,
    total_chunks_completed INTEGER NOT NULL DEFAULT 0,
    total_records_written BIGINT NOT NULL DEFAULT 0,
    
    -- Timestamps
    started_at TIMESTAMP NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMP NOT NULL DEFAULT NOW(),
    
    -- Status tracking
    status VARCHAR(20) NOT NULL DEFAULT 'in_progress',  -- 'in_progress', 'completed', 'failed'
    error_message TEXT,
    
    -- Constraints
    PRIMARY KEY (symbol, timeframe, data_type, source),
    CHECK (status IN ('in_progress', 'completed', 'failed')),
    CHECK (total_chunks_completed >= 0),
    CHECK (total_records_written >= 0)
);

-- Indexes for efficient querying
CREATE INDEX IF NOT EXISTS idx_backfill_checkpoints_status 
    ON system.backfill_checkpoints(status);

CREATE INDEX IF NOT EXISTS idx_backfill_checkpoints_updated 
    ON system.backfill_checkpoints(updated_at DESC);

-- Add comment
COMMENT ON TABLE system.backfill_checkpoints IS 
    'Tracks progress of historical data backfill operations. Enables resumable backfills if interrupted.';

-- ============================================================================
-- Backfill Initialization State Table
-- Tracks overall initialization completion per symbol/timeframe/data_type
-- ============================================================================

CREATE TABLE IF NOT EXISTS system.backfill_initialization_state (
    -- Primary identifiers
    symbol VARCHAR(20) NOT NULL,
    timeframe VARCHAR(10) NOT NULL,
    data_type VARCHAR(20) NOT NULL,
    
    -- Initialization metadata
    initialization_completed_at TIMESTAMP,
    total_records_initialized BIGINT NOT NULL DEFAULT 0,
    status VARCHAR(20) NOT NULL DEFAULT 'pending',  -- 'pending', 'in_progress', 'completed', 'failed'
    
    -- Audit fields
    created_at TIMESTAMP NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMP NOT NULL DEFAULT NOW(),
    
    -- Optional metadata
    metadata JSONB,
    
    -- Constraints
    PRIMARY KEY (symbol, timeframe, data_type),
    CHECK (status IN ('pending', 'in_progress', 'completed', 'failed')),
    CHECK (total_records_initialized >= 0)
);

-- Indexes
CREATE INDEX IF NOT EXISTS idx_initialization_state_status 
    ON system.backfill_initialization_state(status);

CREATE INDEX IF NOT EXISTS idx_initialization_state_completed 
    ON system.backfill_initialization_state(initialization_completed_at DESC)
    WHERE initialization_completed_at IS NOT NULL;

-- Add comment
COMMENT ON TABLE system.backfill_initialization_state IS 
    'Tracks completion status of initial historical data load. Used to determine if real-time ingestion can begin.';

-- ============================================================================
-- Functions for automatic timestamp updates
-- ============================================================================

-- Function to update updated_at timestamp
CREATE OR REPLACE FUNCTION system.update_updated_at_column()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = NOW();
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

-- Trigger for backfill_checkpoints
DROP TRIGGER IF EXISTS update_backfill_checkpoints_updated_at ON system.backfill_checkpoints;
CREATE TRIGGER update_backfill_checkpoints_updated_at
    BEFORE UPDATE ON system.backfill_checkpoints
    FOR EACH ROW
    EXECUTE FUNCTION system.update_updated_at_column();

-- Trigger for backfill_initialization_state
DROP TRIGGER IF EXISTS update_initialization_state_updated_at ON system.backfill_initialization_state;
CREATE TRIGGER update_initialization_state_updated_at
    BEFORE UPDATE ON system.backfill_initialization_state
    FOR EACH ROW
    EXECUTE FUNCTION system.update_updated_at_column();

-- ============================================================================
-- Helper Views
-- ============================================================================

-- View: Active backfills (in progress)
CREATE OR REPLACE VIEW system.v_active_backfills AS
SELECT 
    symbol,
    timeframe,
    data_type,
    source,
    total_chunks_completed,
    total_records_written,
    started_at,
    updated_at,
    EXTRACT(EPOCH FROM (NOW() - started_at)) / 3600 AS hours_running
FROM system.backfill_checkpoints
WHERE status = 'in_progress'
ORDER BY updated_at DESC;

COMMENT ON VIEW system.v_active_backfills IS 
    'Shows currently running backfill operations with runtime statistics.';

-- View: Initialization progress summary
CREATE OR REPLACE VIEW system.v_initialization_progress AS
SELECT 
    COUNT(*) AS total_combinations,
    SUM(CASE WHEN status = 'completed' THEN 1 ELSE 0 END) AS completed,
    SUM(CASE WHEN status = 'in_progress' THEN 1 ELSE 0 END) AS in_progress,
    SUM(CASE WHEN status = 'failed' THEN 1 ELSE 0 END) AS failed,
    SUM(CASE WHEN status = 'pending' THEN 1 ELSE 0 END) AS pending,
    SUM(total_records_initialized) AS total_records,
    ROUND(
        100.0 * SUM(CASE WHEN status = 'completed' THEN 1 ELSE 0 END) / NULLIF(COUNT(*), 0),
        2
    ) AS completion_percentage
FROM system.backfill_initialization_state;

COMMENT ON VIEW system.v_initialization_progress IS 
    'Summary of overall initialization progress across all symbols/timeframes.';

-- ============================================================================
-- Sample Queries (for documentation)
-- ============================================================================

-- Check if initialization is complete for a symbol/timeframe
-- SELECT * FROM system.backfill_initialization_state 
-- WHERE symbol = 'BTC' AND timeframe = '1h' AND data_type = 'ohlc' AND status = 'completed';

-- Resume backfill from checkpoint
-- SELECT * FROM system.backfill_checkpoints 
-- WHERE symbol = 'BTC' AND timeframe = '1h' AND status = 'in_progress';

-- View all active backfills
-- SELECT * FROM system.v_active_backfills;

-- View initialization progress
-- SELECT * FROM system.v_initialization_progress;

-- Cleanup completed checkpoints (optional)
-- DELETE FROM system.backfill_checkpoints WHERE status = 'completed' AND updated_at < NOW() - INTERVAL '30 days';
