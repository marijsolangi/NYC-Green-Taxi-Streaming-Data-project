-- =============================================================================
-- NYC GREEN TAXI STREAMING - LANDING ZONE SETUP
-- Author: Muhammad Marij
-- Email: mohammedmarij@gmail.com
-- =============================================================================

-- Connect to PostgreSQL using DBeaver:
--   Host: localhost
--   Port: 5432
--   Database: postgres
--   Username: postgres
--   Password: postgres

-- Create the landing zone table for processed events
-- This table serves as the sink for Flink job outputs
DROP TABLE IF EXISTS processed_events CASCADE;

CREATE TABLE processed_events (
    test_data INTEGER,
    event_timestamp TIMESTAMP
);

-- Verify table creation
SELECT * FROM processed_events LIMIT 5;
