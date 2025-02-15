-- schema.sql: distributed-backend-toolkit

-- ── Events table (range-partitioned by month) ──────────────────────────────
CREATE TABLE IF NOT EXISTS events (
    id              BIGSERIAL,
    device_id       VARCHAR(20)     NOT NULL,
    event_type      VARCHAR(20)     NOT NULL DEFAULT 'normal',
    payload         JSONB           NOT NULL DEFAULT '{}',
    event_timestamp TIMESTAMPTZ     NOT NULL DEFAULT NOW(),
    ingested_at     TIMESTAMPTZ     NOT NULL DEFAULT NOW(),
    CONSTRAINT events_pkey PRIMARY KEY (device_id, event_timestamp)
) PARTITION BY RANGE (event_timestamp);

-- Monthly partitions
CREATE TABLE IF NOT EXISTS events_2026_04
    PARTITION OF events FOR VALUES FROM ('2026-04-01') TO ('2026-05-01');
CREATE TABLE IF NOT EXISTS events_2026_05
    PARTITION OF events FOR VALUES FROM ('2026-05-01') TO ('2026-06-01');
CREATE TABLE IF NOT EXISTS events_2026_06
    PARTITION OF events FOR VALUES FROM ('2026-06-01') TO ('2026-07-01');
CREATE TABLE IF NOT EXISTS events_2026_07
    PARTITION OF events FOR VALUES FROM ('2026-07-01') TO ('2026-08-01');

-- Composite B-tree index: covers (device_id, event_timestamp DESC) lookups
-- Used by getRecentEvents: index-only scan, no heap access
CREATE INDEX IF NOT EXISTS idx_events_device_time
    ON events (device_id, event_timestamp DESC);

-- Partial index for critical events: small fraction of rows, fast dashboard queries
CREATE INDEX IF NOT EXISTS idx_events_critical
    ON events (event_timestamp DESC)
    WHERE event_type = 'critical';

-- GIN index on JSONB payload: for ad-hoc JSON field queries
CREATE INDEX IF NOT EXISTS idx_events_payload
    ON events USING GIN (payload);
