-- =============================================================================
-- TremorLens Database Schema (PostgreSQL)
-- =============================================================================

-- Table: usgs_events
-- Holds validated records ingested from USGS earthquake feeds.
CREATE TABLE IF NOT EXISTS usgs_events (
    event_id VARCHAR(255) PRIMARY KEY,
    event_time TIMESTAMPTZ NOT NULL,
    latitude DOUBLE PRECISION NOT NULL,
    longitude DOUBLE PRECISION NOT NULL,
    depth_km DOUBLE PRECISION,
    magnitude DOUBLE PRECISION,
    mag_type VARCHAR(50),
    place TEXT,
    felt INTEGER,
    cdi DOUBLE PRECISION,
    mmi DOUBLE PRECISION,
    alert VARCHAR(50),
    status VARCHAR(50),
    tsunami INTEGER,
    sig INTEGER,
    net VARCHAR(50),
    code VARCHAR(50),
    nst INTEGER,
    dmin DOUBLE PRECISION,
    rms DOUBLE PRECISION,
    gap DOUBLE PRECISION,
    raw_payload JSONB,
    ingested_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- Table: emsc_events
-- Holds validated records ingested from EMSC earthquake feeds.
CREATE TABLE IF NOT EXISTS emsc_events (
    event_id VARCHAR(255) PRIMARY KEY,
    event_time TIMESTAMPTZ NOT NULL,
    latitude DOUBLE PRECISION NOT NULL,
    longitude DOUBLE PRECISION NOT NULL,
    depth_km DOUBLE PRECISION,
    magnitude DOUBLE PRECISION,
    mag_type VARCHAR(50),
    place TEXT,
    felt INTEGER,
    cdi DOUBLE PRECISION,
    mmi DOUBLE PRECISION,
    sig INTEGER,
    auth VARCHAR(50),
    source_catalog VARCHAR(50),
    last_update TIMESTAMPTZ,
    raw_payload JSONB,
    ingested_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- Table: events_clean
-- Unified, deduplicated, and standardized earthquake events.
CREATE TABLE IF NOT EXISTS events_clean (
    event_id VARCHAR(255) PRIMARY KEY,
    source VARCHAR(50) NOT NULL,
    event_time TIMESTAMPTZ NOT NULL,
    latitude DOUBLE PRECISION NOT NULL,
    longitude DOUBLE PRECISION NOT NULL,
    depth_km DOUBLE PRECISION,
    magnitude DOUBLE PRECISION,
    mag_type VARCHAR(50),
    place TEXT,
    felt INTEGER,
    cdi DOUBLE PRECISION,
    mmi DOUBLE PRECISION,
    sig INTEGER,
    matched_usgs_id VARCHAR(255),
    matched_emsc_id VARCHAR(255),
    depth_is_default BOOLEAN,
    ingested_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- Table: rejected_records
-- Records that failed schema validation or quality checks during ingestion.
CREATE TABLE IF NOT EXISTS rejected_records (
    id BIGSERIAL PRIMARY KEY,
    source VARCHAR(50) NOT NULL,
    raw_payload JSONB NOT NULL,
    error_message TEXT NOT NULL,
    rejected_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- Table: ingest_runs
-- Metadata and execution statistics for each ingestion batch or run.
CREATE TABLE IF NOT EXISTS ingest_runs (
    id BIGSERIAL PRIMARY KEY,
    source VARCHAR(50) NOT NULL,
    started_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    finished_at TIMESTAMPTZ,
    rows_fetched INTEGER DEFAULT 0,
    rows_loaded INTEGER DEFAULT 0,
    rows_rejected INTEGER DEFAULT 0,
    status VARCHAR(50) NOT NULL,
    error_message TEXT
);

-- Indexes for events_clean
CREATE INDEX IF NOT EXISTS idx_events_clean_event_time ON events_clean (event_time DESC);
CREATE INDEX IF NOT EXISTS idx_events_clean_magnitude ON events_clean (magnitude);
CREATE INDEX IF NOT EXISTS idx_events_clean_coords ON events_clean (latitude, longitude);
CREATE INDEX IF NOT EXISTS idx_events_clean_source ON events_clean (source);
CREATE INDEX IF NOT EXISTS idx_events_clean_matched_usgs ON events_clean (matched_usgs_id);
CREATE INDEX IF NOT EXISTS idx_events_clean_matched_emsc ON events_clean (matched_emsc_id);

-- Indexes for usgs_events
CREATE INDEX IF NOT EXISTS idx_usgs_events_event_time ON usgs_events (event_time DESC);
CREATE INDEX IF NOT EXISTS idx_usgs_events_magnitude ON usgs_events (magnitude);

-- Indexes for emsc_events
CREATE INDEX IF NOT EXISTS idx_emsc_events_event_time ON emsc_events (event_time DESC);
CREATE INDEX IF NOT EXISTS idx_emsc_events_magnitude ON emsc_events (magnitude);

-- Indexes for rejected_records
CREATE INDEX IF NOT EXISTS idx_rejected_records_source ON rejected_records (source);
CREATE INDEX IF NOT EXISTS idx_rejected_records_rejected_at ON rejected_records (rejected_at DESC);

-- Indexes for ingest_runs
CREATE INDEX IF NOT EXISTS idx_ingest_runs_source ON ingest_runs (source);
CREATE INDEX IF NOT EXISTS idx_ingest_runs_started_at ON ingest_runs (started_at DESC);
