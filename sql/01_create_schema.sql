
-- Belgian Energy Data Pipeline
-- Initial PostgreSQL schema
-- Source: Elia ODS001

CREATE SCHEMA IF NOT EXISTS staging;
CREATE SCHEMA IF NOT EXISTS analytics;

CREATE TABLE IF NOT EXISTS staging.elia_load (
    timestamp_utc TIMESTAMPTZ NOT NULL,
    resolution_code VARCHAR(10) NOT NULL,

    total_load_mw DOUBLE PRECISION,

    most_recent_forecast_mw DOUBLE PRECISION NOT NULL,
    most_recent_p10_mw DOUBLE PRECISION NOT NULL,
    most_recent_p90_mw DOUBLE PRECISION NOT NULL,

    day_ahead_forecast_mw DOUBLE PRECISION NOT NULL,
    day_ahead_p10_mw DOUBLE PRECISION NOT NULL,
    day_ahead_p90_mw DOUBLE PRECISION NOT NULL,

    week_ahead_forecast_mw DOUBLE PRECISION,

    CONSTRAINT pk_elia_load
        PRIMARY KEY (timestamp_utc, resolution_code),

    CONSTRAINT chk_resolution
        CHECK (resolution_code = 'PT15M')
);
