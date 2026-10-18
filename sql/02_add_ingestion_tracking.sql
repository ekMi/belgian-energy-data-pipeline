
-- Track CSV ingestion executions
CREATE TABLE staging.ingestion_batch (
    batch_id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    source_file TEXT NOT NULL,
    file_sha256 CHAR(64) NOT NULL,
    started_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    finished_at TIMESTAMPTZ,
    status VARCHAR(20) NOT NULL DEFAULT 'RUNNING',
    rows_loaded INTEGER NOT NULL DEFAULT 0,

    CONSTRAINT chk_batch_status
        CHECK (status IN ('RUNNING', 'SUCCESS', 'FAILED')),

    CONSTRAINT chk_rows_loaded
        CHECK (rows_loaded >= 0)
);

-- Associate observations with their ingestion batch
ALTER TABLE staging.elia_load
    ADD COLUMN ingestion_batch_id BIGINT NOT NULL
    REFERENCES staging.ingestion_batch(batch_id);
