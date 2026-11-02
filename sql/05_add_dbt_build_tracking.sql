-- Apply once after migrations 01-04, with pipeline writes stopped.
-- Manual migration only; never run automatically against belgian_energy.
BEGIN;
SET LOCAL lock_timeout = '10s';

CREATE TABLE staging.dbt_build_run (
    build_id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    target_batch_id BIGINT NOT NULL REFERENCES staging.ingestion_batch(batch_id),
    started_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    finished_at TIMESTAMPTZ,
    status VARCHAR(20) NOT NULL DEFAULT 'RUNNING',
    error_type TEXT,
    CONSTRAINT chk_dbt_build_state CHECK (
        (status = 'RUNNING' AND finished_at IS NULL AND error_type IS NULL)
        OR (status = 'SUCCESS' AND finished_at IS NOT NULL AND error_type IS NULL)
        OR (status = 'FAILED' AND finished_at IS NOT NULL
            AND error_type IS NOT NULL AND btrim(error_type) <> '')
    ),
    CONSTRAINT chk_dbt_build_time CHECK (
        finished_at IS NULL OR finished_at >= started_at
    )
);

-- A partial index serves the highest successful watermark lookup; the batch
-- index serves filtered attempt history for a covered ingestion batch.
CREATE INDEX idx_dbt_build_success_latest
    ON staging.dbt_build_run (target_batch_id DESC, build_id DESC)
    WHERE status = 'SUCCESS';
CREATE INDEX idx_dbt_build_target_history
    ON staging.dbt_build_run (target_batch_id, build_id DESC);

COMMENT ON TABLE staging.dbt_build_run IS
    'Tracked dbt attempts; only SUCCESS proves an analytical rebuild.';
COMMENT ON COLUMN staging.dbt_build_run.target_batch_id IS
    'Last successful ingestion batch used as the dbt rebuild reference; concurrent ingestion is not fenced yet.';
COMMIT;
