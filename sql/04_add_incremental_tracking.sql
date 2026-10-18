-- D1 only: additive schema contract, no merge implementation.
-- Apply once after 01, 02 and 03, with ingestion stopped.
BEGIN;
SET LOCAL lock_timeout = '10s';

-- Stabilize the duplicate precheck against concurrent batch writes.
LOCK TABLE staging.ingestion_batch IN SHARE ROW EXCLUSIVE MODE;
DO $$
BEGIN
    IF EXISTS (
        SELECT 1 FROM staging.ingestion_batch
        WHERE status = 'SUCCESS'
        GROUP BY file_sha256 HAVING count(*) > 1
    ) THEN
        RAISE EXCEPTION 'Migration 04 refused: duplicate SUCCESS SHA-256 values; review manually';
    END IF;
END;
$$;

ALTER TABLE staging.ingestion_batch
    ADD COLUMN rows_received BIGINT,
    ADD COLUMN rows_inserted BIGINT,
    ADD COLUMN rows_unchanged BIGINT,
    ADD COLUMN rows_updated BIGINT,
    ADD COLUMN extracted_at TIMESTAMPTZ,
    ADD COLUMN source_reference TEXT,
    ADD COLUMN input_scope TEXT,
    ADD COLUMN correction_policy TEXT,
    ADD COLUMN correction_authorized_sha256 CHAR(64),
    ADD COLUMN correction_reason TEXT,
    ADD CONSTRAINT chk_incremental_counts_complete CHECK (
        num_nonnulls(rows_received, rows_inserted, rows_unchanged, rows_updated) IN (0, 4)
    ),
    ADD CONSTRAINT chk_incremental_counts_nonnegative CHECK (
        rows_received >= 0 AND rows_inserted >= 0
        AND rows_unchanged >= 0 AND rows_updated >= 0
    ),
    ADD CONSTRAINT chk_incremental_success_counts CHECK (
        status <> 'SUCCESS' OR rows_received IS NULL OR (
            rows_received = rows_inserted + rows_unchanged + rows_updated
            AND rows_loaded::BIGINT = rows_received
        )
    ),
    ADD CONSTRAINT chk_incremental_uncommitted_counts CHECK (
        status = 'SUCCESS' OR rows_received IS NULL OR (
            rows_inserted = 0 AND rows_updated = 0 AND rows_unchanged = 0
        )
    ),
    ADD CONSTRAINT chk_incremental_input_scope CHECK (
        input_scope IN ('continuous', 'partial')
    ),
    ADD CONSTRAINT chk_incremental_correction_policy CHECK (
        correction_policy IN ('deny', 'authorized')
    ),
    ADD CONSTRAINT chk_incremental_correction_authorization CHECK (
        (correction_policy IS DISTINCT FROM 'authorized'
            AND correction_authorized_sha256 IS NULL AND correction_reason IS NULL)
        OR
        (correction_policy IS NOT DISTINCT FROM 'authorized'
            AND correction_authorized_sha256 IS NOT NULL
            AND correction_authorized_sha256::TEXT ~ '^[0-9a-f]{64}$'
            AND correction_authorized_sha256 = file_sha256
            AND correction_reason IS NOT NULL AND btrim(correction_reason) <> '')
    );

CREATE UNIQUE INDEX uq_ingestion_batch_success_sha256
    ON staging.ingestion_batch(file_sha256) WHERE status = 'SUCCESS';

CREATE TABLE staging.elia_load_correction (
    correction_id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    timestamp_utc TIMESTAMPTZ NOT NULL,
    resolution_code VARCHAR(10) NOT NULL CHECK (resolution_code = 'PT15M'),
    previous_batch_id BIGINT NOT NULL REFERENCES staging.ingestion_batch(batch_id),
    correction_batch_id BIGINT NOT NULL REFERENCES staging.ingestion_batch(batch_id),
    corrected_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    old_total_load_mw DOUBLE PRECISION,
    old_most_recent_forecast_mw DOUBLE PRECISION NOT NULL,
    old_most_recent_p10_mw DOUBLE PRECISION NOT NULL,
    old_most_recent_p90_mw DOUBLE PRECISION NOT NULL,
    old_day_ahead_forecast_mw DOUBLE PRECISION NOT NULL,
    old_day_ahead_p10_mw DOUBLE PRECISION NOT NULL,
    old_day_ahead_p90_mw DOUBLE PRECISION NOT NULL,
    old_week_ahead_forecast_mw DOUBLE PRECISION,
    new_total_load_mw DOUBLE PRECISION,
    new_most_recent_forecast_mw DOUBLE PRECISION NOT NULL,
    new_most_recent_p10_mw DOUBLE PRECISION NOT NULL,
    new_most_recent_p90_mw DOUBLE PRECISION NOT NULL,
    new_day_ahead_forecast_mw DOUBLE PRECISION NOT NULL,
    new_day_ahead_p10_mw DOUBLE PRECISION NOT NULL,
    new_day_ahead_p90_mw DOUBLE PRECISION NOT NULL,
    new_week_ahead_forecast_mw DOUBLE PRECISION,
    CONSTRAINT fk_correction_observation FOREIGN KEY (timestamp_utc, resolution_code)
        REFERENCES staging.elia_load(timestamp_utc, resolution_code),
    CONSTRAINT uq_correction_batch_key UNIQUE (
        correction_batch_id, timestamp_utc, resolution_code
    ),
    CONSTRAINT chk_correction_distinct_batches CHECK (previous_batch_id <> correction_batch_id),
    CONSTRAINT chk_correction_changed_values CHECK (
        ROW(old_total_load_mw, old_most_recent_forecast_mw, old_most_recent_p10_mw, old_most_recent_p90_mw, old_day_ahead_forecast_mw, old_day_ahead_p10_mw, old_day_ahead_p90_mw, old_week_ahead_forecast_mw)
        IS DISTINCT FROM ROW(new_total_load_mw, new_most_recent_forecast_mw, new_most_recent_p10_mw, new_most_recent_p90_mw, new_day_ahead_forecast_mw, new_day_ahead_p10_mw, new_day_ahead_p90_mw, new_week_ahead_forecast_mw)
    )
);
CREATE INDEX idx_correction_observation
    ON staging.elia_load_correction(timestamp_utc, resolution_code, correction_id);
CREATE INDEX idx_correction_previous_batch
    ON staging.elia_load_correction(previous_batch_id);

COMMENT ON COLUMN staging.elia_load.ingestion_batch_id IS
    'Batch that produced the current business values; unchanged observations retain this provenance.';
COMMENT ON COLUMN staging.ingestion_batch.rows_loaded IS
    'Legacy count. For successful incremental batches: accepted input rows, including unchanged rows.';
COMMENT ON COLUMN staging.ingestion_batch.rows_received IS
    'Validated input observations; NULL means not recorded. May be known on failed batches.';
COMMENT ON COLUMN staging.ingestion_batch.rows_inserted IS
    'New observations committed by this batch; zero before success, NULL if not recorded.';
COMMENT ON COLUMN staging.ingestion_batch.rows_unchanged IS
    'Identical observations accepted by a successful batch, without changing their provenance.';
COMMENT ON COLUMN staging.ingestion_batch.rows_updated IS
    'Corrections committed by this batch; zero before success, NULL if not recorded.';
COMMENT ON COLUMN staging.ingestion_batch.extracted_at IS
    'Declared extraction time, not ingestion time or proof of source publication freshness.';
COMMENT ON COLUMN staging.ingestion_batch.correction_authorized_sha256 IS
    'Exact input SHA-256 approved for corrections; does not itself prove authority or freshness.';
COMMENT ON TABLE staging.elia_load_correction IS
    'Append-only by application contract: old/new typed values for committed corrections. No backfill.';
COMMIT;
