BEGIN;
ALTER TABLE staging.ingestion_batch
 ALTER COLUMN started_at SET DEFAULT clock_timestamp(),
 ADD COLUMN error_type TEXT;
CREATE TABLE staging.quality_run (
 quality_run_id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
 batch_id BIGINT NOT NULL REFERENCES staging.ingestion_batch(batch_id),
 evaluated_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
 as_of TIMESTAMPTZ NOT NULL, scope TEXT NOT NULL, parameters JSONB NOT NULL
);
CREATE TABLE staging.quality_result (
 quality_run_id BIGINT NOT NULL REFERENCES staging.quality_run(quality_run_id),
 check_id TEXT NOT NULL, check_version INTEGER NOT NULL CHECK (check_version > 0),
 severity TEXT NOT NULL CHECK (severity IN ('ERROR','WARNING')),
 evaluated_count BIGINT NOT NULL CHECK (evaluated_count >= 0),
 anomaly_count BIGINT NOT NULL CHECK (anomaly_count >= 0 AND anomaly_count <= evaluated_count),
 examples JSONB NOT NULL, PRIMARY KEY (quality_run_id, check_id)
);
CREATE INDEX quality_run_batch_idx ON staging.quality_run(batch_id);
COMMIT;
