# Incremental ingestion contract — D1

## Scope and migration order

D1 introduces schema and contracts only. The current step-C ingestion code is
unchanged and does not populate the new metadata or write correction history.
Temporary COPY, classification, authorization enforcement, merging and ingestion
locks belong to D2-D4. No migration is applied automatically to development.

For a new empty database: apply 01_create_schema.sql, then
02_add_ingestion_tracking.sql, then 03_add_quality_tracking.sql, then
04_add_incremental_tracking.sql. For an existing step-C database, apply only 04.
Never replay 01-03 against the populated database. In particular, 02 originally
requires an empty observation table to add its mandatory batch reference.

Migration 04 is additive and transactional. No existing row is updated or
deleted; no historical extraction date, counter or correction is invented.
New fields have no defaults and remain NULL for old records. Quality tables,
reports and their associations are not changed. rows_loaded keeps its INTEGER
type and existing values. Future accepted batch size therefore remains limited
by that legacy column despite the new BIGINT counters.

## Batch counters

All four new counters are either NULL together (not recorded) or populated
together. Known counts must be nonnegative.

| Column | Contract |
| --- | --- |
| rows_received | Validated input observations, before classification. Not raw physical CSV lines. On parse failure it may remain unknown. |
| rows_inserted | New observations committed by this batch. |
| rows_unchanged | Identical observations accepted by a successful batch; their data and provenance remain unchanged. |
| rows_updated | Existing observations whose business values were changed and committed. |
| rows_loaded | Legacy accepted-input count on SUCCESS, including unchanged observations. Zero for failed attempts produced by the application. |

For SUCCESS with recorded counters:
rows_received = rows_inserted + rows_unchanged + rows_updated, and
rows_loaded = rows_received.

For RUNNING or FAILED with recorded counters, rows_inserted, rows_unchanged and
rows_updated are zero: they describe committed acceptance, not candidate
classification. rows_received may be known. Candidate correction counts belong
in diagnostic reports. Before processing is complete, all counters may be NULL.
Existing historical rows are exempt from new equations through NULL counters;
no backfill is performed. The old rows_loaded/status constraints remain intact.

D2-D4 must populate the counters for new incremental attempts. D1 intentionally
does not require them on every new row, preserving compatibility with step C.

## Provenance and extraction metadata

staging.elia_load.ingestion_batch_id identifies the batch that produced the
current business values. New observations receive the inserting batch; a real
correction changes the identifier; unchanged values retain it. D1 changes only
the column comment, not any observation or provenance value.

extracted_at is a declared timezone-aware extraction time, not started_at, file
modification time or proof of upstream publication freshness. Unknown stays NULL.
source_reference holds an export reference or provenance description, never a
password or credential-bearing URL. input_scope is continuous or partial; NULL
means it was not recorded. A continuous scope is a declared expectation that
must later be checked, not a database guarantee.

Existing quality reports remain immutable snapshots. After corrections,
filtering current observations by an old batch ID cannot reconstruct its entire
original file. Keep raw exports immutable and identified by SHA-256. Full
batch-to-observation membership or upstream publication versioning is deferred.

## Correction policy

correction_policy is deny or authorized. NULL means unspecified/legacy and must
be treated as deny by future application code.

authorized requires:
- correction_authorized_sha256, exactly matching file_sha256 and formatted as
  64 lowercase hexadecimal characters;
- a nonempty correction_reason.

Otherwise authorization hash and reason must both be NULL. The database binds
the declaration to exact bytes; it cannot prove a human approved it or that
the source is trustworthy. These checks belong to D4. No approval mechanism is
implemented by D1.

Future default: reject the entire input batch if any correction is unauthorized.
A newer extraction date alone does not authorize a correction. Unknown freshness
requires explicit review; demonstrably older extractions must not overwrite newer
values. An authorized value-to-NULL transition needs explicit review of that
data loss; no automatic coalescing or silent deletion is permitted.
Missing observations in an extraction never imply deletion.
The precise review interface and freshness comparison will be implemented in D4.

The partial unique index allows multiple FAILED/RUNNING attempts for one hash,
but at most one SUCCESS. An identical successful file remains a no-op, including
when replayed after a later correction. Failed attempts may be retried.
The index is not an ingestion concurrency lock; that comes in D3.

## Correction history

staging.elia_load_correction contains:
- correction_id: identity key;
- timestamp_utc and resolution_code: existing observation key;
- previous_batch_id: producer of the replaced values;
- correction_batch_id: batch responsible for the correction;
- corrected_at: database wall-clock recording time, not source publication time;
- old_* and new_* DOUBLE PRECISION fields for each of the eight numeric measures.

Only Total Load and Week-ahead forecast are nullable in either version, matching
the current observation schema. NULL-to-value and value-to-NULL changes are
representable. ROW(old values) IS DISTINCT FROM ROW(new values) rejects no-op
history rows while handling NULL comparisons correctly.

Foreign keys reference both batches and the current observation key; no cascade
deletion is configured. Previous and correction batches must differ. Uniqueness
on (correction_batch_id, timestamp_utc, resolution_code) prevents two corrections
of the same key in one batch. Indexes support observation history and previous
batch lookup; the unique index also supports correction batch lookup.

D4 must write history and current data in one transaction and validate that the
old values/previous batch match the current row. It must also enforce authorization
and commit the responsible batch as SUCCESS. Ordinary CHECK constraints do not
enforce these cross-table lifecycle conditions. D1 does not introduce triggers.
History is append-only by application contract; no restrictive role/permission
changes or database immutability trigger are introduced in D1.

## Manual preflight

Stop ingestion before migration and take the usual database backup. Select the
intended server and database explicitly. Review migration 04 and its permissions:
the applying role must own the affected table and be able to create tables in
staging. Do not grant broader runtime permissions merely to apply the migration.

In a psql session connected to the intended database, verify identity and schema:

    SELECT current_database(), current_user, version();
    SELECT to_regclass('staging.quality_result'),
           to_regclass('staging.elia_load_correction');
    SELECT column_name FROM information_schema.columns
    WHERE table_schema='staging' AND table_name='ingestion_batch'
      AND column_name IN ('error_type','rows_received');

Before first application, quality_result/error_type must exist and
elia_load_correction/rows_received must not exist. Check duplicates:

    SELECT file_sha256, count(*)
    FROM staging.ingestion_batch
    WHERE status='SUCCESS'
    GROUP BY file_sha256 HAVING count(*) > 1;

If any rows appear, stop and review them manually. Do not delete or relabel
historical batches to make migration pass. The migration repeats this check
inside its transaction after acquiring a batch-write lock, so the external
preflight is not trusted as a race-free guarantee.

Record counts of observations, batches, quality runs/results. For exact
preservation evidence, compare read-only ordered exports before and after,
selecting the original batch columns explicitly (new columns change SELECT *).
Integration tests perform field-level comparisons, not only count comparisons.

## Manual application (only after approval)

From the repository root, with psql on PATH, first validate on the disposable
test database. For the principal database, the following is an instruction for
an explicitly approved manual action, not an action performed by this task:

    psql -X -h localhost -p 5432 -U energy_app -d belgian_energy -v ON_ERROR_STOP=1 -f sql/04_add_incremental_tracking.sql

Use the actual schema owner when different from energy_app. The migration owns
BEGIN/COMMIT; do not wrap it in psql --single-transaction. It waits at most ten
seconds for each lock acquisition. Index creation is non-concurrent to keep
migration atomic. This is acceptable for the small batch tracking table; run
during an ingestion maintenance window.

On any error, the connection closes with the transaction rolled back. No partial
schema or cleanup is intended. A second application fails rather than silently
ignoring schema drift. There is no destructive automatic downgrade.

After successful application, verify counts/exports, inspect constraints and
indexes with psql, and check historical metadata:

    SELECT count(*) FROM staging.elia_load_correction;
    SELECT count(*) FROM staging.ingestion_batch
    WHERE rows_received IS NOT NULL OR rows_inserted IS NOT NULL
       OR rows_unchanged IS NOT NULL OR rows_updated IS NOT NULL
       OR extracted_at IS NOT NULL OR source_reference IS NOT NULL
       OR input_scope IS NOT NULL OR correction_policy IS NOT NULL
       OR correction_authorized_sha256 IS NOT NULL OR correction_reason IS NOT NULL;

Both counts are zero immediately after D1, before any future incremental writer.
Existing reports and observations must match the preflight exports.

## Tests

All writes use the existing guarded belgian_energy_test fixture. Existing 136
tests and fixture initialization remain unchanged (schema 03). Dedicated tests
populate schema 03 through real ingestion, then apply the exact migration 04.

    .\.venv\Scripts\python.exe -B -m pytest tests/integration/test_incremental_migration.py -p no:cacheprovider -ra
    .\.venv\Scripts\python.exe -B -m pytest -p no:cacheprovider -ra
    .\.venv\Scripts\ruff.exe check --no-cache .
    .\.venv\Scripts\ruff.exe format --check --no-cache .

See integration_tests.md for TEST_PG configuration. Missing configuration yields
explicit skips; these do not validate PostgreSQL behavior. Tests cover populated
data/report preservation, NULL historical metadata, continued step-C ingestion,
duplicate-hash rejection and rollback, retry hashes, counters, authorization,
typed correction values, NULL transitions, foreign keys, no-op/duplicate history
and second-application failure. No production connection is made by these tests.
