# Data quality and failure tracking (step C)

## Manual migration

Do not run ingestion with the new code until migration 03 is approved and applied.
The read-only historical quality command works without migration 03.

Review sql/03_add_quality_tracking.sql. Apply it once, after scripts 01 and 02,
using an authorized schema owner. From the repository root, explicitly choose
the intended host and database (psql must be on PATH):

    psql -X -h localhost -p 5432 -U energy_app -d belgian_energy -v ON_ERROR_STOP=1 -f sql/03_add_quality_tracking.sql

This command is provided for manual execution, not executed by the application.
Use the actual schema owner if energy_app does not own the tracking table.
The migration is transactional and not intended to be rerun. It adds two tables,
an index, an error_type column and changes the default for future started_at
values. Existing observations, batches and timestamps are not rewritten.
If migration fails, its transaction rolls back. Deploy code and migration as a
coordinated change; no automatic downgrade or DROP script is provided.

Only guarded integration fixtures automatically apply migration 03, exclusively
in belgian_energy_test. See integration_tests.md for explicit TEST_PG configuration.

## Rule contract, version 1

quality_ods001.evaluate_quality accepts the ten typed, UTC-normalized business
columns produced by parse_row or read from PostgreSQL. Structural parsing,
required input fields and finite-number validation remain in validate_ods001.
The quality functions do not mutate inputs. Numeric column indices follow the
zero-based CSV order: 2=Total Load, 3..8=most-recent/day-ahead forecast triples,
9=Week-ahead forecast.

| Rule | Severity | Definition |
| --- | --- | --- |
| technical.unique_key | ERROR | All rows participating in a duplicate timestamp/resolution key count as anomalies. |
| technical.quarter_hour | ERROR | Resolution must be PT15M; UTC minute divisible by 15; zero seconds/microseconds. |
| technical.continuity | WARNING | Consecutive distinct timestamps differ from 15 minutes. Counts gap intervals, not estimated missing rows. |
| completeness.N.historical | WARNING for 2/9, ERROR otherwise | NULL at or before as_of minus 24 hours. |
| completeness.N.pending | WARNING | NULL after that cutoff; pending publication or future observation, not asserted to be historical data loss. |
| business.N.negative | WARNING | Available numeric value below zero. |
| business.N.outlier | WARNING | Absolute deviation from median exceeds 6 * 1.4826 * median absolute deviation (MAD). At least 20 available values and MAD > 0 required. |
| business.3.interval / business.6.interval | WARNING | P10 <= forecast <= P90 is false, including reversed bounds. Only complete triples are evaluated. |
| technical.ingestion | ERROR | One failed attempt, with sanitized exception class. Counts attempts, not source rows. |

The 24-hour grace period is an initial configurable-in-code policy, not a proven
Elia publication SLA. Missing future forecasts are reported separately, not
silently omitted. Re-evaluation with an explicit as_of makes aging reproducible.
The rule version must change if thresholds or semantics change.

Outliers are exploratory: global batch median/MAD ignores seasonality. Zero MAD
or small samples produce evaluated_count=0. Negative values and the known 846 MW
forecast are preserved. No imputation, clipping, deletion or replacement occurs.
Forecast interval comparisons with missing fields are excluded from their
denominator; completeness reports cover those missing values.

Continuity is scoped to the supplied batch, not all database history; endpoint
coverage outside its min/max is not inferred. Disable continuity for a sparse
future incremental input and evaluate it separately over the consolidated range.
This does not implement incremental ingestion.

## Persistence and transactions

quality_run records the ingestion batch, analysis time, as_of, scope and policy
parameters. quality_result records rule identifier/version, ERROR or WARNING,
evaluated and anomaly counts and up to five timestamp/value examples. Repeated
analyses append new runs; previous reports are never overwritten.

1. Register and commit RUNNING with clock_timestamp().
2. Parse the bounded input, evaluate rules and COPY within one data transaction.
3. Blocking rules or SQL errors roll back all new observations.
4. Success commits observations, quality report and SUCCESS together.
5. On failure, commit FAILED and sanitized error_type after rollback. Save the
   failure report in another transaction so report persistence failure cannot
   undo FAILED. Preserve the original exception and attach a note if tracking fails.

Durations for new attempts use real started_at/finished_at clocks. Old durations
cannot be repaired reliably and are left unchanged. SHA-256 SUCCESS deduplication
is unchanged; FAILED attempts may be retried.

An unreadable file cannot be hashed/registered. Connection loss, process kill or
an ambiguous commit can leave RUNNING; automatic recovery is not implemented.
A known committed SUCCESS is never relabeled FAILED. Failure report persistence
is best effort after the durable failure status. Concurrent ingestions remain
uncoordinated.

Inputs are currently buffered to compute exact median/MAD and batch-wide rules.
This increases memory use versus the previous streaming-only parser. It is
appropriate for the current bounded historical export; larger inputs will need
a separate streaming or SQL strategy. No external dependency was added.

## Historical data: no CSV re-import

Find the desired successful batch using a read-only SQL query:

    SELECT batch_id, source_file, rows_loaded, started_at
    FROM staging.ingestion_batch WHERE status='SUCCESS' ORDER BY batch_id;

Then analyze it, substituting the actual batch identifier and explicit reference
time (example below). The command sets a repeatable-read, read-only transaction:

    .\.venv\Scripts\python.exe -B src/check_quality_ods001.py --batch-id 1 --as-of 2026-10-18T12:00:00+00:00

The output is JSON. To append the report after migration approval/application,
explicitly add --persist. This writes only quality tables, not observations or
existing ingestion batches:

    .\.venv\Scripts\python.exe -B src/check_quality_ods001.py --batch-id 1 --as-of 2026-10-18T12:00:00+00:00 --persist

Future sparse-batch evaluations can use --no-continuity. The application never
executes a migration. Historical reports are batch-scoped; they do not infer
provenance beyond the existing ingestion_batch_id.

## Verification

    .\.venv\Scripts\ruff.exe check --no-cache .
    .\.venv\Scripts\ruff.exe format --check --no-cache .
    .\.venv\Scripts\python.exe -B -m pytest -p no:cacheprovider -ra

Existing rollback tests retain their scenarios, but now expect an additional
durable FAILED batch rather than disappearance of the failed attempt.
Integration tests also cover warning persistence, historical analysis without
observation changes, quality rejection, post-COPY failure and report outage.
