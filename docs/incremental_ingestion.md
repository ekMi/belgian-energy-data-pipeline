# Historical incremental ingestion contract

> Historical design and validation record. Statements in future tense describe the implementation stage at that time. For current operation, see [README](../README.md), [operations](operations.md) and [data quality](data_quality.md).

## Prerequisites and commands

Migration 04 must already be applied manually before running this version of
ingestion. The application never applies migrations. No correction handling
or history writer is enabled by D3.

The original command still loads the configured historical CSV:

    .\.venv\Scripts\python.exe -B src/ingest_ods001.py

To load a specific extraction:

    .\.venv\Scripts\python.exe -B src/ingest_ods001.py data/raw/new_export.csv --input-scope partial

input-scope defaults to continuous. Use partial for intentionally sparse input.
Both modes validate row structure, duplicate keys and business quality rules.
Only continuous enables the existing continuity warning on the incoming file.
The selected scope and continuity parameter are persisted with the batch/report.

These are write commands: execute only when an actual load is intended.
For tests, use the explicit TEST_PG configuration described in integration_tests.md.
The application itself uses the existing PG environment configuration.

## Shared classification and atomicity

merge_ods001.load_incoming streams CSV rows via COPY into a temporary table and
rejects duplicates; classify_incoming performs the same NULL-sensitive SQL
comparison used by the D2 preview. Preview still rolls back without permanent
writes. Ingestion always recalculates classification; preview results are not
accepted as an execution plan.

The ingestion sequence is:

1. Acquire the ODS001 session advisory lock (81001, 1), with a 10-second lock wait.
2. Compute and check SHA-256 while holding the lock. A previous SUCCESS returns
   None without a new batch, report or observation.
3. Commit a RUNNING batch with input_scope and correction_policy=deny.
4. In a new transaction, load the temporary table, classify and evaluate quality.
5. Reject the entire batch if any CHANGED row exists, including NULL transitions.
   No new observations from that batch are inserted.
6. Reject blocking quality errors. Otherwise INSERT only NEW rows with the new
   batch identifier. UNCHANGED rows are never updated or reassigned.
7. Check inserted count; persist quality results, counters and SUCCESS together.
8. On failure, roll back observation writes, commit FAILED, then persist the
   failure report separately. Report failures do not undo a durable FAILED.
9. Release the advisory lock in finally. Connection closure also releases it.

The lock spans the separate RUNNING commit and failure-report transactions.
The integration fixture's reservation lock is distinct, (81002, 1).
A timeout before acquiring the lock creates no ingestion batch and propagates an
error. Failed explicit unlock does not mask the original load error.
All cooperating ingestions are serialized. Direct SQL writers that ignore this
advisory lock remain outside its guarantee; D3 does not introduce table-wide
write locks or permission changes.

No ON CONFLICT DO NOTHING masks concurrent external writes: an unexpected key
conflict rolls back the batch. Observations absent from an input are untouched.
No correction-history row is ever created by this implementation.

## Counts and provenance

SUCCESS:
- rows_received / rows_loaded = accepted input observations;
- rows_inserted = committed NEW observations;
- rows_unchanged = accepted identical observations;
- rows_updated = 0.

A different hash containing only unchanged observations creates a SUCCESS batch,
with rows_inserted=0, without changing observation provenance. The return value
and CLI count represent accepted input, not inserted observations.

FAILED:
- rows_loaded remains zero;
- if classification completed, rows_received is known and all three committed
  action counters are zero;
- if parsing/duplicate detection failed before classification, the four new
  counters remain NULL (unknown), consistent with D1.

Existing historical batches and their NULL counters are never rewritten.
ERROR technical.corrections_forbidden records the number of CHANGED candidates.
Its examples are restricted to CHANGED entries in the bounded classification
sample, so examples can be empty if earlier NEW entries filled the sample.
The original exception type is recorded by the existing FAILED mechanism.

## Quality and limits

Quality runs cover the incoming batch, including unchanged observations.
The existing checks still buffer incoming typed rows for median/MAD; the full
current observation table is not loaded into Python. No post-merge global
quality scan is introduced here. Empty/header-only input, invalid required
fields, nonfinite numbers and duplicate normalized keys remain blocking.

Connection loss, abrupt termination or ambiguous commits can leave RUNNING.
Known committed SUCCESS is not overwritten as FAILED. Automatic recovery,
correction authorization, freshness decisions and history writes remain deferred.
Files should stay immutable; existing before/after hash verification is retained.

## Tests

    .\.venv\Scripts\python.exe -B -m pytest -p no:cacheprovider -ra
    .\.venv\Scripts\ruff.exe check --no-cache .
    .\.venv\Scripts\ruff.exe format --check --no-cache .

The 203 existing cases are retained. Fixtures now initialize schema 04 for
ingestion/preview tests. Migration-04 tests still initialize schema 03 and seed
historical data explicitly, preserving their pre-migration assertions. Duplicate
ingestion tests now expect the shared DuplicateInputError before permanent
insertion rather than a final-table UniqueViolation.

New tests cover the lock lifecycle, invalid scope, NEW/UNCHANGED combinations,
different hashes, NULL corrections, failed counters, partial-scope reports,
reclassification after preview and two real concurrent connections. Events
coordinate the concurrency tests; the fixture connection also verifies that the
session lock remains held after the RUNNING commit.

In the current implementation session, TEST_PG variables were unavailable:
129 unit tests passed; 86 integration cases were explicitly skipped.
PostgreSQL behavior and concurrency must still be validated in the configured
belgian_energy_test environment.
