# Incremental preview — D2

## Command

From the repository root, set the existing PGHOST, PGPORT, PGDATABASE, PGUSER and
PGPASSWORD configuration, then run:

    .\.venv\Scripts\python.exe -B src/merge_ods001.py data/raw/new_export.csv --sample-size 5

To save every NEW and CHANGED observation for review:

    .\.venv\Scripts\python.exe -B src/merge_ods001.py data/raw/ods001_20261026.csv --full-report --output data/processed/preview_full_20261026.json

`--full-report` and `--output` must be used together. The output directory must
already exist. The command prints a compact count and change summary; the file
contains the full report. `--sample-size` still controls only the ordinary
preview (default 5, range 0..100).

The command prints JSON and never invokes ingestion or a migration. No batch,
quality run or correction history is created. The CSV is opened for reading only.
The preview requires SELECT on staging.elia_load and permission to create TEMP
tables in the selected database. No permanent table write privilege is required.
D1's migration is not required for the preview query itself.

## Result contract

- file_sha256: checksum of the input bytes.
- rows_received: total parsed input observations.
- rows_new: keys absent from the current table.
- rows_unchanged: keys whose eight numeric measures match.
- rows_changed: keys with at least one differing numeric measure.
- differences: at most sample-size NEW/CHANGED observations, ordered by UTC
  timestamp and resolution. Each contains classification, key, old/new values,
  and changed_measures with old/new values per affected measure.

rows_received = rows_new + rows_unchanged + rows_changed.
These are candidate counts, not committed ingestion counters. A CHANGED row is
not authorized for correction. A NEW sample has old_values=null and lists all
eight incoming measures. An UNCHANGED row is not included in the sample.
The sample is chronological, not statistically representative or stratified:
NEW rows can fill the limit before any CHANGED rows appear.
sample-size accepts 0..100 (default 5). Zero returns counts without examples.

The full report keeps the same top-level checksum and counters. Its `differences`
array contains every NEW and CHANGED observation, with no UNCHANGED entries or
sample limit. Each CHANGED entry keeps `old_values`, `new_values`, and
`changed_measures`. `change_summary` contains `changed_by_measure`,
`null_to_value`, and `value_to_null` maps for all eight measures, plus
`first_changed_utc` and `last_changed_utc` (null if there are no corrections).
Only CHANGED observations contribute to this summary. A row changing several
measures contributes once to each affected measure.

## SQL and transaction design

1. Reuse validate_csv and parse_row: ordered headers, ten fields, explicit UTC
   offset, UTC normalization, PT15M, mandatory fields, finite numeric values.
2. Open a fresh Psycopg connection owned by preview_csv.
3. Start one REPEATABLE READ transaction and set its timezone to UTC.
4. CREATE TEMP TABLE ... AS SELECT the ten business columns WITH NO DATA,
   with ON COMMIT DROP; add a temporary source_line for error diagnostics.
5. Stream parsed input through COPY to pg_temp.ods001_preview.
6. Detect repeated normalized keys with GROUP BY/HAVING count(*) > 1.
   Reject identical and contradictory duplicates, reporting the first key.
7. LEFT JOIN incoming rows to staging.elia_load on timestamp/resolution.
   SQL CASE classifies rows; ROW(eight incoming measures) IS DISTINCT FROM
   ROW(eight stored measures) handles NULL transitions and NULL equality.
8. A materialized CTE supplies exact counts and a bounded JSON sample in one
   statement. Python only formats differences within that sample; it never
   loads all stored observations or classifies them in pandas.
9. Roll back even on success, then close the connection. Temporary objects and
   copied rows are discarded. Errors also roll back.

Full report mode uses the same transaction and exact count query, followed by a
server-side cursor over NEW/CHANGED rows in the same database snapshot. Python
writes the JSON incrementally to a temporary file beside the output and replaces
the requested path only after the array and summary are complete and the counts
match. On failure, the temporary file is removed and a prior report is retained.
The CLI exits with an error class on stderr if writing fails; the Python function
raises the underlying exception for a caller to inspect.

CREATE TEMP TABLE requires a read/write transaction; this is therefore not a
SET TRANSACTION READ ONLY session. Nevertheless, the only DDL/DML targets are
temporary objects. Permanent tables are only selected, and no application
commit is performed. force_rollback provides additional cleanup, not a substitute
for reviewing SQL targets.

The existing ingestion, migrations and quality rules are unchanged. No business
anomaly gate is added here: a forecast such as 846 MW is compared as supplied.
No absent observation is deleted or classified as a deletion.

## Reproducibility and limits

The query sees a consistent PostgreSQL snapshot, but this is a preview, not a
reservation. D3/D4 must classify again under their future concurrency policy
before applying changes. No ingestion advisory lock or correction authorization
is implemented in D2.

Input headers are checked again while copying; empty input is rejected. SHA-256
is checked before and after reading. Keep exports immutable while previewing:
the check detects ordinary file replacement but is not a filesystem snapshot.
Matching a previously ingested hash does not skip classification: the current
database may have changed since that ingestion.

Parsing is streamed. PostgreSQL may materialize the classification in memory or
temporary storage; large inputs can require temporary disk space. Only a bounded
sample and counters reach Python. Database equality uses existing DOUBLE
PRECISION values without introducing tolerance or rounding.
Full report mode transfers all differences and writes all of them to disk, so its
runtime and report size grow with the number of NEW/CHANGED observations. Python
holds only a fetched batch of 1,000 differences and the small summary at a time.
The server-side cursor and sorted query can use PostgreSQL temporary storage.

## Tests

Unit tests cover full-report streaming and output failures. D2 integration fixtures use the existing
guarded belgian_energy_test connection and apply the approved D1 migration only
to that disposable test schema. They never use development PG configuration
as a fallback. See integration_tests.md for TEST_PG variables.

    .\.venv\Scripts\python.exe -B -m pytest tests/unit/test_merge_ods001.py -p no:cacheprovider
    .\.venv\Scripts\python.exe -B -m pytest tests/integration/test_preview.py -p no:cacheprovider -ra
    .\.venv\Scripts\python.exe -B -m pytest -p no:cacheprovider -ra
    .\.venv\Scripts\ruff.exe check --no-cache .
    .\.venv\Scripts\ruff.exe format --check --no-cache .

Integration scenarios cover all NEW, all UNCHANGED, mixed classifications,
each individual measure, both nullable fields in both directions, NULL equality,
normalized duplicate keys, missing required fields, invalid resolution,
nonfinite values, zero-sized samples, repeated previews and changed input hashes.
Snapshots compare all rows in observations, batches, quality runs/results and
correction history before and after previews, including rejected inputs.

Missing TEST_PG configuration explicitly skips integration tests. Passing unit
tests alone does not validate COPY or the classification SQL on PostgreSQL.
