# Pipeline operations

## Runtime configuration

Use Python 3.13 and the pinned requirements in the existing virtual environment.
Run commands from the repository root or use absolute script and CSV paths.
Python loads the local .env through config.get_connection_params() without
overriding existing process variables. dbt reads process variables directly;
it does not load .env itself. No scheduler is integrated.

Required connection environment variables: PGHOST, PGPORT, PGDATABASE, PGUSER,
PGPASSWORD. Missing or empty variables are reported by name at the Python API
boundary; values are never included in the configuration error. .env is ignored
by Git. Existing process variables take precedence over its values.

Optional settings:
- PIPELINE_CONNECT_TIMEOUT: integer seconds, default 5, range 1..3600. Passed to
  Psycopg/libpq; it bounds connection attempts, not query execution.
- PIPELINE_LOCK_TIMEOUT: integer seconds, default 10, range 1..3600. Applied to
  the ingestion advisory-lock acquisition and data-transaction lock waits.
  It is a per-lock wait bound, not a total job runtime limit.

Do not put passwords in source, command arguments, logs or committed files.
Provision process environment through the execution account. Restrict access
to exported reports/log files. Do not print the whole environment, DSN or locals.
The library still raises original exceptions to callers; custom embedding code
must not blindly log them. Existing libpq PG* settings remain effective; inspect
the intended host/database configuration before running a write command.
No environment values are dumped by these commands.

## Logging and exit codes

Command entry points configure stdlib logging on stderr, with UTC timestamps,
level and module. Structured JSON outputs and existing human-readable reports
remain on stdout. Ingestion records:
- ingestion_started with file basename (escaped repr) and input scope;
- batch_started with durable batch identifier;
- ingestion_skipped for a previously successful hash;
- ingestion_finished with received/inserted/unchanged/updated and monotonic duration;
- quality_warnings with total rule-anomaly occurrences (not distinct bad rows);
- ingestion_failed with batch, duration and exception class;
- failure_tracking_incomplete when secondary tracking could not be persisted.

The CLI deliberately omits raw exception text, tracebacks, authorization reasons,
source references and credentials. Its command_failed entry contains the exception
class. Inspect the batch reports and preflight settings for details; it is not a
replacement for interactive debugging in a controlled environment.

Exit codes:
- 0: success, including idempotent skip; quality WARNING alone does not fail;
- 1: command/configuration/data/database failure;
- 2: argparse usage error;
- 130: KeyboardInterrupt (Ctrl+C).

Python functions preserve exceptions for callers. argparse --help retains its
normal exit behavior. Successful diagnostics do not fail merely because a saved
report contains anomalies; inspect their JSON if automating policy decisions.

The Python `ingest_csv()` API returns `IngestionResult | None`. A successful new
batch returns immutable facts: `batch_id`, `rows_received` (accepted input),
`rows_inserted` (new observations), `rows_unchanged` (identical observations),
and `rows_updated` (automatic trusted-source or exceptionally authorized corrections). The counters are available after
the successful transaction commits. A previously successful SHA-256 returns
`None` without a new batch. Failures still raise exceptions. The CLI success
message uses `rows_received`; an idempotent skip prints no success message.

Ctrl+C during the data transaction rolls back and attempts to persist FAILED
with error_type=KeyboardInterrupt. An interruption before batch creation leaves
no batch. Repeated interruption, process kill, SIGTERM or connection loss can
prevent failure tracking and leave RUNNING. No unsafe signal-handler database
writes or automatic retry loop is added.

## Commands and output capture

Run commands from the repository root. Before a write, verify PGHOST, PGPORT,
PGDATABASE and PGUSER without printing PGPASSWORD. Keep the input CSV immutable.

Local CSV diagnostics do not connect to PostgreSQL; they use
data/raw/ods001.csv by default:

    .\.venv\Scripts\python.exe -B src/validate_ods001.py
    .\.venv\Scripts\python.exe -B src/profile_ods001.py

To inspect batch counters without changing them:

    SELECT batch_id, source_file, status, rows_received, rows_inserted,
           rows_unchanged, rows_updated, started_at, finished_at
    FROM staging.ingestion_batch ORDER BY batch_id DESC;

To read saved quality reports for a real batch ID:

    .\.venv\Scripts\python.exe -B src/check_quality_ods001.py --batch-id 1 --mode historical

To evaluate current versions still attributed to that batch:

    .\.venv\Scripts\python.exe -B src/check_quality_ods001.py --batch-id 1 --mode current --as-of 2026-10-26T12:00:00+00:00

Without --persist, both commands use read-only transactions. In current mode,
--persist appends a report in PostgreSQL; historical mode rejects it. Current
mode cannot reconstruct the original file after corrections. See
[data quality](data_quality.md).

Preview creates a temporary PostgreSQL table and rolls back. The full report
writes a local JSON file; --full-report and --output must be used together.
Ingestion reclassifies the CSV independently of preview. A successful SHA-256
replay is skipped; different bytes with identical observations can create a
new SUCCESS batch with zero inserts. Trusted full Elia ODS001 exports use
`--source-policy elia-full`: NEW and CHANGED are accepted automatically, with
old/new correction history and all technical quality checks retained. Other
CSVs default to `restricted`; partial imports remain restrictive. See
[correction policy](authorized_corrections.md).

The trust declaration is shared by ingestion, the full pipeline, preview and
Prefect. It does not authenticate the file: the operator must supply the latest
complete Elia export. Inbox presence and file age/stability only concern copy
readiness. Never invent `extracted_at`; known older extractions still fail the
existing freshness check. An unknown date remains NULL. `elia-full` cannot be
combined with partial scope or manual hash/reason authorization.

Normal trusted full-export commands (write operations):

    .\.venv\Scripts\python.exe -B src/local_pipeline.py data/raw/full_export.csv --source-policy elia-full
    .\.venv\Scripts\python.exe -B src/ingest_ods001.py data/raw/full_export.csv --source-policy elia-full

The automatic reason stored with the exact file hash is
`Automatic update from authoritative Elia ODS001 full export`, not a claim of
human approval. The existing schema stores it using `correction_policy=authorized`.
No migration or retrospective history change is required. The pipeline returns
SUCCESS/NO_CHANGE, or DBT_FAILED if the analytical rebuild fails after ingestion;
committed ingestion is retained. AWAITING_APPROVAL remains possible only under
the restrictive policy. Both Prefect deployments select `elia-full`; use
`source_policy=restricted` for an exceptional partial run of the manual deployment.

After an accepted ingestion, check the dbt target, rebuild the analytical
tables (database write), and manually refresh the Power BI report connected
to analytics:

    .\.venv\Scripts\python.exe -B src/dbt_guard.py --check-only
    .\.venv\Scripts\python.exe -B src/dbt_guard.py

The guard loads .env through the existing Python configuration and passes the
resolved PG settings to the separate dbt executable. It rejects missing or
unexpected settings, profile drift, and an unexpected effective PostgreSQL
target before starting dbt build. A refusal exits 1 and leaves analytics
untouched by this command; inspect configuration without printing credentials.
Direct manual dbt.exe build bypasses this preflight. dbt build also runs the
project tests. See [dbt setup](dbt_setup.md).

## Tracked dbt rebuild attempts (schema 05)

`staging.dbt_build_run` records one row per attempt. `start_build(target_batch_id)`
requires a SUCCESS ingestion batch and creates RUNNING; `mark_success(build_id)`
or `mark_failed(build_id, error_type)` completes it exactly once. Failure types
are limited to `DBT_BUILD_FAILED`, `DBT_PREFLIGHT_FAILED`, `INTERRUPTED`, and
`UNKNOWN_FAILURE`; raw exceptions and connection details must never be stored.
`latest_success()` returns the SUCCESS attempt with the highest
`target_batch_id`, breaking ties by the highest `build_id`; `list_builds()`
provides attempt history. A later build covering an older batch cannot lower
the analytical watermark. FAILED and RUNNING do not establish an analytical
watermark. An interrupted process can leave
RUNNING indefinitely. A retry creates a new build ID.

The target batch is the last successful ingestion used as a reference for a
full dbt rebuild, including validated observations from earlier batches. An
ingestion SUCCESS only proves staging was updated. dbt SUCCESS proves analytics
was rebuilt only when the caller records it after dbt actually finishes. If dbt
finishes but the tracking update is lost, the attempt remains unconfirmed and
must be rebuilt later. Older manual dbt runs are not backfilled: until the first
tracked SUCCESS, analytical synchronization is unproven. Legacy ingestion
counters that are NULL do not prove the absence of business changes.

This release provides storage only. It does not invoke dbt or connect the store
to the preview. To apply migration 05 later, stop pipeline writes, verify the
intended database and role without exposing credentials, confirm 01-04 already
applied, then run `sql/05_add_dbt_build_tracking.sql` once with psql against the
explicitly selected `belgian_energy` database. For example, in an authorized
terminal with PGHOST, PGPORT and PGUSER set and credentials supplied securely:

    psql -X -v ON_ERROR_STOP=1 -d belgian_energy -f sql/05_add_dbt_build_tracking.sql

The script owns its transaction and rolls back on SQL failure. Do not apply it
automatically. The target batch alone does not fence concurrent ingestion during
dbt; a global lock is planned for C3.3. A model change can also make a previously
successful build stale without changing its stored status. Review and rebuild
after model changes. The current store does not record a model revision.

Read-only diagnostics:

    .\.venv\Scripts\python.exe -B src/check_db_connection.py
    .\.venv\Scripts\python.exe -B src/check_running_batches.py --older-than-hours 24
    .\.venv\Scripts\python.exe -B src/check_quality_ods001.py --batch-id 1 --mode historical
    .\.venv\Scripts\python.exe -B src/merge_ods001.py data/raw/new_export.csv

Complete preview report for diagnostics or exceptional restricted-source review:

    .\.venv\Scripts\python.exe -B src/merge_ods001.py data/raw/ods001_20261026.csv --full-report --output data/processed/preview_full_20261026.json

The preview reads permanent tables without changing them. Its JSON report contains
all NEW/CHANGED observations, counts, checksum and a per-measure correction
summary. The output directory must exist and have enough free space. The report
does not authorize corrections or ingest data; ingestion is a separate command
and reclassifies the file against its own database snapshot. See
`incremental_preview.md` for the report structure and performance limits.

Actual ingestion (write operation; requires schema 04):

    .\.venv\Scripts\python.exe -B src/ingest_ods001.py data/raw/new_export.csv --input-scope partial

For automation, use the exact interpreter path, explicit working directory,
immutable input path and explicit environment. Capture stdout and stderr
separately and preserve the child exit code. Example PowerShell wrapper:

    & .\.venv\Scripts\python.exe -B src/ingest_ods001.py data/raw/new_export.csv --input-scope partial 1> run-output.txt 2> run-log.txt
    $pipelineExitCode = $LASTEXITCODE
    exit $pipelineExitCode

Choose a separate log destination per execution and manage retention externally.
No log file is created by the Python code itself. Do not permanently embed a
manual correction authorization in a recurring job. Trusted full exports instead
select `--source-policy elia-full`, which binds its automatic reason to the
hash calculated under the lock. Exceptional manual approvals remain bound to reviewed
file bytes. Hash idempotence is preserved, and lock contention fails after the
configured wait rather than running overlapping cooperative ingestions.

## RUNNING review: no automatic mutation

check_running_batches.py opens a read-only transaction. It lists IDs, start times
and ages above the requested threshold. Age only selects review candidates;
a long-running valid ingestion can be older than the threshold.

Before any manual state decision:
1. Stop scheduling new ingestions and identify the owning process/session.
2. Review its logs and PostgreSQL activity/locks using authorized administrative
   access. Do not expose SQL arguments or connection secrets in shared reports.
3. Confirm no cooperative ingestion is active. An authorized diagnostic session
   may test pg_try_advisory_lock(81001,1); if it acquires the lock, release it with
   pg_advisory_unlock(81001,1) before closing. False means someone holds it.
   A free lock alone does not prove the outcome of every past connection.
4. Re-read the batch status and counters, its quality reports, observations
   attributed to the batch, and correction history for correction_batch_id.
5. If status is SUCCESS, do not relabel it. A lost client acknowledgement is not
   proof of failure. If RUNNING has unexpected committed artifacts, investigate.
6. If abandonment is established, prepare an explicit reviewed repair scoped to
   that batch and consistent with its evidence. This release supplies no UPDATE,
   automatic recovery or fabricated finish time. Obtain approval before repair.

A later execution can retry the same hash unless SUCCESS already exists. An
abandoned RUNNING record is not silently reused or overwritten. Examine stale
attempts before retrying corrections and keep their evidence.

## Tests and remaining limits

    .\.venv\Scripts\python.exe -B -m pytest -p no:cacheprovider -ra
    .\.venv\Scripts\ruff.exe check --no-cache .
    .\.venv\Scripts\ruff.exe format --check --no-cache .

Only explicit TEST_PG configuration permits PostgreSQL tests; fixtures enforce
belgian_energy_test. Application configuration tests cover the connection timeout;
existing guarded integration fixtures supply their own explicit connect timeout.
The new interruption integration case checks rollback, FAILED and lock release.

Deferred: automatic abandoned-batch repair, retries/backoff, external log
rotation/retention, job-level runtime limits, statement execution timeouts,
metrics, scheduling and API acquisition. Analytical transformations are
implemented separately in dbt; see [dbt setup](dbt_setup.md).
Current logging prioritizes avoiding credential/data disclosure over verbose
tracebacks. SQL exception classes alone may require additional manual diagnosis.
