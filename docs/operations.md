# Pipeline operations — phase 02-E

## Runtime configuration

Use Python 3.13 and the pinned requirements in the existing virtual environment.
Run commands from the repository root or use absolute script and CSV paths.
No .env loader, scheduler or new framework is introduced.

Required connection environment variables: PGHOST, PGPORT, PGDATABASE, PGUSER,
PGPASSWORD. Missing or empty variables are reported by name at the Python API
boundary; values are never included in the configuration error. .env is ignored
by Git but is not loaded automatically.

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

Ctrl+C during the data transaction rolls back and attempts to persist FAILED
with error_type=KeyboardInterrupt. An interruption before batch creation leaves
no batch. Repeated interruption, process kill, SIGTERM or connection loss can
prevent failure tracking and leave RUNNING. No unsafe signal-handler database
writes or automatic retry loop is added.

## Commands and output capture

Read-only diagnostics:

    .\.venv\Scripts\python.exe -B src/check_db_connection.py
    .\.venv\Scripts\python.exe -B src/check_running_batches.py --older-than-hours 24
    .\.venv\Scripts\python.exe -B src/check_quality_ods001.py --batch-id 1 --mode historical
    .\.venv\Scripts\python.exe -B src/merge_ods001.py data/raw/new_export.csv

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
correction authorization in a recurring job. Approvals remain bound to reviewed
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
metrics, scheduling, API acquisition and analytical transformations.
Current logging prioritizes avoiding credential/data disclosure over verbose
tracebacks. SQL exception classes alone may require additional manual diagnosis.
