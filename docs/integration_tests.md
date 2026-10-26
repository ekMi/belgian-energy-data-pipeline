# PostgreSQL integration tests

## Dedicated, disposable database

These tests exclusively authorize the exact database name belgian_energy_test
on PostgreSQL 18. They never use development PG environment variables as a
fallback. Each connection checks current_database() and server_version_num
before any schema or data write, including connections opened by ingest_csv.

WARNING: The staging and analytics schemas in this dedicated test database are
fully dropped and recreated for each test, then dropped during teardown. Do not
store valuable data there. No test database is created automatically. The SQL
scripts in sql/ are reused unchanged. Production schemas and data are untouched.

A session advisory lock rejects simultaneous runs using these fixtures. It does
not coordinate with other tools or applications. Use this database only for tests.
After an interrupted run, the next run resets these two schemas. Tests do not
depend on execution order or sequence identifiers.

## One-time setup (manual)

If the database does not exist, connect as an administrator to the maintenance
postgres database, explicitly selecting the intended server. Do not connect to
belgian_energy. For example, from PowerShell with psql on PATH:

    psql -h localhost -p 5432 -U postgres -d postgres

Run these statements separately, outside a transaction:

    CREATE ROLE energy_test LOGIN;
    CREATE DATABASE belgian_energy_test OWNER energy_test;

Use the psql interactive password command to set the dedicated role password
without placing it in SQL history:

    \password energy_test

Use a dedicated role with no development database privileges. Do not reuse
energy_app credentials. If either object already exists, inspect its ownership
and purpose instead of recreating it. The test role owns the disposable database
so it can create and remove its schemas. No superuser access is needed for tests.

## Explicit test configuration

Set these variables in the terminal used to run pytest:

    $env:TEST_PGHOST = 'localhost'
    $env:TEST_PGPORT = '5432'
    $env:TEST_PGDATABASE = 'belgian_energy_test'
    $env:TEST_PGUSER = 'energy_test'
    $testCredential = Get-Credential -UserName 'energy_test' -Message 'Test database credentials'
    $env:TEST_PGPASSWORD = $testCredential.GetNetworkCredential().Password
    Remove-Variable testCredential

The password is passed through the process environment and is never printed by
the fixtures. Do not commit credentials or use pytest --showlocals with secrets
in the environment or fixture variables. Test fixtures use only explicit
TEST_PG variables and do not load .env.

All five variables must be present and nonempty. Missing variables explicitly
skip integration tests. A configured unauthorized database name, wrong server
version or connection failure is an error, not a skip. Connection errors use a
generic diagnostic so connection details are not echoed. Connection timeout is
five seconds. Inherited PG* settings are temporarily removed during each fixture;
pytest restores them afterward. No discovery or connection attempt is made when
the test configuration is missing.

## Commands

From the repository root, unit tests only:

    .\.venv\Scripts\python.exe -B -m pytest -p no:cacheprovider -m 'not integration' -ra

Integration tests only:

    .\.venv\Scripts\python.exe -B -m pytest -p no:cacheprovider -m integration -ra

All tests:

    .\.venv\Scripts\python.exe -B -m pytest -p no:cacheprovider -ra

Static checks:

    .\.venv\Scripts\ruff.exe check --no-cache .
    .\.venv\Scripts\ruff.exe format --check --no-cache .

After testing, remove the password from this terminal's environment:

    Remove-Item Env:TEST_PGPASSWORD

## Coverage and limits

The examples below illustrate the integration scope. For the full current
scope, inspect tests/integration/ and the historical
[consolidated quality acceptance](phase_d_acceptance.md). All configured
integration tests use the disposable database described above.

The integration scenarios use tiny synthetic CSVs, never the historical export:

- Successful COPY, exact UTC timestamps, values, row count, SUCCESS status,
  source filename, SHA-256, batch timestamps and observation-to-batch links.
- Allowed empty fields become SQL NULL while other numeric values are preserved.
- Repeated ingestion and renamed identical bytes create no additional batch or row.
- Duplicate keys within a file roll back the whole load.
- Invalid numeric data after several valid rows rolls back the whole load.
- Both failure scenarios preserve a previously committed baseline batch and row.
- Direct SQL checks exercise the resolution CHECK, mandatory columns, batch
  foreign key, allowed statuses and nonnegative row counts. Savepoints isolate
  expected constraint errors, independently of the Python parser.

The fixture patches only configuration and the connection factory. The actual
COPY, commits, rollbacks and SQL constraints use real Psycopg/PostgreSQL. No
SQLite or simulated database replaces an integration test. Separate unit tests
use mocks only to verify the database identity safety guard.

Failed ingestion rolls back observations and retains a FAILED batch. Current
ingestion fixtures apply migrations 01-04 only in the authorized test database.
Migration-specific cases start at schema 03 to test migration 04 itself. The
suite also covers concurrency, incremental loads, corrections and quality
reports; see the cases under tests/integration/.

## Historical initial test result

The integration suite was collected but not executed against PostgreSQL because
none of the TEST_PG variables were configured. The 18 integration cases were
reported as skipped. This is not evidence of successful PostgreSQL validation.
Run them against the configured dedicated database before considering the
integration behavior verified.
