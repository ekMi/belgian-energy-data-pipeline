# Belgian Energy Data Pipeline

Python and PostgreSQL pipeline for Belgian electricity grid data published by
Elia. The project progressively targets an analytical PostgreSQL model, Power BI,
and possible future Microsoft Fabric integration.

## Status

Phase 01 (source qualification and initial storage) is complete. Phase 02, step A
consolidates configuration, CSV parsing and unit tests. Step B adds PostgreSQL 18
integration tests with an explicitly isolated test database. Incremental loading,
correction history is not implemented. Step C adds quality reports and durable FAILED tracking; see [Data quality](docs/data_quality.md) for the required manual migration.

Source: Elia ODS001, https://opendata.elia.be/explore/dataset/ods001/
The historical export contains 414,336 records and ten columns. Raw data is not
tracked in Git. Keep the original file unchanged at data/raw/ods001.csv.

## Environment and installation

Windows, Python 3.13 and PostgreSQL 18. From the repository root in PowerShell:

    py -3.13 -m venv .venv
    .\.venv\Scripts\python.exe -m pip install -r requirements.txt
    .\.venv\Scripts\python.exe -m pip check

requirements.txt is the sole dependency version reference, including transitive
and development dependencies, encoded in UTF-8. pyproject.toml configures Ruff
and pytest only. Update pins deliberately using a clean environment, then
validate installation and tests before replacing the reference file.

Direct tools: psycopg and its binary distribution for PostgreSQL, pandas for
profiling, pytest for tests and Ruff for linting and formatting.

## Architecture

    src/
        config.py                 Shared paths and PostgreSQL configuration
        validate_ods001.py         Header checks, row parsing and SHA-256
        ingest_ods001.py           Transactional COPY and file deduplication
        profile_ods001.py          Exploratory quality and temporal reports
        check_db_connection.py     Database connection diagnostics
    sql/
        01_create_schema.sql
        02_add_ingestion_tracking.sql
    tests/unit/
        test_config.py
        test_validate_ods001.py
        test_postgres_safety.py
    tests/integration/
        conftest.py
        test_ingestion.py
    tests/postgres_support.py      Database identity safety guard
    data/raw/                     Local immutable source exports
    data/processed/               Reserved for derived local files
    docs/                         Reserved for further documentation

Paths resolve relative to the project location, independently of the working
directory. Scripts remain directly executable; no package installation is
required. pytest adds src to its module search path.

## PostgreSQL configuration

Set PGHOST, PGPORT, PGDATABASE, PGUSER and PGPASSWORD in the process environment
before running database commands. All five must be nonempty. Do not commit
credentials or print them in logs. Configuration errors report variable names,
not values. A .env file is not loaded automatically.

The existing database is belgian_energy, using energy_app, with staging and
analytics schemas. Python does not execute the SQL files. Do not rerun these
initialization scripts on the populated database. The second script assumes an
empty elia_load table when adding its non-nullable batch reference. Future
schema changes require explicit, reviewed migrations.

## CSV contract

- UTF-8 with optional BOM, semicolon delimiter and exact ordered ODS001 headers.
- At least one data record; empty and header-only files are rejected.
- Exactly ten fields per record.
- ISO 8601 timestamps with an explicit UTC offset (Z accepted), normalized to UTC.
- Resolution code PT15M, matching the existing SQL constraint.
- Only Total Load and Week-ahead forecast accept an empty string as NULL.
  Whitespace-only numeric fields are not NULL markers.
- All other fields are required. Numeric values must be finite: NaN, infinities
  and overflowing numeric literals are rejected.
- Row errors identify the physical CSV line and column name/position. For a
  multiline quoted record, the line number is its ending physical line.
- No business correction is applied. The 846 MW forecast on 2015-06-24 remains.

validate_csv checks the header and presence of records, not every data value.
parse_row validates each record during ingestion. It returns ten business values
without importing PostgreSQL libraries or attaching a batch identifier.

## Commands

Read-only commands, with no database connection:

    .\.venv\Scripts\python.exe -B src/validate_ods001.py
    .\.venv\Scripts\python.exe -B src/profile_ods001.py
    .\.venv\Scripts\ruff.exe check --no-cache .
    .\.venv\Scripts\ruff.exe format --check --no-cache .
    .\.venv\Scripts\python.exe -B -m pytest -p no:cacheprovider

Connection diagnostic:

    .\.venv\Scripts\python.exe src/check_db_connection.py

Ingestion writes to PostgreSQL; run only when a load is intended:

    .\.venv\Scripts\python.exe src/ingest_ods001.py

With the virtual environment activated, python src/ingest_ods001.py remains
supported. Ingestion validates the file, hashes its raw bytes and skips a hash
already associated with a SUCCESS batch. Otherwise it streams rows with COPY,
checks the stored count and commits observations and batch success together.
Parsing and database errors roll back the transaction.

## Tests and limitations

Unit tests cover configuration, headers, missing data, hashing and row parsing,
including UTC conversion, required fields, NULLs and nonfinite numbers. They do
not require PostgreSQL. Integration tests require the dedicated PostgreSQL 18
database described in [the integration test guide](docs/integration_tests.md).
Missing TEST_PG configuration produces explicit skips, not successful tests.

- Failed attempts are tracked separately; connection loss or process termination
  may leave RUNNING. New attempts use real clock timestamps.
- Concurrent executions are not coordinated.
- Files with overlapping keys fail; incremental updates and correction history
  are not implemented.
- Separate header, hash and ingestion reads assume an immutable source file.
- Profiling reports are exploratory, not persisted quality gates. Missing values
  must eventually be evaluated against measurement and forecast horizons.
- No automatic retries, scheduling or structured operational logging.
- API acquisition, analytical transformations and Power BI remain future work.

## Data quality (step C)

See [rules, migration and historical analysis](docs/data_quality.md). Migration 03
must be applied manually before ingestion with this version. Historical analysis
is read-only by default and does not re-import the CSV.
