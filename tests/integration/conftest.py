"""Fixtures exclusively for a disposable, explicitly configured PostgreSQL database."""

import csv
import os

import psycopg
import pytest
from postgres_support import TEST_DATABASE, verify_test_database

import ingest_ods001
from config import PROJECT_ROOT
from validate_ods001 import EXPECTED_COLUMNS

VARIABLES = {
    "host": "TEST_PGHOST",
    "port": "TEST_PGPORT",
    "dbname": "TEST_PGDATABASE",
    "user": "TEST_PGUSER",
    "password": "TEST_PGPASSWORD",
}


@pytest.fixture
def test_database(monkeypatch):
    missing = [name for name in VARIABLES.values() if not os.environ.get(name)]
    if missing:
        pytest.skip(
            "PostgreSQL integration configuration missing: " + ", ".join(missing)
        )
    params = {key: os.environ[name] for key, name in VARIABLES.items()}
    if params["dbname"] != TEST_DATABASE:
        pytest.fail("TEST_PGDATABASE must be belgian_energy_test", pytrace=False)

    # Do not inherit development libpq service, host, user or other PG settings.
    for name in list(os.environ):
        if name.startswith("PG"):
            monkeypatch.delenv(name)
    params["connect_timeout"] = 5
    real_connect = psycopg.connect

    def guarded_connect(*args, **kwargs):
        # Ignore no arguments silently: ingestion must use the explicit test config.
        if args or kwargs != params:
            raise RuntimeError("Unexpected connection parameters in integration test")
        try:
            connection = real_connect(**params)
        except psycopg.OperationalError:
            pytest.fail(
                "Cannot connect to the configured test database. "
                "Check its existence, credentials and PostgreSQL availability.",
                pytrace=False,
            )
        try:
            verify_test_database(connection)
            connection.commit()
        except BaseException:
            connection.close()
            raise
        return connection

    monkeypatch.setattr(ingest_ods001, "get_connection_params", lambda: params.copy())
    monkeypatch.setattr(psycopg, "connect", guarded_connect)

    with guarded_connect(**params) as admin:
        # Hold a session lock across setup, ingestion connections and teardown.
        locked = admin.execute("SELECT pg_try_advisory_lock(81002, 1)").fetchone()[0]
        admin.commit()
        if not locked:
            pytest.fail(
                "Another integration run is using the test database", pytrace=False
            )

        def reset_schemas():
            verify_test_database(admin)
            admin.execute("DROP SCHEMA IF EXISTS staging CASCADE")
            admin.execute("DROP SCHEMA IF EXISTS analytics CASCADE")
            admin.commit()

        try:
            reset_schemas()
            for name in ("01_create_schema.sql", "02_add_ingestion_tracking.sql"):
                admin.execute((PROJECT_ROOT / "sql" / name).read_text(encoding="utf-8"))
            admin.commit()
            admin.execute(
                (PROJECT_ROOT / "sql/03_add_quality_tracking.sql").read_text(
                    encoding="utf-8"
                )
            )
            admin.commit()
            yield admin
        finally:
            admin.rollback()
            reset_schemas()
            # Closing the admin connection releases the session advisory lock.


@pytest.fixture
def csv_file(tmp_path):
    def create(rows, name="sample.csv"):
        path = tmp_path / name
        with path.open("w", encoding="utf-8", newline="") as stream:
            writer = csv.writer(stream, delimiter=";")
            writer.writerow(EXPECTED_COLUMNS)
            writer.writerows(rows)
        return path

    return create


@pytest.fixture
def rows():
    return [
        [
            f"2025-01-01T01:{minute}:00+01:00",
            "PT15M",
            "10000",
            "846",
            "7791.92",
            "8699.21",
            "8500",
            "8000",
            "9000",
            "9500",
        ]
        for minute in ("00", "15", "30", "45")
    ]
