from datetime import UTC, datetime

import psycopg
import pytest

from ingest_ods001 import ingest_csv
from validate_ods001 import calculate_sha256

pytestmark = pytest.mark.integration


def snapshot(connection):
    observations = connection.execute(
        "SELECT * FROM staging.elia_load ORDER BY timestamp_utc, resolution_code"
    ).fetchall()
    batches = connection.execute(
        "SELECT * FROM staging.ingestion_batch ORDER BY batch_id"
    ).fetchall()
    return observations, batches


def test_successful_copy(test_database, csv_file, rows):
    path = csv_file(rows)
    assert ingest_csv(path) == 4
    records = test_database.execute(
        "SELECT timestamp_utc, resolution_code, total_load_mw, "
        "most_recent_forecast_mw, ingestion_batch_id "
        "FROM staging.elia_load ORDER BY timestamp_utc"
    ).fetchall()
    batch = test_database.execute(
        "SELECT batch_id, source_file, file_sha256, status, rows_loaded, "
        "started_at, finished_at FROM staging.ingestion_batch"
    ).fetchall()
    assert len(batch) == 1
    batch_id, source, checksum, status, count, started, finished = batch[0]
    assert (source, checksum, status, count) == (
        path.name,
        calculate_sha256(path),
        "SUCCESS",
        4,
    )
    assert started is not None and finished is not None
    assert records == [
        (datetime(2025, 1, 1, 0, minute, tzinfo=UTC), "PT15M", 10000.0, 846.0, batch_id)
        for minute in (0, 15, 30, 45)
    ]


def test_nullable_values(test_database, csv_file, rows):
    rows[0][2] = rows[0][9] = ""
    assert ingest_csv(csv_file(rows[:1])) == 1
    assert test_database.execute(
        "SELECT total_load_mw, most_recent_forecast_mw, most_recent_p10_mw, "
        "most_recent_p90_mw, day_ahead_forecast_mw, day_ahead_p10_mw, "
        "day_ahead_p90_mw, week_ahead_forecast_mw FROM staging.elia_load"
    ).fetchone() == (None, 846.0, 7791.92, 8699.21, 8500.0, 8000.0, 9000.0, None)


def test_sha256_idempotence(test_database, csv_file, rows):
    path = csv_file(rows)
    assert ingest_csv(path) == 4
    before = snapshot(test_database)
    assert ingest_csv(path) is None
    # A different filename with identical bytes must also be skipped.
    assert ingest_csv(csv_file(rows, "renamed.csv")) is None
    assert snapshot(test_database) == before
    assert len(before[0]) == 4
    assert len(before[1]) == 1


@pytest.mark.parametrize("failure", ["duplicate", "invalid_number"])
def test_rollback_preserves_existing_data(test_database, csv_file, rows, failure):
    assert ingest_csv(csv_file(rows[:1], "baseline.csv")) == 1
    before = snapshot(test_database)
    incoming = [row.copy() for row in rows[1:]]
    if failure == "duplicate":
        incoming.append(incoming[0].copy())
        expected_error = psycopg.errors.UniqueViolation
    else:
        invalid = rows[0].copy()
        invalid[2] = "not-a-number"
        incoming.append(invalid)
        expected_error = ValueError
    with pytest.raises(expected_error):
        ingest_csv(csv_file(incoming, "failed.csv"))
    after = snapshot(test_database)
    assert after[0] == before[0]
    assert after[1][:-1] == before[1]
    assert test_database.execute(
        "SELECT status,rows_loaded,finished_at>started_at "
        "FROM staging.ingestion_batch ORDER BY batch_id DESC LIMIT 1"
    ).fetchone() == ("FAILED", 0, True)


@pytest.mark.parametrize(
    ("column", "value", "expected_error"),
    [
        ("resolution_code", "PT60M", psycopg.errors.CheckViolation),
        ("timestamp_utc", None, psycopg.errors.NotNullViolation),
        ("resolution_code", None, psycopg.errors.NotNullViolation),
        ("most_recent_forecast_mw", None, psycopg.errors.NotNullViolation),
        ("most_recent_p10_mw", None, psycopg.errors.NotNullViolation),
        ("most_recent_p90_mw", None, psycopg.errors.NotNullViolation),
        ("day_ahead_forecast_mw", None, psycopg.errors.NotNullViolation),
        ("day_ahead_p10_mw", None, psycopg.errors.NotNullViolation),
        ("day_ahead_p90_mw", None, psycopg.errors.NotNullViolation),
        ("ingestion_batch_id", None, psycopg.errors.NotNullViolation),
        ("ingestion_batch_id", -1, psycopg.errors.ForeignKeyViolation),
    ],
)
def test_database_constraints(
    test_database, csv_file, rows, column, value, expected_error
):
    assert ingest_csv(csv_file(rows[:1])) == 1
    before = snapshot(test_database)
    # Test SQL directly so Python validation cannot hide a missing SQL constraint.
    with pytest.raises(expected_error), test_database.transaction():
        test_database.execute(
            psycopg.sql.SQL("UPDATE staging.elia_load SET {} = %s").format(
                psycopg.sql.Identifier(column)
            ),
            (value,),
        )
    assert snapshot(test_database) == before


@pytest.mark.parametrize(
    ("column", "value"),
    [("status", "UNKNOWN"), ("rows_loaded", -1)],
)
def test_batch_constraints(test_database, csv_file, rows, column, value):
    assert ingest_csv(csv_file(rows[:1])) == 1
    before = snapshot(test_database)
    with pytest.raises(psycopg.errors.CheckViolation), test_database.transaction():
        test_database.execute(
            psycopg.sql.SQL("UPDATE staging.ingestion_batch SET {} = %s").format(
                psycopg.sql.Identifier(column)
            ),
            (value,),
        )
    assert snapshot(test_database) == before
