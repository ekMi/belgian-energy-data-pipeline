"""Migration 04 on populated test-only schemas; no incremental ingestion."""

import csv
from datetime import UTC, datetime

import psycopg
import pytest
from postgres_support import verify_test_database

from config import PROJECT_ROOT
from ingest_ods001 import ingest_csv
from quality_ods001 import evaluate_quality
from quality_store import COLUMNS, save_results
from validate_ods001 import calculate_sha256, parse_row

pytestmark = pytest.mark.integration

NEW_COLUMNS = (
    "rows_received, rows_inserted, rows_unchanged, rows_updated, extracted_at, "
    "source_reference, input_scope, correction_policy, "
    "correction_authorized_sha256, correction_reason"
)
METRICS = (
    "total_load_mw",
    "most_recent_forecast_mw",
    "most_recent_p10_mw",
    "most_recent_p90_mw",
    "day_ahead_forecast_mw",
    "day_ahead_p10_mw",
    "day_ahead_p90_mw",
    "week_ahead_forecast_mw",
)


def apply_migration(conn):
    verify_test_database(conn)
    conn.commit()
    # The actual migration owns its BEGIN/COMMIT, including failure rollback.
    conn.autocommit = True
    try:
        conn.execute((PROJECT_ROOT / "sql/04_add_incremental_tracking.sql").read_text())
    except Exception:
        conn.execute("ROLLBACK")
        raise
    finally:
        conn.autocommit = False


def snapshot(conn):
    ordering = {
        "elia_load": "timestamp_utc, resolution_code",
        "ingestion_batch": "batch_id",
        "quality_run": "quality_run_id",
        "quality_result": "quality_run_id, check_id",
    }
    return {
        name: conn.execute(
            psycopg.sql.SQL("SELECT to_jsonb(t) FROM staging.{} t ORDER BY {}").format(
                psycopg.sql.Identifier(name), psycopg.sql.SQL(order)
            )
        ).fetchall()
        for name, order in ordering.items()
    }


def seed_legacy(conn, path):
    batch = conn.execute(
        "INSERT INTO staging.ingestion_batch (source_file,file_sha256) "
        "VALUES (%s,%s) RETURNING batch_id",
        (path.name, calculate_sha256(path)),
    ).fetchone()[0]
    with path.open(encoding="utf-8", newline="") as stream:
        reader = csv.reader(stream, delimiter=";")
        next(reader)
        records = [parse_row(r, line_number=reader.line_num) for r in reader]
    with (
        conn.cursor() as cur,
        cur.copy(
            f"COPY staging.elia_load ({COLUMNS},ingestion_batch_id) FROM STDIN"
        ) as copy,
    ):
        for record in records:
            copy.write_row((*record, batch))
    now = datetime.now(UTC)
    save_results(
        conn,
        batch,
        evaluate_quality(records, as_of=now),
        as_of=now,
        scope="incoming_file",
    )
    conn.execute(
        "UPDATE staging.ingestion_batch SET status='SUCCESS',rows_loaded=%s,"
        "finished_at=clock_timestamp() WHERE batch_id=%s",
        (len(records), batch),
    )
    conn.commit()
    return len(records)


@pytest.fixture
def migrated(test_database, csv_file, rows):
    assert seed_legacy(test_database, csv_file(rows)) == 4
    apply_migration(test_database)
    return test_database


def test_preserves_populated_database(test_database, csv_file, rows):
    assert seed_legacy(test_database, csv_file(rows)) == 4
    # Keep historical FAILED and RUNNING records, also with unknown metadata.
    test_database.execute(
        "INSERT INTO staging.ingestion_batch (source_file,file_sha256,status) "
        "VALUES ('failed',%s,'FAILED'),('running',%s,'RUNNING')",
        ("b" * 64, "c" * 64),
    )
    before = snapshot(test_database)
    apply_migration(test_database)
    after = snapshot(test_database)
    for table in ("elia_load", "quality_run", "quality_result"):
        assert after[table] == before[table]
    old_batches = [r[0] for r in before["ingestion_batch"]]
    new_batches = [r[0] for r in after["ingestion_batch"]]
    assert [{k: r[k] for k in old_batches[0]} for r in new_batches] == old_batches
    assert (
        test_database.execute(
            f"SELECT {NEW_COLUMNS} FROM staging.ingestion_batch"
        ).fetchall()
        == [(None,) * 10] * 3
    )
    assert (
        test_database.execute(
            "SELECT count(*) FROM staging.elia_load_correction"
        ).fetchone()[0]
        == 0
    )
    # Existing step-C ingestion is still usable after the additive migration.
    new_rows = [r.copy() for r in rows]
    for row in new_rows:
        row[0] = row[0].replace("2025-01-01", "2025-01-02")
    assert ingest_csv(csv_file(new_rows, "next.csv")) == 4


def test_duplicate_success_precheck_rolls_back(test_database, csv_file, rows):
    seed_legacy(test_database, csv_file(rows))
    test_database.execute(
        "INSERT INTO staging.ingestion_batch (source_file,file_sha256,status) "
        "SELECT 'duplicate',file_sha256,'SUCCESS' FROM staging.ingestion_batch"
    )
    before = snapshot(test_database)
    with pytest.raises(psycopg.errors.RaiseException, match="duplicate SUCCESS"):
        apply_migration(test_database)
    assert snapshot(test_database) == before
    assert (
        test_database.execute(
            "SELECT to_regclass('staging.elia_load_correction')"
        ).fetchone()[0]
        is None
    )
    assert (
        test_database.execute(
            "SELECT count(*) FROM information_schema.columns WHERE table_schema='staging' "
            "AND table_name='ingestion_batch' AND column_name='rows_received'"
        ).fetchone()[0]
        == 0
    )


def test_success_hash_uniqueness_allows_failed_retries(migrated):
    checksum = migrated.execute(
        "SELECT file_sha256 FROM staging.ingestion_batch"
    ).fetchone()[0]
    migrated.execute(
        "INSERT INTO staging.ingestion_batch (source_file,file_sha256,status) "
        "VALUES ('retry',%s,'FAILED'),('retry',%s,'RUNNING')",
        (checksum, checksum),
    )
    with pytest.raises(psycopg.errors.UniqueViolation), migrated.transaction():
        migrated.execute(
            "UPDATE staging.ingestion_batch SET status='SUCCESS' WHERE status='RUNNING'"
        )


@pytest.mark.parametrize(
    "assignment",
    [
        "rows_received=4",
        "rows_received=-1,rows_inserted=-1,rows_unchanged=0,rows_updated=0",
        "rows_received=4,rows_inserted=3,rows_unchanged=0,rows_updated=0",
        "rows_received=3,rows_inserted=3,rows_unchanged=0,rows_updated=0",
        "input_scope='unknown'",
        "correction_policy='unknown'",
        "correction_policy='authorized'",
        "correction_reason='unbound reason'",
        "correction_policy='authorized',correction_reason=' ',"
        "correction_authorized_sha256=file_sha256",
        "correction_policy='authorized',correction_reason='reviewed',"
        "correction_authorized_sha256=repeat('0',64)",
        "status='FAILED',rows_received=4,rows_inserted=4,rows_unchanged=0,rows_updated=0",
    ],
)
def test_invalid_batch_contract_rejected(migrated, assignment):
    with pytest.raises(psycopg.errors.CheckViolation), migrated.transaction():
        migrated.execute("UPDATE staging.ingestion_batch SET " + assignment)


def test_valid_counts_and_bound_authorization(migrated):
    migrated.execute(
        "UPDATE staging.ingestion_batch SET rows_received=4,rows_inserted=1,"
        "rows_unchanged=2,rows_updated=1,input_scope='partial',"
        "correction_policy='authorized',correction_authorized_sha256=file_sha256,"
        "correction_reason='Reviewed source correction'"
    )
    assert migrated.execute(
        "SELECT rows_loaded,rows_received,extracted_at FROM staging.ingestion_batch"
    ).fetchone() == (4, 4, None)


def correction_payload(conn):
    old = conn.execute(
        "SELECT timestamp_utc,resolution_code,ingestion_batch_id,"
        + ",".join(METRICS)
        + " FROM staging.elia_load ORDER BY timestamp_utc LIMIT 1"
    ).fetchone()
    batch = conn.execute(
        "INSERT INTO staging.ingestion_batch (source_file,file_sha256) "
        "VALUES ('correction',%s) RETURNING batch_id",
        ("d" * 64,),
    ).fetchone()[0]
    return [old[0], old[1], old[2], batch, *old[3:], *old[3:]]


def insert_correction(conn, values):
    columns = [
        "timestamp_utc",
        "resolution_code",
        "previous_batch_id",
        "correction_batch_id",
    ]
    columns += ["old_" + m for m in METRICS] + ["new_" + m for m in METRICS]
    conn.execute(
        psycopg.sql.SQL(
            "INSERT INTO staging.elia_load_correction ({}) VALUES ({})"
        ).format(
            psycopg.sql.SQL(",").join(map(psycopg.sql.Identifier, columns)),
            psycopg.sql.SQL(",").join([psycopg.sql.Placeholder()] * len(values)),
        ),
        values,
    )


@pytest.mark.parametrize("transition", ["value_to_null", "null_to_value"])
def test_typed_history_and_null_transitions(migrated, transition):
    values = correction_payload(migrated)
    values[4 if transition == "null_to_value" else 12] = None
    insert_correction(migrated, values)
    stored = migrated.execute(
        "SELECT old_total_load_mw,new_total_load_mw,corrected_at "
        "FROM staging.elia_load_correction"
    ).fetchone()
    assert stored[:2] == (values[4], values[12])
    assert stored[2] is not None
    with pytest.raises(psycopg.errors.UniqueViolation), migrated.transaction():
        insert_correction(migrated, values)


@pytest.mark.parametrize(
    ("case", "error"),
    [
        ("no_change", psycopg.errors.CheckViolation),
        ("same_batch", psycopg.errors.CheckViolation),
        ("missing_batch", psycopg.errors.ForeignKeyViolation),
        ("required_null", psycopg.errors.NotNullViolation),
        ("wrong_resolution", psycopg.errors.CheckViolation),
    ],
)
def test_history_constraints(migrated, case, error):
    values = correction_payload(migrated)
    if case != "no_change":
        values[12] = 42.0
    if case == "same_batch":
        values[3] = values[2]
    elif case == "missing_batch":
        values[3] = -1
    elif case == "required_null":
        values[13] = None
    elif case == "wrong_resolution":
        values[1] = "PT60M"
    with pytest.raises(error), migrated.transaction():
        insert_correction(migrated, values)


def test_second_application_fails_without_data_loss(migrated):
    before = snapshot(migrated)
    with pytest.raises(psycopg.errors.DuplicateColumn):
        apply_migration(migrated)
    assert snapshot(migrated) == before
