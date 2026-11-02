"""Tracked dbt metadata on the disposable PostgreSQL test database."""

import os

import psycopg
import pytest
from postgres_support import TEST_DATABASE

import dbt_run_store as store

pytestmark = pytest.mark.integration


@pytest.fixture
def tracked_database(test_database, monkeypatch):
    monkeypatch.setattr(
        store,
        "get_connection_params",
        lambda: {
            "host": os.environ["TEST_PGHOST"],
            "port": os.environ["TEST_PGPORT"],
            "dbname": TEST_DATABASE,
            "user": os.environ["TEST_PGUSER"],
            "password": os.environ["TEST_PGPASSWORD"],
            "connect_timeout": 5,
        },
    )
    monkeypatch.setattr(store, "INGESTION_DATABASE", TEST_DATABASE)
    return test_database


def batch(conn, status="SUCCESS"):
    number = conn.execute("SELECT count(*) FROM staging.ingestion_batch").fetchone()[0]
    batch_id = conn.execute(
        "INSERT INTO staging.ingestion_batch (source_file, file_sha256, status) "
        "VALUES (%s, %s, %s) RETURNING batch_id",
        (f"source-{number}", f"{number:064x}", status),
    ).fetchone()[0]
    conn.commit()
    return batch_id


def test_success_watermark_and_history(tracked_database):
    first = batch(tracked_database)
    second = batch(tracked_database)
    assert store.latest_success() is None
    running = store.start_build(first)
    assert running.status == "RUNNING"
    assert running.finished_at is None
    assert store.latest_success() is None
    failed = store.mark_failed(running.build_id, "DBT_BUILD_FAILED")
    assert failed.status == "FAILED"
    assert failed.finished_at >= failed.started_at
    assert failed.error_type == "DBT_BUILD_FAILED"
    assert store.latest_success() is None
    retry = store.start_build(first)
    assert retry.build_id != running.build_id
    assert store.mark_success(retry.build_id).status == "SUCCESS"
    assert store.latest_success().target_batch_id == first
    later = store.start_build(second)
    store.mark_success(later.build_id)
    assert store.latest_success().target_batch_id == second
    assert [r.build_id for r in store.list_builds(first)] == [
        retry.build_id,
        running.build_id,
    ]
    assert [r.build_id for r in store.list_builds()] == [
        later.build_id,
        retry.build_id,
        running.build_id,
    ]


def test_later_success_on_older_batch_does_not_regress_watermark(tracked_database):
    older_batch = batch(tracked_database)
    newer_batch = batch(tracked_database)
    covered = store.mark_success(store.start_build(newer_batch).build_id)
    older_build = store.mark_success(store.start_build(older_batch).build_id)

    assert older_build.build_id > covered.build_id
    assert store.latest_success() == covered
    assert store.latest_success().target_batch_id == newer_batch
    retry = store.mark_success(store.start_build(newer_batch).build_id)
    assert store.latest_success() == retry


def test_invalid_targets_and_transitions(tracked_database):
    pending = batch(tracked_database, "RUNNING")
    failed_batch = batch(tracked_database, "FAILED")
    for target in (pending, failed_batch, -1):
        with pytest.raises(ValueError, match="SUCCESS ingestion batch"):
            store.start_build(target)
    first = store.start_build(batch(tracked_database))
    with pytest.raises(ValueError, match="approved failure category"):
        store.mark_failed(first.build_id, "password=secret")
    assert store.list_builds()[0].status == "RUNNING"
    store.mark_success(first.build_id)
    for transition in (
        lambda: store.mark_success(first.build_id),
        lambda: store.mark_failed(first.build_id, "INTERRUPTED"),
        lambda: store.mark_success(-1),
    ):
        with pytest.raises(ValueError, match="no longer RUNNING"):
            transition()
    second = store.start_build(first.target_batch_id)
    store.mark_failed(second.build_id, "INTERRUPTED")
    with pytest.raises(ValueError, match="no longer RUNNING"):
        store.mark_success(second.build_id)


@pytest.mark.parametrize(
    ("changes", "error"),
    [
        ("status='UNKNOWN'", psycopg.errors.CheckViolation),
        ("status='SUCCESS'", psycopg.errors.CheckViolation),
        (
            "status='FAILED', finished_at=clock_timestamp()",
            psycopg.errors.CheckViolation,
        ),
        ("finished_at=clock_timestamp()", psycopg.errors.CheckViolation),
        ("finished_at=started_at - interval '1 second'", psycopg.errors.CheckViolation),
        ("target_batch_id=-1", psycopg.errors.ForeignKeyViolation),
    ],
)
def test_sql_constraints_roll_back(tracked_database, changes, error):
    run = store.start_build(batch(tracked_database))
    with pytest.raises(error), tracked_database.transaction():
        tracked_database.execute(
            "UPDATE staging.dbt_build_run SET " + changes + " WHERE build_id=%s",
            (run.build_id,),
        )
    tracked_database.rollback()
    assert store.list_builds()[0] == run
