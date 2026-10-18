from concurrent.futures import ThreadPoolExecutor
from contextlib import contextmanager
from datetime import UTC, datetime
from threading import Event

import psycopg
import pytest

import ingest_ods001
from ingest_ods001 import ingest_csv
from merge_ods001 import MEASURES
from validate_ods001 import calculate_sha256

pytestmark = pytest.mark.integration


def authorize(path, **kwargs):
    return ingest_csv(
        path,
        authorized_sha256=calculate_sha256(path),
        correction_reason="Reviewed exact source file",
        **kwargs,
    )


def current(conn):
    return conn.execute(
        "SELECT * FROM staging.elia_load ORDER BY timestamp_utc"
    ).fetchall()


def counts(conn):
    return conn.execute(
        "SELECT status,rows_received,rows_inserted,rows_unchanged,rows_updated,rows_loaded "
        "FROM staging.ingestion_batch ORDER BY batch_id DESC LIMIT 1"
    ).fetchone()


@pytest.mark.parametrize("index", range(2, 10))
def test_single_measure_history(test_database, csv_file, rows, index):
    ingest_csv(csv_file(rows[:2], "baseline.csv"))
    before = current(test_database)
    reports = test_database.execute(
        "SELECT * FROM staging.quality_result ORDER BY quality_run_id,check_id"
    ).fetchall()
    changed = rows[0].copy()
    changed[index] = "42"
    path = csv_file([changed, rows[1], rows[2]], "correction.csv")
    assert authorize(path) == 3
    assert counts(test_database) == ("SUCCESS", 3, 1, 1, 1, 3)
    after = current(test_database)
    assert after[1] == before[1]  # unchanged provenance and values
    history = test_database.execute(
        "SELECT previous_batch_id,correction_batch_id,corrected_at,"
        + ",".join("old_" + m for m in MEASURES)
        + ","
        + ",".join("new_" + m for m in MEASURES)
        + " FROM staging.elia_load_correction"
    ).fetchone()
    assert history[0] == before[0][-1]
    assert history[1] == after[0][-1]
    assert history[0] != history[1] and history[2] is not None
    assert history[3:11] == before[0][2:10]
    assert history[11:19] == after[0][2:10]
    assert after[0][index] == 42
    assert (
        test_database.execute(
            "SELECT * FROM staging.quality_result WHERE quality_run_id=1 ORDER BY quality_run_id,check_id"
        ).fetchall()
        == reports
    )
    assert test_database.execute(
        "SELECT correction_policy,correction_authorized_sha256,correction_reason "
        "FROM staging.ingestion_batch ORDER BY batch_id DESC LIMIT 1"
    ).fetchone() == ("authorized", calculate_sha256(path), "Reviewed exact source file")


@pytest.mark.parametrize("index", [2, 9])
@pytest.mark.parametrize(("old", "new"), [("", "123"), ("123", "")])
def test_authorized_null_transitions(test_database, csv_file, rows, index, old, new):
    record = rows[0].copy()
    record[index] = old
    ingest_csv(csv_file([record], "baseline.csv"))
    record[index] = new
    authorize(csv_file([record], "correction.csv"))
    measure = MEASURES[index - 2]
    assert test_database.execute(
        f"SELECT old_{measure},new_{measure} FROM staging.elia_load_correction"
    ).fetchone() == (float(old) if old else None, float(new) if new else None)


def test_successive_and_multiple_corrections_and_old_replay(
    test_database, csv_file, rows
):
    baseline = csv_file(rows[:2], "baseline.csv")
    ingest_csv(baseline)
    changed = [r.copy() for r in rows[:2]]
    for r in changed:
        r[2] = "12000"
    first = csv_file(changed, "first.csv")
    authorize(first)
    for r in changed:
        r[2] = "13000"
    authorize(csv_file(changed, "second.csv"))
    history = test_database.execute(
        "SELECT previous_batch_id,correction_batch_id,old_total_load_mw,new_total_load_mw "
        "FROM staging.elia_load_correction ORDER BY correction_batch_id,timestamp_utc"
    ).fetchall()
    assert history == [
        (1, 2, 10000.0, 12000.0),
        (1, 2, 10000.0, 12000.0),
        (2, 3, 12000.0, 13000.0),
        (2, 3, 12000.0, 13000.0),
    ]
    before = current(test_database)
    assert ingest_csv(baseline) is None
    assert authorize(first) is None
    assert current(test_database) == before
    assert (
        test_database.execute(
            "SELECT count(*) FROM staging.ingestion_batch"
        ).fetchone()[0]
        == 3
    )


def test_wrong_hash_even_on_replay(test_database, csv_file, rows):
    path = csv_file(rows)
    ingest_csv(path)
    before = current(test_database)
    with pytest.raises(ValueError, match="does not match"):
        ingest_csv(path, authorized_sha256="0" * 64, correction_reason="reviewed")
    assert current(test_database) == before
    assert (
        test_database.execute(
            "SELECT count(*) FROM staging.ingestion_batch"
        ).fetchone()[0]
        == 1
    )


def test_known_stale_rejected(test_database, csv_file, rows):
    ingest_csv(
        csv_file(rows[:1], "baseline.csv"),
        extracted_at=datetime(2025, 2, 1, tzinfo=UTC),
    )
    changed = rows[0].copy()
    changed[2] = "42"
    before = current(test_database)
    with pytest.raises(ValueError, match="Stale"):
        authorize(csv_file([changed]), extracted_at=datetime(2025, 1, 1, tzinfo=UTC))
    assert current(test_database) == before
    assert counts(test_database) == ("FAILED", 1, 0, 0, 0, 0)
    assert (
        test_database.execute(
            "SELECT count(*) FROM staging.elia_load_correction"
        ).fetchone()[0]
        == 0
    )


@pytest.mark.parametrize(
    "phase", ["after_history", "during_update", "after_update", "report"]
)
def test_correction_rollback(test_database, csv_file, rows, monkeypatch, phase):
    ingest_csv(csv_file(rows[:1], "baseline.csv"))
    before = current(test_database)
    changed = rows[0].copy()
    changed[2] = "42"
    if phase == "report":
        original = ingest_ods001.save_results

        def broken(conn, batch, results, **kwargs):
            if kwargs["scope"] == "incoming_file":
                raise RuntimeError("injected")
            return original(conn, batch, results, **kwargs)

        monkeypatch.setattr(ingest_ods001, "save_results", broken)
    else:
        original = ingest_ods001.update_corrections

        def broken(conn, batch):
            assert (
                conn.execute(
                    "SELECT count(*) FROM staging.elia_load_correction"
                ).fetchone()[0]
                == 1
            )
            if phase == "during_update":
                conn.execute(
                    "UPDATE staging.elia_load SET most_recent_forecast_mw=NULL"
                )
            if phase == "after_update":
                original(conn, batch)
            raise RuntimeError("injected")

        monkeypatch.setattr(ingest_ods001, "update_corrections", broken)
    expected = (
        psycopg.errors.NotNullViolation if phase == "during_update" else RuntimeError
    )
    with pytest.raises(expected):
        authorize(csv_file([changed, rows[1]], "correction.csv"))
    assert current(test_database) == before
    assert counts(test_database) == ("FAILED", 2, 0, 0, 0, 0)
    assert (
        test_database.execute(
            "SELECT count(*) FROM staging.elia_load_correction"
        ).fetchone()[0]
        == 0
    )


def test_concurrent_corrections_recheck_freshness(
    test_database, csv_file, rows, monkeypatch
):
    ingest_csv(csv_file(rows[:1], "baseline.csv"))
    changed = rows[0].copy()
    changed[2] = "12000"
    first = csv_file([changed], "newer.csv")
    changed[2] = "13000"
    second = csv_file([changed], "older.csv")
    archived = Event()
    release = Event()
    attempted = Event()
    original_archive = ingest_ods001.archive_corrections
    original_lock = ingest_ods001.ingestion_lock

    def pause(conn, batch, **kwargs):
        result = original_archive(conn, batch, **kwargs)
        archived.set()
        if not release.wait(5):
            raise TimeoutError("test timeout")
        return result

    @contextmanager
    def watched(conn):
        if archived.is_set():
            attempted.set()
        with original_lock(conn):
            yield

    monkeypatch.setattr(ingest_ods001, "archive_corrections", pause)
    monkeypatch.setattr(ingest_ods001, "ingestion_lock", watched)
    with ThreadPoolExecutor(max_workers=2) as pool:
        one = pool.submit(
            authorize, first, extracted_at=datetime(2025, 3, 1, tzinfo=UTC)
        )
        try:
            assert archived.wait(5)
            two = pool.submit(
                authorize, second, extracted_at=datetime(2025, 2, 1, tzinfo=UTC)
            )
            assert attempted.wait(5)
        finally:
            release.set()
        assert one.result(timeout=10) == 1
        with pytest.raises(ValueError, match="Stale"):
            two.result(timeout=10)
    assert current(test_database)[0][2] == 12000.0
    assert (
        test_database.execute(
            "SELECT count(*) FROM staging.elia_load_correction"
        ).fetchone()[0]
        == 1
    )


def test_unknown_date_does_not_erase_known_history(test_database, csv_file, rows):
    record = rows[0].copy()
    ingest_csv(
        csv_file([record], "baseline.csv"),
        extracted_at=datetime(2025, 3, 1, tzinfo=UTC),
    )
    record[2] = "12000"
    authorize(csv_file([record], "unknown_date.csv"))
    record[2] = "13000"
    with pytest.raises(ValueError, match="Stale"):
        authorize(
            csv_file([record], "older.csv"),
            extracted_at=datetime(2025, 2, 1, tzinfo=UTC),
        )
    assert current(test_database)[0][2] == 12000.0
    assert (
        test_database.execute(
            "SELECT count(*) FROM staging.elia_load_correction"
        ).fetchone()[0]
        == 1
    )
