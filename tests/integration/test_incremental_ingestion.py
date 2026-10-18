from concurrent.futures import ThreadPoolExecutor
from contextlib import contextmanager
from threading import Event

import pytest

import ingest_ods001
from ingest_ods001 import INGESTION_LOCK, ChangedObservationsError, ingest_csv
from merge_ods001 import preview_csv

pytestmark = pytest.mark.integration


def observations(conn):
    return conn.execute(
        "SELECT * FROM staging.elia_load ORDER BY timestamp_utc"
    ).fetchall()


def counters(conn):
    return conn.execute(
        "SELECT status,rows_loaded,rows_received,rows_inserted,rows_unchanged,rows_updated "
        "FROM staging.ingestion_batch ORDER BY batch_id DESC LIMIT 1"
    ).fetchone()


def test_new_unchanged_and_mixed(test_database, csv_file, rows):
    assert ingest_csv(csv_file(rows[:2], "first.csv")) == 2
    assert counters(test_database) == ("SUCCESS", 2, 2, 2, 0, 0)
    old = observations(test_database)
    assert ingest_csv(csv_file(rows[2:], "new.csv")) == 2
    assert counters(test_database) == ("SUCCESS", 2, 2, 2, 0, 0)
    assert ingest_csv(csv_file(rows[::-1], "reordered.csv")) == 4
    assert counters(test_database) == ("SUCCESS", 4, 4, 0, 4, 0)
    assert observations(test_database)[:2] == old
    extra = rows[0].copy()
    extra[0] = "2025-01-02T01:00:00+01:00"
    assert (
        ingest_csv(csv_file([rows[0], extra], "mixed.csv"), input_scope="partial") == 2
    )
    assert counters(test_database) == ("SUCCESS", 2, 2, 1, 1, 0)
    assert len(observations(test_database)) == 5
    assert observations(test_database)[:2] == old
    assert (
        test_database.execute(
            "SELECT count(*) FROM staging.elia_load_correction"
        ).fetchone()[0]
        == 0
    )


@pytest.mark.parametrize(("old", "new"), [("10000", "12345"), ("", "123"), ("123", "")])
def test_changed_rejects_entire_batch(test_database, csv_file, rows, old, new):
    baseline = rows[0].copy()
    baseline[2] = old
    ingest_csv(csv_file([baseline], "baseline.csv"))
    before = observations(test_database)
    changed = baseline.copy()
    changed[2] = new
    with pytest.raises(ChangedObservationsError):
        ingest_csv(csv_file([rows[1], changed], "changed.csv"))
    assert observations(test_database) == before
    assert counters(test_database) == ("FAILED", 0, 2, 0, 0, 0)
    assert (
        test_database.execute(
            "SELECT count(*) FROM staging.elia_load_correction"
        ).fetchone()[0]
        == 0
    )
    assert (
        test_database.execute(
            "SELECT anomaly_count FROM staging.quality_result "
            "WHERE check_id='technical.corrections_forbidden'"
        ).fetchone()[0]
        == 1
    )
    # Failed ingestion must release the session lock and permit a later load.
    assert ingest_csv(csv_file([rows[1]], "valid.csv")) == 1


def test_partial_scope_recorded_in_quality(test_database, csv_file, rows):
    ingest_csv(csv_file([rows[0], rows[3]]), input_scope="partial")
    assert (
        test_database.execute(
            "SELECT input_scope FROM staging.ingestion_batch"
        ).fetchone()[0]
        == "partial"
    )
    assert (
        test_database.execute(
            "SELECT parameters->'continuity' FROM staging.quality_run"
        ).fetchone()[0]
        is False
    )
    assert (
        test_database.execute(
            "SELECT count(*) FROM staging.quality_result WHERE check_id='technical.continuity'"
        ).fetchone()[0]
        == 0
    )


def test_preview_is_not_reused(test_database, csv_file, rows, monkeypatch):
    import merge_ods001

    monkeypatch.setattr(
        merge_ods001, "get_connection_params", ingest_ods001.get_connection_params
    )
    path = csv_file(rows)
    assert preview_csv(path).rows_new == 4
    ingest_csv(csv_file(rows[:1], "baseline.csv"))
    ingest_csv(path)
    assert counters(test_database) == ("SUCCESS", 4, 4, 3, 1, 0)


@pytest.mark.parametrize("same_file", [True, False])
def test_concurrent_ingestions_are_serialized(
    test_database, csv_file, rows, monkeypatch, same_file
):
    first = csv_file(rows[:2], "first.csv")
    second = first if same_file else csv_file(rows[1:], "second.csv")
    first_classified = Event()
    release_first = Event()
    second_attempted = Event()
    second_acquired = Event()
    real_classify = ingest_ods001.classify_incoming
    real_lock = ingest_ods001.ingestion_lock

    def classify(conn, checksum):
        result = real_classify(conn, checksum)
        if not first_classified.is_set():
            first_classified.set()
            if not release_first.wait(5):
                raise TimeoutError("Test failed to release first ingestion")
        return result

    @contextmanager
    def watched_lock(conn):
        is_second = first_classified.is_set()
        if is_second:
            second_attempted.set()
        with real_lock(conn):
            if is_second:
                second_acquired.set()
            yield

    monkeypatch.setattr(ingest_ods001, "classify_incoming", classify)
    monkeypatch.setattr(ingest_ods001, "ingestion_lock", watched_lock)
    with ThreadPoolExecutor(max_workers=2) as pool:
        one = pool.submit(ingest_csv, first)
        try:
            assert first_classified.wait(5)
            two = pool.submit(ingest_csv, second)
            assert second_attempted.wait(5)
            # A third real connection confirms the session lock remains held
            # after the RUNNING commit and during the data transaction.
            locked = test_database.execute(
                "SELECT pg_try_advisory_lock(%s,%s)", INGESTION_LOCK
            ).fetchone()[0]
            if locked:
                test_database.execute(
                    "SELECT pg_advisory_unlock(%s,%s)", INGESTION_LOCK
                )
            assert not locked
            assert not second_acquired.is_set()
        finally:
            release_first.set()
        assert one.result(timeout=10) == 2
        assert two.result(timeout=10) == (None if same_file else 3)
    assert second_acquired.is_set()
    assert len(observations(test_database)) == (2 if same_file else 4)
    assert test_database.execute(
        "SELECT count(*) FROM staging.ingestion_batch WHERE status='SUCCESS'"
    ).fetchone()[0] == (1 if same_file else 2)
    if not same_file:
        assert counters(test_database) == ("SUCCESS", 3, 3, 2, 1, 0)
