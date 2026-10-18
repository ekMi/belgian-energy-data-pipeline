import pytest

import ingest_ods001

pytestmark = pytest.mark.integration


def test_keyboard_interrupt_rolls_back_and_tracks_failed(
    test_database, csv_file, rows, monkeypatch
):
    def interrupt(*args, **kwargs):
        raise KeyboardInterrupt()

    monkeypatch.setattr(ingest_ods001, "check_consolidated", interrupt)
    with pytest.raises(KeyboardInterrupt):
        ingest_ods001.ingest_csv(csv_file(rows))
    assert (
        test_database.execute("SELECT count(*) FROM staging.elia_load").fetchone()[0]
        == 0
    )
    assert test_database.execute(
        "SELECT status,error_type FROM staging.ingestion_batch"
    ).fetchone() == ("FAILED", "KeyboardInterrupt")
    # Session advisory lock was released during exception unwinding.
    acquired = test_database.execute(
        "SELECT pg_try_advisory_lock(%s,%s)", ingest_ods001.INGESTION_LOCK
    ).fetchone()[0]
    try:
        assert acquired
    finally:
        if acquired:
            test_database.execute(
                "SELECT pg_advisory_unlock(%s,%s)", ingest_ods001.INGESTION_LOCK
            )
