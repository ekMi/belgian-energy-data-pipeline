from datetime import UTC, datetime

import pytest

import ingest_ods001
from check_quality_ods001 import analyze_batch

pytestmark = pytest.mark.integration


def test_warning_persisted_and_historical_read_only(test_database, csv_file, rows):
    assert ingest_ods001.ingest_csv(csv_file(rows)) == 4
    batch = test_database.execute(
        "SELECT batch_id FROM staging.ingestion_batch"
    ).fetchone()[0]
    before = test_database.execute(
        "SELECT * FROM staging.elia_load ORDER BY timestamp_utc"
    ).fetchall()
    assert (
        test_database.execute(
            "SELECT anomaly_count FROM staging.quality_result WHERE check_id='business.3.interval'"
        ).fetchone()[0]
        == 4
    )
    results = analyze_batch(
        test_database, batch, as_of=datetime(2025, 1, 3, tzinfo=UTC)
    )
    assert any(r.anomaly_count for r in results)
    assert (
        test_database.execute("SELECT count(*) FROM staging.quality_run").fetchone()[0]
        == 2
    )
    analyze_batch(
        test_database, batch, as_of=datetime(2025, 1, 3, tzinfo=UTC), persist=True
    )
    assert (
        test_database.execute("SELECT count(*) FROM staging.quality_run").fetchone()[0]
        == 3
    )
    assert (
        test_database.execute(
            "SELECT * FROM staging.elia_load ORDER BY timestamp_utc"
        ).fetchall()
        == before
    )


def test_blocking_quality_rolls_back_and_retains_report(test_database, csv_file, rows):
    rows[0][0] = "2025-01-01T01:01:00+01:00"
    with pytest.raises(ValueError, match="Blocking"):
        ingest_ods001.ingest_csv(csv_file(rows))
    assert (
        test_database.execute("SELECT count(*) FROM staging.elia_load").fetchone()[0]
        == 0
    )
    assert (
        test_database.execute("SELECT status FROM staging.ingestion_batch").fetchone()[
            0
        ]
        == "FAILED"
    )
    assert (
        test_database.execute(
            "SELECT anomaly_count FROM staging.quality_result WHERE check_id='technical.quarter_hour'"
        ).fetchone()[0]
        == 1
    )


def test_invalid_header_traced(test_database, csv_file):
    path = csv_file([])
    with pytest.raises(ValueError):
        ingest_ods001.ingest_csv(path)
    assert test_database.execute(
        "SELECT status,rows_loaded,finished_at>started_at FROM staging.ingestion_batch"
    ).fetchone() == ("FAILED", 0, True)


def test_failure_after_copy(test_database, csv_file, rows, monkeypatch):
    original = ingest_ods001.save_results

    def fail_success(conn, batch_id, results, **kwargs):
        if kwargs["scope"] == "incoming_file":
            raise RuntimeError("Synthetic report failure after COPY")
        return original(conn, batch_id, results, **kwargs)

    monkeypatch.setattr(ingest_ods001, "save_results", fail_success)
    with pytest.raises(RuntimeError):
        ingest_ods001.ingest_csv(csv_file(rows))
    assert (
        test_database.execute("SELECT count(*) FROM staging.elia_load").fetchone()[0]
        == 0
    )
    assert (
        test_database.execute("SELECT status FROM staging.ingestion_batch").fetchone()[
            0
        ]
        == "FAILED"
    )


def test_report_outage_keeps_failed_status(test_database, csv_file, rows, monkeypatch):
    def unavailable(*args, **kwargs):
        raise RuntimeError("Synthetic report outage")

    monkeypatch.setattr(ingest_ods001, "save_results", unavailable)
    with pytest.raises(RuntimeError) as error:
        ingest_ods001.ingest_csv(csv_file(rows))
    assert "incomplete" in error.value.__notes__[0]
    assert (
        test_database.execute("SELECT count(*) FROM staging.elia_load").fetchone()[0]
        == 0
    )
    assert (
        test_database.execute("SELECT status FROM staging.ingestion_batch").fetchone()[
            0
        ]
        == "FAILED"
    )
