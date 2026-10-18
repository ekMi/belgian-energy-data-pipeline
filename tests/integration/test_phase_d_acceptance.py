from datetime import UTC, datetime

import pytest

import ingest_ods001
from check_quality_ods001 import analyze_batch, historical_reports
from ingest_ods001 import ChangedObservationsError, ingest_csv
from quality_ods001 import QualityResult
from validate_ods001 import calculate_sha256

pytestmark = pytest.mark.integration


def latest_batch(conn):
    return conn.execute("SELECT max(batch_id) FROM staging.ingestion_batch").fetchone()[
        0
    ]


def report(conn, batch, check):
    return conn.execute(
        "SELECT r.anomaly_count,q.parameters FROM staging.quality_run q "
        "JOIN staging.quality_result r USING (quality_run_id) "
        "WHERE q.batch_id=%s AND q.scope='consolidated_range' AND r.check_id=%s",
        (batch, check),
    ).fetchone()


def correct(path):
    return ingest_csv(
        path,
        authorized_sha256=calculate_sha256(path),
        correction_reason="Acceptance reviewed source",
        input_scope="partial",
    )


def test_phase_d_end_to_end(test_database, csv_file, rows):
    first = csv_file(rows[:2], "first.csv")
    assert ingest_csv(first) == 2
    original_reports = historical_reports(test_database, 1)
    assert len(original_reports) == 2
    assert ingest_csv(csv_file(rows, "extended.csv")) == 4
    assert ingest_csv(csv_file(rows[::-1], "unchanged.csv")) == 4
    before = test_database.execute(
        "SELECT * FROM staging.elia_load ORDER BY timestamp_utc"
    ).fetchall()
    fixed = rows[1].copy()
    fixed[3] = "8200"  # Fix one forecast without modifying the baseline fixture.
    assert correct(csv_file([fixed], "correction.csv")) == 1
    batch = latest_batch(test_database)
    assert report(test_database, batch, "business.3.interval")[0] == 0
    assert report(test_database, batch, "technical.consolidated_matches_input")[0] == 0
    after = test_database.execute(
        "SELECT * FROM staging.elia_load ORDER BY timestamp_utc"
    ).fetchall()
    assert after[0] == before[0] and after[2:] == before[2:]
    assert after[1][3] == 8200
    assert after[1][-1] == batch
    assert historical_reports(test_database, 1) == original_reports
    refused = fixed.copy()
    refused[2] = "9999"
    with pytest.raises(ChangedObservationsError):
        ingest_csv(csv_file([refused], "refused.csv"))
    assert ingest_csv(first) is None
    assert (
        test_database.execute(
            "SELECT * FROM staging.elia_load ORDER BY timestamp_utc"
        ).fetchall()
        == after
    )
    assert (
        test_database.execute(
            "SELECT count(*) FROM staging.elia_load_correction"
        ).fetchone()[0]
        == 1
    )


def test_partial_has_no_false_gaps_and_uses_neighbors(test_database, csv_file, rows):
    ingest_csv(csv_file(rows, "full.csv"))
    ingest_csv(csv_file([rows[1], rows[3]], "partial.csv"), input_scope="partial")
    batch = latest_batch(test_database)
    count, params = report(test_database, batch, "technical.consolidated_continuity")
    assert count == 0
    assert params["selected_key_count"] == 2
    assert params["edge_count"] == 3  # Actual 00->15,15->30,30->45 edges.
    assert params["evaluation_state"] == "committed"


def test_real_gap_at_boundary_and_repair(test_database, csv_file, rows):
    ingest_csv(csv_file([rows[0]], "initial.csv"))
    ingest_csv(csv_file([rows[2]], "gap.csv"), input_scope="partial")
    assert (
        report(
            test_database,
            latest_batch(test_database),
            "technical.consolidated_continuity",
        )[0]
        == 1
    )
    ingest_csv(csv_file([rows[1]], "fill.csv"), input_scope="partial")
    count, params = report(
        test_database, latest_batch(test_database), "technical.consolidated_continuity"
    )
    assert count == 0 and params["edge_count"] == 2


def test_diagnostic_empty_current_provenance_retains_history(
    test_database, csv_file, rows
):
    ingest_csv(csv_file(rows[:1], "initial.csv"))
    original = historical_reports(test_database, 1)
    changed = rows[0].copy()
    changed[2] = "12345"
    correct(csv_file([changed], "corrected.csv"))
    results = analyze_batch(test_database, 1, as_of=datetime(2025, 1, 3, tzinfo=UTC))
    assert all(r.evaluated_count == 0 for r in results)
    assert historical_reports(test_database, 1) == original
    assert not any(r.check_id == "technical.continuity" for r in results)


def test_post_merge_error_rolls_back_history_and_retains_failed_report(
    test_database, csv_file, rows, monkeypatch
):
    ingest_csv(csv_file(rows[:1], "initial.csv"))
    before = test_database.execute("SELECT * FROM staging.elia_load").fetchall()
    original_reports = historical_reports(test_database, 1)
    original = ingest_ods001.check_consolidated

    def fail(conn, **kwargs):
        results, params = original(conn, **kwargs)
        results.append(QualityResult("technical.injected_failure", "ERROR", 1, 1, []))
        return results, params

    monkeypatch.setattr(ingest_ods001, "check_consolidated", fail)
    changed = rows[0].copy()
    changed[2] = "12345"
    with pytest.raises(ValueError, match="consolidated"):
        correct(csv_file([changed, rows[1]], "failed.csv"))
    assert test_database.execute("SELECT * FROM staging.elia_load").fetchall() == before
    assert (
        test_database.execute(
            "SELECT count(*) FROM staging.elia_load_correction"
        ).fetchone()[0]
        == 0
    )
    assert historical_reports(test_database, 1) == original_reports
    batch = latest_batch(test_database)
    assert (
        test_database.execute(
            "SELECT status FROM staging.ingestion_batch WHERE batch_id=%s", (batch,)
        ).fetchone()[0]
        == "FAILED"
    )
    count, params = report(test_database, batch, "technical.injected_failure")
    assert count == 1 and params["evaluation_state"] == "rolled_back"


def test_consolidated_report_storage_failure_rolls_back(
    test_database, csv_file, rows, monkeypatch
):
    ingest_csv(csv_file(rows[:1], "initial.csv"))
    before = test_database.execute("SELECT * FROM staging.elia_load").fetchall()
    original = ingest_ods001.save_results

    def fail(conn, batch, results, **kwargs):
        if kwargs["scope"] == "consolidated_range":
            raise RuntimeError("Injected consolidated report outage")
        return original(conn, batch, results, **kwargs)

    monkeypatch.setattr(ingest_ods001, "save_results", fail)
    with pytest.raises(RuntimeError, match="outage"):
        ingest_csv(csv_file(rows, "failed.csv"))
    assert test_database.execute("SELECT * FROM staging.elia_load").fetchall() == before
    assert (
        test_database.execute(
            "SELECT status FROM staging.ingestion_batch ORDER BY batch_id DESC LIMIT 1"
        ).fetchone()[0]
        == "FAILED"
    )


def test_distant_unaffected_history_is_not_rechecked(test_database, csv_file, rows):
    far = rows[0].copy()
    far[0] = "2020-01-01T01:00:00+01:00"
    ingest_csv(csv_file([far, *rows], "baseline.csv"), input_scope="partial")
    # The old distant gap exists, but neither end is incident to these keys.
    ingest_csv(csv_file([rows[1], rows[2]], "local.csv"), input_scope="partial")
    count, params = report(
        test_database, latest_batch(test_database), "technical.consolidated_continuity"
    )
    assert count == 0
    assert params["selected_key_count"] == 2
    assert params["edge_count"] == 3
