"""Full local pipeline on the disposable database; dbt subprocess is simulated."""

from datetime import UTC, datetime
from types import SimpleNamespace
from unittest.mock import Mock

import pytest

import dbt_guard
import dbt_run_store
import ingest_ods001
import local_pipeline
import merge_ods001
import orchestration_store
from validate_ods001 import calculate_sha256

pytestmark = pytest.mark.integration


@pytest.fixture
def pipeline_database(test_database, monkeypatch, tmp_path):
    params = ingest_ods001.get_connection_params()
    for module in (dbt_guard, dbt_run_store, orchestration_store, merge_ods001):
        monkeypatch.setattr(module, "get_connection_params", lambda: params.copy())
        if hasattr(module, "INGESTION_DATABASE"):
            monkeypatch.setattr(module, "INGESTION_DATABASE", "belgian_energy_test")
    monkeypatch.setattr(dbt_guard, "_verify_profile", lambda: None)
    executable = tmp_path / "simulated-dbt"
    executable.touch()
    monkeypatch.setattr(dbt_guard, "DBT_PYTHON", executable)
    run = Mock(return_value=SimpleNamespace(returncode=0))
    monkeypatch.setattr(dbt_guard.subprocess, "run", run)
    return test_database, run


@pytest.mark.parametrize("fail", [False, True, "launch"])
def test_ingestion_build_retry_and_noop(pipeline_database, csv_file, rows, fail):
    conn, run = pipeline_database
    path = csv_file(rows)
    if fail == "launch":
        run.side_effect = OSError("simulated Windows launch failure")
    elif fail:
        run.return_value.returncode = 1
    first = local_pipeline.run_pipeline(path)
    assert first.status == ("DBT_FAILED" if fail else "SUCCESS")
    assert conn.execute(
        "SELECT status FROM staging.ingestion_batch WHERE batch_id=%s",
        (first.ingestion_batch_id,),
    ).fetchone() == ("SUCCESS",)
    conn.commit()
    assert orchestration_store.get_analytics_sync_status().dbt_required is bool(fail)
    assert dbt_run_store.list_builds()[0].status == ("FAILED" if fail else "SUCCESS")
    run.side_effect = None
    run.return_value.returncode = 0
    second = local_pipeline.run_pipeline(path)
    assert second.status == ("SUCCESS" if fail else "NO_CHANGE")
    assert run.call_count == (2 if fail else 1)
    assert local_pipeline.run_pipeline(path).status == "NO_CHANGE"
    assert not orchestration_store.get_analytics_sync_status().dbt_required


@pytest.mark.parametrize("kind", ["new", "corrections", "mixed"])
@pytest.mark.parametrize("dbt_fails", [False, True])
def test_automatic_full_export_history_and_replay(
    pipeline_database, csv_file, rows, kind, dbt_fails
):
    from correction_policy import AUTOMATIC_CORRECTION_REASON

    conn, run = pipeline_database
    if kind != "new":
        ingest_ods001.ingest_csv(csv_file(rows[:2], "baseline.csv"))
    before = conn.execute(
        "SELECT * FROM staging.elia_load ORDER BY timestamp_utc"
    ).fetchall()
    conn.commit()
    incoming = [row.copy() for row in (rows[:2] if kind == "corrections" else rows)]
    if kind != "new":
        incoming[0][2] = "10001"
    path = csv_file(incoming, "full.csv")
    run.return_value.returncode = int(dbt_fails)
    result = local_pipeline.run_pipeline(path, source_policy="elia-full")
    assert result.status == ("DBT_FAILED" if dbt_fails else "SUCCESS")
    expected_new = {"new": 4, "corrections": 0, "mixed": 2}[kind]
    expected_changed = int(kind != "new")
    assert (result.rows_new, result.rows_changed) == (expected_new, expected_changed)
    assert conn.execute(
        "SELECT status,correction_policy,correction_authorized_sha256,"
        "correction_reason,extracted_at FROM staging.ingestion_batch WHERE batch_id=%s",
        (result.ingestion_batch_id,),
    ).fetchone() == (
        "SUCCESS",
        "authorized",
        calculate_sha256(path),
        AUTOMATIC_CORRECTION_REASON,
        None,
    )
    after = conn.execute(
        "SELECT * FROM staging.elia_load ORDER BY timestamp_utc"
    ).fetchall()
    history = conn.execute(
        "SELECT previous_batch_id,correction_batch_id,"
        + ",".join("old_" + measure for measure in merge_ods001.MEASURES)
        + ","
        + ",".join("new_" + measure for measure in merge_ods001.MEASURES)
        + " FROM staging.elia_load_correction"
    ).fetchall()
    assert len(history) == expected_changed
    if expected_changed:
        assert history[0][:2] == (before[0][-1], result.ingestion_batch_id)
        assert history[0][2:10] == before[0][2:10]
        assert history[0][10:] == after[0][2:10]
        assert before[1] == after[1]  # Preserve unchanged values and provenance.
    batches = conn.execute("SELECT count(*) FROM staging.ingestion_batch").fetchone()
    conn.commit()
    run.return_value.returncode = 0
    assert local_pipeline.run_pipeline(path, source_policy="elia-full").status == (
        "SUCCESS" if dbt_fails else "NO_CHANGE"
    )
    assert (
        local_pipeline.run_pipeline(path, source_policy="elia-full").status
        == "NO_CHANGE"
    )
    assert (
        conn.execute("SELECT count(*) FROM staging.ingestion_batch").fetchone()
        == batches
    )
    assert conn.execute(
        "SELECT count(*) FROM staging.elia_load_correction"
    ).fetchone() == (expected_changed,)
    conn.commit()


@pytest.mark.parametrize("failure", ["invalid", "quality", "stale", "consolidated"])
def test_automatic_policy_preserves_data_on_rejection(
    pipeline_database, csv_file, rows, monkeypatch, failure
):
    from quality_ods001 import QualityResult

    conn, run = pipeline_database
    ingest_ods001.ingest_csv(
        csv_file(rows[:2], "baseline.csv"),
        extracted_at=datetime(2025, 2, 1, tzinfo=UTC),
    )
    before = conn.execute(
        "SELECT * FROM staging.elia_load ORDER BY timestamp_utc"
    ).fetchall()
    conn.commit()
    rows[0][2] = "10001"
    path = csv_file(rows, "full.csv")
    options = {"source_policy": "elia-full"}
    if failure == "invalid":
        path.write_text("invalid header\n", encoding="utf-8")
    elif failure == "stale":
        options["extracted_at"] = datetime(2025, 1, 1, tzinfo=UTC)
    else:
        blocking = [QualityResult("test.blocking", "ERROR", 1, 1, [])]
        if failure == "quality":
            monkeypatch.setattr(
                ingest_ods001, "evaluate_quality", lambda *a, **k: blocking
            )
        else:
            monkeypatch.setattr(
                ingest_ods001, "check_consolidated", lambda *a, **k: (blocking, {})
            )
    with pytest.raises(ValueError):
        local_pipeline.run_pipeline(path, **options)
    run.assert_not_called()
    assert (
        conn.execute(
            "SELECT * FROM staging.elia_load ORDER BY timestamp_utc"
        ).fetchall()
        == before
    )
    assert conn.execute(
        "SELECT count(*) FROM staging.elia_load_correction"
    ).fetchone() == (0,)
    conn.commit()


def test_partial_export_still_waits_for_manual_authorization(
    pipeline_database, csv_file, rows
):
    conn, run = pipeline_database
    ingest_ods001.ingest_csv(csv_file(rows[:2], "baseline.csv"))
    rows[0][2] = "10001"
    result = local_pipeline.run_pipeline(
        csv_file(rows, "partial.csv"), input_scope="partial"
    )
    assert result.status == "AWAITING_APPROVAL"
    run.assert_not_called()
    assert conn.execute(
        "SELECT count(*) FROM staging.elia_load_correction"
    ).fetchone() == (0,)
    conn.commit()


def test_new_csv_ingested_before_pending_rebuild(pipeline_database, csv_file, rows):
    conn, run = pipeline_database
    ingest_ods001.ingest_csv(csv_file(rows))
    assert orchestration_store.get_analytics_sync_status().dbt_required
    added = [row.copy() for row in rows]
    for row in added:
        row[0] = row[0].replace("T01:", "T02:")
    result = local_pipeline.run_pipeline(csv_file(rows + added, "extended.csv"))
    assert result.status == "SUCCESS"
    assert result.target_batch_id == result.ingestion_batch_id
    run.assert_called_once()
    assert conn.execute("SELECT count(*) FROM staging.elia_load").fetchone() == (8,)
    conn.commit()


def test_corrections_do_not_ingest_or_build(pipeline_database, csv_file, rows):
    conn, run = pipeline_database
    ingest_ods001.ingest_csv(csv_file(rows))
    rows[0][2] = "10001"
    result = local_pipeline.run_pipeline(csv_file(rows, "correction.csv"))
    assert result.status == "AWAITING_APPROVAL"
    run.assert_not_called()
    assert conn.execute("SELECT count(*) FROM staging.ingestion_batch").fetchone() == (
        1,
    )
    conn.commit()


@pytest.mark.parametrize("fail", [False, True])
def test_authorized_corrections_history_retry_and_noop(
    pipeline_database, csv_file, rows, fail
):
    conn, run = pipeline_database
    baseline = ingest_ods001.ingest_csv(csv_file(rows[:2], "baseline.csv"))
    rows[0][2] = "10001"
    path = csv_file(rows, "correction.csv")
    options = dict(
        authorized_sha256=calculate_sha256(path),
        correction_reason="Historical Elia corrections reviewed",
        extracted_at=datetime(2025, 2, 1, tzinfo=UTC),
        source_reference="Elia export",
    )
    run.return_value.returncode = int(fail)
    result = local_pipeline.run_pipeline(path, **options)
    assert result.status == ("DBT_FAILED" if fail else "SUCCESS")
    assert (result.rows_new, result.rows_changed) == (2, 1)
    run.assert_called_once()
    assert conn.execute(
        "SELECT status,rows_inserted,rows_updated,correction_authorized_sha256,"
        "correction_reason,extracted_at,source_reference "
        "FROM staging.ingestion_batch WHERE batch_id=%s",
        (result.ingestion_batch_id,),
    ).fetchone() == (
        "SUCCESS",
        2,
        1,
        options["authorized_sha256"],
        options["correction_reason"],
        options["extracted_at"],
        options["source_reference"],
    )
    assert conn.execute(
        "SELECT previous_batch_id,correction_batch_id,"
        "old_total_load_mw,new_total_load_mw FROM staging.elia_load_correction"
    ).fetchall() == [(baseline.batch_id, result.ingestion_batch_id, 10000, 10001)]
    conn.commit()
    assert dbt_run_store.list_builds()[0].status == ("FAILED" if fail else "SUCCESS")
    run.return_value.returncode = 0
    # A committed correction needs no new authorization to retry dbt.
    assert local_pipeline.run_pipeline(path).status == (
        "SUCCESS" if fail else "NO_CHANGE"
    )
    assert local_pipeline.run_pipeline(path, **options).status == "NO_CHANGE"
    assert run.call_count == (2 if fail else 1)
    assert conn.execute(
        "SELECT count(*) FROM staging.elia_load_correction"
    ).fetchone() == (1,)
    assert conn.execute("SELECT count(*) FROM staging.ingestion_batch").fetchone() == (
        2,
    )
    conn.commit()


@pytest.mark.parametrize("rejection", ["hash", "reason", "stale", "quality"])
def test_invalid_or_blocked_authorization_preserves_observations(
    pipeline_database, csv_file, rows, rejection
):
    conn, run = pipeline_database
    ingest_ods001.ingest_csv(
        csv_file(rows[:2], "baseline.csv"),
        extracted_at=datetime(2025, 2, 1, tzinfo=UTC),
    )
    before = conn.execute(
        "SELECT * FROM staging.elia_load ORDER BY timestamp_utc"
    ).fetchall()
    conn.commit()
    rows[0][2] = "10001"
    if rejection == "quality":
        rows[0][4] = ""  # mandatory most_recent_forecast_mw
    path = csv_file(rows, "correction.csv")
    options = dict(
        authorized_sha256=calculate_sha256(path), correction_reason="reviewed"
    )
    if rejection == "hash":
        options["authorized_sha256"] = "0" * 64
    elif rejection == "reason":
        options.pop("correction_reason")
    elif rejection == "stale":
        options["extracted_at"] = datetime(2025, 1, 1, tzinfo=UTC)
    with pytest.raises(ValueError):
        local_pipeline.run_pipeline(path, **options)
    run.assert_not_called()
    assert (
        conn.execute(
            "SELECT * FROM staging.elia_load ORDER BY timestamp_utc"
        ).fetchall()
        == before
    )
    assert conn.execute(
        "SELECT count(*) FROM staging.elia_load_correction"
    ).fetchone() == (0,)
    if rejection in ("hash", "reason"):
        assert conn.execute(
            "SELECT count(*) FROM staging.ingestion_batch"
        ).fetchone() == (1,)
    conn.commit()
