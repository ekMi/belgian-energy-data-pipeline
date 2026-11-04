"""Official API path on belgian_energy_test only; HTTP and dbt are simulated."""

from datetime import UTC, datetime, timedelta
from types import SimpleNamespace
from unittest.mock import Mock
from urllib.error import HTTPError

import pytest
from elia_api_support import api_transport, http_response

import acquire_elia
import api_pipeline
import dbt_guard
import dbt_run_store
import ingest_ods001
import local_pipeline
import merge_ods001
import orchestration_store
from correction_policy import API_CORRECTION_REASON

pytestmark = pytest.mark.integration
START = datetime(2025, 1, 1, tzinfo=UTC)
END = START + timedelta(days=1)


@pytest.fixture
def api_database(test_database, monkeypatch, tmp_path):
    # test_database rejects any non-test database/user before destructive setup.
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
    monkeypatch.setattr(
        acquire_elia, "urlopen", Mock(side_effect=AssertionError("Live HTTP forbidden"))
    )
    return test_database, run


def execute(tmp_path, rows, start=START, end=END):
    return api_pipeline.run_api_pipeline(
        start,
        end,
        output_dir=tmp_path / "api",
        opener=api_transport(rows),
        sleep=Mock(),
    )


def current(conn):
    return conn.execute(
        "SELECT * FROM staging.elia_load ORDER BY timestamp_utc"
    ).fetchall()


def test_new_unchanged_corrected_and_null_to_actual(
    api_database, csv_file, rows, tmp_path
):
    conn, dbt = api_database
    baseline = [row.copy() for row in rows[:3]]
    baseline[0][2] = ""
    ingest_ods001.ingest_csv(csv_file(baseline, "manual-baseline.csv"))
    before = current(conn)
    conn.commit()
    incoming = [row.copy() for row in rows]
    incoming[1][3] = "9000"  # A corrected forecast as well as NULL -> actual load.
    result = execute(tmp_path, incoming)
    assert result.status == "SUCCESS"
    assert (result.pipeline.rows_new, result.pipeline.rows_changed) == (1, 2)
    after = current(conn)
    assert len(after) == 4
    assert before[2] == after[2]  # UNCHANGED preserves its batch provenance.
    batch = conn.execute(
        "SELECT source_file,file_sha256,input_scope,correction_policy,"
        "correction_reason,extracted_at,source_reference,"
        "rows_received,rows_inserted,rows_unchanged,rows_updated "
        "FROM staging.ingestion_batch WHERE batch_id=%s",
        (result.pipeline.ingestion_batch_id,),
    ).fetchone()
    receipt = result.acquisition
    assert batch == (
        receipt.file_path.name,
        receipt.sha256,
        "partial",
        "authorized",
        API_CORRECTION_REASON,
        receipt.extracted_at,
        receipt.source_reference,
        4,
        1,
        1,
        2,
    )
    assert conn.execute(
        "SELECT old_total_load_mw,new_total_load_mw "
        "FROM staging.elia_load_correction ORDER BY timestamp_utc LIMIT 1"
    ).fetchone() == (None, 10000.0)
    assert conn.execute(
        "SELECT count(*) FROM staging.quality_run WHERE batch_id=%s",
        (result.pipeline.ingestion_batch_id,),
    ).fetchone() == (2,)
    assert conn.execute(
        "SELECT parameters->>'continuity' FROM staging.quality_run "
        "WHERE batch_id=%s AND scope='incoming_file'",
        (result.pipeline.ingestion_batch_id,),
    ).fetchone() == ("false",)
    assert receipt.file_path.is_file()
    dbt.assert_called_once()
    conn.commit()


@pytest.mark.parametrize("dbt_fails", [False, True])
def test_api_idempotence_and_missing_dbt_recovery(
    api_database, rows, tmp_path, dbt_fails
):
    conn, dbt = api_database
    dbt.return_value.returncode = int(dbt_fails)
    first = execute(tmp_path, rows)
    assert first.status == ("DBT_FAILED" if dbt_fails else "SUCCESS")
    assert conn.execute(
        "SELECT status FROM staging.ingestion_batch WHERE batch_id=%s",
        (first.pipeline.ingestion_batch_id,),
    ).fetchone() == ("SUCCESS",)
    assert len(current(conn)) == 4
    conn.commit()
    assert orchestration_store.get_analytics_sync_status().dbt_required is dbt_fails
    dbt.return_value.returncode = 0
    second = execute(tmp_path, rows)
    assert second.status == ("SUCCESS" if dbt_fails else "NO_CHANGE")
    assert second.acquisition.sha256 == first.acquisition.sha256
    assert second.acquisition.file_path != first.acquisition.file_path
    assert second.pipeline.ingestion_batch_id == first.pipeline.ingestion_batch_id
    assert execute(tmp_path, rows).status == "NO_CHANGE"
    assert conn.execute("SELECT count(*) FROM staging.ingestion_batch").fetchone() == (
        1,
    )
    assert conn.execute(
        "SELECT count(*) FROM staging.elia_load_correction"
    ).fetchone() == (0,)
    assert dbt.call_count == (2 if dbt_fails else 1)
    conn.commit()
    assert not orchestration_store.get_analytics_sync_status().dbt_required


def test_known_older_api_extraction_cannot_correct(
    api_database, csv_file, rows, tmp_path, monkeypatch
):
    conn, dbt = api_database
    ingest_ods001.ingest_csv(
        csv_file(rows[:1]), extracted_at=datetime(2025, 2, 1, tzinfo=UTC)
    )
    before = current(conn)
    conn.commit()

    class HistoricalClock(datetime):
        @classmethod
        def now(cls, tz=None):
            return datetime(2025, 1, 2, tzinfo=UTC)

    monkeypatch.setattr(acquire_elia, "datetime", HistoricalClock)
    rows[0][2] = "12000"
    with pytest.raises(ValueError, match="Stale extraction"):
        execute(tmp_path, rows[:1])
    assert current(conn) == before
    assert conn.execute(
        "SELECT count(*) FROM staging.elia_load_correction"
    ).fetchone() == (0,)
    assert conn.execute(
        "SELECT status,rows_updated,correction_reason FROM staging.ingestion_batch "
        "ORDER BY batch_id DESC LIMIT 1"
    ).fetchone() == ("FAILED", 0, API_CORRECTION_REASON)
    dbt.assert_not_called()
    conn.commit()


@pytest.mark.parametrize("failure", ["http", "validation"])
def test_failed_acquisition_has_no_database_effect(
    api_database, rows, tmp_path, failure
):
    conn, dbt = api_database
    before = current(conn)
    conn.commit()
    opener = Mock(
        side_effect=HTTPError("https://example.test", 403, "forbidden", {}, None)
    )
    if failure == "validation":
        opener = Mock(return_value=http_response(b"wrong columns\n"))
    with pytest.raises(acquire_elia.AcquisitionError):
        api_pipeline.run_api_pipeline(
            START, END, output_dir=tmp_path / "api", opener=opener, sleep=Mock()
        )
    assert current(conn) == before
    assert conn.execute("SELECT count(*) FROM staging.ingestion_batch").fetchone() == (
        0,
    )
    dbt.assert_not_called()
    conn.commit()


def test_untrusted_csv_api_policy_rejected_without_batch(api_database, csv_file, rows):
    conn, dbt = api_database
    with pytest.raises(ValueError, match="issued by acquire_elia"):
        local_pipeline.run_pipeline(
            csv_file(rows), input_scope="partial", source_policy="elia-api"
        )
    assert conn.execute("SELECT count(*) FROM staging.ingestion_batch").fetchone() == (
        0,
    )
    assert current(conn) == []
    dbt.assert_not_called()
    conn.commit()


def test_missing_rows_are_never_deleted(api_database, csv_file, rows, tmp_path):
    conn, _ = api_database
    ingest_ods001.ingest_csv(csv_file(rows, "baseline.csv"))
    before = current(conn)
    conn.commit()
    result = execute(tmp_path, rows[:1])
    assert result.status == "SUCCESS"
    assert (result.pipeline.rows_new, result.pipeline.rows_changed) == (0, 0)
    assert current(conn) == before
    conn.commit()


def test_future_null_load_and_absent_horizon_tail(api_database, rows, tmp_path):
    conn, _ = api_database
    start = datetime(2090, 1, 1, tzinfo=UTC)
    incoming = [row.copy() for row in rows[:2]]
    for row in incoming:
        row[0] = row[0].replace("2025-01-01", "2090-01-01")
        row[2] = ""
    result = execute(tmp_path, incoming, start=start, end=start + timedelta(days=8))
    assert result.status == "SUCCESS"
    assert [row[2] for row in current(conn)] == [None, None]
    assert conn.execute(
        "SELECT anomaly_count,severity FROM staging.quality_result "
        "WHERE check_id='completeness.2.pending'"
    ).fetchall() == [(2, "WARNING"), (2, "WARNING")]
    conn.commit()


def test_partial_dst_window_uses_normalized_utc(api_database, rows, tmp_path):
    conn, _ = api_database
    incoming = [row.copy() for row in rows[:2]]
    incoming[0][0] = "2025-10-26T02:00:00+02:00"
    incoming[1][0] = "2025-10-26T02:00:00+01:00"
    start = datetime(2025, 10, 26, tzinfo=UTC)
    assert (
        execute(tmp_path, incoming, start=start, end=start + timedelta(days=1)).status
        == "SUCCESS"
    )
    actual_utc = [row[0].astimezone(UTC) for row in current(conn)]

    assert actual_utc == [
        start,
        start + timedelta(hours=1),
    ]
    conn.commit()


def test_existing_schema_still_rejects_missing_required_forecast(
    api_database, rows, tmp_path
):
    conn, dbt = api_database
    rows[0][3] = ""
    with pytest.raises(ValueError, match="required field is empty"):
        execute(tmp_path, rows[:1])
    assert current(conn) == []
    assert conn.execute("SELECT count(*) FROM staging.ingestion_batch").fetchone() == (
        0,
    )
    dbt.assert_not_called()
    conn.commit()
