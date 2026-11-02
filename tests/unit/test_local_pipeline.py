"""Orchestrator sequencing, failures and lock boundaries without real dbt."""

from contextlib import contextmanager
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import Mock

import pytest

import local_pipeline as pipeline


@pytest.fixture
def setup(monkeypatch):
    events = []
    state = SimpleNamespace(
        action="INGEST",
        successful_batch_id=None,
        file_sha256="a" * 64,
        rows_new=2,
        rows_changed=1,
    )
    monkeypatch.setattr(pipeline, "plan_csv", lambda path: state)
    ingestion = Mock(
        return_value=SimpleNamespace(batch_id=2, rows_inserted=2, rows_updated=0)
    )

    def ingest(*args, **kwargs):
        assert events.count("lock") == events.count("unlock")
        events.append("ingest")
        return ingestion(*args, **kwargs)

    monkeypatch.setattr(pipeline, "ingest_csv", ingest)
    conn = Mock()
    build = Mock(side_effect=lambda: events.append("build"))

    @contextmanager
    def locked():
        events.append("lock")
        try:
            yield SimpleNamespace(connection=conn, build=build)
        finally:
            events.append("unlock")

    monkeypatch.setattr(pipeline, "locked_build", locked)
    sync = SimpleNamespace(dbt_required=True, latest_ingestion_batch=2)

    def read_sync(*, connection):
        assert events[-1] == "lock"
        assert connection is conn
        return sync

    monkeypatch.setattr(pipeline, "get_analytics_sync_status", read_sync)
    start = Mock(return_value=SimpleNamespace(build_id=7))
    success, failed = Mock(), Mock()
    monkeypatch.setattr(pipeline, "start_build", start)
    monkeypatch.setattr(pipeline, "mark_success", success)
    monkeypatch.setattr(pipeline, "mark_failed", failed)
    return SimpleNamespace(**locals())


@pytest.mark.parametrize("known,pending", [(True, False), (True, True), (False, True)])
def test_pipeline_paths(setup, known, pending):
    s = setup
    if known:
        s.state.action = "NO_CHANGE"
        s.state.successful_batch_id = 1
        s.ingestion.return_value = None
    s.sync.dbt_required = pending
    result = pipeline.run_pipeline(Path("input.csv"))
    assert result.status == ("NO_CHANGE" if known and not pending else "SUCCESS")
    assert s.build.call_count == int(pending)
    assert s.success.call_count == int(pending)
    if pending:
        s.start.assert_called_once_with(2, connection=s.conn)
        s.success.assert_called_once_with(7, connection=s.conn)
        assert s.conn.commit.call_count == 2
    s.failed.assert_not_called()
    assert s.events[0] == "ingest"
    assert s.events[-1] == "unlock"


def test_new_file_with_previous_pending_build_rechecks_after_ingestion(setup):
    # The preview has no authority over the final analytical target.
    setup.state.dbt_required = True
    setup.sync.latest_ingestion_batch = 3
    result = pipeline.run_pipeline(Path("input.csv"))
    assert result.target_batch_id == 3
    assert setup.events == ["ingest", "lock", "build", "unlock"]
    setup.build.assert_called_once()


def test_blocked_corrections(setup):
    setup.state.action = "AWAITING_APPROVAL"
    result = pipeline.run_pipeline(Path("input.csv"))
    assert result.status == "AWAITING_APPROVAL"
    assert (result.rows_new, result.rows_changed) == (2, 1)
    assert setup.events == []
    setup.success.assert_not_called()


def test_corrections_appearing_after_preview(setup):
    setup.ingestion.side_effect = pipeline.ChangedObservationsError()
    assert pipeline.run_pipeline(Path("input.csv")).status == "AWAITING_APPROVAL"
    assert setup.events == ["ingest"]


@pytest.mark.parametrize("authorized", [False, True])
def test_build_failure_preserves_ingestion_and_can_retry(setup, authorized):
    options = {}
    if authorized:
        setup.state.action = "AWAITING_APPROVAL"
        setup.ingestion.return_value.rows_updated = 1
        options = dict(authorized_sha256="a" * 64, correction_reason="reviewed")
    setup.build.side_effect = RuntimeError("secret connection details")
    first = pipeline.run_pipeline(Path("input.csv"), **options)
    assert (first.rows_new, first.rows_changed) == (2, int(authorized))
    assert first.status == "DBT_FAILED"
    assert first.ingestion_batch_id == 2
    assert first.error_type == "DBT_BUILD_FAILED"
    setup.success.assert_not_called()
    setup.failed.assert_called_once_with(7, "DBT_BUILD_FAILED", connection=setup.conn)
    setup.ingestion.return_value = None
    setup.state.successful_batch_id = 2
    setup.state.action = "NO_CHANGE"
    setup.build.side_effect = None
    assert pipeline.run_pipeline(Path("input.csv")).status == "SUCCESS"
    setup.success.assert_called_once()


def test_ingestion_failure_does_not_build(setup):
    setup.ingestion.side_effect = ValueError("quality failed")
    with pytest.raises(ValueError):
        pipeline.run_pipeline(Path("input.csv"))
    setup.build.assert_not_called()
    setup.success.assert_not_called()


def test_interruption_never_marks_success(setup):
    setup.build.side_effect = KeyboardInterrupt()
    with pytest.raises(KeyboardInterrupt):
        pipeline.run_pipeline(Path("input.csv"))
    setup.success.assert_not_called()
    setup.failed.assert_called_once_with(7, "INTERRUPTED", connection=setup.conn)


@pytest.mark.parametrize(
    "status,exit_code", [("SUCCESS", 0), ("DBT_FAILED", 1), ("AWAITING_APPROVAL", 1)]
)
def test_cli_json_and_status(monkeypatch, capsys, status, exit_code):
    import json
    import sys

    monkeypatch.setattr(sys, "argv", ["local_pipeline", "input.csv"])
    monkeypatch.setattr(
        pipeline, "run_pipeline", lambda *a, **k: pipeline.PipelineResult(status)
    )
    if exit_code:
        with pytest.raises(SystemExit) as error:
            pipeline.main()
        assert error.value.code == exit_code
    else:
        pipeline.main()
    assert json.loads(capsys.readouterr().out)["status"] == status


@pytest.mark.parametrize("launch_failure", [False, True])
def test_real_guard_failure_never_marks_success(setup, monkeypatch, launch_failure):
    import dbt_guard

    connection = Mock(closed=False)
    session = dbt_guard.LockedDbtBuild(connection, {"PGPASSWORD": "secret"})
    run = Mock(return_value=SimpleNamespace(returncode=1))
    if launch_failure:
        run.side_effect = OSError("secret")
    monkeypatch.setattr(dbt_guard.subprocess, "run", run)
    setup.build.side_effect = session.build
    result = pipeline.run_pipeline(Path("input.csv"))
    assert result.status == "DBT_FAILED"
    setup.success.assert_not_called()
    setup.failed.assert_called_once_with(7, "DBT_BUILD_FAILED", connection=setup.conn)


@pytest.mark.parametrize(
    "authorization",
    [
        {"authorized_sha256": "a" * 64},
        {"authorized_sha256": "a" * 64, "correction_reason": " "},
        {"authorized_sha256": "invalid", "correction_reason": "reviewed"},
        {"correction_reason": "reviewed"},
    ],
)
def test_invalid_authorization_refused_before_preview(
    setup, monkeypatch, authorization
):
    preview = Mock()
    monkeypatch.setattr(pipeline, "plan_csv", preview)
    with pytest.raises(ValueError):
        pipeline.run_pipeline(Path("input.csv"), **authorization)
    preview.assert_not_called()
    setup.ingestion.assert_not_called()
    assert setup.events == []


@pytest.mark.parametrize("action", ["AWAITING_APPROVAL", "NO_CHANGE", "INGEST"])
def test_wrong_hash_refused_before_ingestion(setup, action):
    setup.state.action = action
    with pytest.raises(ValueError, match="does not match"):
        pipeline.run_pipeline(
            Path("input.csv"),
            authorized_sha256="b" * 64,
            correction_reason="reviewed",
        )
    setup.ingestion.assert_not_called()
    assert setup.events == []


def test_authorized_corrections_and_metadata_forwarded(setup):
    from datetime import UTC, datetime

    setup.state.action = "AWAITING_APPROVAL"
    setup.ingestion.return_value.rows_updated = 1
    options = dict(
        input_scope="partial",
        authorized_sha256="a" * 64,
        correction_reason="reviewed",
        extracted_at=datetime(2025, 2, 1, tzinfo=UTC),
        source_reference="Elia export",
    )
    path = Path("input.csv")
    result = pipeline.run_pipeline(path, **options)
    setup.ingestion.assert_called_once_with(path, **options)
    assert result.status == "SUCCESS"
    assert (result.rows_new, result.rows_changed) == (2, 1)
    assert setup.events == ["ingest", "lock", "build", "unlock"]


def test_authorization_does_not_override_ingestion_rejection(setup):
    setup.state.action = "AWAITING_APPROVAL"
    setup.ingestion.side_effect = ValueError("Stale extraction")
    with pytest.raises(ValueError, match="Stale"):
        pipeline.run_pipeline(
            Path("input.csv"),
            authorized_sha256="a" * 64,
            correction_reason="reviewed",
        )
    assert setup.events == ["ingest"]
    setup.build.assert_not_called()


def test_cli_forwards_authorization(monkeypatch):
    import sys
    from datetime import datetime

    monkeypatch.setattr(
        sys,
        "argv",
        [
            "local_pipeline",
            "input.csv",
            "--input-scope",
            "partial",
            "--authorize-sha256",
            "a" * 64,
            "--correction-reason",
            "reviewed",
            "--extracted-at",
            "2025-02-01T12:00:00+01:00",
            "--source-reference",
            "Elia export",
        ],
    )
    run = Mock(return_value=pipeline.PipelineResult("SUCCESS"))
    monkeypatch.setattr(pipeline, "run_pipeline", run)
    pipeline.main()
    run.assert_called_once_with(
        Path("input.csv"),
        input_scope="partial",
        authorized_sha256="a" * 64,
        correction_reason="reviewed",
        extracted_at=datetime.fromisoformat("2025-02-01T12:00:00+01:00"),
        source_reference="Elia export",
    )


def test_naive_extraction_date_refused_before_preview(setup, monkeypatch):
    from datetime import datetime

    preview = Mock()
    monkeypatch.setattr(pipeline, "plan_csv", preview)
    with pytest.raises(ValueError, match="UTC offset"):
        pipeline.run_pipeline(Path("input.csv"), extracted_at=datetime(2025, 2, 1))
    preview.assert_not_called()
    setup.ingestion.assert_not_called()
