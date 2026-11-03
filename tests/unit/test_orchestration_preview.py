import json
import sys
from pathlib import Path
from types import SimpleNamespace

import pytest

import orchestration_preview as orchestration

SHA256 = "a" * 64
CSV_PATH = Path("fake.csv")


@pytest.fixture(autouse=True)
def mock_sync_status(monkeypatch):
    monkeypatch.setattr(
        orchestration,
        "get_analytics_sync_status",
        lambda: SimpleNamespace(dbt_required=False),
    )


def test_already_ingested_skips_preview(monkeypatch):
    monkeypatch.setattr(
        orchestration,
        "calculate_sha256",
        lambda path: SHA256,
    )
    monkeypatch.setattr(
        orchestration,
        "find_successful_batch",
        lambda sha: 1,
    )

    def unexpected_preview(path):
        pytest.fail("Preview should not be called")

    monkeypatch.setattr(
        orchestration,
        "preview_csv",
        unexpected_preview,
    )

    plan = orchestration.plan_csv(CSV_PATH)

    assert plan.action == "NO_CHANGE"
    assert plan.successful_batch_id == 1
    assert plan.rows_new is None
    assert plan.rows_changed is None


def test_new_file_without_corrections(monkeypatch):
    monkeypatch.setattr(
        orchestration,
        "calculate_sha256",
        lambda path: SHA256,
    )
    monkeypatch.setattr(
        orchestration,
        "find_successful_batch",
        lambda sha: None,
    )

    preview = SimpleNamespace(
        file_sha256=SHA256,
        rows_new=96,
        rows_changed=0,
    )

    monkeypatch.setattr(
        orchestration,
        "preview_csv",
        lambda path: preview,
    )

    plan = orchestration.plan_csv(CSV_PATH)

    assert plan.action == "INGEST"
    assert plan.rows_new == 96
    assert plan.rows_changed == 0
    assert plan.successful_batch_id is None


def test_new_file_with_corrections(monkeypatch):
    monkeypatch.setattr(
        orchestration,
        "calculate_sha256",
        lambda path: SHA256,
    )
    monkeypatch.setattr(
        orchestration,
        "find_successful_batch",
        lambda sha: None,
    )

    preview = SimpleNamespace(
        file_sha256=SHA256,
        rows_new=96,
        rows_changed=3,
    )

    monkeypatch.setattr(
        orchestration,
        "preview_csv",
        lambda path: preview,
    )

    plan = orchestration.plan_csv(CSV_PATH)

    assert plan.action == "AWAITING_APPROVAL"
    assert plan.rows_new == 96
    assert plan.rows_changed == 3
    assert plan.successful_batch_id is None


def test_sha_mismatch_between_calculation_and_preview(monkeypatch):
    monkeypatch.setattr(
        orchestration,
        "calculate_sha256",
        lambda path: SHA256,
    )
    monkeypatch.setattr(
        orchestration,
        "find_successful_batch",
        lambda sha: None,
    )

    preview = SimpleNamespace(
        file_sha256="b" * 64,
        rows_new=96,
        rows_changed=3,
    )

    monkeypatch.setattr(
        orchestration,
        "preview_csv",
        lambda path: preview,
    )

    with pytest.raises(RuntimeError, match="CSV changed during preview"):
        orchestration.plan_csv(CSV_PATH)


def test_cli_outputs_json(monkeypatch, capsys):
    expected = orchestration.PipelinePlan(
        file_sha256=SHA256,
        successful_batch_id=1,
        rows_new=None,
        rows_changed=None,
        action="NO_CHANGE",
    )

    def fake_plan_csv(path, **kwargs):
        assert path == CSV_PATH
        assert kwargs == {"source_policy": "restricted", "input_scope": "continuous"}
        return expected

    monkeypatch.setattr(
        orchestration,
        "plan_csv",
        fake_plan_csv,
    )

    monkeypatch.setattr(
        sys,
        "argv",
        ["orchestration_preview.py", str(CSV_PATH)],
    )

    orchestration.main()

    output = capsys.readouterr().out
    result = json.loads(output)

    assert result["action"] == "NO_CHANGE"
    assert result["successful_batch_id"] == 1
    assert result["rows_new"] is None


def test_already_ingested_but_dbt_pending(monkeypatch):
    monkeypatch.setattr(
        orchestration,
        "calculate_sha256",
        lambda path: SHA256,
    )

    monkeypatch.setattr(
        orchestration,
        "find_successful_batch",
        lambda sha: 2,
    )

    monkeypatch.setattr(
        orchestration,
        "get_analytics_sync_status",
        lambda: SimpleNamespace(dbt_required=True),
    )

    def unexpected_preview(path):
        pytest.fail("Preview should not be called")

    monkeypatch.setattr(
        orchestration,
        "preview_csv",
        unexpected_preview,
    )

    plan = orchestration.plan_csv(CSV_PATH)

    assert plan.successful_batch_id == 2
    assert plan.action == "NO_CHANGE"
    assert plan.dbt_required is True


@pytest.mark.parametrize("changed,action", [(0, "INGEST"), (3, "AWAITING_APPROVAL")])
def test_pending_dbt_never_overrides_csv_decision(monkeypatch, changed, action):
    monkeypatch.setattr(orchestration, "calculate_sha256", lambda path: SHA256)
    monkeypatch.setattr(orchestration, "find_successful_batch", lambda sha: None)
    monkeypatch.setattr(
        orchestration,
        "get_analytics_sync_status",
        lambda: SimpleNamespace(dbt_required=True),
    )
    monkeypatch.setattr(
        orchestration,
        "preview_csv",
        lambda path: SimpleNamespace(
            file_sha256=SHA256, rows_new=96, rows_changed=changed
        ),
    )
    plan = orchestration.plan_csv(CSV_PATH)
    assert plan.action == action
    assert plan.dbt_required is True


def test_trusted_preview_with_changes_plans_ingestion(monkeypatch):
    monkeypatch.setattr(orchestration, "calculate_sha256", lambda path: SHA256)
    monkeypatch.setattr(orchestration, "find_successful_batch", lambda sha: None)
    monkeypatch.setattr(
        orchestration,
        "preview_csv",
        lambda path: SimpleNamespace(
            file_sha256=SHA256,
            rows_new=96,
            rows_changed=3,
        ),
    )
    plan = orchestration.plan_csv(CSV_PATH, source_policy="elia-full")
    assert (plan.action, plan.rows_new, plan.rows_changed) == ("INGEST", 96, 3)
