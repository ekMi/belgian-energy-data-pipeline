"""Actual parent/task/subflow states against a disposable Prefect SQLite server."""

import json
import os
import time
from pathlib import Path
from types import SimpleNamespace

import pytest

pytest.importorskip("prefect.flows", reason="Use .venv-prefect for Prefect tests")

from prefect.testing.utilities import prefect_test_harness  # noqa: E402
from src.prefect import api, inbox, pipeline  # noqa: E402


@pytest.fixture(scope="module")
def isolated_prefect():
    # The harness selects its own API URL, ephemeral port and SQLite database.
    # No request is sent to the user's server or PostgreSQL.
    with prefect_test_harness():
        yield


@pytest.mark.parametrize(
    "status,directory,completed",
    [
        ("SUCCESS", "archive", True),
        ("NO_CHANGE", "archive", True),
        ("DBT_FAILED", "failed", False),
        ("TECHNICAL_ERROR", "failed", False),
    ],
)
def test_parent_child_and_task_states(
    isolated_prefect, monkeypatch, tmp_path, status, directory, completed
):
    monkeypatch.setattr(inbox, "PROJECT_ROOT", tmp_path)
    monkeypatch.setattr(pipeline, "PROJECT_ROOT", tmp_path)
    monkeypatch.setattr(inbox, "STABILITY_SECONDS", 0)
    source = tmp_path / "data/inbox/export.csv"
    source.parent.mkdir(parents=True)
    source.write_text("mocked CSV", encoding="utf-8")
    old = time.time() - 120
    os.utime(source, (old, old))
    real_run = pipeline.subprocess.run
    calls = []

    def run(args, **kwargs):
        if len(args) < 3 or args[2] != str(tmp_path / "src/local_pipeline.py"):
            return real_run(args, **kwargs)
        calls.append(args)
        assert Path(args[3]).parent == tmp_path / "data/processing"
        assert Path(args[3]).exists()
        assert args[4:] == [
            "--source-policy",
            "elia-full",
            "--input-scope",
            "continuous",
        ]
        return SimpleNamespace(
            returncode=0 if status in {"SUCCESS", "NO_CHANGE"} else 1,
            stdout=json.dumps(
                {
                    "status": status,
                    "ingestion_batch_id": 4,
                    "build_id": 7,
                    "rows_new": 3,
                    "rows_changed": 2,
                }
            )
            if status != "TECHNICAL_ERROR"
            else "",
            stderr="mock CLI log",
        )

    monkeypatch.setattr(pipeline.subprocess, "run", run)
    state = inbox.energy_inbox(return_state=True)
    assert state.is_completed() is completed
    assert state.is_failed() is (not completed)
    from prefect.client.orchestration import get_client
    from prefect.client.schemas.filters import FlowRunFilter

    with get_client(sync_client=True) as client:
        children = client.read_flow_runs(
            flow_run_filter=FlowRunFilter(
                parent_flow_run_id={"any_": [state.state_details.flow_run_id]}
            )
        )
        assert len(children) == 1
        assert children[0].state.is_completed() is completed
        assert children[0].state.is_failed() is (not completed)
        tasks = client.read_task_runs(
            flow_run_filter=FlowRunFilter(id={"any_": [children[0].id]})
        )
        assert len(tasks) == 1
        assert tasks[0].state.is_completed() is completed
        assert tasks[0].state.is_failed() is (not completed)
    assert len(calls) == 1
    target = tmp_path / "data" / directory / "export.csv"
    assert target.read_text("utf-8") == "mocked CSV"
    report = json.loads(target.with_suffix(".csv.result.json").read_text("utf-8"))
    assert report["status"] == status
    assert report["stderr"] == "mock CLI log"
    if status != "TECHNICAL_ERROR":
        assert report["ingestion_batch_id"] == 4
        assert report["rows_changed"] == 2
    assert not (tmp_path / "data/pending_approval").exists()
    assert not list((tmp_path / "data/processing").glob("*.csv"))


@pytest.mark.parametrize(
    "source_policy,status",
    [("elia-full", "SUCCESS"), ("restricted", "AWAITING_APPROVAL")],
)
def test_manual_flow_retains_both_source_policies(
    isolated_prefect, monkeypatch, tmp_path, source_policy, status
):
    monkeypatch.setattr(pipeline, "PROJECT_ROOT", tmp_path)
    source = tmp_path / "manual.csv"
    source.write_text("mocked CSV", encoding="utf-8")
    real_run = pipeline.subprocess.run
    calls = []

    def run(args, **kwargs):
        if len(args) < 3 or args[2] != str(tmp_path / "src/local_pipeline.py"):
            return real_run(args, **kwargs)
        calls.append(args)
        return SimpleNamespace(
            returncode=0 if status == "SUCCESS" else 1,
            stdout=json.dumps({"status": status, "rows_changed": 2}),
            stderr="mock log",
        )

    monkeypatch.setattr(pipeline.subprocess, "run", run)
    state = pipeline.energy_pipeline(
        str(source), source_policy=source_policy, return_state=True
    )
    assert state.is_completed()
    assert state.result()["status"] == status
    assert state.result()["rows_changed"] == 2
    assert calls[0][4:] == [
        "--source-policy",
        source_policy,
        "--input-scope",
        "continuous",
    ]
    assert len(calls) == 1
    assert source.read_text("utf-8") == "mocked CSV"


@pytest.mark.parametrize(
    "status",
    [
        "SUCCESS",
        "NO_CHANGE",
        "DBT_FAILED",
        "ACQUISITION_FAILED",
        "VALIDATION_FAILED",
        "INGESTION_FAILED",
        "INVALID_JSON",
        "INTERRUPTED",
    ],
)
def test_api_flow_and_task_terminal_states(
    isolated_prefect, monkeypatch, tmp_path, status
):
    monkeypatch.setattr(api, "PROJECT_ROOT", tmp_path)
    real_run = pipeline.subprocess.run
    calls = []
    completed = status in {"SUCCESS", "NO_CHANGE"}
    start, end = "2025-10-26T00:00:00Z", "2025-10-26T02:00:00Z"

    def run(args, **kwargs):
        if len(args) < 3 or args[2] != str(tmp_path / "src/api_pipeline.py"):
            return real_run(args, **kwargs)
        calls.append(args)
        assert args == [
            str(tmp_path / ".venv/Scripts/python.exe"),
            "-B",
            str(tmp_path / "src/api_pipeline.py"),
            "--start",
            start,
            "--end",
            end,
        ]
        payload = {"status": status, "error_type": "SyntheticError"}
        if status in {"SUCCESS", "NO_CHANGE", "DBT_FAILED"}:
            payload = {
                "status": status,
                "acquisition": {
                    "file_path": "data/raw/api/mock.csv",
                    "row_count": 8,
                    "window_start_utc": start,
                    "window_end_utc": end,
                    "extracted_at": "2025-10-26T10:00:00Z",
                    "sha256": "a" * 64,
                    "source_reference": "https://external-elia.opendatasoft.com/api/explore/v2.1/catalog/datasets/ods001/exports/csv",
                },
                "pipeline": {
                    "status": status,
                    "ingestion_batch_id": 4,
                    "target_batch_id": None if status == "NO_CHANGE" else 4,
                    "build_id": None if status == "NO_CHANGE" else 7,
                    "rows_new": None if status == "NO_CHANGE" else 3,
                    "rows_changed": None if status == "NO_CHANGE" else 2,
                    "error_type": "DBT_BUILD_FAILED"
                    if status == "DBT_FAILED"
                    else None,
                },
            }
        return SimpleNamespace(
            returncode=0 if completed else 130 if status == "INTERRUPTED" else 1,
            stdout="" if status == "INVALID_JSON" else json.dumps(payload),
            stderr="mock API diagnostic",
        )

    monkeypatch.setattr(pipeline.subprocess, "run", run)
    state = api.energy_api(start=start, end=end, return_state=True)
    assert state.is_completed() is completed
    assert state.is_failed() is (not completed)
    result = state.result(raise_on_failure=False)
    if completed:
        assert result["status"] == status
    else:
        assert isinstance(result, pipeline.PipelineExecutionError)
        assert result.result["status"] == (
            "TECHNICAL_ERROR" if status == "INVALID_JSON" else status
        )
        assert result.result["stderr"] == "mock API diagnostic"
    assert len(calls) == 1  # No automatic repeat of the complete API pipeline.
    from prefect.client.orchestration import get_client
    from prefect.client.schemas.filters import FlowRunFilter, LogFilter
    from prefect.logging.handlers import APILogWorker

    # Log transport is batched independently of terminal flow/task states.
    # Drain deterministically instead of relying on a timing-sensitive sleep.
    APILogWorker.drain_all(timeout=5)
    with get_client(sync_client=True) as client:
        identifier = state.state_details.flow_run_id
        tasks = client.read_task_runs(
            flow_run_filter=FlowRunFilter(id={"any_": [identifier]})
        )
        assert len(tasks) == 1
        assert tasks[0].state.is_completed() is completed
        assert tasks[0].state.is_failed() is (not completed)
        logs = client.read_logs(
            log_filter=LogFilter(flow_run_id={"any_": [identifier]})
        )
        assert any("mock API diagnostic" in log.message for log in logs)
