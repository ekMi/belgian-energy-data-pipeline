"""Local file lifecycle and mocked CLI only: no PostgreSQL or Prefect API."""

import json
import os
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import Mock

import pytest

pytest.importorskip("prefect.flows", reason="Use .venv-prefect for Prefect tests")

from src.prefect import inbox, pipeline  # noqa: E402


@pytest.fixture
def local(monkeypatch, tmp_path):
    monkeypatch.setattr(inbox, "PROJECT_ROOT", tmp_path)
    monkeypatch.setattr(pipeline, "PROJECT_ROOT", tmp_path)
    monkeypatch.setattr(inbox, "STABILITY_SECONDS", 0)
    for name in inbox.DIRECTORIES:
        (tmp_path / "data" / name).mkdir(parents=True)
    # Any unexpected CLI call fails instead of touching a real database.
    run = Mock(side_effect=AssertionError("Unexpected subprocess"))
    monkeypatch.setattr(pipeline.subprocess, "run", run)
    return tmp_path, run


def ready(root, name="export.csv", content="csv content"):
    path = root / "data/inbox" / name
    path.write_text(content, encoding="utf-8")
    old = time.time() - 120
    os.utime(path, (old, old))
    return path


def test_empty_inbox(local, monkeypatch):
    call = Mock()
    monkeypatch.setattr(inbox, "energy_pipeline", call)
    assert inbox.energy_inbox.fn() == {"status": "EMPTY"}
    call.assert_not_called()


def test_one_csv_in_deterministic_order(local, monkeypatch):
    root, _ = local
    ready(root, "b.csv")
    first = ready(root, "A.csv")
    call = Mock(return_value={"status": "SUCCESS"})
    monkeypatch.setattr(inbox, "energy_pipeline", call)
    inbox.energy_inbox.fn()
    call.assert_called_once_with(str(root / "data/processing/A.csv"))
    assert not first.exists()
    assert (root / "data/inbox/b.csv").exists()


@pytest.mark.parametrize(
    "name", ["export.csv.tmp", "export.part.csv", ".export.csv", "~export.csv"]
)
def test_temporary_file_ignored(local, name):
    root, _ = local
    source = ready(root, name)
    assert inbox.claim_csv(root / "data") is None
    assert source.exists()


@pytest.mark.parametrize("empty", [False, True])
def test_recent_or_empty_csv_ignored(local, empty):
    root, _ = local
    source = ready(root, content="" if empty else "csv")
    if not empty:
        os.utime(source, None)
    assert inbox.claim_csv(root / "data") is None
    assert source.exists()


def test_file_changing_during_sample_ignored(local, monkeypatch):
    root, _ = local
    source = ready(root)
    monkeypatch.setattr(inbox.time, "sleep", lambda _: source.write_text("copying"))
    assert inbox.claim_csv(root / "data") is None
    assert source.exists()


def test_open_windows_file_deferred(local, monkeypatch):
    root, _ = local
    source = ready(root)
    monkeypatch.setattr(inbox, "move_csv", Mock(side_effect=PermissionError))
    assert inbox.claim_csv(root / "data") is None
    assert source.exists()


@pytest.mark.parametrize(
    "status,directory",
    [
        ("SUCCESS", "archive"),
        ("NO_CHANGE", "archive"),
        ("AWAITING_APPROVAL", "pending_approval"),
    ],
)
def test_business_classification_and_diagnostic(local, monkeypatch, status, directory):
    root, _ = local
    source = ready(root)
    result = {"status": status, "ingestion_batch_id": 4, "rows_new": 3}

    def call(path):
        assert Path(path).parent == root / "data/processing"
        assert Path(path).read_bytes() == b"csv content"
        assert not source.exists()
        assert not list((root / "data" / directory).iterdir())
        return result

    monkeypatch.setattr(inbox, "energy_pipeline", call)
    report = inbox.energy_inbox.fn()
    target = root / "data" / directory / source.name
    assert target.read_bytes() == b"csv content"
    assert report["classified_path"] == str(target)
    assert (
        json.loads(target.with_suffix(".csv.result.json").read_text("utf-8")) == report
    )
    assert not list((root / "data/processing").glob("*.csv"))


@pytest.mark.parametrize("dbt", [False, True])
def test_error_classified_and_propagated(local, monkeypatch, dbt):
    root, _ = local
    ready(root)
    result = {"status": "DBT_FAILED", "ingestion_batch_id": 4, "build_id": 8}
    error = (
        pipeline.PipelineExecutionError("dbt failed", result) if dbt else ValueError()
    )
    monkeypatch.setattr(inbox, "energy_pipeline", Mock(side_effect=error))
    with pytest.raises(type(error)):
        inbox.energy_inbox.fn()
    target = root / "data/failed/export.csv"
    assert target.exists()
    report = json.loads(target.with_suffix(".csv.result.json").read_text("utf-8"))
    if dbt:
        assert report["ingestion_batch_id"] == 4
        assert report["status"] == "DBT_FAILED"
    else:
        assert report["status"] == "TECHNICAL_ERROR"
        assert report["error_type"] == "ValueError"


@pytest.mark.parametrize("directory", ["processing", "archive"])
def test_never_overwrite_existing_csv(local, monkeypatch, directory):
    root, _ = local
    ready(root)
    original = root / "data" / directory / "export.csv"
    original.write_text("previous", encoding="utf-8")
    monkeypatch.setattr(
        inbox, "energy_pipeline", Mock(return_value={"status": "SUCCESS"})
    )
    report = inbox.energy_inbox.fn()
    assert original.read_text("utf-8") == "previous"
    assert Path(report["classified_path"]).read_text("utf-8") == "csv content"
    assert Path(report["classified_path"]) != original


def test_destination_created_during_rename_is_preserved(local, monkeypatch):
    root, _ = local
    source = ready(root)
    original_rename = Path.rename
    target = root / "data/processing/export.csv"
    once = False

    def rename(path, destination):
        nonlocal once
        if not once:
            once = True
            target.write_text("concurrent file", encoding="utf-8")
        return original_rename(path, destination)

    monkeypatch.setattr(Path, "rename", rename)
    claimed = inbox.claim_csv(root / "data")
    assert target.read_text("utf-8") == "concurrent file"
    assert claimed.read_text("utf-8") == "csv content"
    assert not source.exists()


def test_interruption_keeps_processing_file(local, monkeypatch):
    root, _ = local
    ready(root)
    monkeypatch.setattr(inbox, "energy_pipeline", Mock(side_effect=KeyboardInterrupt))
    with pytest.raises(KeyboardInterrupt):
        inbox.energy_inbox.fn()
    assert (root / "data/processing/export.csv").read_bytes() == b"csv content"
    assert not list((root / "data/failed").iterdir())
    with inbox.inbox_lock(root / "data/processing/.inbox.lock") as acquired:
        assert acquired


def test_simultaneous_runs_only_process_source_once(local, monkeypatch):
    root, _ = local
    ready(root)
    started = threading.Event()
    release = threading.Event()

    def process(path):
        started.set()
        assert release.wait(timeout=5)
        return {"status": "SUCCESS"}

    call = Mock(side_effect=process)
    monkeypatch.setattr(inbox, "energy_pipeline", call)
    with ThreadPoolExecutor(max_workers=2) as executor:
        first = executor.submit(inbox.energy_inbox.fn)
        assert started.wait(timeout=5)
        try:
            second = executor.submit(inbox.energy_inbox.fn)
            assert second.result(timeout=5) == {"status": "BUSY"}
        finally:
            release.set()
        assert first.result(timeout=5)["status"] == "SUCCESS"
    call.assert_called_once()
    assert len(list((root / "data/archive").glob("*.csv"))) == 1


@pytest.mark.parametrize("status,code", list(pipeline.EXIT_CODES.items()))
def test_cli_transport_and_business_result(local, status, code, capsys):
    root, run = local
    source = ready(root, "énergie avec espaces.csv")
    payload = {
        "status": status,
        "ingestion_batch_id": 4,
        "build_id": 7,
        "rows_new": 3,
        "rows_changed": 2,
    }
    run.side_effect = None
    run.return_value = SimpleNamespace(
        returncode=code,
        stdout=json.dumps(payload, indent=2),
        stderr="diagnostic métier",
    )
    if status == "DBT_FAILED":
        with pytest.raises(pipeline.PipelineExecutionError) as failure:
            pipeline.run_local_pipeline.fn(str(source.relative_to(root)))
        result = failure.value.result
    else:
        result = pipeline.run_local_pipeline.fn(str(source.relative_to(root)))
    for key, value in payload.items():
        assert result[key] == value
    assert result["exit_code"] == code
    assert result["stderr"] == "diagnostic métier"
    args, kwargs = run.call_args
    assert args[0] == [
        str(root / ".venv/Scripts/python.exe"),
        "-B",
        str(root / "src/local_pipeline.py"),
        str(source),
    ]
    assert kwargs["cwd"] == root
    assert kwargs["env"]["PYTHONIOENCODING"] == "utf-8"
    assert "diagnostic métier" in capsys.readouterr().out
    run.assert_called_once()


@pytest.mark.parametrize(
    "stdout,code",
    [
        ("", 1),
        ("not json", 0),
        ('{"status":"SUCCESS"}', 1),
        ('{"status":"AWAITING_APPROVAL"}', 0),
        ('{"status":"UNKNOWN"}', 0),
        ("[]", 0),
        ('{"status":[]}', 0),
        ('{"status":"SUCCESS"}', 130),
    ],
)
def test_invalid_cli_result_is_technical_failure(local, stdout, code):
    root, run = local
    source = ready(root)
    run.side_effect = None
    run.return_value = SimpleNamespace(returncode=code, stdout=stdout, stderr="details")
    with pytest.raises(pipeline.PipelineExecutionError) as failure:
        pipeline.run_local_pipeline.fn(str(source))
    assert failure.value.result["status"] == "TECHNICAL_ERROR"
    assert failure.value.result["stdout"] == stdout
    assert failure.value.result["stderr"] == "details"


def test_manual_flow_accepts_csv_independently(local, monkeypatch):
    call = Mock(return_value={"status": "SUCCESS"})
    monkeypatch.setattr(pipeline, "run_local_pipeline", call)
    assert pipeline.energy_pipeline.fn("data/raw/manual.csv")["status"] == "SUCCESS"
    call.assert_called_once_with("data/raw/manual.csv")


def test_no_business_retries():
    assert pipeline.run_local_pipeline.retries == 0
    assert pipeline.energy_pipeline.retries == 0
    assert inbox.energy_inbox.retries == 0


def test_deployment_config_with_installed_prefect_391(monkeypatch):
    import prefect
    import yaml
    from prefect.cli.deploy._schedules import _schedule_config_to_deployment_schedule
    from prefect.client.schemas.objects import ConcurrencyLimitConfig
    from prefect.deployments.runner import RunnerDeployment
    from prefect.flows import load_flow_from_entrypoint
    from prefect.utilities.templating import apply_values

    assert prefect.__version__ == "3.9.1"
    root = Path(__file__).resolve().parents[2]
    config = yaml.safe_load((root / "prefect.yaml").read_text("utf-8-sig"))
    monkeypatch.setenv("ENERGY_PROJECT_ROOT", str(root))
    pull = apply_values(config["pull"], os.environ)
    assert pull[0]["prefect.deployments.steps.set_working_directory"][
        "directory"
    ] == str(root)
    manual, auto = config["deployments"]
    assert manual["schedules"] == []
    assert manual["name"] == "elia-manual"
    assert auto["name"] == "elia-inbox"
    limits = ConcurrencyLimitConfig(**auto["concurrency_limit"])
    assert limits.limit == 1
    assert limits.collision_strategy == "CANCEL_NEW"
    schedule = _schedule_config_to_deployment_schedule(auto["schedules"][0])
    assert schedule["active"] is False
    assert schedule["schedule"].cron == "*/5 * * * *"
    assert schedule["schedule"].timezone == "Europe/Brussels"
    for deployment in (manual, auto):
        loaded = load_flow_from_entrypoint(str(root / deployment["entrypoint"]))
        built = RunnerDeployment.from_flow(
            loaded,
            name=deployment["name"],
            work_pool_name=deployment["work_pool"]["name"],
            parameters=deployment.get("parameters", {}),
            schedules=[schedule] if deployment is auto else [],
            concurrency_limit=limits if deployment is auto else None,
        )
        assert built.work_pool_name == "energy-local"
        if deployment is auto:
            assert built.concurrency_limit == 1
            assert built.schedules[0].active is False
