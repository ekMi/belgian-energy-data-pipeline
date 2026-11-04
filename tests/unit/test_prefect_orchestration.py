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

from src.prefect import api, inbox, pipeline  # noqa: E402


@pytest.fixture
def local(monkeypatch, tmp_path):
    monkeypatch.setattr(api, "PROJECT_ROOT", tmp_path)
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
    call.assert_called_once_with(
        str(root / "data/processing/A.csv"), source_policy="elia-full"
    )
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
    ],
)
def test_business_classification_and_diagnostic(local, monkeypatch, status, directory):
    root, _ = local
    source = ready(root)
    result = {"status": status, "ingestion_batch_id": 4, "rows_new": 3}

    def call(path, **kwargs):
        assert kwargs == {"source_policy": "elia-full"}
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

    def process(path, **kwargs):
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
        "--source-policy",
        "restricted",
        "--input-scope",
        "continuous",
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
    call.assert_called_once_with(
        "data/raw/manual.csv", source_policy="restricted", input_scope="continuous"
    )


def test_manual_prefect_uses_same_full_export_cli_policy(local):
    root, run = local
    source = ready(root)
    run.side_effect = None
    run.return_value = SimpleNamespace(
        returncode=0, stdout='{"status":"SUCCESS","rows_changed":2}', stderr=""
    )
    result = pipeline.run_local_pipeline.fn(str(source), source_policy="elia-full")
    assert result["rows_changed"] == 2
    assert run.call_args.args[0][4:] == [
        "--source-policy",
        "elia-full",
        "--input-scope",
        "continuous",
    ]


def test_inbox_never_routes_full_export_to_pending_approval(local, monkeypatch):
    root, _ = local
    previous = root / "data/pending_approval/previous.csv"
    previous.parent.mkdir()
    previous.write_text("keep historical file", encoding="utf-8")
    ready(root)
    monkeypatch.setattr(
        inbox, "energy_pipeline", Mock(return_value={"status": "AWAITING_APPROVAL"})
    )
    with pytest.raises(pipeline.PipelineExecutionError):
        inbox.energy_inbox.fn()
    assert (root / "data/failed/export.csv").exists()
    assert previous.read_text("utf-8") == "keep historical file"
    assert len(list(previous.parent.iterdir())) == 1


def test_no_business_retries():
    assert pipeline.run_local_pipeline.retries == 0
    assert pipeline.energy_pipeline.retries == 0
    assert inbox.energy_inbox.retries == 0
    assert api.run_api_pipeline_cli.retries == 0
    assert api.energy_api.retries == 0


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
    deployments = {item["name"]: item for item in config["deployments"]}
    assert set(deployments) == {"elia-manual", "elia-inbox", "elia-api-daily"}
    manual, auto = deployments["elia-manual"], deployments["elia-inbox"]
    assert manual["schedules"] == []
    assert manual["name"] == "elia-manual"
    assert manual["parameters"]["source_policy"] == "elia-full"
    assert manual["parameters"]["input_scope"] == "continuous"
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


def api_payload(status="SUCCESS"):
    return {
        "status": status,
        "acquisition": {
            "file_path": "data/raw/api/export.csv",
            "window_start_utc": "2025-10-01T00:00:00Z",
            "window_end_utc": "2025-11-08T00:00:00Z",
            "extracted_at": "2025-10-31T10:00:00Z",
            "source_reference": "https://external-elia.opendatasoft.com/api/explore/v2.1/catalog/datasets/ods001/exports/csv?where=filter",
            "row_count": 10,
            "sha256": "a" * 64,
        },
        "pipeline": {
            "status": status,
            "ingestion_batch_id": 4,
            "target_batch_id": None if status == "NO_CHANGE" else 4,
            "build_id": None if status == "NO_CHANGE" else 7,
            "rows_new": None if status == "NO_CHANGE" else 3,
            "rows_changed": None if status == "NO_CHANGE" else 2,
            "error_type": "DBT_BUILD_FAILED" if status == "DBT_FAILED" else None,
        },
    }


@pytest.mark.parametrize("status", ["SUCCESS", "NO_CHANGE", "DBT_FAILED"])
def test_api_subprocess_and_diagnostics(local, status, capsys):
    root, run = local
    payload = api_payload(status)
    run.side_effect = None
    run.return_value = SimpleNamespace(
        returncode=api.EXIT_CODES[status],
        stdout=json.dumps(payload),
        stderr="api_acquired\ningestion_finished\nquality_warnings\ndbt_finished",
    )
    if status == "DBT_FAILED":
        with pytest.raises(pipeline.PipelineExecutionError) as error:
            api.run_api_pipeline_cli.fn()
        result = error.value.result
    else:
        result = api.run_api_pipeline_cli.fn()
    assert result["status"] == status
    assert result["acquisition"] == payload["acquisition"]
    assert result["pipeline"] == payload["pipeline"]
    assert result["rows_unchanged"] == (None if status == "NO_CHANGE" else 5)
    assert run.call_args.args[0] == [
        str(root / ".venv/Scripts/python.exe"),
        "-B",
        str(root / "src/api_pipeline.py"),
    ]
    assert run.call_args.kwargs["cwd"] == root
    assert run.call_args.kwargs["env"]["PYTHONIOENCODING"] == "utf-8"
    assert run.call_args.kwargs["capture_output"] is True
    assert run.call_args.kwargs["encoding"] == "utf-8"
    assert run.call_args.kwargs["check"] is False
    run.assert_called_once()
    logs = capsys.readouterr().out
    for event in (
        "api_acquisition_started",
        "api_acquisition_finished",
        "export.csv",
        "row_count=10",
        "ingestion batch_id=4",
        "NEW=",
        "UNCHANGED=",
        "CHANGED=",
        "dbt status=",
        f"final_status={status}",
        "quality_warnings",
    ):
        assert event in logs


def test_api_historical_window_passed_verbatim(local):
    _, run = local
    run.side_effect = None
    run.return_value = SimpleNamespace(
        returncode=0, stdout=json.dumps(api_payload()), stderr=""
    )
    start, end = "2025-10-26T00:00:00Z", "2025-10-26T02:00:00Z"
    api.run_api_pipeline_cli.fn(start, end)
    assert run.call_args.args[0][3:] == ["--start", start, "--end", end]


@pytest.mark.parametrize(
    "start,end", [("2025-10-26T00:00:00Z", None), (None, "2025-10-26T02:00:00Z")]
)
def test_api_partial_window_never_launches(local, start, end):
    _, run = local
    with pytest.raises(ValueError, match="both"):
        api.run_api_pipeline_cli.fn(start, end)
    run.assert_not_called()


@pytest.mark.parametrize(
    "status",
    [
        "ACQUISITION_FAILED",
        "VALIDATION_FAILED",
        "INGESTION_FAILED",
        "TECHNICAL_ERROR",
        "INTERRUPTED",
    ],
)
def test_api_error_categories_are_propagated(local, status, capsys):
    _, run = local
    run.side_effect = None
    run.return_value = SimpleNamespace(
        returncode=api.EXIT_CODES[status],
        stdout=json.dumps({"status": status, "error_type": "SyntheticError"}),
        stderr="safe diagnostic",
    )
    with pytest.raises(pipeline.PipelineExecutionError) as error:
        api.run_api_pipeline_cli.fn()
    assert error.value.result["status"] == status
    assert error.value.result["error_type"] == "SyntheticError"
    assert error.value.result["stderr"] == "safe diagnostic"
    assert error.value.result["stdout"] == run.return_value.stdout
    assert f"final_status={status}" in capsys.readouterr().out
    run.assert_called_once()


@pytest.mark.parametrize(
    "stdout,code",
    [
        ("", 1),
        ("", 0),
        ("not json", 0),
        ("[]", 0),
        ("{}", 0),
        ('{"status":[]}', 0),
        ('{"status":"UNKNOWN"}', 0),
        ('{"status":"SUCCESS"}', 0),
        ('{"status":"ACQUISITION_FAILED"}', 1),
    ],
)
def test_api_invalid_or_empty_json_fails(local, stdout, code):
    _, run = local
    run.side_effect = None
    run.return_value = SimpleNamespace(
        returncode=code, stdout=stdout, stderr="diagnostic"
    )
    with pytest.raises(pipeline.PipelineExecutionError) as error:
        api.run_api_pipeline_cli.fn()
    assert error.value.result["status"] == "TECHNICAL_ERROR"
    assert error.value.result["exit_code"] == code
    assert error.value.result["stdout"] == stdout
    assert error.value.result["stderr"] == "diagnostic"


@pytest.mark.parametrize("code", [1, 2, 17])
def test_api_success_with_nonzero_exit_never_succeeds(local, code):
    _, run = local
    run.side_effect = None
    run.return_value = SimpleNamespace(
        returncode=code, stdout=json.dumps(api_payload()), stderr=""
    )
    with pytest.raises(pipeline.PipelineExecutionError):
        api.run_api_pipeline_cli.fn()


@pytest.mark.parametrize(
    "field,value",
    [
        ("acquisition", None),
        ("pipeline", {}),
        ("acquisition.file_path", ""),
        ("acquisition.row_count", True),
        ("acquisition.row_count", 0),
        ("acquisition.sha256", "invalid"),
        ("acquisition.window_start_utc", "2025-10-01"),
        ("pipeline.status", "NO_CHANGE"),
        ("pipeline.ingestion_batch_id", -1),
        ("pipeline.rows_new", 9),
        ("pipeline.rows_changed", -1),
    ],
)
def test_api_incomplete_or_inconsistent_envelope_rejected(local, field, value):
    _, run = local
    payload = api_payload()
    keys = field.split(".")
    parent = payload if len(keys) == 1 else payload[keys[0]]
    parent[keys[-1]] = value
    run.side_effect = None
    run.return_value = SimpleNamespace(
        returncode=0, stdout=json.dumps(payload), stderr=""
    )
    with pytest.raises(pipeline.PipelineExecutionError) as error:
        api.run_api_pipeline_cli.fn()
    assert error.value.result["status"] == "TECHNICAL_ERROR"


@pytest.mark.parametrize("code", [130, -2, -1073741510, 3221225786])
def test_api_interrupted_process_without_json(local, code):
    _, run = local
    run.side_effect = None
    run.return_value = SimpleNamespace(returncode=code, stdout="", stderr="interrupted")
    with pytest.raises(pipeline.PipelineExecutionError) as error:
        api.run_api_pipeline_cli.fn()
    assert error.value.result["status"] == "INTERRUPTED"


def test_api_launch_failure_retains_safe_category(local):
    _, run = local
    run.side_effect = OSError("password=must-not-be-printed")
    with pytest.raises(pipeline.PipelineExecutionError) as error:
        api.run_api_pipeline_cli.fn()
    assert error.value.result["status"] == "TECHNICAL_ERROR"
    assert error.value.result["error_type"] == "OSError"
    assert "must-not-be-printed" not in str(error.value)


def test_api_transport_diagnostics_cannot_be_replaced_by_json(local):
    _, run = local
    payload = {**api_payload(), "stdout": "forged", "stderr": "forged", "exit_code": 42}
    run.side_effect = None
    run.return_value = SimpleNamespace(
        returncode=0, stdout=json.dumps(payload), stderr="real diagnostic"
    )
    result = api.run_api_pipeline_cli.fn()
    assert result["exit_code"] == 0
    assert result["stdout"] == run.return_value.stdout
    assert result["stderr"] == "real diagnostic"


def test_api_flow_forwards_only_optional_bounds(local, monkeypatch):
    call = Mock(return_value={"status": "SUCCESS"})
    monkeypatch.setattr(api, "run_api_pipeline_cli", call)
    assert api.energy_api.fn() == {"status": "SUCCESS"}
    call.assert_called_once_with(start=None, end=None)
    call.reset_mock()
    api.energy_api.fn(start="2025-01-01T00:00:00Z", end="2025-01-02T00:00:00Z")
    call.assert_called_once_with(
        start="2025-01-01T00:00:00Z", end="2025-01-02T00:00:00Z"
    )


def test_api_daily_deployment_and_brussels_dst():
    from datetime import UTC, datetime

    import yaml
    from prefect.cli.deploy._schedules import _schedule_config_to_deployment_schedule
    from prefect.deployments.runner import RunnerDeployment
    from prefect.flows import load_flow_from_entrypoint
    from prefect.server.schemas.schedules import CronSchedule

    root = Path(__file__).resolve().parents[2]
    config = yaml.safe_load((root / "prefect.yaml").read_text("utf-8-sig"))
    item = next(
        item for item in config["deployments"] if item["name"] == "elia-api-daily"
    )
    assert item["parameters"] == {}
    assert item["work_pool"]["name"] == "energy-local"
    schedule = _schedule_config_to_deployment_schedule(item["schedules"][0])
    assert schedule["active"] is False
    assert schedule["schedule"].cron == "0 10 * * *"
    assert schedule["schedule"].timezone == "Europe/Brussels"
    loaded = load_flow_from_entrypoint(str(root / item["entrypoint"]))
    assert loaded.name == "belgian-energy-api"
    built = RunnerDeployment.from_flow(
        loaded,
        name=item["name"],
        work_pool_name="energy-local",
        parameters={},
        schedules=[schedule],
    )
    assert built.schedules[0].active is False
    assert built.work_pool_name == "energy-local"
    assert built._parameter_openapi_schema.properties["start"]["default"] is None
    cron = CronSchedule(cron="0 10 * * *", timezone="Europe/Brussels")
    for day, hours in (
        (datetime(2025, 3, 29, tzinfo=UTC), [9, 8, 8]),
        (datetime(2025, 10, 25, tzinfo=UTC), [8, 9, 9]),
    ):
        iterator = cron._get_dates_generator(n=3, start=day)
        dates = [next(iterator) for _ in range(3)]
        assert [date.astimezone(UTC).hour for date in dates] == hours
        assert all(date.hour == 10 for date in dates)
