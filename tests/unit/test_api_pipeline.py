"""API provenance and orchestration, with HTTP/dbt/PostgreSQL simulated."""

import json
import sys
from contextlib import contextmanager
from dataclasses import replace
from datetime import UTC, datetime, timedelta
from types import SimpleNamespace
from unittest.mock import Mock
from urllib.error import HTTPError

import pytest
from elia_api_support import api_transport, http_response

import acquire_elia as acquisition
import api_pipeline
import ingest_ods001
import local_pipeline
import orchestration_preview
from correction_policy import automatic_corrections
from ingest_ods001 import IngestionResult, validate_authorization
from orchestration_preview import PipelinePlan
from quality_ods001 import evaluate_quality, has_errors

START = datetime(2025, 1, 1, tzinfo=UTC)
END = START + timedelta(days=1)
ROWS = [["2025-01-01T00:00:00Z", "PT15M", *["1000"] * 8]]


@pytest.fixture(autouse=True)
def forbid_live_io(monkeypatch):
    monkeypatch.setattr(
        acquisition, "urlopen", Mock(side_effect=AssertionError("Live HTTP forbidden"))
    )
    monkeypatch.setattr(
        ingest_ods001.psycopg,
        "connect",
        Mock(side_effect=AssertionError("Live DB forbidden")),
    )


@pytest.fixture
def receipt(tmp_path):
    return acquisition.acquire_elia(
        START, END, output_dir=tmp_path, opener=api_transport(ROWS), sleep=Mock()
    )


@pytest.fixture
def downstream(monkeypatch):
    state = SimpleNamespace(known=False, pending=True)
    events = []

    def plan(path, **kwargs):
        events.append("plan")
        return PipelinePlan(
            acquisition.calculate_sha256(path),
            7 if state.known else None,
            None if state.known else 1,
            0,
            "NO_CHANGE" if state.known else "INGEST",
        )

    def ingest(*args, **kwargs):
        events.append("ingest")
        return None if state.known else IngestionResult(7, 1, 1, 0, 0)

    preview = Mock(side_effect=plan)
    ingestion = Mock(side_effect=ingest)
    monkeypatch.setattr(local_pipeline, "plan_csv", preview)
    monkeypatch.setattr(local_pipeline, "ingest_csv", ingestion)
    conn = Mock()
    build = Mock(side_effect=lambda: events.append("build"))

    @contextmanager
    def locked():
        events.append("lock")
        try:
            yield SimpleNamespace(connection=conn, build=build)
        finally:
            events.append("unlock")

    monkeypatch.setattr(local_pipeline, "locked_build", locked)
    monkeypatch.setattr(
        local_pipeline,
        "get_analytics_sync_status",
        lambda **kwargs: SimpleNamespace(
            dbt_required=state.pending, latest_ingestion_batch=7
        ),
    )
    start = Mock(return_value=SimpleNamespace(build_id=12))
    success, failed = Mock(), Mock()
    monkeypatch.setattr(local_pipeline, "start_build", start)
    monkeypatch.setattr(local_pipeline, "mark_success", success)
    monkeypatch.setattr(local_pipeline, "mark_failed", failed)
    return SimpleNamespace(**locals())


def execute(tmp_path, **kwargs):
    return api_pipeline.run_api_pipeline(
        START,
        END,
        output_dir=tmp_path,
        opener=api_transport(ROWS),
        sleep=Mock(),
        **kwargs,
    )


def test_valid_acquisition_then_existing_pipeline(tmp_path, downstream):
    result = execute(tmp_path)
    assert result.status == "SUCCESS"
    assert downstream.events == ["plan", "ingest", "lock", "build", "unlock"]
    downstream.ingestion.assert_called_once_with(
        result.acquisition.file_path,
        input_scope="partial",
        source_policy="elia-api",
        extracted_at=result.acquisition.extracted_at,
        source_reference=result.acquisition.source_reference,
        api_acquisition=result.acquisition,
        authorized_sha256=None,
        correction_reason=None,
    )
    assert downstream.preview.call_args.kwargs["api_acquisition"] is result.acquisition
    payload = result.to_dict()
    assert payload["pipeline"]["ingestion_batch_id"] == 7
    assert payload["acquisition"]["sha256"] == result.acquisition.sha256
    assert json.loads(json.dumps(payload))["status"] == "SUCCESS"


def test_dbt_failure_preserves_committed_ingestion_and_retries(tmp_path, downstream):
    downstream.build.side_effect = RuntimeError("simulated dbt failure")
    first = execute(tmp_path)
    assert first.status == "DBT_FAILED"
    assert first.pipeline.ingestion_batch_id == 7
    assert first.acquisition.file_path.exists()
    downstream.failed.assert_called_once_with(
        12, "DBT_BUILD_FAILED", connection=downstream.conn
    )
    downstream.success.assert_not_called()
    downstream.state.known = True
    downstream.build.side_effect = None
    second = execute(tmp_path)
    assert second.status == "SUCCESS"
    assert second.acquisition.sha256 == first.acquisition.sha256
    assert second.acquisition.file_path != first.acquisition.file_path
    downstream.success.assert_called_once_with(12, connection=downstream.conn)
    downstream.state.pending = False
    assert execute(tmp_path).status == "NO_CHANGE"
    assert downstream.build.call_count == 2


@pytest.mark.parametrize("failure", ["http", "validation"])
def test_acquisition_failure_never_starts_pipeline(tmp_path, downstream, failure):
    opener = Mock(
        side_effect=HTTPError("https://example.test", 403, "forbidden", {}, None)
    )
    if failure == "validation":
        opener = Mock(return_value=http_response(b"invalid header\n"))
    with pytest.raises(acquisition.AcquisitionError):
        api_pipeline.run_api_pipeline(
            START, END, output_dir=tmp_path, opener=opener, sleep=Mock()
        )
    assert downstream.events == []
    assert not list(tmp_path.iterdir())


@pytest.mark.parametrize("kind", ["none", "copy"])
def test_constructed_or_copied_receipts_are_not_authority(receipt, kind):
    candidate = None if kind == "none" else replace(receipt)
    with pytest.raises(ValueError, match="issued by acquire_elia"):
        acquisition.verify_acquisition(candidate)
    acquisition.verify_acquisition(receipt)


def test_forged_acquisition_result_rejected_by_entrypoint(
    receipt, downstream, monkeypatch
):
    monkeypatch.setattr(
        api_pipeline, "acquire_elia", Mock(return_value=replace(receipt))
    )
    with pytest.raises(ValueError, match="issued by acquire_elia"):
        api_pipeline.run_api_pipeline(START, END)
    assert downstream.events == []


@pytest.mark.parametrize("mutation", ["modified", "deleted", "wrong_path"])
def test_receipt_binds_original_file(receipt, mutation, tmp_path):
    path = receipt.file_path
    if mutation == "modified":
        path.write_bytes(path.read_bytes() + b"\n")
    elif mutation == "deleted":
        path.unlink()
    else:
        path = tmp_path / "copy.csv"
        path.write_bytes(receipt.file_path.read_bytes())
    with pytest.raises(ValueError, match="SHA-256|missing|file path"):
        acquisition.verify_acquisition(receipt, path)


@pytest.mark.parametrize("entrypoint", ["ingestion", "pipeline", "preview"])
def test_arbitrary_csv_cannot_select_api_policy(entrypoint, tmp_path, monkeypatch):
    path = tmp_path / "arbitrary.csv"
    path.write_text("not acquired", encoding="utf-8")
    query = Mock(side_effect=AssertionError("DB lookup forbidden"))
    monkeypatch.setattr(orchestration_preview, "find_successful_batch", query)
    target = {
        "ingestion": ingest_ods001.ingest_csv,
        "pipeline": local_pipeline.run_pipeline,
        "preview": orchestration_preview.plan_csv,
    }[entrypoint]
    with pytest.raises(ValueError, match="issued by acquire_elia"):
        target(path, source_policy="elia-api", input_scope="partial")
    query.assert_not_called()
    ingest_ods001.psycopg.connect.assert_not_called()


@pytest.mark.parametrize("field", ["extracted_at", "source_reference"])
def test_metadata_cannot_override_receipt(receipt, field):
    options = dict(
        source_policy="elia-api",
        input_scope="partial",
        api_acquisition=receipt,
        csv_path=receipt.file_path,
        source_reference=receipt.source_reference,
    )
    extracted = receipt.extracted_at
    if field == "extracted_at":
        extracted -= timedelta(days=1)
    else:
        options["source_reference"] = "untrusted source"
    with pytest.raises(ValueError, match="metadata"):
        validate_authorization(None, None, extracted, **options)


def test_api_policy_is_partial_and_not_manual_authorization(receipt):
    assert automatic_corrections("elia-api", "partial")
    with pytest.raises(ValueError, match="partial"):
        automatic_corrections("elia-api", "continuous")
    with pytest.raises(ValueError, match="manual authorization"):
        validate_authorization(
            receipt.sha256,
            "reviewed",
            receipt.extracted_at,
            source_policy="elia-api",
            input_scope="partial",
            api_acquisition=receipt,
            csv_path=receipt.file_path,
            source_reference=receipt.source_reference,
        )
    with pytest.raises(ValueError, match="requires source_policy"):
        validate_authorization(None, None, None, api_acquisition=receipt)


def test_partial_future_and_delayed_actual_load_are_warnings_only():
    as_of = datetime(2025, 1, 3, tzinfo=UTC)
    records = [
        (START, "PT15M", None, *[1000.0] * 7),
        (as_of + timedelta(days=2), "PT15M", None, *[1000.0] * 7),
    ]
    checks = evaluate_quality(records, as_of=as_of, check_continuity=False)
    assert not has_errors(checks)
    assert not any(check.check_id == "technical.continuity" for check in checks)
    assert (
        next(
            check for check in checks if check.check_id == "completeness.2.historical"
        ).anomaly_count
        == 1
    )
    assert (
        next(
            check for check in checks if check.check_id == "completeness.2.pending"
        ).anomaly_count
        == 1
    )
    assert any(check.check_id.startswith("business.") for check in checks)
    assert has_errors(
        evaluate_quality(records + records, as_of=as_of, check_continuity=False)
    )


@pytest.mark.parametrize(
    "status,code", [("SUCCESS", 0), ("NO_CHANGE", 0), ("DBT_FAILED", 1)]
)
def test_cli_json_exit_and_explicit_window(monkeypatch, capsys, receipt, status, code):
    monkeypatch.setattr(
        sys,
        "argv",
        [
            "api_pipeline",
            "--start",
            "2025-01-01T00:00:00Z",
            "--end",
            "2025-01-02T00:00:00Z",
        ],
    )
    run = Mock(
        return_value=api_pipeline.ApiPipelineResult(
            receipt, local_pipeline.PipelineResult(status)
        )
    )
    monkeypatch.setattr(api_pipeline, "run_api_pipeline", run)
    if code:
        with pytest.raises(SystemExit) as error:
            api_pipeline.main()
        assert error.value.code == code
    else:
        api_pipeline.main()
    assert json.loads(capsys.readouterr().out)["status"] == status
    assert run.call_args.args == (START, END)


@pytest.mark.parametrize(
    "error,status,code",
    [
        (acquisition.AcquisitionError("HTTP 503"), "ACQUISITION_FAILED", 1),
        (acquisition.AcquisitionValidationError("invalid CSV"), "VALIDATION_FAILED", 1),
        (ValueError("invalid input"), "VALIDATION_FAILED", 1),
        (
            ingest_ods001.psycopg.OperationalError("password=hidden"),
            "INGESTION_FAILED",
            1,
        ),
        (RuntimeError("password=hidden"), "TECHNICAL_ERROR", 1),
        (KeyboardInterrupt(), "INTERRUPTED", 130),
    ],
)
def test_cli_structured_error_categories(monkeypatch, capsys, error, status, code):
    monkeypatch.setattr(sys, "argv", ["api_pipeline"])
    monkeypatch.setattr(api_pipeline, "run_api_pipeline", Mock(side_effect=error))
    with pytest.raises(SystemExit) as exit_error:
        api_pipeline.main()
    assert exit_error.value.code == code
    captured = capsys.readouterr()
    payload = json.loads(captured.out)
    assert payload == {"status": status, "error_type": type(error).__name__}
    assert "hidden" not in captured.out + captured.err


def test_actual_csv_validation_is_reported_as_validation_failure(
    monkeypatch, tmp_path, capsys
):
    monkeypatch.setattr(
        sys,
        "argv",
        [
            "api_pipeline",
            "--start",
            "2025-01-01T00:00:00Z",
            "--end",
            "2025-01-02T00:00:00Z",
            "--output-dir",
            str(tmp_path),
        ],
    )
    monkeypatch.setattr(
        acquisition, "urlopen", Mock(return_value=http_response(b"invalid header\n"))
    )
    with pytest.raises(SystemExit) as error:
        api_pipeline.main()
    assert error.value.code == 1
    assert json.loads(capsys.readouterr().out)["status"] == "VALIDATION_FAILED"
    ingest_ods001.psycopg.connect.assert_not_called()
