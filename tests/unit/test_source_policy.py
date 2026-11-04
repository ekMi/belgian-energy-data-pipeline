"""Trust selection and real ingestion control flow with a simulated database."""

from contextlib import contextmanager, nullcontext
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import MagicMock, Mock

import pytest

import ingest_ods001 as ingestion
from correction_policy import AUTOMATIC_CORRECTION_REASON, automatic_corrections
from quality_ods001 import QualityResult


@pytest.mark.parametrize("scope", ["continuous", "partial"])
def test_generic_input_remains_restricted(scope):
    assert not automatic_corrections("restricted", scope)


@pytest.mark.parametrize(
    "options",
    [
        {"source_policy": "unknown"},
        {"source_policy": "elia-full", "input_scope": "partial"},
        {
            "source_policy": "elia-full",
            "authorized_sha256": "a" * 64,
            "correction_reason": "human review",
        },
    ],
)
def test_invalid_policy_never_connects(monkeypatch, options):
    connect = Mock()
    monkeypatch.setattr(ingestion.psycopg, "connect", connect)
    with pytest.raises(ValueError):
        ingestion.ingest_csv(Path("unused.csv"), **options)
    connect.assert_not_called()


@pytest.fixture
def simulated(monkeypatch):
    conn = MagicMock()
    conn.__enter__.return_value = conn
    conn.transaction.side_effect = lambda: nullcontext()
    events = []
    state = SimpleNamespace(new=2, changed=1, known=False)

    def execute(sql, params=None):
        result = MagicMock()
        if sql.startswith("SELECT batch_id"):
            result.fetchone.return_value = (7,) if state.known else None
        elif sql.startswith("INSERT INTO staging.ingestion_batch"):
            result.fetchone.return_value = (7,)
        elif sql.startswith("SELECT status"):
            result.fetchone.return_value = ("RUNNING",)
        elif sql.startswith("INSERT INTO staging.elia_load ("):
            events.append("insert")
            result.rowcount = state.new
        else:
            result.fetchall.return_value = []
        return result

    conn.execute.side_effect = execute

    @contextmanager
    def locked(connection):
        events.append("lock")
        yield
        events.append("unlock")

    def checksum(path):
        assert events[-1] == "lock"
        return "a" * 64

    monkeypatch.setattr(ingestion, "INGESTION_DATABASE", "belgian_energy_test")
    monkeypatch.setattr(
        ingestion, "get_connection_params", lambda: {"dbname": "belgian_energy_test"}
    )
    monkeypatch.setattr(ingestion.psycopg, "connect", Mock(return_value=conn))
    monkeypatch.setattr(ingestion, "verify_database_target", Mock())
    monkeypatch.setattr(ingestion, "ingestion_lock", locked)
    monkeypatch.setattr(ingestion, "calculate_sha256", checksum)
    load = Mock()
    monkeypatch.setattr(ingestion, "load_incoming", load)
    monkeypatch.setattr(
        ingestion,
        "classify_incoming",
        lambda *args: SimpleNamespace(
            rows_received=state.new + state.changed + 1,
            rows_new=state.new,
            rows_changed=state.changed,
            rows_unchanged=1,
            differences=[],
        ),
    )
    monkeypatch.setattr(ingestion, "evaluate_quality", Mock(return_value=[]))
    monkeypatch.setattr(ingestion, "check_consolidated", Mock(return_value=([], {})))
    monkeypatch.setattr(ingestion, "save_results", Mock())
    archive = Mock(
        side_effect=lambda *a, **k: events.append("archive") or state.changed
    )
    update = Mock(side_effect=lambda *a: events.append("update") or state.changed)
    monkeypatch.setattr(ingestion, "archive_corrections", archive)
    monkeypatch.setattr(ingestion, "update_corrections", update)
    return SimpleNamespace(**locals())


@pytest.mark.parametrize("new,changed", [(2, 0), (0, 2), (2, 2)])
def test_full_export_reuses_historized_merge(simulated, new, changed):
    s = simulated
    s.state.new, s.state.changed = new, changed
    result = ingestion.ingest_csv(Path("full.csv"), source_policy="elia-full")
    assert (result.rows_inserted, result.rows_updated, result.rows_unchanged) == (
        new,
        changed,
        1,
    )
    assert s.events == [
        "lock",
        *(["archive", "update"] if changed else []),
        "insert",
        "unlock",
    ]
    batch = next(
        call
        for call in s.conn.execute.call_args_list
        if call.args[0].startswith("INSERT INTO staging.ingestion_batch")
    )
    assert batch.args[1][2:] == (
        "continuous",
        "authorized",
        "a" * 64,
        AUTOMATIC_CORRECTION_REASON,
        None,
        None,
    )
    if changed:
        s.archive.assert_called_once_with(s.conn, 7, extracted_at=None)


def test_same_successful_full_export_is_noop(simulated):
    simulated.state.known = True
    assert ingestion.ingest_csv(Path("full.csv"), source_policy="elia-full") is None
    simulated.load.assert_not_called()
    simulated.archive.assert_not_called()


@pytest.mark.parametrize("scope", ["partial", "continuous"])
def test_untrusted_corrections_still_rejected(simulated, scope):
    with pytest.raises(ingestion.ChangedObservationsError):
        ingestion.ingest_csv(Path("generic.csv"), input_scope=scope)
    simulated.archive.assert_not_called()
    simulated.update.assert_not_called()


@pytest.mark.parametrize("failure", ["invalid", "quality", "stale"])
def test_automatic_policy_never_bypasses_rejection(simulated, monkeypatch, failure):
    if failure == "invalid":
        simulated.load.side_effect = ValueError("Invalid CSV")
    elif failure == "quality":
        monkeypatch.setattr(
            ingestion,
            "evaluate_quality",
            Mock(return_value=[QualityResult("test.blocking", "ERROR", 1, 1, [])]),
        )
    else:
        simulated.archive.side_effect = ValueError("Stale extraction")
    with pytest.raises(ValueError):
        ingestion.ingest_csv(Path("full.csv"), source_policy="elia-full")
    simulated.update.assert_not_called()


@pytest.mark.parametrize(
    "entrypoint", ["ingest_ods001", "local_pipeline", "orchestration_preview"]
)
def test_cli_forwards_explicit_full_export_policy(monkeypatch, entrypoint):
    import importlib
    import sys

    module = importlib.import_module(entrypoint)
    monkeypatch.setattr(
        sys, "argv", [entrypoint, "full.csv", "--source-policy", "elia-full"]
    )
    if entrypoint == "orchestration_preview":
        result = module.PipelinePlan("a" * 64, None, 2, 1, "INGEST")
        name = "plan_csv"
    elif entrypoint == "local_pipeline":
        result, name = module.PipelineResult("SUCCESS"), "run_pipeline"
    else:
        result, name = None, "ingest_csv"
    call = Mock(return_value=result)
    monkeypatch.setattr(module, name, call)
    module.main()
    assert call.call_args.kwargs["source_policy"] == "elia-full"
    assert call.call_args.kwargs["input_scope"] == "continuous"


@pytest.fixture
def api_receipt(tmp_path):
    from datetime import UTC, datetime, timedelta

    from elia_api_support import api_transport

    from acquire_elia import acquire_elia

    start = datetime(2025, 1, 1, tzinfo=UTC)
    return acquire_elia(
        start,
        start + timedelta(days=1),
        output_dir=tmp_path,
        opener=api_transport([["2025-01-01T00:00:00Z", "PT15M", *["1000"] * 8]]),
        sleep=Mock(),
    )


def test_api_policy_uses_same_historized_merge_with_receipt(
    simulated, api_receipt, monkeypatch
):
    from correction_policy import API_CORRECTION_REASON

    receipt = api_receipt
    monkeypatch.setattr(ingestion, "calculate_sha256", lambda path: receipt.sha256)
    result = ingestion.ingest_csv(
        receipt.file_path,
        source_policy="elia-api",
        input_scope="partial",
        api_acquisition=receipt,
        extracted_at=receipt.extracted_at,
        source_reference=receipt.source_reference,
    )
    assert (result.rows_inserted, result.rows_updated, result.rows_unchanged) == (
        2,
        1,
        1,
    )
    assert simulated.events == ["lock", "archive", "update", "insert", "unlock"]
    simulated.archive.assert_called_once_with(
        simulated.conn, 7, extracted_at=receipt.extracted_at
    )
    batch = next(
        call
        for call in simulated.conn.execute.call_args_list
        if call.args[0].startswith("INSERT INTO staging.ingestion_batch")
    )
    assert batch.args[1][2:] == (
        "partial",
        "authorized",
        receipt.sha256,
        API_CORRECTION_REASON,
        receipt.extracted_at,
        receipt.source_reference,
    )
    assert ingestion.evaluate_quality.call_args.kwargs["check_continuity"] is False
    ingestion.check_consolidated.assert_called_once()


def test_api_hash_rechecked_under_ingestion_lock(simulated, api_receipt, monkeypatch):
    receipt = api_receipt
    # The real receipt verification succeeds before connection; simulate a file
    # change only when the ingestion lock's second checksum is read.
    monkeypatch.setattr(ingestion, "calculate_sha256", lambda path: "b" * 64)
    with pytest.raises(ValueError, match="changed before ingestion"):
        ingestion.ingest_csv(
            receipt.file_path,
            source_policy="elia-api",
            input_scope="partial",
            api_acquisition=receipt,
            extracted_at=receipt.extracted_at,
            source_reference=receipt.source_reference,
        )
    simulated.load.assert_not_called()
    simulated.archive.assert_not_called()
    assert not any(
        call.args[0].startswith("INSERT INTO staging.ingestion_batch")
        for call in simulated.conn.execute.call_args_list
    )


@pytest.mark.parametrize("failure", ["quality", "stale"])
def test_api_policy_keeps_quality_and_stale_guards(
    simulated, api_receipt, monkeypatch, failure
):
    receipt = api_receipt
    monkeypatch.setattr(ingestion, "calculate_sha256", lambda path: receipt.sha256)
    if failure == "quality":
        ingestion.evaluate_quality.return_value = [
            QualityResult("test.blocking", "ERROR", 1, 1, [])
        ]
    else:
        simulated.archive.side_effect = ValueError("Stale extraction")
    with pytest.raises(ValueError, match="Blocking data quality|Stale extraction"):
        ingestion.ingest_csv(
            receipt.file_path,
            source_policy="elia-api",
            input_scope="partial",
            api_acquisition=receipt,
            extracted_at=receipt.extracted_at,
            source_reference=receipt.source_reference,
        )
    simulated.update.assert_not_called()
    assert not any(
        call.args[0].startswith("INSERT INTO staging.elia_load (")
        for call in simulated.conn.execute.call_args_list
    )
    if failure == "quality":
        simulated.archive.assert_not_called()
    else:
        simulated.archive.assert_called_once_with(
            simulated.conn, 7, extracted_at=receipt.extracted_at
        )
