import json
from pathlib import Path
from unittest.mock import MagicMock, Mock

import merge_ods001
import pytest
from merge_ods001 import (
    MEASURES,
    PreviewResult,
    measure_differences,
    preview_csv,
    write_full_report,
)


@pytest.mark.parametrize("limit", [-1, 101, 1.5, True, None])
def test_invalid_sample_limit_never_connects(monkeypatch, limit):
    connect = Mock()
    monkeypatch.setattr(merge_ods001.psycopg, "connect", connect)
    with pytest.raises(ValueError, match="sample_size"):
        preview_csv(Path("unused.csv"), sample_size=limit)
    connect.assert_not_called()


def test_invalid_header_never_connects(monkeypatch, tmp_path):
    path = tmp_path / "invalid.csv"
    path.write_text("wrong;header", encoding="utf-8")
    connect = Mock()
    monkeypatch.setattr(merge_ods001.psycopg, "connect", connect)
    with pytest.raises(ValueError, match="header"):
        preview_csv(path)
    connect.assert_not_called()


@pytest.mark.parametrize(("old", "new"), [(None, 4.0), (4.0, None), (4.0, 5.0)])
def test_measure_difference_handles_null(old, new):
    before = dict.fromkeys(MEASURES, 1.0)
    after = before.copy()
    before[MEASURES[0]] = old
    after[MEASURES[0]] = new
    assert measure_differences(before, after) == {MEASURES[0]: {"old": old, "new": new}}


def test_unchanged_sample():
    values = dict.fromkeys(MEASURES, None)
    assert measure_differences(values, values) == {}


def test_new_sample_has_all_measures():
    values = dict.fromkeys(MEASURES, 1.0)
    result = measure_differences(None, values)
    assert len(result) == 8
    assert all(v == {"old": None, "new": 1.0} for v in result.values())


def test_result_json_contract():
    result = PreviewResult("a" * 64, 3, 1, 1, 1, [])
    assert json.loads(json.dumps(result.to_dict()))["rows_received"] == 3


def test_full_report_streams_all_differences_and_summarizes(tmp_path):
    first, second = MEASURES[:2]
    before = dict.fromkeys(MEASURES, 1.0)
    after = before.copy()
    before[first] = None
    after[second] = None
    changed = {
        "timestamp_utc": "2026-10-01T00:00:00+00:00",
        "resolution_code": "PT15M",
        "classification": "CHANGED",
        "old_values": before,
        "new_values": after,
    }
    new = {
        **changed,
        "timestamp_utc": "2026-10-01T00:15:00+00:00",
        "classification": "NEW",
        "old_values": None,
    }
    rows = [(changed,)] + [(new,) for _ in range(120)]

    class Cursor:
        def __enter__(self):
            return self

        def __exit__(self, *_):
            pass

        def execute(self, sql):
            assert "WHERE existing.timestamp_utc IS NULL OR" in sql

        def fetchmany(self, size):
            assert size == 1000
            batch, rows[:] = rows[:size], rows[size:]
            return batch

    conn = Mock()
    conn.cursor.return_value = Cursor()
    output = tmp_path / "preview.json"
    result = PreviewResult("a" * 64, 122, 120, 1, 1, [])
    summary = write_full_report(conn, result, output)
    report = json.loads(output.read_text(encoding="utf-8"))
    assert report["rows_received"] == 122
    assert len(report["differences"]) == 121
    assert sum(d["classification"] == "NEW" for d in report["differences"]) == 120
    assert summary == report["change_summary"]
    assert summary["changed_by_measure"][first] == 1
    assert summary["changed_by_measure"][second] == 1
    assert summary["null_to_value"][first] == 1
    assert summary["value_to_null"][second] == 1
    assert summary["first_changed_utc"] == changed["timestamp_utc"]
    assert summary["last_changed_utc"] == changed["timestamp_utc"]


def test_full_report_invalid_output_preserves_existing_file(tmp_path):
    output = tmp_path / "missing" / "preview.json"
    conn = Mock()
    with pytest.raises(OSError):
        write_full_report(conn, PreviewResult("a" * 64, 0, 0, 0, 0, []), output)
    conn.cursor.assert_not_called()
    assert not list(tmp_path.rglob("*.tmp"))


def test_full_report_count_mismatch_does_not_replace_existing_file(tmp_path):
    output = tmp_path / "preview.json"
    output.write_text("existing", encoding="utf-8")
    conn = MagicMock()
    cursor = conn.cursor.return_value.__enter__.return_value
    cursor.fetchmany.return_value = []
    with pytest.raises(ValueError, match="count"):
        write_full_report(conn, PreviewResult("a" * 64, 1, 1, 0, 0, []), output)
    assert output.read_text(encoding="utf-8") == "existing"
    assert not list(tmp_path.glob("*.tmp"))


def test_cli_default_and_sample_size_unchanged(monkeypatch, capsys):
    monkeypatch.setattr(
        "sys.argv", ["merge_ods001.py", "input.csv", "--sample-size", "7"]
    )
    called = Mock(return_value=PreviewResult("a" * 64, 0, 0, 0, 0, []))
    monkeypatch.setattr(merge_ods001, "preview_csv", called)
    merge_ods001.main()
    called.assert_called_once_with(Path("input.csv"), sample_size=7)
    assert json.loads(capsys.readouterr().out)["differences"] == []


def test_cli_full_report_uses_output_without_sample_limit(monkeypatch, capsys):
    monkeypatch.setattr(
        "sys.argv",
        ["merge_ods001.py", "input.csv", "--full-report", "--output", "report.json"],
    )
    called = Mock(return_value={"rows_new": 101})
    monkeypatch.setattr(merge_ods001, "preview_full_csv", called)
    merge_ods001.main()
    called.assert_called_once_with(Path("input.csv"), Path("report.json"))
    assert json.loads(capsys.readouterr().out) == {"rows_new": 101}
