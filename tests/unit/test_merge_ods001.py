import json
from pathlib import Path
from unittest.mock import Mock

import pytest

import merge_ods001
from merge_ods001 import MEASURES, PreviewResult, measure_differences, preview_csv


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
