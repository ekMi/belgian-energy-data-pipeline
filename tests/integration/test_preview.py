import psycopg
import pytest

import ingest_ods001
import merge_ods001
from merge_ods001 import DuplicateInputError, preview_csv
from validate_ods001 import calculate_sha256

pytestmark = pytest.mark.integration


@pytest.fixture
def preview_database(test_database, monkeypatch):
    monkeypatch.setattr(
        merge_ods001, "get_connection_params", ingest_ods001.get_connection_params
    )
    return test_database


def snapshot(conn):
    names = (
        "elia_load",
        "ingestion_batch",
        "quality_run",
        "quality_result",
        "elia_load_correction",
    )
    return {
        name: conn.execute(
            psycopg.sql.SQL(
                "SELECT to_jsonb(t) FROM staging.{} t ORDER BY to_jsonb(t)::text"
            ).format(psycopg.sql.Identifier(name))
        ).fetchall()
        for name in names
    }


def test_entirely_new_and_repeat_preview(preview_database, csv_file, rows):
    path = csv_file(rows)
    before = snapshot(preview_database)
    result = preview_csv(path, sample_size=2)
    assert (
        result.rows_received,
        result.rows_new,
        result.rows_unchanged,
        result.rows_changed,
    ) == (4, 4, 0, 0)
    assert len(result.differences) == 2
    assert all(
        r["classification"] == "NEW" and r["old_values"] is None
        for r in result.differences
    )
    assert result.file_sha256 == calculate_sha256(path)
    assert preview_csv(path, sample_size=2) == result
    assert snapshot(preview_database) == before


def test_entirely_unchanged(preview_database, csv_file, rows):
    path = csv_file(rows)
    ingest_ods001.ingest_csv(path)
    before = snapshot(preview_database)
    result = preview_csv(path)
    assert (result.rows_new, result.rows_unchanged, result.rows_changed) == (0, 4, 0)
    assert result.differences == []
    assert snapshot(preview_database) == before


def test_mixed_input_and_omitted_observations(preview_database, csv_file, rows):
    ingest_ods001.ingest_csv(csv_file(rows[:3], "baseline.csv"))
    before = snapshot(preview_database)
    changed = rows[1].copy()
    changed[2] = "12345"
    path = csv_file([rows[3], changed, rows[0]])
    result = preview_csv(path)
    assert (result.rows_new, result.rows_unchanged, result.rows_changed) == (1, 1, 1)
    assert [r["classification"] for r in result.differences] == ["CHANGED", "NEW"]
    assert result.differences[0]["changed_measures"] == {
        "total_load_mw": {"old": 10000.0, "new": 12345.0}
    }
    assert snapshot(preview_database) == before


@pytest.mark.parametrize("index", range(2, 10))
def test_single_measure_change(preview_database, csv_file, rows, index):
    ingest_ods001.ingest_csv(csv_file(rows[:1], "baseline.csv"))
    before = snapshot(preview_database)
    changed = rows[0].copy()
    changed[index] = "42"
    result = preview_csv(csv_file([changed]))
    assert result.rows_changed == 1
    assert len(result.differences[0]["changed_measures"]) == 1
    assert snapshot(preview_database) == before


@pytest.mark.parametrize("index", [2, 9])
@pytest.mark.parametrize(
    ("old", "new", "expected"),
    [
        ("", "123", "CHANGED"),
        ("123", "", "CHANGED"),
        ("", "", "UNCHANGED"),
    ],
)
def test_null_comparisons(preview_database, csv_file, rows, index, old, new, expected):
    original = rows[0].copy()
    original[index] = old
    ingest_ods001.ingest_csv(csv_file([original], "baseline.csv"))
    changed = original.copy()
    changed[index] = new
    before = snapshot(preview_database)
    result = preview_csv(csv_file([changed]))
    assert result.rows_changed == (expected == "CHANGED")
    assert result.rows_unchanged == (expected == "UNCHANGED")
    assert snapshot(preview_database) == before


@pytest.mark.parametrize("contradictory", [False, True])
def test_duplicate_keys_rejected(preview_database, csv_file, rows, contradictory):
    duplicate = rows[0].copy()
    # Different offset, same normalized business key.
    duplicate[0] = "2025-01-01T00:00:00Z"
    if contradictory:
        duplicate[2] = "42"
    before = snapshot(preview_database)
    with pytest.raises(DuplicateInputError, match="duplicate key"):
        preview_csv(csv_file([rows[0], duplicate]))
    assert snapshot(preview_database) == before
    assert preview_csv(csv_file(rows)).rows_new == 4


@pytest.mark.parametrize(
    ("index", "value"),
    [
        (1, "PT60M"),
        (0, ""),
        (1, ""),
        (3, ""),
        (4, ""),
        (5, ""),
        (6, ""),
        (7, ""),
        (8, ""),
        (2, "NaN"),
        (9, "inf"),
    ],
)
def test_invalid_input_preserves_database(
    preview_database, csv_file, rows, index, value
):
    invalid = rows[1].copy()
    invalid[index] = value
    before = snapshot(preview_database)
    with pytest.raises(ValueError, match="Line 3, column"):
        preview_csv(csv_file([rows[0], invalid]))
    assert snapshot(preview_database) == before


def test_zero_samples_keeps_counts(preview_database, csv_file, rows):
    result = preview_csv(csv_file(rows), sample_size=0)
    assert result.rows_new == 4
    assert result.differences == []


def test_modified_file_is_rejected(preview_database, csv_file, rows, monkeypatch):
    path = csv_file(rows)
    hashes = iter(["a" * 64, "b" * 64])
    monkeypatch.setattr(merge_ods001, "calculate_sha256", lambda path: next(hashes))
    before = snapshot(preview_database)
    with pytest.raises(ValueError, match="changed during preview"):
        preview_csv(path)
    assert snapshot(preview_database) == before
