import csv
import hashlib
from datetime import UTC, datetime

import pytest

from validate_ods001 import EXPECTED_COLUMNS, calculate_sha256, parse_row, validate_csv


@pytest.fixture
def row():
    return [
        "2015-06-24T05:30:00+02:00",
        "PT15M",
        "10000",
        "846",
        "7791.92",
        "8699.21",
        "8500",
        "8000",
        "9000",
        "9500",
    ]


def test_parse_row_preserves_values_and_normalizes_utc(row):
    result = parse_row(row, line_number=2)
    assert result == (
        datetime(2015, 6, 24, 3, 30, tzinfo=UTC),
        "PT15M",
        10000.0,
        846.0,
        7791.92,
        8699.21,
        8500.0,
        8000.0,
        9000.0,
        9500.0,
    )


@pytest.mark.parametrize(
    "timestamp",
    [
        "2015-06-24T03:30:00Z",
        "2015-06-24T03:30:00+00:00",
        "2015-06-23T22:30:00-05:00",
    ],
)
def test_equivalent_offsets(row, timestamp):
    row[0] = timestamp
    assert parse_row(row, line_number=2)[0] == datetime(2015, 6, 24, 3, 30, tzinfo=UTC)


def test_nullable_fields(row):
    row[2] = row[9] = ""
    result = parse_row(row, line_number=2)
    assert result[2] is None
    assert result[9] is None


@pytest.mark.parametrize("timestamp", ["2015-06-24T05:30:00", "2015-06-24", "invalid"])
def test_invalid_or_naive_timestamp(row, timestamp):
    row[0] = timestamp
    with pytest.raises(ValueError, match=r"Line 7, column 1 \(Datetime\)"):
        parse_row(row, line_number=7)


@pytest.mark.parametrize("index", range(2, 10))
@pytest.mark.parametrize("value", ["NaN", "inf", "-Infinity", "1e999", "invalid"])
def test_invalid_numbers(row, index, value):
    row[index] = value
    with pytest.raises(ValueError, match=f"Line 8, column {index + 1} "):
        parse_row(row, line_number=8)


@pytest.mark.parametrize("index", [0, 1, 3, 4, 5, 6, 7, 8])
@pytest.mark.parametrize("value", ["", " "])
def test_required_fields(row, index, value):
    row[index] = value
    with pytest.raises(ValueError, match=f"Line 9, column {index + 1} .*required"):
        parse_row(row, line_number=9)


def test_wrong_resolution(row):
    row[1] = "PT60M"
    with pytest.raises(ValueError, match="column 2 .*PT15M"):
        parse_row(row, line_number=2)


@pytest.mark.parametrize("index", [2, 9])
def test_whitespace_is_not_a_null_marker(row, index):
    row[index] = " "
    with pytest.raises(ValueError, match=f"column {index + 1} "):
        parse_row(row, line_number=2)


@pytest.mark.parametrize("count", [9, 11])
def test_wrong_field_count(row, count):
    row = (row + ["1"])[:count]
    with pytest.raises(
        ValueError, match=f"Line 4, columns: expected 10 fields, got {count}"
    ):
        parse_row(row, line_number=4)


def write_csv(tmp_path, rows, encoding="utf-8"):
    path = tmp_path / "input.csv"
    with path.open("w", encoding=encoding, newline="") as stream:
        csv.writer(stream, delimiter=";").writerows(rows)
    return path


@pytest.mark.parametrize("encoding", ["utf-8", "utf-8-sig"])
def test_valid_header(tmp_path, row, encoding):
    validate_csv(write_csv(tmp_path, [EXPECTED_COLUMNS, row], encoding))


@pytest.mark.parametrize(
    "header",
    [
        EXPECTED_COLUMNS[::-1],
        EXPECTED_COLUMNS[:-1],
        ["Unexpected", *EXPECTED_COLUMNS[1:]],
    ],
)
def test_invalid_header(tmp_path, row, header):
    with pytest.raises(ValueError, match="Line 1, columns: unexpected CSV header"):
        validate_csv(write_csv(tmp_path, [header, row]))


@pytest.mark.parametrize("rows", [[], [EXPECTED_COLUMNS], [EXPECTED_COLUMNS, [], []]])
def test_empty_csv(tmp_path, rows):
    with pytest.raises(ValueError):
        validate_csv(write_csv(tmp_path, rows))


def test_missing_file(tmp_path):
    with pytest.raises(FileNotFoundError):
        validate_csv(tmp_path / "missing.csv")


def test_sha256_hashes_raw_bytes(tmp_path):
    path = tmp_path / "sample"
    content = b"example\r\n" * 200000
    path.write_bytes(content)
    assert calculate_sha256(path) == hashlib.sha256(content).hexdigest()
