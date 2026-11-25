import pytest
from ingestion.elia_load.validate import validate_csv

HEADER = (
    "Datetime;Resolution code;Total Load;Most recent forecast;"
    "Most recent P10;Most recent P90;Day-ahead 6PM forecast;"
    "Day-ahead 6PM P10;Day-ahead 6PM P90;Week-ahead forecast\n"
)

VALID_ROW = (
    "2019-01-17T10:45:00+01:00;PT15M;12348.83;11905.51;"
    "11269.19;12541.83;12639.83;11990.91;13288.75;12732.22\n"
)


def create_csv(tmp_path, content):
    path = tmp_path / "test.csv"
    path.write_text(content, encoding="utf-8")
    return path


def test_valid_csv(tmp_path):
    path = create_csv(tmp_path, HEADER + VALID_ROW)

    rows = validate_csv(path)

    assert len(rows) == 1
    assert rows[0][1] == "PT15M"


def test_empty_numeric_values(tmp_path):
    row = (
        "2019-01-17T10:45:00+01:00;PT15M;;11905.51;"
        "11269.19;12541.83;12639.83;11990.91;13288.75;12732.22\n"
    )
    path = create_csv(tmp_path, HEADER + row)

    rows = validate_csv(path)

    assert rows[0][2] is None


def test_invalid_header(tmp_path):
    path = create_csv(tmp_path, "Wrong;Header\n" + VALID_ROW)

    with pytest.raises(ValueError, match="Unexpected CSV headers"):
        validate_csv(path)


def test_invalid_numeric_value(tmp_path):
    row = VALID_ROW.replace("12348.83", "invalid")
    path = create_csv(tmp_path, HEADER + row)

    with pytest.raises(ValueError, match="CSV validation failed"):
        validate_csv(path)


def test_invalid_row_length(tmp_path):
    row = "2019-01-17T10:45:00+01:00;PT15M;12348.83\n"
    path = create_csv(tmp_path, HEADER + row)

    with pytest.raises(ValueError, match="CSV validation failed"):
        validate_csv(path)


def test_duplicate_datetime(tmp_path):
    path = create_csv(tmp_path, HEADER + VALID_ROW + VALID_ROW)

    with pytest.raises(ValueError, match="CSV validation failed"):
        validate_csv(path)
