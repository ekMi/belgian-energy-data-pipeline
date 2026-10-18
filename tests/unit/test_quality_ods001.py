from datetime import UTC, datetime, timedelta

import pytest

from quality_ods001 import evaluate_quality, has_errors

AS_OF = datetime(2025, 1, 3, tzinfo=UTC)


def row(minute=0):
    return (
        datetime(2025, 1, 1, tzinfo=UTC) + timedelta(minutes=minute),
        "PT15M",
        100.0,
        100.0,
        90.0,
        110.0,
        100.0,
        90.0,
        110.0,
        100.0,
    )


def reports(rows, **kwargs):
    return {r.check_id: r for r in evaluate_quality(rows, as_of=AS_OF, **kwargs)}


def test_valid():
    result = reports([row(), row(15)])
    assert not any(r.anomaly_count for r in result.values())
    assert not has_errors(result.values())


def test_duplicate():
    result = reports([row(), row()])
    assert result["technical.unique_key"].anomaly_count == 2
    assert has_errors(result.values())


def test_quarter_hour():
    assert reports([row(1)])["technical.quarter_hour"].anomaly_count == 1


def test_gap_and_scope():
    assert reports([row(), row(30)])["technical.continuity"].anomaly_count == 1
    assert "technical.continuity" not in reports([row()], check_continuity=False)


@pytest.mark.parametrize("index", range(2, 10))
def test_missing_historical_and_pending(index):
    historical = list(row())
    historical[index] = None
    future = historical.copy()
    future[0] = AS_OF + timedelta(days=1)
    result = reports([historical, future])
    assert result[f"completeness.{index}.historical"].anomaly_count == 1
    assert result[f"completeness.{index}.pending"].anomaly_count == 1
    assert result[f"completeness.{index}.historical"].severity == (
        "WARNING" if index in (2, 9) else "ERROR"
    )


@pytest.mark.parametrize("index", range(2, 10))
def test_negative(index):
    record = list(row())
    record[index] = -1
    assert reports([record])[f"business.{index}.negative"].anomaly_count == 1


@pytest.mark.parametrize("forecast", [3, 6])
def test_forecast_interval(forecast):
    record = list(row())
    record[forecast] = 846
    report = reports([record])[f"business.{forecast}.interval"]
    assert report.anomaly_count == 1
    assert report.examples[0]["forecast"] == 846
    assert record[forecast] == 846


def test_outlier_and_zero_mad():
    rows = []
    for i in range(25):
        record = list(row(15 * i))
        record[2] = 100 + i % 5
        rows.append(record)
    rows[-1][2] = 10000
    assert reports(rows)["business.2.outlier"].anomaly_count == 1
    assert reports([row()] * 25)["business.2.outlier"].evaluated_count == 0


def test_examples_bounded_and_empty():
    result = reports([row()] * 10)["technical.unique_key"]
    assert len(result.examples) == 5
    assert result.anomaly_count == 10
    assert all(r.evaluated_count == 0 for r in reports([]).values())


def test_as_of_requires_offset():
    with pytest.raises(ValueError):
        evaluate_quality([row()], as_of=datetime(2025, 1, 3))


def test_publication_cutoff_inclusive():
    record = list(row())
    record[0] = AS_OF - timedelta(hours=24)
    record[2] = None
    assert reports([record])["completeness.2.historical"].anomaly_count == 1
