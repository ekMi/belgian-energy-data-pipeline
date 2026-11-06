"""National calendar reference dates and reproducible versioned seed."""

import csv
from collections import Counter
from datetime import date, timedelta
from pathlib import Path

import pytest
from scripts.generate_belgian_holidays import (
    belgian_holidays,
    easter_sunday,
    write_calendar,
)


@pytest.mark.parametrize(
    "year,easter,mobile",
    [
        (2015, "2015-04-05", ["2015-04-06", "2015-05-14", "2015-05-25"]),
        (2026, "2026-04-05", ["2026-04-06", "2026-05-14", "2026-05-25"]),
        (2038, "2038-04-25", ["2038-04-26", "2038-06-03", "2038-06-14"]),
    ],
)
def test_reference_holidays(year, easter, mobile):
    assert easter_sunday(year) == date.fromisoformat(easter)
    expected = {
        date(year, 1, 1): "Nouvel An",
        date.fromisoformat(mobile[0]): "Lundi de Pâques",
        date(year, 5, 1): "Fête du travail",
        date.fromisoformat(mobile[1]): "Ascension",
        date.fromisoformat(mobile[2]): "Lundi de Pentecôte",
        date(year, 7, 21): "Fête nationale",
        date(year, 8, 15): "Assomption",
        date(year, 11, 1): "Toussaint",
        date(year, 11, 11): "Armistice",
        date(year, 12, 25): "Noël",
    }
    assert belgian_holidays(year) == sorted(expected.items())


@pytest.mark.parametrize("year", range(2015, 2041))
def test_mobile_offsets_and_unique_sorted_dates(year):
    holidays = belgian_holidays(year)
    dates = [day for day, _ in holidays]
    assert len(dates) == len(set(dates)) == 10
    assert dates == sorted(dates)
    assert all(day.year == year for day in dates)
    easter = easter_sunday(year)
    assert easter.weekday() == 6
    by_name = {name: day for day, name in holidays}
    for name, offset, weekday in (
        ("Lundi de Pâques", 1, 0),
        ("Ascension", 39, 3),
        ("Lundi de Pentecôte", 50, 0),
    ):
        assert by_name[name] == easter + timedelta(days=offset)
        assert by_name[name].weekday() == weekday


def test_generated_seed_is_complete_deterministic_and_versionable(tmp_path):
    output = tmp_path / "seeds/holidays.csv"
    write_calendar(output)
    first = output.read_bytes()
    write_calendar(output)
    assert output.read_bytes() == first
    checked_in = (
        Path(__file__).resolve().parents[2] / "dbt/seeds/belgian_public_holidays.csv"
    )
    # Git may convert checkout line endings on Windows.
    assert first.decode("utf-8") == checked_in.read_text(encoding="utf-8")
    with output.open(encoding="utf-8", newline="") as stream:
        reader = csv.DictReader(stream)
        assert reader.fieldnames == ["holiday_date", "holiday_name"]
        rows = list(reader)
    assert len(rows) == 260
    dates = [date.fromisoformat(row["holiday_date"]) for row in rows]
    assert dates == sorted(set(dates))
    assert Counter(day.year for day in dates) == dict.fromkeys(range(2015, 2041), 10)
    assert dates[0] == date(2015, 1, 1)
    assert dates[-1] == date(2040, 12, 25)
    assert all(row["holiday_name"] for row in rows)


def test_custom_year_range_and_reversed_range(tmp_path):
    output = tmp_path / "holidays.csv"
    write_calendar(output, 2026, 2026)
    with output.open(encoding="utf-8") as stream:
        assert len(list(csv.DictReader(stream))) == 10
    with pytest.raises(ValueError, match="start_year"):
        write_calendar(output, 2040, 2015)
