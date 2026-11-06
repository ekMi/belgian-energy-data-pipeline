import csv
from datetime import date, timedelta
from pathlib import Path


def easter_sunday(year: int) -> date:
    """Compute Easter Sunday with the Gregorian Meeus/Jones/Butcher algorithm."""
    a = year % 19
    b, c = divmod(year, 100)
    d, e = divmod(b, 4)
    f = (b + 8) // 25
    g = (b - f + 1) // 3
    h = (19 * a + b - d - g + 15) % 30
    i, k = divmod(c, 4)
    weekday_correction = (32 + 2 * e + 2 * i - h - k) % 7
    m = (a + 11 * h + 22 * weekday_correction) // 451
    n = h + weekday_correction - 7 * m + 114
    return date(year, n // 31, n % 31 + 1)


def belgian_holidays(year: int) -> list[tuple[date, str]]:
    """Return the ten national holidays, without company replacement days."""
    easter = easter_sunday(year)

    return sorted(
        [
            (date(year, 1, 1), "Nouvel An"),
            (easter + timedelta(days=1), "Lundi de Pâques"),
            (date(year, 5, 1), "Fête du travail"),
            (easter + timedelta(days=39), "Ascension"),
            (easter + timedelta(days=50), "Lundi de Pentecôte"),
            (date(year, 7, 21), "Fête nationale"),
            (date(year, 8, 15), "Assomption"),
            (date(year, 11, 1), "Toussaint"),
            (date(year, 11, 11), "Armistice"),
            (date(year, 12, 25), "Noël"),
        ]
    )


def write_calendar(output: Path, start_year: int = 2015, end_year: int = 2040) -> None:
    """Write an inclusive year range as a deterministic UTF-8 CSV."""
    if start_year > end_year:
        raise ValueError("start_year must not exceed end_year")
    output.parent.mkdir(parents=True, exist_ok=True)
    with output.open("w", newline="", encoding="utf-8") as file:
        writer = csv.writer(file, lineterminator="\n")
        writer.writerow(["holiday_date", "holiday_name"])
        for year in range(start_year, end_year + 1):
            for holiday_date, name in belgian_holidays(year):
                writer.writerow([holiday_date.isoformat(), name])


def main() -> None:
    root = Path(__file__).resolve().parents[1]
    output = root / "dbt" / "seeds" / "belgian_public_holidays.csv"
    write_calendar(output)
    print(f"Generated: {output}")


if __name__ == "__main__":
    main()
