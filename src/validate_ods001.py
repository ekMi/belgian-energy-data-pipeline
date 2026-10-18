import csv
import hashlib
import math
from datetime import UTC, datetime
from pathlib import Path

from config import CSV_PATH
from operations import run_cli

EXPECTED_COLUMNS = [
    "Datetime",
    "Resolution code",
    "Total Load",
    "Most recent forecast",
    "Most recent P10",
    "Most recent P90",
    "Day-ahead 6PM forecast",
    "Day-ahead 6PM P10",
    "Day-ahead 6PM P90",
    "Week-ahead forecast",
]


def calculate_sha256(path: Path) -> str:
    digest = hashlib.sha256()

    with path.open("rb") as file:
        for chunk in iter(lambda: file.read(1024 * 1024), b""):
            digest.update(chunk)

    return digest.hexdigest()


def parse_row(row: list[str], *, line_number: int) -> tuple:
    """Parse ten business fields; report physical CSV line and column on errors."""
    if len(row) != len(EXPECTED_COLUMNS):
        raise ValueError(
            f"Line {line_number}, columns: expected 10 fields, got {len(row)}"
        )

    def error(index: int, message: str) -> ValueError:
        return ValueError(
            f"Line {line_number}, column {index + 1} "
            f"({EXPECTED_COLUMNS[index]}): {message}"
        )

    # Only Total Load and Week-ahead forecast are nullable in the SQL schema.
    for index, value in enumerate(row):
        if index not in (2, 9) and not value.strip():
            raise error(index, "required field is empty")

    try:
        timestamp = datetime.fromisoformat(row[0])
    except ValueError:
        raise error(0, "expected an ISO 8601 timestamp with a UTC offset") from None
    if timestamp.utcoffset() is None:
        raise error(0, "UTC offset is required")
    try:
        timestamp = timestamp.astimezone(UTC)
    except (OverflowError, ValueError):
        raise error(0, "timestamp is outside the supported UTC range") from None

    if row[1] != "PT15M":
        raise error(1, "expected PT15M")

    values = []
    for index in range(2, 10):
        value = row[index]
        if value == "" and index in (2, 9):
            values.append(None)
            continue
        try:
            number = float(value)
        except ValueError:
            raise error(index, "expected a number or an allowed empty field") from None
        if not math.isfinite(number):
            raise error(index, "numeric value must be finite")
        values.append(number)

    return (timestamp, row[1], *values)


def validate_csv(path: Path) -> None:
    """Validate the exact header and presence of data, not every data row."""
    if not path.is_file():
        raise FileNotFoundError(f"CSV file not found: {path}")

    with path.open("r", encoding="utf-8-sig", newline="") as file:
        reader = csv.reader(file, delimiter=";")
        header = next(reader, None)
        if header != EXPECTED_COLUMNS:
            raise ValueError(
                f"Line 1, columns: unexpected CSV header. "
                f"Expected: {EXPECTED_COLUMNS}; found: {header}"
            )
        if not any(row for row in reader):
            raise ValueError("Line 2, columns: CSV contains no data rows")


def main() -> None:
    validate_csv(CSV_PATH)
    checksum = calculate_sha256(CSV_PATH)

    print("CSV validation: OK")
    print(f"Source: {CSV_PATH}")
    print(f"SHA-256: {checksum}")


if __name__ == "__main__":
    raise SystemExit(run_cli(main))
