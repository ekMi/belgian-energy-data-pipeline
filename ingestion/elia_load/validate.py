import csv
from datetime import datetime
from decimal import Decimal, InvalidOperation
from pathlib import Path

from ingestion.common.logger import get_logger

logger = get_logger(__name__)

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


NUMERIC_COLUMNS = range(2, 10)


def validate_csv(path: Path) -> list[tuple]:
    logger.info("Starting validation of %s", path)

    validated_rows = []
    errors = []
    seen_timestamps = set()

    with open(path, encoding="utf-8-sig", newline="") as csvfile:
        reader = csv.reader(csvfile, delimiter=";")

        headers = next(reader, None)

        if headers != EXPECTED_COLUMNS:
            raise ValueError(f"Unexpected CSV headers: {headers}")

        for line_number, row in enumerate(reader, start=2):
            if len(row) != len(EXPECTED_COLUMNS):
                errors.append(f"Line {line_number}: expected 10 fields, got {len(row)}")
                continue

            try:
                # Validate datetime
                timestamp = datetime.fromisoformat(row[0])
                if timestamp in seen_timestamps:
                    raise ValueError(f"Duplicate datetime: {timestamp}")

                seen_timestamps.add(timestamp)

                # Validate resolution
                if row[1] != "PT15M":
                    raise ValueError(f"Unexpected resolution: {row[1]}")

                # Validate numeric fields
                values = []
                for value in row[2:]:
                    values.append(Decimal(value) if value.strip() else None)

                validated_rows.append((timestamp, row[1], *values))

            except (ValueError, InvalidOperation) as exc:
                errors.append(f"Line {line_number}: {exc}")

    if errors:
        logger.error("CSV validation failed: %s errors detected", len(errors))
        for error in errors:
            logger.error(error)

        raise ValueError("CSV validation failed. No data loaded.")

    logger.info("No duplicate timestamps detected")
    logger.info("Validation successful: %s rows", len(validated_rows))

    return validated_rows
