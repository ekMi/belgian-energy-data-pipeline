"""Preview ODS001 classification in PostgreSQL. No permanent data writes."""

import argparse
import csv
import json
from dataclasses import asdict, dataclass
from pathlib import Path

import psycopg

from config import get_connection_params
from quality_store import COLUMNS
from validate_ods001 import EXPECTED_COLUMNS, calculate_sha256, parse_row, validate_csv

MEASURES = tuple(COLUMNS.split(", ")[2:])
DISTINCT_MEASURES = (
    "ROW(" + ", ".join("incoming." + c for c in MEASURES) + ") "
    "IS DISTINCT FROM ROW(" + ", ".join("existing." + c for c in MEASURES) + ")"
)
CLASSIFICATION_SQL = f"""
    WITH classified AS MATERIALIZED (
        SELECT incoming.timestamp_utc, incoming.resolution_code,
            CASE WHEN existing.timestamp_utc IS NULL THEN 'NEW'
                 WHEN {DISTINCT_MEASURES} THEN 'CHANGED'
                 ELSE 'UNCHANGED' END AS classification,
            CASE WHEN existing.timestamp_utc IS NULL THEN NULL
                 ELSE to_jsonb(existing) - 'ingestion_batch_id' END AS old_values,
            to_jsonb(incoming) - 'source_line' AS new_values
        FROM pg_temp.ods001_preview incoming
        LEFT JOIN staging.elia_load existing
            USING (timestamp_utc, resolution_code)
    )
    SELECT count(*),
        count(*) FILTER (WHERE classification = 'NEW'),
        count(*) FILTER (WHERE classification = 'UNCHANGED'),
        count(*) FILTER (WHERE classification = 'CHANGED'),
        COALESCE((
            SELECT jsonb_agg(to_jsonb(sample) ORDER BY timestamp_utc, resolution_code)
            FROM (
                SELECT * FROM classified WHERE classification <> 'UNCHANGED'
                ORDER BY timestamp_utc, resolution_code LIMIT %s
            ) sample
        ), '[]'::jsonb)
    FROM classified
"""


@dataclass
class PreviewResult:
    file_sha256: str
    rows_received: int
    rows_new: int
    rows_unchanged: int
    rows_changed: int
    differences: list[dict]

    def to_dict(self):
        return asdict(self)


class DuplicateInputError(ValueError):
    """The incoming file contains repeated normalized business keys."""


def measure_differences(old_values, new_values):
    """Format only a bounded SQL sample; classification itself remains in SQL."""
    return {
        name: {
            "old": old_values[name] if old_values is not None else None,
            "new": new_values[name],
        }
        for name in MEASURES
        if old_values is None or old_values[name] != new_values[name]
    }


def load_incoming(conn, path, checksum):
    validate_csv(path)
    # CREATE TEMP needs a read/write transaction. Only temporary objects
    # are written; force_rollback also cleans them on successful preview.
    conn.execute(
        f"CREATE TEMP TABLE ods001_preview ON COMMIT DROP AS "
        f"SELECT {COLUMNS} FROM staging.elia_load WITH NO DATA"
    )
    conn.execute(
        "ALTER TABLE pg_temp.ods001_preview ADD COLUMN source_line BIGINT NOT NULL"
    )
    with path.open(encoding="utf-8-sig", newline="") as stream:
        reader = csv.reader(stream, delimiter=";")
        if next(reader, None) != EXPECTED_COLUMNS:
            raise ValueError("Line 1: CSV header changed before COPY")
        copied_rows = 0
        with (
            conn.cursor() as cur,
            cur.copy(
                f"COPY pg_temp.ods001_preview ({COLUMNS}, source_line) FROM STDIN"
            ) as copy,
        ):
            for row in reader:
                copy.write_row(
                    (
                        *parse_row(row, line_number=reader.line_num),
                        reader.line_num,
                    )
                )
                copied_rows += 1
        if copied_rows == 0:
            raise ValueError("CSV contains no data rows")

    if calculate_sha256(path) != checksum:
        raise ValueError("Input file changed during preview")
    duplicate = conn.execute(
        "SELECT timestamp_utc, resolution_code, count(*), min(source_line) "
        "FROM pg_temp.ods001_preview GROUP BY timestamp_utc, resolution_code "
        "HAVING count(*) > 1 ORDER BY timestamp_utc, resolution_code LIMIT 1"
    ).fetchone()
    if duplicate:
        timestamp, resolution, count, line = duplicate
        raise DuplicateInputError(
            f"Line {line}: duplicate key ({timestamp.isoformat()}, {resolution}); "
            f"{count} occurrences"
        )


def classify_incoming(conn, checksum, sample_size=5):
    total, new, unchanged, changed, samples = conn.execute(
        CLASSIFICATION_SQL, (sample_size,)
    ).fetchone()
    for sample in samples:
        sample["changed_measures"] = measure_differences(
            sample["old_values"], sample["new_values"]
        )
    return PreviewResult(checksum, total, new, unchanged, changed, samples)


def preview_csv(path: Path, *, sample_size: int = 5) -> PreviewResult:
    """Own a fresh connection and always roll back its temporary-table transaction."""
    if (
        isinstance(sample_size, bool)
        or not isinstance(sample_size, int)
        or not 0 <= sample_size <= 100
    ):
        raise ValueError("sample_size must be an integer between 0 and 100")
    validate_csv(path)
    checksum = calculate_sha256(path)
    with psycopg.connect(**get_connection_params()) as conn:
        with conn.transaction(force_rollback=True):
            conn.execute("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ")
            conn.execute("SET LOCAL TIME ZONE 'UTC'")
            load_incoming(conn, path, checksum)
            result = classify_incoming(conn, checksum, sample_size)
        return result


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("csv_path", type=Path)
    parser.add_argument("--sample-size", type=int, default=5)
    args = parser.parse_args()
    if not 0 <= args.sample_size <= 100:
        parser.error("--sample-size must be between 0 and 100")
    result = preview_csv(args.csv_path, sample_size=args.sample_size)
    print(json.dumps(result.to_dict(), indent=2))


if __name__ == "__main__":
    main()
