"""Preview ODS001 classification in PostgreSQL. No permanent data writes."""

import argparse
import csv
import json
import os
import tempfile
from dataclasses import asdict, dataclass
from pathlib import Path

import psycopg

from config import get_connection_params
from operations import run_cli
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
FULL_DIFFERENCES_SQL = f"""
    SELECT to_jsonb(classified)
    FROM (
        SELECT incoming.timestamp_utc, incoming.resolution_code,
            CASE WHEN existing.timestamp_utc IS NULL THEN 'NEW'
                 ELSE 'CHANGED' END AS classification,
            CASE WHEN existing.timestamp_utc IS NULL THEN NULL
                 ELSE to_jsonb(existing) - 'ingestion_batch_id' END AS old_values,
            to_jsonb(incoming) - 'source_line' AS new_values
        FROM pg_temp.ods001_preview incoming
        LEFT JOIN staging.elia_load existing
            USING (timestamp_utc, resolution_code)
        WHERE existing.timestamp_utc IS NULL OR {DISTINCT_MEASURES}
    ) classified
    ORDER BY timestamp_utc, resolution_code
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


def write_full_report(conn, result, output):
    """Stream differences to a sibling file, publishing only a complete report."""
    summary = {
        "changed_by_measure": dict.fromkeys(MEASURES, 0),
        "null_to_value": dict.fromkeys(MEASURES, 0),
        "value_to_null": dict.fromkeys(MEASURES, 0),
        "first_changed_utc": None,
        "last_changed_utc": None,
    }
    temporary = None
    try:
        with tempfile.NamedTemporaryFile(
            mode="w",
            encoding="utf-8",
            newline="",
            dir=output.parent,
            prefix=f".{output.name}.",
            suffix=".tmp",
            delete=False,
        ) as stream:
            temporary = Path(stream.name)
            stream.write("{\n")
            for name in (
                "file_sha256",
                "rows_received",
                "rows_new",
                "rows_unchanged",
                "rows_changed",
            ):
                stream.write(
                    f"  {json.dumps(name)}: {json.dumps(getattr(result, name))},\n"
                )
            stream.write('  "differences": [\n')
            count = 0
            with conn.cursor(name="ods001_full_differences") as cursor:
                cursor.execute(FULL_DIFFERENCES_SQL)
                while rows := cursor.fetchmany(1000):
                    for (difference,) in rows:
                        difference["changed_measures"] = measure_differences(
                            difference["old_values"], difference["new_values"]
                        )
                        if difference["classification"] == "CHANGED":
                            timestamp = difference["timestamp_utc"]
                            summary["first_changed_utc"] = (
                                summary["first_changed_utc"] or timestamp
                            )
                            summary["last_changed_utc"] = timestamp
                            for name, values in difference["changed_measures"].items():
                                summary["changed_by_measure"][name] += 1
                                if values["old"] is None:
                                    summary["null_to_value"][name] += 1
                                elif values["new"] is None:
                                    summary["value_to_null"][name] += 1
                        if count:
                            stream.write(",\n")
                        stream.write("    ")
                        json.dump(difference, stream)
                        count += 1
            if count != result.rows_new + result.rows_changed:
                raise ValueError("Full report count does not match classification")
            stream.write('\n  ],\n  "change_summary": ')
            json.dump(summary, stream)
            stream.write("\n}\n")
        os.replace(temporary, output)
    finally:
        if temporary is not None:
            temporary.unlink(missing_ok=True)
    return summary


def preview_full_csv(path: Path, output: Path) -> dict:
    """Produce a complete report under the same isolated rollback policy."""
    if path.resolve() == output.resolve():
        raise ValueError("Output path must differ from input CSV")
    validate_csv(path)
    checksum = calculate_sha256(path)
    with psycopg.connect(**get_connection_params()) as conn:
        with conn.transaction(force_rollback=True):
            conn.execute("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ")
            conn.execute("SET LOCAL TIME ZONE 'UTC'")
            load_incoming(conn, path, checksum)
            result = classify_incoming(conn, checksum, 0)
            summary = write_full_report(conn, result, output)
    return {
        "output": str(output),
        "file_sha256": checksum,
        "rows_received": result.rows_received,
        "rows_new": result.rows_new,
        "rows_unchanged": result.rows_unchanged,
        "rows_changed": result.rows_changed,
        "change_summary": summary,
    }


def archive_corrections(conn, batch_id, *, extracted_at=None):
    """Lock current rows, reject known stale inputs and archive before UPDATE."""
    conn.execute(
        f"SELECT existing.timestamp_utc FROM staging.elia_load existing "
        "JOIN pg_temp.ods001_preview incoming USING (timestamp_utc,resolution_code) "
        f"WHERE {DISTINCT_MEASURES} FOR UPDATE OF existing"
    )
    if extracted_at is not None:
        stale = conn.execute(
            f"SELECT count(*) FROM staging.elia_load existing "
            "JOIN pg_temp.ods001_preview incoming USING (timestamp_utc,resolution_code) "
            f"WHERE {DISTINCT_MEASURES} AND EXISTS ("
            "SELECT 1 FROM staging.ingestion_batch b WHERE b.extracted_at > %s AND ("
            "b.batch_id=existing.ingestion_batch_id OR EXISTS ("
            "SELECT 1 FROM staging.elia_load_correction h "
            "WHERE h.timestamp_utc=existing.timestamp_utc "
            "AND h.resolution_code=existing.resolution_code "
            "AND b.batch_id IN (h.previous_batch_id,h.correction_batch_id))))",
            (extracted_at,),
        ).fetchone()[0]
        if stale:
            raise ValueError(
                f"Stale extraction: {stale} corrections predate known versions"
            )
    names = ", ".join(
        ["timestamp_utc", "resolution_code", "previous_batch_id", "correction_batch_id"]
        + ["old_" + m for m in MEASURES]
        + ["new_" + m for m in MEASURES]
    )
    values = ", ".join(
        ["existing." + m for m in MEASURES] + ["incoming." + m for m in MEASURES]
    )
    return conn.execute(
        f"INSERT INTO staging.elia_load_correction ({names}) "
        f"SELECT existing.timestamp_utc,existing.resolution_code,"
        f"existing.ingestion_batch_id,%s,{values} FROM staging.elia_load existing "
        "JOIN pg_temp.ods001_preview incoming USING (timestamp_utc,resolution_code) "
        f"WHERE {DISTINCT_MEASURES}",
        (batch_id,),
    ).rowcount


def update_corrections(conn, batch_id):
    """Apply only this batch's archived changes, checking the old version."""
    assignments = ", ".join(m + "=history.new_" + m for m in MEASURES)
    old_row = ", ".join("history.old_" + m for m in MEASURES)
    current_row = ", ".join("existing." + m for m in MEASURES)
    return conn.execute(
        f"UPDATE staging.elia_load existing SET {assignments}, "
        "ingestion_batch_id=history.correction_batch_id "
        "FROM staging.elia_load_correction history "
        "WHERE history.correction_batch_id=%s "
        "AND existing.timestamp_utc=history.timestamp_utc "
        "AND existing.resolution_code=history.resolution_code "
        "AND existing.ingestion_batch_id=history.previous_batch_id "
        f"AND ROW({current_row}) IS NOT DISTINCT FROM ROW({old_row})",
        (batch_id,),
    ).rowcount


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
    parser.add_argument("--full-report", action="store_true")
    parser.add_argument("--output", type=Path)
    args = parser.parse_args()
    if not 0 <= args.sample_size <= 100:
        parser.error("--sample-size must be between 0 and 100")
    if args.full_report != (args.output is not None):
        parser.error("--full-report and --output must be used together")
    if args.full_report:
        print(json.dumps(preview_full_csv(args.csv_path, args.output), indent=2))
        return
    result = preview_csv(args.csv_path, sample_size=args.sample_size)
    print(json.dumps(result.to_dict(), indent=2))


if __name__ == "__main__":
    raise SystemExit(run_cli(main))
