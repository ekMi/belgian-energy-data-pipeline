import csv
from datetime import UTC, datetime
from pathlib import Path

import psycopg

from config import CSV_PATH, get_connection_params
from quality_ods001 import QualityResult, evaluate_quality, has_errors
from quality_store import save_results
from validate_ods001 import (
    calculate_sha256,
    parse_row,
    validate_csv,
)

COPY_SQL = """
    COPY staging.elia_load (
        timestamp_utc,
        resolution_code,
        total_load_mw,
        most_recent_forecast_mw,
        most_recent_p10_mw,
        most_recent_p90_mw,
        day_ahead_forecast_mw,
        day_ahead_p10_mw,
        day_ahead_p90_mw,
        week_ahead_forecast_mw,
        ingestion_batch_id
    )
    FROM STDIN
"""


def ingest_csv(path: Path) -> int | None:
    checksum = calculate_sha256(path)
    as_of = datetime.now(UTC)
    results = []
    with psycopg.connect(**get_connection_params()) as conn:
        existing = conn.execute(
            "SELECT batch_id FROM staging.ingestion_batch "
            "WHERE file_sha256=%s AND status='SUCCESS' LIMIT 1",
            (checksum,),
        ).fetchone()
        if existing:
            print(f"File already ingested successfully (batch_id={existing[0]}).")
            return None
        batch_id = conn.execute(
            "INSERT INTO staging.ingestion_batch (source_file,file_sha256,started_at) "
            "VALUES (%s,%s,clock_timestamp()) RETURNING batch_id",
            (path.name, checksum),
        ).fetchone()[0]
        conn.commit()
        try:
            with conn.transaction():
                validate_csv(path)
                records = []
                with path.open(encoding="utf-8-sig", newline="") as stream:
                    reader = csv.reader(stream, delimiter=";")
                    next(reader)
                    for row in reader:
                        records.append(parse_row(row, line_number=reader.line_num))
                results = evaluate_quality(records, as_of=as_of)
                with conn.cursor() as cur, cur.copy(COPY_SQL) as copy:
                    for record in records:
                        copy.write_row((*record, batch_id))
                if has_errors(results):
                    raise ValueError("Blocking data quality errors")
                count = conn.execute(
                    "SELECT count(*) FROM staging.elia_load WHERE ingestion_batch_id=%s",
                    (batch_id,),
                ).fetchone()[0]
                if count != len(records):
                    raise ValueError("Loaded row count mismatch")
                save_results(
                    conn, batch_id, results, as_of=as_of, scope="incoming_file"
                )
                conn.execute(
                    "UPDATE staging.ingestion_batch SET status='SUCCESS', "
                    "finished_at=clock_timestamp(),rows_loaded=%s WHERE batch_id=%s",
                    (count, batch_id),
                )
        except Exception as error:
            try:
                with conn.transaction():
                    status = conn.execute(
                        "SELECT status FROM staging.ingestion_batch WHERE batch_id=%s",
                        (batch_id,),
                    ).fetchone()[0]
                    if status != "SUCCESS":
                        results.append(
                            QualityResult(
                                "technical.ingestion",
                                "ERROR",
                                1,
                                1,
                                [{"error_type": type(error).__name__}],
                            )
                        )
                        conn.execute(
                            "UPDATE staging.ingestion_batch SET status='FAILED', "
                            "finished_at=clock_timestamp(),error_type=%s WHERE batch_id=%s",
                            (type(error).__name__, batch_id),
                        )
                if status != "SUCCESS":
                    # A report failure must not undo the durable FAILED status.
                    with conn.transaction():
                        save_results(
                            conn, batch_id, results, as_of=as_of, scope="failed_attempt"
                        )
            except Exception:
                error.add_note(
                    "Failure tracking incomplete; inspect the batch status and reports."
                )
            raise
    return count


def main() -> None:
    count = ingest_csv(CSV_PATH)

    if count is not None:
        print(f"Ingestion successful: {count:,} rows")


if __name__ == "__main__":
    main()
