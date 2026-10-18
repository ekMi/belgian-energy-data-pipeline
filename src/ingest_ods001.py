import csv
from pathlib import Path

import psycopg

from config import CSV_PATH, get_connection_params
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
    validate_csv(path)
    checksum = calculate_sha256(path)

    connection_params = get_connection_params()

    with psycopg.connect(**connection_params) as conn, conn.cursor() as cur:
        # Check whether this exact file was already ingested.
        cur.execute(
            """
            SELECT batch_id
            FROM staging.ingestion_batch
            WHERE file_sha256 = %s
              AND status = 'SUCCESS'
            ORDER BY batch_id DESC
            LIMIT 1
            """,
            (checksum,),
        )

        existing_batch = cur.fetchone()

        if existing_batch is not None:
            print(f"File already ingested successfully (batch_id={existing_batch[0]}).")
            print("Skipping ingestion.")
            return None

        # Create the ingestion batch.
        cur.execute(
            """
                INSERT INTO staging.ingestion_batch
                    (source_file, file_sha256)
                VALUES (%s, %s)
                RETURNING batch_id
                """,
            (path.name, checksum),
        )
        batch_id = cur.fetchone()[0]

        # Stream CSV rows into PostgreSQL.
        rows_loaded = 0

        with path.open("r", encoding="utf-8-sig", newline="") as file:
            reader = csv.reader(file, delimiter=";")
            next(reader)  # Header already validated

            with cur.copy(COPY_SQL) as copy:
                for row in reader:
                    record = (*parse_row(row, line_number=reader.line_num), batch_id)
                    copy.write_row(record)
                    rows_loaded += 1

        # Verify the actual number of inserted rows.
        cur.execute(
            """
                SELECT COUNT(*)
                FROM staging.elia_load
                WHERE ingestion_batch_id = %s
                """,
            (batch_id,),
        )
        database_count = cur.fetchone()[0]

        if database_count != rows_loaded:
            raise ValueError(
                f"Row count mismatch: {rows_loaded} sent, {database_count} stored"
            )

        # Mark the batch as successful.
        cur.execute(
            """
                UPDATE staging.ingestion_batch
                SET status = 'SUCCESS',
                    finished_at = CURRENT_TIMESTAMP,
                    rows_loaded = %s
                WHERE batch_id = %s
                """,
            (rows_loaded, batch_id),
        )

        # Connection context commits on success,
        # rolls back on exception.

    return rows_loaded


def main() -> None:
    count = ingest_csv(CSV_PATH)

    if count is not None:
        print(f"Ingestion successful: {count:,} rows")


if __name__ == "__main__":
    main()
