"""ODS001 ingestion: insert new rows, retain unchanged rows, reject corrections."""

import argparse
import sys
from contextlib import contextmanager
from datetime import UTC, datetime
from pathlib import Path

import psycopg

from config import CSV_PATH, get_connection_params
from merge_ods001 import classify_incoming, load_incoming
from quality_ods001 import QualityResult, evaluate_quality, has_errors
from quality_store import COLUMNS, save_results
from validate_ods001 import calculate_sha256

# Session lock: distinct from the integration fixture's (81002, 1).
INGESTION_LOCK = (81001, 1)


class ChangedObservationsError(ValueError):
    """D3 never updates existing business values."""


@contextmanager
def ingestion_lock(conn):
    # Set a bounded wait without changing caller-wide/server settings.
    with conn.transaction():
        conn.execute("SET LOCAL lock_timeout = '10s'")
        conn.execute("SELECT pg_advisory_lock(%s, %s)", INGESTION_LOCK)
    try:
        yield
    finally:
        # Connection closure also releases the session lock if communication fails.
        original_error = sys.exception()
        try:
            if not conn.closed:
                conn.rollback()
                with conn.transaction():
                    conn.execute("SELECT pg_advisory_unlock(%s, %s)", INGESTION_LOCK)
        except Exception:
            if original_error is None:
                raise
            original_error.add_note(
                "Explicit unlock failed; connection closure releases the lock."
            )


def ingest_csv(path: Path, *, input_scope: str = "continuous") -> int | None:
    if input_scope not in ("continuous", "partial"):
        raise ValueError("input_scope must be continuous or partial")
    with psycopg.connect(**get_connection_params()) as conn, ingestion_lock(conn):
        checksum = calculate_sha256(path)
        existing = conn.execute(
            "SELECT batch_id FROM staging.ingestion_batch "
            "WHERE file_sha256=%s AND status='SUCCESS' LIMIT 1",
            (checksum,),
        ).fetchone()
        if existing:
            print(f"File already ingested successfully (batch_id={existing[0]}).")
            return None
        batch_id = conn.execute(
            "INSERT INTO staging.ingestion_batch "
            "(source_file,file_sha256,started_at,input_scope,correction_policy) "
            "VALUES (%s,%s,clock_timestamp(),%s,'deny') RETURNING batch_id",
            (path.name, checksum, input_scope),
        ).fetchone()[0]
        conn.commit()
        as_of = datetime.now(UTC)
        results = []
        received = None
        continuity = input_scope == "continuous"
        try:
            with conn.transaction():
                conn.execute("SET LOCAL TIME ZONE 'UTC'")
                load_incoming(conn, path, checksum)
                classification = classify_incoming(conn, checksum)
                received = classification.rows_received
                # Existing quality rules require a bounded input snapshot, not
                # all existing database observations.
                records = conn.execute(
                    f"SELECT {COLUMNS} FROM pg_temp.ods001_preview ORDER BY timestamp_utc"
                ).fetchall()
                results = evaluate_quality(
                    records, as_of=as_of, check_continuity=continuity
                )
                if classification.rows_changed:
                    results.append(
                        QualityResult(
                            "technical.corrections_forbidden",
                            "ERROR",
                            received,
                            classification.rows_changed,
                            [
                                r
                                for r in classification.differences
                                if r["classification"] == "CHANGED"
                            ],
                        )
                    )
                    raise ChangedObservationsError(
                        f"{classification.rows_changed} changed observations; D3 rejects corrections"
                    )
                if has_errors(results):
                    raise ValueError("Blocking data quality errors")
                inserted = conn.execute(
                    f"INSERT INTO staging.elia_load ({COLUMNS},ingestion_batch_id) "
                    f"SELECT {', '.join('i.' + c for c in COLUMNS.split(', '))},%s "
                    "FROM pg_temp.ods001_preview i WHERE NOT EXISTS ("
                    "SELECT 1 FROM staging.elia_load e WHERE "
                    "e.timestamp_utc=i.timestamp_utc AND e.resolution_code=i.resolution_code)",
                    (batch_id,),
                ).rowcount
                if inserted != classification.rows_new:
                    raise ValueError("Inserted row count mismatch")
                save_results(
                    conn,
                    batch_id,
                    results,
                    as_of=as_of,
                    scope="incoming_file",
                    continuity=continuity,
                )
                conn.execute(
                    "UPDATE staging.ingestion_batch SET status='SUCCESS',"
                    "finished_at=clock_timestamp(),rows_loaded=%s,rows_received=%s,"
                    "rows_inserted=%s,rows_unchanged=%s,rows_updated=0 WHERE batch_id=%s",
                    (
                        received,
                        received,
                        inserted,
                        classification.rows_unchanged,
                        batch_id,
                    ),
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
                        zero = 0 if received is not None else None
                        conn.execute(
                            "UPDATE staging.ingestion_batch SET status='FAILED',"
                            "finished_at=clock_timestamp(),error_type=%s,"
                            "rows_received=%s,rows_inserted=%s,rows_unchanged=%s,"
                            "rows_updated=%s WHERE batch_id=%s",
                            (
                                type(error).__name__,
                                received,
                                zero,
                                zero,
                                zero,
                                batch_id,
                            ),
                        )
                if status != "SUCCESS":
                    with conn.transaction():
                        save_results(
                            conn,
                            batch_id,
                            results,
                            as_of=as_of,
                            scope="failed_attempt",
                            continuity=continuity,
                        )
            except Exception:
                error.add_note(
                    "Failure tracking incomplete; inspect the batch status and reports."
                )
            raise
    return received


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("csv_path", nargs="?", type=Path, default=CSV_PATH)
    parser.add_argument(
        "--input-scope", choices=("continuous", "partial"), default="continuous"
    )
    args = parser.parse_args()
    count = ingest_csv(args.csv_path, input_scope=args.input_scope)
    if count is not None:
        print(f"Ingestion successful: {count:,} accepted rows")


if __name__ == "__main__":
    main()
