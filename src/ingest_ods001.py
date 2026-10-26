"""ODS001 ingestion with explicit file-bound, historized corrections."""

import argparse
import logging
import re
import sys
import time
from contextlib import contextmanager
from datetime import UTC, datetime
from pathlib import Path

import psycopg

from config import (
    CSV_PATH,
    INGESTION_DATABASE,
    get_connection_params,
    positive_seconds,
    verify_database_target,
)
from consolidated_quality import check_consolidated
from merge_ods001 import (
    archive_corrections,
    classify_incoming,
    load_incoming,
    update_corrections,
)
from operations import run_cli
from quality_ods001 import QualityResult, evaluate_quality, has_errors
from quality_store import COLUMNS, save_results
from validate_ods001 import calculate_sha256

# Session lock: distinct from the integration fixture's (81002, 1).
INGESTION_LOCK = (81001, 1)
logger = logging.getLogger(__name__)


class ChangedObservationsError(ValueError):
    """Changed observations require explicit file-bound authorization."""


@contextmanager
def ingestion_lock(conn):
    # Set a bounded wait without changing caller-wide/server settings.
    with conn.transaction():
        conn.execute(
            "SELECT set_config('lock_timeout', %s, true)",
            (f"{positive_seconds('PIPELINE_LOCK_TIMEOUT', 10)}s",),
        )
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


def validate_authorization(authorized_sha256, correction_reason, extracted_at):
    if authorized_sha256 is None:
        if correction_reason is not None:
            raise ValueError("correction_reason requires authorized_sha256")
    elif (
        re.fullmatch(r"[0-9a-f]{64}", authorized_sha256) is None
        or correction_reason is None
        or not correction_reason.strip()
    ):
        raise ValueError(
            "Authorization requires a lowercase SHA-256 and a nonempty justification"
        )
    if extracted_at is not None and extracted_at.utcoffset() is None:
        raise ValueError("extracted_at requires an explicit UTC offset")


def ingest_csv(
    path: Path,
    *,
    input_scope: str = "continuous",
    authorized_sha256: str | None = None,
    correction_reason: str | None = None,
    extracted_at: datetime | None = None,
    source_reference: str | None = None,
) -> int | None:
    if input_scope not in ("continuous", "partial"):
        raise ValueError("input_scope must be continuous or partial")
    validate_authorization(authorized_sha256, correction_reason, extracted_at)
    started = time.monotonic()
    logger.info("ingestion_started file=%r scope=%s", path.name, input_scope)
    params = get_connection_params()
    if params["dbname"] != INGESTION_DATABASE:
        raise RuntimeError(
            "Refusing database writes: configured database is not authorized"
        )
    with psycopg.connect(**params) as conn, ingestion_lock(conn):
        verify_database_target(conn, params["dbname"], INGESTION_DATABASE)
        checksum = calculate_sha256(path)
        if authorized_sha256 is not None and authorized_sha256 != checksum:
            raise ValueError("Authorization SHA-256 does not match the input file")
        existing = conn.execute(
            "SELECT batch_id FROM staging.ingestion_batch "
            "WHERE file_sha256=%s AND status='SUCCESS' LIMIT 1",
            (checksum,),
        ).fetchone()
        if existing:
            logger.info(
                "ingestion_skipped file=%r batch_id=%s duration_seconds=%.3f",
                path.name,
                existing[0],
                time.monotonic() - started,
            )
            return None
        batch_id = conn.execute(
            "INSERT INTO staging.ingestion_batch "
            "(source_file,file_sha256,started_at,input_scope,correction_policy,"
            "correction_authorized_sha256,correction_reason,extracted_at,source_reference) "
            "VALUES (%s,%s,clock_timestamp(),%s,%s,%s,%s,%s,%s) RETURNING batch_id",
            (
                path.name,
                checksum,
                input_scope,
                "authorized" if authorized_sha256 else "deny",
                authorized_sha256,
                correction_reason,
                extracted_at,
                source_reference,
            ),
        ).fetchone()[0]
        conn.commit()
        logger.info("batch_started file=%r batch_id=%s", path.name, batch_id)
        as_of = datetime.now(UTC)
        results = []
        consolidated = None
        consolidated_parameters = None
        received = None
        continuity = input_scope == "continuous"
        try:
            with conn.transaction():
                conn.execute("SET LOCAL TIME ZONE 'UTC'")
                conn.execute(
                    "SELECT set_config('lock_timeout', %s, true)",
                    (f"{positive_seconds('PIPELINE_LOCK_TIMEOUT', 10)}s",),
                )
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
                if classification.rows_changed and authorized_sha256 is None:
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
                        f"{classification.rows_changed} changed observations; explicit authorization required"
                    )
                if has_errors(results):
                    raise ValueError("Blocking data quality errors")
                updated = 0
                if classification.rows_changed:
                    archived = archive_corrections(
                        conn, batch_id, extracted_at=extracted_at
                    )
                    if archived != classification.rows_changed:
                        raise ValueError("Archived correction count mismatch")
                    updated = update_corrections(conn, batch_id)
                    if updated != archived:
                        raise ValueError("Updated correction count mismatch")
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
                consolidated, consolidated_parameters = check_consolidated(
                    conn, as_of=as_of
                )
                if has_errors(consolidated):
                    raise ValueError("Blocking consolidated quality errors")
                save_results(
                    conn,
                    batch_id,
                    results,
                    as_of=as_of,
                    scope="incoming_file",
                    continuity=continuity,
                )
                save_results(
                    conn,
                    batch_id,
                    consolidated,
                    as_of=as_of,
                    scope="consolidated_range",
                    continuity=True,
                    parameters=consolidated_parameters,
                )
                conn.execute(
                    "UPDATE staging.ingestion_batch SET status='SUCCESS',"
                    "finished_at=clock_timestamp(),rows_loaded=%s,rows_received=%s,"
                    "rows_inserted=%s,rows_unchanged=%s,rows_updated=%s WHERE batch_id=%s",
                    (
                        received,
                        received,
                        inserted,
                        classification.rows_unchanged,
                        updated,
                        batch_id,
                    ),
                )
        except (Exception, KeyboardInterrupt) as error:
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
                    if consolidated is not None:
                        with conn.transaction():
                            save_results(
                                conn,
                                batch_id,
                                consolidated,
                                as_of=as_of,
                                scope="consolidated_range",
                                continuity=True,
                                parameters={
                                    **consolidated_parameters,
                                    "evaluation_state": "rolled_back",
                                },
                            )
            except Exception:
                logger.error("failure_tracking_incomplete batch_id=%s", batch_id)
                error.add_note(
                    "Failure tracking incomplete; inspect the batch status and reports."
                )
            logger.error(
                "ingestion_failed file=%r batch_id=%s error_type=%s duration_seconds=%.3f",
                path.name,
                batch_id,
                type(error).__name__,
                time.monotonic() - started,
            )
            raise
        warnings = sum(
            r.anomaly_count
            for r in [*results, *consolidated]
            if r.severity == "WARNING"
        )
        if warnings:
            logger.warning(
                "quality_warnings batch_id=%s rule_anomaly_total=%s", batch_id, warnings
            )
        logger.info(
            "ingestion_finished file=%r batch_id=%s received=%s inserted=%s unchanged=%s "
            "updated=%s duration_seconds=%.3f",
            path.name,
            batch_id,
            received,
            inserted,
            classification.rows_unchanged,
            updated,
            time.monotonic() - started,
        )
    return received


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("csv_path", nargs="?", type=Path, default=CSV_PATH)
    parser.add_argument(
        "--input-scope", choices=("continuous", "partial"), default="continuous"
    )
    parser.add_argument("--authorize-sha256")
    parser.add_argument("--correction-reason")
    parser.add_argument("--extracted-at", type=datetime.fromisoformat)
    parser.add_argument("--source-reference")
    args = parser.parse_args()
    count = ingest_csv(
        args.csv_path,
        input_scope=args.input_scope,
        authorized_sha256=args.authorize_sha256,
        correction_reason=args.correction_reason,
        extracted_at=args.extracted_at,
        source_reference=args.source_reference,
    )
    if count is not None:
        print(f"Ingestion successful: {count:,} accepted rows")


if __name__ == "__main__":
    raise SystemExit(run_cli(main))
