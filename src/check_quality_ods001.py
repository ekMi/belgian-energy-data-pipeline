"""Inspect current producer-batch values or immutable historical quality reports."""

import argparse
import json
from datetime import datetime

import psycopg

from config import get_connection_params
from quality_ods001 import evaluate_quality
from quality_store import COLUMNS, save_results


def analyze_batch(conn, batch_id, *, as_of, persist=False, continuity=False):
    # Producer-batch membership becomes sparse after corrections. Even legacy
    # callers requesting continuity cannot interpret this as an original export.
    batch = conn.execute(
        "SELECT status FROM staging.ingestion_batch WHERE batch_id=%s", (batch_id,)
    ).fetchone()
    if batch is None or batch[0] != "SUCCESS":
        raise ValueError("Expected an existing SUCCESS batch")
    records = conn.execute(
        f"SELECT {COLUMNS} FROM staging.elia_load WHERE ingestion_batch_id=%s",
        (batch_id,),
    ).fetchall()
    results = evaluate_quality(records, as_of=as_of, check_continuity=False)
    if persist:
        save_results(
            conn,
            batch_id,
            results,
            as_of=as_of,
            scope="current_batch_versions",
            continuity=False,
            parameters={
                "selection": "current_producer_batch",
                "selected_key_count": len(records),
                "original_extraction_reconstructed": False,
            },
        )
    return results


def historical_reports(conn, batch_id):
    """Read original reports, including their original scope and parameters."""
    return conn.execute(
        "SELECT to_jsonb(q) || jsonb_build_object('results',"
        "(SELECT coalesce(jsonb_agg(to_jsonb(r) ORDER BY r.check_id),'[]'::jsonb) "
        "FROM staging.quality_result r WHERE r.quality_run_id=q.quality_run_id)) "
        "FROM staging.quality_run q WHERE q.batch_id=%s ORDER BY q.quality_run_id",
        (batch_id,),
    ).fetchall()


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--batch-id", type=int, required=True)
    parser.add_argument("--as-of", type=datetime.fromisoformat)
    parser.add_argument("--persist", action="store_true")
    parser.add_argument(
        "--no-continuity",
        action="store_true",
        help="Compatibility flag: current producer-batch continuity is always disabled",
    )
    parser.add_argument("--mode", choices=("current", "historical"), default="current")
    args = parser.parse_args()
    if args.mode == "historical" and args.persist:
        parser.error("Historical reports cannot be rewritten")
    if args.mode == "current" and args.as_of is None:
        parser.error("--as-of is required for current-value analysis")
    if args.as_of is not None and args.as_of.utcoffset() is None:
        parser.error("--as-of requires a UTC offset")
    with psycopg.connect(**get_connection_params()) as conn:
        conn.execute("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ")
        if not args.persist:
            conn.execute("SET TRANSACTION READ ONLY")
        if args.mode == "historical":
            output = {
                "mode": "historical",
                "reports": [r[0] for r in historical_reports(conn, args.batch_id)],
            }
        else:
            results = analyze_batch(
                conn,
                args.batch_id,
                as_of=args.as_of,
                persist=args.persist,
                continuity=False,
            )
            output = {
                "mode": "current_batch_versions",
                "meaning": "Current values produced by this batch, not its original extraction",
                "continuity_checked": False,
                "results": [r.to_dict() for r in results],
            }
    print(json.dumps(output, indent=2))


if __name__ == "__main__":
    main()
