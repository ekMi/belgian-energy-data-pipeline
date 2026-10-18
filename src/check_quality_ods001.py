"""Analyze stored data; read-only unless --persist is supplied."""

import argparse
import json
from datetime import datetime

import psycopg

from config import get_connection_params
from quality_ods001 import evaluate_quality
from quality_store import COLUMNS, save_results


def analyze_batch(conn, batch_id, *, as_of, persist=False, continuity=True):
    batch = conn.execute(
        "SELECT status FROM staging.ingestion_batch WHERE batch_id=%s", (batch_id,)
    ).fetchone()
    if batch is None or batch[0] != "SUCCESS":
        raise ValueError("Expected an existing SUCCESS batch")
    records = conn.execute(
        f"SELECT {COLUMNS} FROM staging.elia_load WHERE ingestion_batch_id=%s",
        (batch_id,),
    ).fetchall()
    if not records:
        raise ValueError("Batch has no observations")
    results = evaluate_quality(records, as_of=as_of, check_continuity=continuity)
    if persist:
        save_results(
            conn,
            batch_id,
            results,
            as_of=as_of,
            scope="stored_batch",
            continuity=continuity,
        )
    return results


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--batch-id", type=int, required=True)
    parser.add_argument("--as-of", type=datetime.fromisoformat, required=True)
    parser.add_argument("--persist", action="store_true")
    parser.add_argument("--no-continuity", action="store_true")
    args = parser.parse_args()
    if args.as_of.utcoffset() is None:
        parser.error("--as-of requires a UTC offset")
    with psycopg.connect(**get_connection_params()) as conn:
        conn.execute("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ")
        if not args.persist:
            conn.execute("SET TRANSACTION READ ONLY")
        results = analyze_batch(
            conn,
            args.batch_id,
            as_of=args.as_of,
            persist=args.persist,
            continuity=not args.no_continuity,
        )
    print(json.dumps([r.to_dict() for r in results], indent=2))


if __name__ == "__main__":
    main()
