"""Read-only inventory of RUNNING batches; age is not proof of abandonment."""

import argparse
import json

import psycopg

from config import get_connection_params
from operations import run_cli


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--older-than-hours", type=int, default=24)
    args = parser.parse_args()
    if args.older_than_hours < 1:
        parser.error("--older-than-hours must be positive")
    with psycopg.connect(**get_connection_params()) as conn:
        conn.execute("SET TRANSACTION READ ONLY")
        rows = conn.execute(
            "SELECT batch_id,started_at,clock_timestamp()-started_at AS age "
            "FROM staging.ingestion_batch WHERE status='RUNNING' "
            "AND started_at < clock_timestamp() - (%s * interval '1 hour') "
            "ORDER BY started_at",
            (args.older_than_hours,),
        ).fetchall()
    print(
        json.dumps(
            [
                {
                    "batch_id": r[0],
                    "started_at": r[1].isoformat(),
                    "age": str(r[2]),
                    "assessment": "requires_manual_review",
                }
                for r in rows
            ],
            indent=2,
        )
    )


if __name__ == "__main__":
    raise SystemExit(run_cli(main))
