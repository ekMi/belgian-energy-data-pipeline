"""Exclusive session lock shared by ingestion and dbt; caller owns the connection."""

import sys
from contextlib import contextmanager

from config import positive_seconds

PIPELINE_LOCK = (81001, 1)


@contextmanager
def pipeline_lock(conn):
    # Set a bounded wait without changing caller-wide/server settings.
    with conn.transaction():
        conn.execute(
            "SELECT set_config('lock_timeout', %s, true)",
            (f"{positive_seconds('PIPELINE_LOCK_TIMEOUT', 10)}s",),
        )
        conn.execute("SELECT pg_advisory_lock(%s, %s)", PIPELINE_LOCK)
    try:
        yield conn
    finally:
        # Connection closure also releases the session lock if communication fails.
        original_error = sys.exception()
        try:
            if not conn.closed:
                conn.rollback()
                with conn.transaction():
                    conn.execute("SELECT pg_advisory_unlock(%s, %s)", PIPELINE_LOCK)
        except Exception:
            if original_error is None:
                raise
            original_error.add_note(
                "Explicit unlock failed; connection closure releases the lock."
            )
