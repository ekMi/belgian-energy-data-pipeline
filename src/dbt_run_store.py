"""Persist tracked dbt rebuild attempts, without executing dbt."""

from contextlib import contextmanager
from dataclasses import dataclass
from datetime import datetime
from typing import Literal

import psycopg

from config import (
    INGESTION_DATABASE,
    get_connection_params,
    verify_database_target,
)

FailureCategory = Literal[
    "DBT_BUILD_FAILED", "DBT_PREFLIGHT_FAILED", "INTERRUPTED", "UNKNOWN_FAILURE"
]
FAILURE_CATEGORIES = frozenset(
    {"DBT_BUILD_FAILED", "DBT_PREFLIGHT_FAILED", "INTERRUPTED", "UNKNOWN_FAILURE"}
)


@dataclass(frozen=True)
class DbtBuildRun:
    build_id: int
    target_batch_id: int
    started_at: datetime
    finished_at: datetime | None
    status: str
    error_type: str | None


_COLUMNS = "build_id, target_batch_id, started_at, finished_at, status, error_type"


def _connection():
    params = get_connection_params()
    if params["dbname"] != INGESTION_DATABASE:
        raise RuntimeError(
            "Refusing database access: configured database is not authorized"
        )
    return psycopg.connect(**params), params["dbname"]


def _verified(connection, configured_database):
    verify_database_target(connection, configured_database, INGESTION_DATABASE)


def start_build(target_batch_id: int, *, connection=None) -> DbtBuildRun:
    """Create a RUNNING attempt for an existing SUCCESS ingestion batch."""
    if isinstance(target_batch_id, bool) or not isinstance(target_batch_id, int):
        raise ValueError("target_batch_id must be an integer")
    with _connection_context(connection=connection) as conn:
        row = conn.execute(
            "SELECT batch_id FROM staging.ingestion_batch "
            "WHERE batch_id=%s AND status='SUCCESS' FOR SHARE",
            (target_batch_id,),
        ).fetchone()
        if row is None:
            raise ValueError("target_batch_id must identify a SUCCESS ingestion batch")
        created = conn.execute(
            f"INSERT INTO staging.dbt_build_run (target_batch_id) "
            f"VALUES (%s) RETURNING {_COLUMNS}",
            (target_batch_id,),
        ).fetchone()
        return DbtBuildRun(*created)


def mark_success(build_id: int, *, connection=None) -> DbtBuildRun:
    """Finish exactly one RUNNING attempt after the caller confirms dbt success."""
    return _finish(build_id, "SUCCESS", None, connection=connection)


def mark_failed(
    build_id: int, error_type: FailureCategory, *, connection=None
) -> DbtBuildRun:
    """Finish a RUNNING attempt with an approved, non-sensitive category."""
    if error_type not in FAILURE_CATEGORIES:
        raise ValueError("error_type must be an approved failure category")
    return _finish(build_id, "FAILED", error_type, connection=connection)


def _finish(
    build_id: int, status: str, error_type: str | None, *, connection=None
) -> DbtBuildRun:
    if isinstance(build_id, bool) or not isinstance(build_id, int):
        raise ValueError("build_id must be an integer")
    with _connection_context(connection=connection) as conn:
        row = conn.execute(
            f"UPDATE staging.dbt_build_run "
            f"SET status=%s, finished_at=clock_timestamp(), error_type=%s "
            f"WHERE build_id=%s AND status='RUNNING' RETURNING {_COLUMNS}",
            (status, error_type, build_id),
        ).fetchone()
        if row is None:
            raise ValueError("build attempt is absent or no longer RUNNING")
        return DbtBuildRun(*row)


def latest_success() -> DbtBuildRun | None:
    """Return the successful build covering the highest target batch, if any."""
    with _connection_context(read_only=True) as conn:
        row = conn.execute(
            f"SELECT {_COLUMNS} FROM staging.dbt_build_run "
            "WHERE status='SUCCESS' "
            "ORDER BY target_batch_id DESC, build_id DESC LIMIT 1"
        ).fetchone()
        return DbtBuildRun(*row) if row else None


def list_builds(target_batch_id: int | None = None) -> list[DbtBuildRun]:
    """List attempts newest first, optionally limited to one target batch."""
    with _connection_context(read_only=True) as conn:
        if target_batch_id is None:
            rows = conn.execute(
                f"SELECT {_COLUMNS} FROM staging.dbt_build_run ORDER BY build_id DESC"
            ).fetchall()
        else:
            rows = conn.execute(
                f"SELECT {_COLUMNS} FROM staging.dbt_build_run "
                "WHERE target_batch_id=%s ORDER BY build_id DESC",
                (target_batch_id,),
            ).fetchall()
        return [DbtBuildRun(*row) for row in rows]


@contextmanager
def _connection_context(*, read_only: bool = False, connection=None):
    if connection is not None:
        params = get_connection_params()
        _verified(connection, params["dbname"])
        yield connection
        return
    conn, configured_database = _connection()
    with conn:
        if read_only:
            conn.execute("SET TRANSACTION READ ONLY")
        _verified(conn, configured_database)
        yield conn
