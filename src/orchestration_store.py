"""Read-only access to pipeline execution metadata."""

import psycopg

from config import (
    INGESTION_DATABASE,
    get_connection_params,
    verify_database_target,
)
from orchestration_sync import AnalyticsSyncStatus, is_dbt_required


def find_successful_batch(file_sha256: str) -> int | None:
    """Return the successful batch ID for a SHA-256, if any."""

    params = get_connection_params()

    if params["dbname"] != INGESTION_DATABASE:
        raise RuntimeError("Unauthorized database target")

    with psycopg.connect(**params) as conn:
        conn.execute("SET TRANSACTION READ ONLY")

        verify_database_target(
            conn,
            params["dbname"],
            INGESTION_DATABASE,
        )

        row = conn.execute(
            """
            SELECT batch_id
            FROM staging.ingestion_batch
            WHERE file_sha256 = %s
              AND status = 'SUCCESS'
            LIMIT 1
            """,
            (file_sha256,),
        ).fetchone()

        return row[0] if row is not None else None


def get_analytics_sync_status() -> AnalyticsSyncStatus:
    """Read analytical synchronization state from PostgreSQL."""

    params = get_connection_params()

    if params["dbname"] != INGESTION_DATABASE:
        raise RuntimeError("Unauthorized database target")

    with psycopg.connect(**params) as conn:
        conn.execute("SET TRANSACTION READ ONLY")

        verify_database_target(
            conn,
            params["dbname"],
            INGESTION_DATABASE,
        )

        row = conn.execute(
            """
            SELECT
                (
                    SELECT MAX(batch_id)
                    FROM staging.ingestion_batch
                    WHERE status = 'SUCCESS'
                ) AS latest_ingestion_batch,

                (
                    SELECT MAX(batch_id)
                    FROM staging.ingestion_batch
                    WHERE status = 'SUCCESS'
                      AND (
                          rows_inserted > 0
                          OR rows_updated > 0
                          OR rows_inserted IS NULL
                          OR rows_updated IS NULL
                      )
                ) AS latest_relevant_batch,

                (
                    SELECT target_batch_id
                    FROM staging.dbt_build_run
                    WHERE status = 'SUCCESS'
                    ORDER BY target_batch_id DESC, build_id DESC
                    LIMIT 1
                ) AS latest_dbt_batch
            """
        ).fetchone()

    # Extract the three values from row
    latest_ingestion, latest_relevant, latest_dbt = row

    dbt_required = is_dbt_required(
        latest_ingestion_batch=latest_ingestion,
        latest_relevant_batch=latest_relevant,
        latest_dbt_batch=latest_dbt,
    )

    return AnalyticsSyncStatus(
        latest_dbt_batch=latest_dbt,
        latest_relevant_batch=latest_relevant,
        latest_ingestion_batch=latest_ingestion,
        dbt_required=dbt_required,
    )
