"""Read-only access to pipeline execution metadata."""

import psycopg

from config import (
    INGESTION_DATABASE,
    get_connection_params,
    verify_database_target,
)


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
