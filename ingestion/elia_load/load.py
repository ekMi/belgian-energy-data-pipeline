from ingestion.common.database import get_connection
from ingestion.common.logger import get_logger

INSERT_SQL = """
    INSERT INTO raw.elia_load (
        datetime, resolution_code, total_load,
        most_recent_forecast, most_recent_p10, most_recent_p90,
        day_ahead_6pm_forecast, day_ahead_6pm_p10,
        day_ahead_6pm_p90, week_ahead_forecast
    )
    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    ON CONFLICT (datetime) DO UPDATE SET
        resolution_code = EXCLUDED.resolution_code,
        total_load = EXCLUDED.total_load,
        most_recent_forecast = EXCLUDED.most_recent_forecast,
        most_recent_p10 = EXCLUDED.most_recent_p10,
        most_recent_p90 = EXCLUDED.most_recent_p90,
        day_ahead_6pm_forecast = EXCLUDED.day_ahead_6pm_forecast,
        day_ahead_6pm_p10 = EXCLUDED.day_ahead_6pm_p10,
        day_ahead_6pm_p90 = EXCLUDED.day_ahead_6pm_p90,
        week_ahead_forecast = EXCLUDED.week_ahead_forecast
"""

logger = get_logger(__name__)


def load_ods001(rows: list[tuple]) -> int:
    logger.info("Starting database load")

    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.executemany(INSERT_SQL, rows)

    logger.info("Load completed: %s rows", len(rows))

    return len(rows)
