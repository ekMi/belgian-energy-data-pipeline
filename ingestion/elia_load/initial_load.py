import csv
from pathlib import Path

from ingestion.common.database import get_connection

PROJECT_ROOT = Path(__file__).resolve().parents[2]
ODS_PATH = PROJECT_ROOT / "data" / "raw" / "ods001.csv"


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


def load_ods001(path: Path):
    with get_connection() as conn:
        with open(path, encoding="utf-8", newline="") as csvfile:
            csv_reader = csv.reader(csvfile, delimiter=";")
            next(csv_reader)  # Skip header

            for row in csv_reader:
                row = [value if value != "" else None for value in row]
                conn.execute(INSERT_SQL, row)

    print("ODS001 loaded successfully.")


if __name__ == "__main__":
    load_ods001(ODS_PATH)
