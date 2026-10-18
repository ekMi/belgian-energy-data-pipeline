"""Append quality reports; never update observations."""

from psycopg.types.json import Jsonb

COLUMNS = (
    "timestamp_utc, resolution_code, total_load_mw, most_recent_forecast_mw, "
    "most_recent_p10_mw, most_recent_p90_mw, day_ahead_forecast_mw, "
    "day_ahead_p10_mw, day_ahead_p90_mw, week_ahead_forecast_mw"
)


def save_results(conn, batch_id, results, *, as_of, scope, continuity=True):
    run_id = conn.execute(
        "INSERT INTO staging.quality_run (batch_id, as_of, scope, parameters) "
        "VALUES (%s,%s,%s,%s) RETURNING quality_run_id",
        (
            batch_id,
            as_of,
            scope,
            Jsonb(
                {
                    "publication_lag_hours": 24,
                    "continuity": continuity,
                    "outlier_min_samples": 20,
                    "outlier_mad_multiplier": 6,
                    "mad_scale": 1.4826,
                }
            ),
        ),
    ).fetchone()[0]
    for r in results:
        conn.execute(
            "INSERT INTO staging.quality_result VALUES (%s,%s,%s,%s,%s,%s,%s)",
            (
                run_id,
                r.check_id,
                r.check_version,
                r.severity,
                r.evaluated_count,
                r.anomaly_count,
                Jsonb(r.examples),
            ),
        )
    return run_id
