"""Bounded post-merge checks over incoming keys and actual database neighbors."""

from quality_ods001 import QualityResult, evaluate_quality
from quality_store import COLUMNS

EDGE_SQL = """
WITH edges AS MATERIALIZED (
    SELECT i.resolution_code, p.timestamp_utc AS previous, i.timestamp_utc AS following
    FROM pg_temp.ods001_preview i
    CROSS JOIN LATERAL (
        SELECT timestamp_utc FROM staging.elia_load e
        WHERE e.resolution_code=i.resolution_code AND e.timestamp_utc<i.timestamp_utc
        ORDER BY timestamp_utc DESC LIMIT 1
    ) p
    UNION
    SELECT i.resolution_code, i.timestamp_utc, n.timestamp_utc
    FROM pg_temp.ods001_preview i
    CROSS JOIN LATERAL (
        SELECT timestamp_utc FROM staging.elia_load e
        WHERE e.resolution_code=i.resolution_code AND e.timestamp_utc>i.timestamp_utc
        ORDER BY timestamp_utc LIMIT 1
    ) n
), gaps AS (
    SELECT * FROM edges WHERE following-previous <> interval '15 minutes'
)
SELECT (SELECT count(*) FROM edges), (SELECT count(*) FROM gaps),
    COALESCE((SELECT jsonb_agg(to_jsonb(sample)) FROM
        (SELECT * FROM gaps ORDER BY previous,following,resolution_code LIMIT 5) sample),
        '[]'::jsonb)
"""


def check_consolidated(conn, *, as_of):
    columns = ", ".join("e." + c for c in COLUMNS.split(", "))
    # Select only keys in the input, never the full historical table.
    records = conn.execute(
        f"SELECT {columns} FROM staging.elia_load e JOIN pg_temp.ods001_preview i "
        "USING (timestamp_utc,resolution_code) ORDER BY e.timestamp_utc"
    ).fetchall()
    results = evaluate_quality(records, as_of=as_of, check_continuity=False)
    measures = COLUMNS.split(", ")[2:]
    comparison = (
        "ROW(" + ", ".join("e." + m for m in measures) + ") IS DISTINCT FROM "
        "ROW(" + ", ".join("i." + m for m in measures) + ")"
    )
    total, mismatches, start, end = conn.execute(
        "SELECT count(*), count(*) FILTER (WHERE e.timestamp_utc IS NULL OR "
        + comparison
        + "), min(i.timestamp_utc),max(i.timestamp_utc) "
        "FROM pg_temp.ods001_preview i LEFT JOIN staging.elia_load e "
        "USING (timestamp_utc,resolution_code)"
    ).fetchone()
    results.append(
        QualityResult(
            "technical.consolidated_matches_input", "ERROR", total, mismatches, []
        )
    )
    evaluated, anomalies, examples = conn.execute(EDGE_SQL).fetchone()
    results.append(
        QualityResult(
            "technical.consolidated_continuity",
            "WARNING",
            evaluated,
            anomalies,
            examples,
        )
    )
    parameters = {
        "selection": "incoming_keys_with_actual_neighbor_edges",
        "selected_key_count": total,
        "min_timestamp": start.isoformat() if start else None,
        "max_timestamp": end.isoformat() if end else None,
        "edge_count": evaluated,
        "value_checks_scope": "incoming_keys_current_values",
        "continuity_scope": "deduplicated_edges_incident_to_incoming_keys",
        "outside_endpoints": "not_inferred",
        "evaluation_state": "committed",
    }
    return results, parameters
