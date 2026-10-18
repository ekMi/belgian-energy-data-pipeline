from datetime import UTC, datetime
from unittest.mock import MagicMock

import pytest

from consolidated_quality import check_consolidated
from quality_ods001 import has_errors


@pytest.mark.parametrize("mismatches", [0, 1])
def test_persistable_scope_and_blocking_mismatch(mismatches):
    conn = MagicMock()
    now = datetime(2025, 1, 1, tzinfo=UTC)
    records = [(now, "PT15M", 100.0, 100.0, 90.0, 110.0, 100.0, 90.0, 110.0, 100.0)]
    fetched = MagicMock()
    fetched.fetchall.return_value = records
    count = MagicMock()
    count.fetchone.return_value = (1, mismatches, now, now)
    edges = MagicMock()
    edges.fetchone.return_value = (
        2,
        1,
        [{"previous": "2024-12-31T23:30:00+00:00", "following": now.isoformat()}],
    )
    conn.execute.side_effect = [fetched, count, edges]
    results, params = check_consolidated(conn, as_of=now)
    assert has_errors(results) == bool(mismatches)
    assert params["selected_key_count"] == 1
    assert params["edge_count"] == 2
    assert params["evaluation_state"] == "committed"
    continuity = next(
        r for r in results if r.check_id == "technical.consolidated_continuity"
    )
    assert continuity.anomaly_count == 1
    assert continuity.severity == "WARNING"
    assert not any(r.check_id == "technical.continuity" for r in results)
