import pytest

from orchestration_sync import is_dbt_required


@pytest.mark.parametrize(
    ("latest_ingestion", "latest_relevant", "latest_dbt", "expected"),
    [
        (None, None, None, False),
        (2, 2, None, True),
        (2, 2, 2, False),
        (3, 3, 2, True),
        (3, 2, 2, False),
        (1, None, None, True),
        (5, 3, 4, False),
        (5, 4, 3, True),
        (5, 3, 3, False),
        (5, None, 4, False),
        (5, 3, None, True),
    ],
)
def test_dbt_synchronization(
    latest_ingestion,
    latest_relevant,
    latest_dbt,
    expected,
):
    result = is_dbt_required(
        latest_ingestion_batch=latest_ingestion,
        latest_relevant_batch=latest_relevant,
        latest_dbt_batch=latest_dbt,
    )

    assert result is expected
