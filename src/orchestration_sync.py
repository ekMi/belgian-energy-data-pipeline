from dataclasses import dataclass


@dataclass(frozen=True)
class AnalyticsSyncStatus:
    latest_ingestion_batch: int | None
    latest_relevant_batch: int | None
    latest_dbt_batch: int | None
    dbt_required: bool


def is_dbt_required(
    *,
    latest_ingestion_batch: int | None,
    latest_relevant_batch: int | None,
    latest_dbt_batch: int | None,
) -> bool:
    """Determine whether analytical models need rebuilding."""

    if latest_ingestion_batch is None:
        return False

    if latest_dbt_batch is None:
        return True

    if latest_relevant_batch is None:
        return False

    return latest_relevant_batch > latest_dbt_batch
