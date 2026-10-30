"""Pure decision rules for local pipeline orchestration."""


def decide_action(
    *,
    already_ingested: bool,
    rows_changed: int | None,
    dbt_recovery_pending: bool = False,
) -> str:
    """Return the next action without executing any processing."""

    if dbt_recovery_pending:
        return "RECOVER_DBT"

    if already_ingested:
        return "NO_CHANGE"

    if rows_changed is None or rows_changed < 0:
        raise ValueError("Invalid or missing preview")

    if rows_changed > 0:
        return "AWAITING_APPROVAL"

    return "INGEST"
