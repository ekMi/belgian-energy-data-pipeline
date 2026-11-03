"""Pure decision rules for local pipeline orchestration."""

from correction_policy import automatic_corrections


def decide_action(
    *,
    already_ingested: bool,
    rows_changed: int | None,
    source_policy: str = "restricted",
    input_scope: str = "continuous",
) -> str:
    """Return the next action without executing any processing."""

    automatic = automatic_corrections(source_policy, input_scope)
    if already_ingested:
        return "NO_CHANGE"

    if rows_changed is None or rows_changed < 0:
        raise ValueError("Invalid or missing preview")

    if rows_changed > 0 and not automatic:
        return "AWAITING_APPROVAL"

    return "INGEST"
