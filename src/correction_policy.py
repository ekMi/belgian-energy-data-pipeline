"""Shared, explicit trust declaration; it does not authenticate a CSV."""

SOURCE_POLICIES = ("restricted", "elia-full", "elia-api")
AUTOMATIC_CORRECTION_REASON = (
    "Automatic update from authoritative Elia ODS001 full export"
)

API_CORRECTION_REASON = (
    "Automatic update from validated official Elia ODS001 API acquisition"
)


def automatic_corrections(source_policy: str, input_scope: str) -> bool:
    if source_policy not in SOURCE_POLICIES:
        raise ValueError("source_policy must be restricted, elia-full or elia-api")
    if input_scope not in ("continuous", "partial"):
        raise ValueError("input_scope must be continuous or partial")
    if source_policy == "elia-full" and input_scope != "continuous":
        raise ValueError(
            "elia-full requires a continuous full export, not partial input"
        )
    if source_policy == "elia-api" and input_scope != "partial":
        raise ValueError("elia-api requires partial input from a validated acquisition")
    return source_policy in ("elia-full", "elia-api")
