"""Shared, explicit trust declaration; it does not authenticate a CSV."""

SOURCE_POLICIES = ("restricted", "elia-full")
AUTOMATIC_CORRECTION_REASON = (
    "Automatic update from authoritative Elia ODS001 full export"
)


def automatic_corrections(source_policy: str, input_scope: str) -> bool:
    if source_policy not in SOURCE_POLICIES:
        raise ValueError("source_policy must be restricted or elia-full")
    if input_scope not in ("continuous", "partial"):
        raise ValueError("input_scope must be continuous or partial")
    if source_policy == "elia-full" and input_scope != "continuous":
        raise ValueError(
            "elia-full requires a continuous full export, not partial input"
        )
    return source_policy == "elia-full"
