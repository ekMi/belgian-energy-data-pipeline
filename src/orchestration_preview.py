"""Read-only orchestration planning for a local CSV."""

import argparse
import json
from dataclasses import asdict, dataclass
from pathlib import Path

from acquire_elia import AcquisitionResult, verify_acquisition
from correction_policy import SOURCE_POLICIES, automatic_corrections
from merge_ods001 import preview_csv
from operations import run_cli
from orchestration_decision import decide_action
from orchestration_store import (
    find_successful_batch,
    get_analytics_sync_status,
)
from validate_ods001 import calculate_sha256


@dataclass(frozen=True)
class PipelinePlan:
    file_sha256: str
    successful_batch_id: int | None
    rows_new: int | None
    rows_changed: int | None
    action: str
    dbt_required: bool = False


def plan_csv(
    csv_path: Path,
    *,
    source_policy: str = "restricted",
    input_scope: str = "continuous",
    api_acquisition: AcquisitionResult | None = None,
) -> PipelinePlan:
    """Determine the next action without running ingestion or dbt."""

    automatic_corrections(source_policy, input_scope)
    if source_policy == "elia-api":
        verify_acquisition(api_acquisition, csv_path)
    # 1. Calculate the file checksum
    checksum = calculate_sha256(csv_path)

    # 2. Check whether the file was already ingested
    batch_id = find_successful_batch(checksum)

    sync_status = get_analytics_sync_status()

    # 3. A successfully ingested file needs no new ingestion
    if batch_id is not None:
        action = decide_action(
            already_ingested=True,
            rows_changed=None,
            source_policy=source_policy,
            input_scope=input_scope,
        )

        return PipelinePlan(
            file_sha256=checksum,
            successful_batch_id=batch_id,
            rows_new=None,
            rows_changed=None,
            action=action,
            dbt_required=sync_status.dbt_required,
        )

    # 4. Preview the file
    preview = preview_csv(csv_path)

    # 5. Check that the checksum has not changed
    if preview.file_sha256 != checksum:
        raise RuntimeError("CSV changed during preview")

    # 6. Call decide_action()
    action = decide_action(
        already_ingested=False,
        rows_changed=preview.rows_changed,
        source_policy=source_policy,
        input_scope=input_scope,
    )

    return PipelinePlan(
        file_sha256=checksum,
        successful_batch_id=None,
        rows_new=preview.rows_new,
        rows_changed=preview.rows_changed,
        action=action,
        dbt_required=sync_status.dbt_required,
    )


def main() -> None:
    """Run the read-only pipeline planner from the CLI."""
    parser = argparse.ArgumentParser(
        description="Preview the next pipeline action without executing it"
    )

    parser.add_argument("csv_path", type=Path, help="Path to the Elia ODS001 CSV file")
    parser.add_argument(
        "--source-policy", choices=SOURCE_POLICIES, default="restricted"
    )
    parser.add_argument(
        "--input-scope", choices=("continuous", "partial"), default="continuous"
    )

    args = parser.parse_args()

    # Call plan_csv
    plan = plan_csv(
        args.csv_path, source_policy=args.source_policy, input_scope=args.input_scope
    )

    # Convert dataclass to dict
    plan = asdict(plan)

    print(json.dumps(plan, indent=2))


if __name__ == "__main__":
    raise (SystemExit(run_cli(main)))
