"""Read-only orchestration planning for a local CSV."""

import argparse
import json
from dataclasses import asdict, dataclass
from pathlib import Path

from merge_ods001 import preview_csv
from operations import run_cli
from orchestration_decision import decide_action
from orchestration_store import find_successful_batch
from validate_ods001 import calculate_sha256


@dataclass(frozen=True)
class PipelinePlan:
    file_sha256: str
    successful_batch_id: int | None
    rows_new: int | None
    rows_changed: int | None
    action: str


def plan_csv(csv_path: Path) -> PipelinePlan:
    """Determine the next action without running ingestion or dbt."""

    # 1. Calculate the file checksum
    checksum = calculate_sha256(csv_path)

    # 2. Check whether the file was already ingested
    batch_id = find_successful_batch(checksum)

    # 3. A successfully ingested file needs no new ingestion
    if batch_id is not None:
        action = decide_action(
            already_ingested=True,
            rows_changed=None,
        )

        return PipelinePlan(
            file_sha256=checksum,
            successful_batch_id=batch_id,
            rows_new=None,
            rows_changed=None,
            action=action,
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
    )

    return PipelinePlan(
        file_sha256=checksum,
        successful_batch_id=None,
        rows_new=preview.rows_new,
        rows_changed=preview.rows_changed,
        action=action,
    )


def main() -> None:
    """Run the read-only pipeline planner from the CLI."""
    parser = argparse.ArgumentParser(
        description="Previex the next pipeline action without executing it"
    )

    parser.add_argument("csv_path", type=Path, help="Path to rhe Elia ODS001 CSV file")

    args = parser.parse_args()

    # Call plan_csv
    plan = plan_csv(args.csv_path)

    # Convert dataclass to dict
    plan = asdict(plan)

    print(json.dumps(plan, indent=2))


if __name__ == "__main__":
    raise (SystemExit(run_cli(main)))
