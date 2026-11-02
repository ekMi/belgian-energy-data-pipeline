"""One manual local CSV ingestion followed by a tracked analytical rebuild."""

import argparse
import json
import logging
from dataclasses import asdict, dataclass
from datetime import datetime
from pathlib import Path

from dbt_guard import locked_build
from dbt_run_store import mark_failed, mark_success, start_build
from ingest_ods001 import (
    ChangedObservationsError,
    ingest_csv,
    validate_authorization,
)
from operations import run_cli
from orchestration_preview import plan_csv
from orchestration_store import get_analytics_sync_status

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class PipelineResult:
    status: str
    ingestion_batch_id: int | None = None
    target_batch_id: int | None = None
    build_id: int | None = None
    error_type: str | None = None
    rows_new: int | None = None
    rows_changed: int | None = None


def run_pipeline(
    csv_path: Path,
    *,
    input_scope: str = "continuous",
    authorized_sha256: str | None = None,
    correction_reason: str | None = None,
    extracted_at: datetime | None = None,
    source_reference: str | None = None,
) -> PipelineResult:
    """Ingest with explicit file-bound authorization, then recheck dbt under its lock.

    Ingestion owns and releases its own lock before locked_build is entered.
    Its transactional classification is authoritative if the preview became stale.
    A committed ingestion is never rolled back by a subsequent build failure.
    """
    validate_authorization(authorized_sha256, correction_reason, extracted_at)
    plan = plan_csv(csv_path)
    if authorized_sha256 is not None and authorized_sha256 != plan.file_sha256:
        raise ValueError("Authorization SHA-256 does not match the input file")
    if plan.action == "AWAITING_APPROVAL" and authorized_sha256 is None:
        return PipelineResult(
            "AWAITING_APPROVAL", rows_new=plan.rows_new, rows_changed=plan.rows_changed
        )

    # Recheck even a known file: its contents or database state may have changed.
    try:
        ingestion = ingest_csv(
            csv_path,
            input_scope=input_scope,
            authorized_sha256=authorized_sha256,
            correction_reason=correction_reason,
            extracted_at=extracted_at,
            source_reference=source_reference,
        )
    except ChangedObservationsError:
        return PipelineResult("AWAITING_APPROVAL")
    batch_id = ingestion.batch_id if ingestion else plan.successful_batch_id
    counts = {
        "rows_new": ingestion.rows_inserted if ingestion else None,
        "rows_changed": ingestion.rows_updated if ingestion else None,
    }

    with locked_build() as session:
        conn = session.connection
        sync = get_analytics_sync_status(connection=conn)
        if not sync.dbt_required:
            conn.commit()
            return PipelineResult(
                "SUCCESS" if ingestion else "NO_CHANGE", batch_id, **counts
            )
        target = sync.latest_ingestion_batch
        attempt = start_build(target, connection=conn)
        conn.commit()
        logger.info(
            "dbt_started build_id=%s target_batch_id=%s", attempt.build_id, target
        )
        try:
            session.build()
        except (Exception, KeyboardInterrupt) as error:
            conn.rollback()
            category = (
                "INTERRUPTED"
                if isinstance(error, KeyboardInterrupt)
                else "DBT_BUILD_FAILED"
            )
            mark_failed(attempt.build_id, category, connection=conn)
            conn.commit()
            logger.error(
                "dbt_failed build_id=%s error_type=%s exception_class=%s",
                attempt.build_id,
                category,
                type(error).__name__,
            )
            if isinstance(error, KeyboardInterrupt):
                raise
            return PipelineResult(
                "DBT_FAILED", batch_id, target, attempt.build_id, category, **counts
            )
        mark_success(attempt.build_id, connection=conn)
        conn.commit()
        logger.info("dbt_succeeded build_id=%s", attempt.build_id)
        return PipelineResult("SUCCESS", batch_id, target, attempt.build_id, **counts)


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("csv_path", type=Path)
    parser.add_argument(
        "--input-scope", choices=("continuous", "partial"), default="continuous"
    )
    parser.add_argument("--authorize-sha256")
    parser.add_argument("--correction-reason")
    parser.add_argument("--extracted-at", type=datetime.fromisoformat)
    parser.add_argument("--source-reference")
    args = parser.parse_args()
    result = run_pipeline(
        args.csv_path,
        input_scope=args.input_scope,
        authorized_sha256=args.authorize_sha256,
        correction_reason=args.correction_reason,
        extracted_at=args.extracted_at,
        source_reference=args.source_reference,
    )
    print(json.dumps(asdict(result), indent=2))
    if result.status in ("DBT_FAILED", "AWAITING_APPROVAL"):
        raise SystemExit(1)


if __name__ == "__main__":
    raise SystemExit(run_cli(main))
