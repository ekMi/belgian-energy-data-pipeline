"""Acquire official ODS001 data, then reuse the tracked ingestion/dbt pipeline."""

import argparse
import json
import logging
from dataclasses import asdict, dataclass
from datetime import datetime
from pathlib import Path
from typing import Callable

import psycopg

from acquire_elia import (
    DEFAULT_OUTPUT_DIR,
    AcquisitionError,
    AcquisitionResult,
    AcquisitionValidationError,
    _utc_argument,
    acquire_elia,
    verify_acquisition,
)
from local_pipeline import PipelineResult, run_pipeline
from operations import run_cli

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class ApiPipelineResult:
    acquisition: AcquisitionResult
    pipeline: PipelineResult

    @property
    def status(self) -> str:
        return self.pipeline.status

    def to_dict(self) -> dict:
        return {
            "status": self.status,
            "acquisition": self.acquisition.to_dict(),
            "pipeline": asdict(self.pipeline),
        }


def run_api_pipeline(
    window_start_utc: datetime | None = None,
    window_end_utc: datetime | None = None,
    *,
    output_dir: Path = DEFAULT_OUTPUT_DIR,
    timeout: float = 60.0,
    max_retries: int = 3,
    opener: Callable | None = None,
    sleep: Callable | None = None,
) -> ApiPipelineResult:
    """Only an acquisition issued here may grant the elia-api policy."""
    logger.info("api_acquisition_started")
    acquisition = acquire_elia(
        window_start_utc,
        window_end_utc,
        output_dir=output_dir,
        timeout=timeout,
        max_retries=max_retries,
        opener=opener,
        sleep=sleep,
    )
    verify_acquisition(acquisition)
    logger.info(
        "api_acquired file=%r row_count=%s sha256=%s extracted_at=%s source_reference=%r",
        str(acquisition.file_path),
        acquisition.row_count,
        acquisition.sha256,
        acquisition.extracted_at.isoformat(),
        acquisition.source_reference,
    )
    pipeline = run_pipeline(
        acquisition.file_path,
        input_scope="partial",
        source_policy="elia-api",
        extracted_at=acquisition.extracted_at,
        source_reference=acquisition.source_reference,
        api_acquisition=acquisition,
    )
    return ApiPipelineResult(acquisition, pipeline)


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--start", type=_utc_argument, help="Inclusive UTC timestamp")
    parser.add_argument("--end", type=_utc_argument, help="Exclusive UTC timestamp")
    parser.add_argument("--output-dir", type=Path, default=DEFAULT_OUTPUT_DIR)
    parser.add_argument("--timeout", type=float, default=60.0)
    parser.add_argument("--max-retries", type=int, default=3)
    args = parser.parse_args()
    try:
        result = run_api_pipeline(
            args.start,
            args.end,
            output_dir=args.output_dir,
            timeout=args.timeout,
            max_retries=args.max_retries,
        )
    except (Exception, KeyboardInterrupt) as error:
        # Only the CLI gains a structured failure envelope. Python callers keep
        # the original exceptions, and no database/HTTP error text is disclosed.
        if isinstance(error, KeyboardInterrupt):
            status, code = "INTERRUPTED", 130
        elif isinstance(error, (AcquisitionValidationError, ValueError)):
            status, code = "VALIDATION_FAILED", 1
        elif isinstance(error, AcquisitionError):
            status, code = "ACQUISITION_FAILED", 1
        elif isinstance(error, psycopg.Error):
            status, code = "INGESTION_FAILED", 1
        else:
            status, code = "TECHNICAL_ERROR", 1
        logger.error(
            "api_pipeline_failed status=%s error_type=%s", status, type(error).__name__
        )
        print(json.dumps({"status": status, "error_type": type(error).__name__}))
        raise SystemExit(code) from None
    print(json.dumps(result.to_dict()))
    if result.status in ("DBT_FAILED", "AWAITING_APPROVAL"):
        raise SystemExit(1)


if __name__ == "__main__":
    raise SystemExit(run_cli(main))
