"""Prefect supervises the API CLI; the business process owns all data operations."""

import json
import re
from datetime import datetime, timedelta

from prefect import flow, task
from src.prefect.pipeline import (
    PROJECT_ROOT,
    PipelineExecutionError,
    read_cli_payload,
    run_pipeline_cli,
)

EXIT_CODES = {
    "SUCCESS": 0,
    "NO_CHANGE": 0,
    "DBT_FAILED": 1,
    "ACQUISITION_FAILED": 1,
    "VALIDATION_FAILED": 1,
    "INGESTION_FAILED": 1,
    "TECHNICAL_ERROR": 1,
    "INTERRUPTED": 130,
}
SUCCESS_STATUSES = {"SUCCESS", "NO_CHANGE"}


def _validate_result(payload: dict) -> None:
    """Check the CLI envelope, without reimplementing CSV or database validation."""
    if payload["status"] not in SUCCESS_STATUSES | {"DBT_FAILED"}:
        if not isinstance(payload.get("error_type"), str) or not payload["error_type"]:
            raise ValueError("Missing error category")
        return
    acquisition, pipeline = payload["acquisition"], payload["pipeline"]
    if not isinstance(acquisition, dict) or not isinstance(pipeline, dict):
        raise ValueError("Missing acquisition/pipeline objects")
    for key in ("file_path", "source_reference", "sha256"):
        if not isinstance(acquisition[key], str) or not acquisition[key]:
            raise ValueError(f"Invalid acquisition {key}")
    if re.fullmatch(r"[0-9a-f]{64}", acquisition["sha256"]) is None:
        raise ValueError("Invalid acquisition SHA-256")
    dates = []
    for key in ("window_start_utc", "window_end_utc", "extracted_at"):
        value = datetime.fromisoformat(acquisition[key])
        if value.utcoffset() != timedelta(0):
            raise ValueError(f"Invalid UTC metadata: {key}")
        dates.append(value)
    if dates[0] >= dates[1]:
        raise ValueError("Invalid acquisition window")
    count = acquisition["row_count"]
    if type(count) is not int or count <= 0:
        raise ValueError("Invalid acquisition row count")
    if pipeline["status"] != payload["status"]:
        raise ValueError("Inconsistent nested pipeline status")
    for key in ("ingestion_batch_id", "target_batch_id", "build_id"):
        value = pipeline[key]
        if value is not None and (type(value) is not int or value <= 0):
            raise ValueError(f"Invalid {key}")
    for key in ("rows_new", "rows_changed"):
        value = pipeline[key]
        if value is not None and (type(value) is not int or not 0 <= value <= count):
            raise ValueError(f"Invalid {key}")
    if pipeline["error_type"] is not None and not isinstance(
        pipeline["error_type"], str
    ):
        raise ValueError("Invalid pipeline error category")
    new, changed = pipeline["rows_new"], pipeline["rows_changed"]
    if new is not None and changed is not None and new + changed > count:
        raise ValueError("Inconsistent ingestion counts")
    if payload["status"] == "DBT_FAILED" and not pipeline["error_type"]:
        raise ValueError("Missing dbt error category")


@task(name="run-api-pipeline", log_prints=True, retries=0, persist_result=False)
def run_api_pipeline_cli(start: str | None = None, end: str | None = None) -> dict:
    if (start is None) != (end is None):
        raise ValueError("Provide both start and end, or neither")
    arguments = [] if start is None else ["--start", start, "--end", end]
    print("api_acquisition_started: launching API acquisition and business pipeline")
    result = run_pipeline_cli("api_pipeline.py", arguments, project_root=PROJECT_ROOT)
    if result["exit_code"] in (130, -2, -1073741510, 3221225786):
        result["status"] = "INTERRUPTED"
    try:
        payload = read_cli_payload(result, EXIT_CODES)
    except PipelineExecutionError:
        print(f"api_pipeline final_status={result['status']}")
        raise
    try:
        _validate_result(payload)
    except (KeyError, ValueError, TypeError) as error:
        # A malformed success envelope must never complete a Prefect flow.
        result["status"] = "TECHNICAL_ERROR"
        print("api_pipeline final_status=TECHNICAL_ERROR")
        raise PipelineExecutionError(
            "Incomplete or invalid API pipeline JSON", result
        ) from error
    # Never allow JSON fields to replace subprocess stdout/stderr/exit diagnostics.
    result["status"] = payload["status"]
    if payload["status"] in SUCCESS_STATUSES | {"DBT_FAILED"}:
        result["acquisition"] = payload["acquisition"]
        result["pipeline"] = payload["pipeline"]
        acquisition, pipeline = result["acquisition"], result["pipeline"]
        print(
            f"api_acquisition_finished file={acquisition['file_path']!r} row_count={acquisition['row_count']}"
        )
        new, changed = pipeline["rows_new"], pipeline["rows_changed"]
        unchanged = (
            acquisition["row_count"] - new - changed
            if new is not None and changed is not None
            else None
        )
        result["rows_unchanged"] = unchanged
        print(
            f"ingestion batch_id={pipeline['ingestion_batch_id']} NEW={new} UNCHANGED={unchanged} CHANGED={changed}"
        )
        dbt_status = (
            "DBT_FAILED"
            if payload["status"] == "DBT_FAILED"
            else "SUCCESS"
            if pipeline["build_id"] is not None
            else "UP_TO_DATE"
        )
        print(
            f"dbt status={dbt_status} build_id={pipeline['build_id']} target_batch_id={pipeline['target_batch_id']}"
        )
    else:
        result["error_type"] = payload["error_type"]
    print("API result: " + json.dumps(payload, ensure_ascii=False))
    print(f"api_pipeline final_status={result['status']}")
    if result["status"] not in SUCCESS_STATUSES:
        raise PipelineExecutionError(
            "dbt failed; committed ingestion is retained for manual recovery"
            if result["status"] == "DBT_FAILED"
            else f"API pipeline failed: {result['status']}",
            result,
        )
    return result


@flow(name="belgian-energy-api", log_prints=True, retries=0, persist_result=False)
def energy_api(start: str | None = None, end: str | None = None) -> dict:
    return run_api_pipeline_cli(start=start, end=end)
