"""Prefect orchestration of the existing local pipeline."""

import json
import os
import subprocess
from pathlib import Path

from prefect import flow, task

PROJECT_ROOT = Path(__file__).resolve().parents[2]
EXIT_CODES = {"SUCCESS": 0, "NO_CHANGE": 0, "AWAITING_APPROVAL": 1, "DBT_FAILED": 1}


class PipelineExecutionError(RuntimeError):
    """A failed execution, retaining the CLI result for inbox diagnostics."""

    def __init__(self, message: str, result: dict | None = None):
        super().__init__(message)
        self.result = result or {"status": "TECHNICAL_ERROR"}


def run_pipeline_cli(
    script_name: str, arguments: list[str], *, project_root: Path
) -> dict:
    """Shared subprocess boundary; keep business and Prefect environments separate."""
    command = [
        str(project_root / ".venv" / "Scripts" / "python.exe"),
        "-B",
        str(project_root / "src" / script_name),
        *arguments,
    ]
    result = {
        "status": "TECHNICAL_ERROR",
        "exit_code": None,
        "stdout": "",
        "stderr": "",
    }
    try:
        process = subprocess.run(
            command,
            cwd=project_root,
            env={**os.environ, "PYTHONIOENCODING": "utf-8"},
            capture_output=True,
            text=True,
            encoding="utf-8",
            errors="replace",
            check=False,
        )
    except (OSError, subprocess.SubprocessError) as error:
        result["error_type"] = type(error).__name__
        print(
            f"{Path(script_name).stem} launch_failed error_type={type(error).__name__}"
        )
        raise PipelineExecutionError(
            "Cannot launch or wait for the business pipeline", result
        ) from None
    result.update(
        exit_code=process.returncode, stdout=process.stdout, stderr=process.stderr
    )
    if process.stdout:
        print(process.stdout)
    if process.stderr:
        print(process.stderr)
    print(f"{Path(script_name).stem} exit_code={process.returncode}")
    return result


def read_cli_payload(result: dict, exit_codes: dict[str, int]) -> dict:
    """Require a JSON object with a known status and a consistent process exit."""
    try:
        payload = json.loads(result["stdout"])
        if not isinstance(payload, dict) or payload.get("status") not in exit_codes:
            raise ValueError("Missing or unknown JSON status")
        if result["exit_code"] != exit_codes[payload["status"]]:
            raise ValueError("Exit code does not match JSON status")
    except (ValueError, TypeError) as error:
        raise PipelineExecutionError(
            f"Invalid CLI result (exit code {result['exit_code']})", result
        ) from error
    return payload


@task(name="run-local-pipeline", log_prints=True, retries=0, persist_result=False)
def run_local_pipeline(
    csv_path: str, source_policy: str = "restricted", input_scope: str = "continuous"
) -> dict:
    path = Path(csv_path)
    if not path.is_absolute():
        path = PROJECT_ROOT / path
    path = path.resolve(strict=True)
    if not path.is_file():
        raise ValueError("Le chemin CSV doit désigner un fichier")

    result = run_pipeline_cli(
        "local_pipeline.py",
        [str(path), "--source-policy", source_policy, "--input-scope", input_scope],
        project_root=PROJECT_ROOT,
    )
    result["csv_path"] = str(path)
    payload = read_cli_payload(result, EXIT_CODES)

    # Keep transport diagnostics authoritative; only copy business result fields.
    for key in (
        "status",
        "ingestion_batch_id",
        "target_batch_id",
        "build_id",
        "rows_new",
        "rows_changed",
        "error_type",
    ):
        result[key] = payload.get(key)
    print("Résultat métier : " + json.dumps(payload, ensure_ascii=False))
    if result["status"] == "DBT_FAILED":
        raise PipelineExecutionError(
            "dbt a échoué ; l'ingestion déjà validée reste conservée", result
        )
    if result["status"] == "AWAITING_APPROVAL":
        print(
            "Corrections en attente de revue manuelle ; aucune autorisation transmise."
        )
    return result


@flow(name="belgian-energy-pipeline", log_prints=True, retries=0, persist_result=False)
def energy_pipeline(
    csv_path: str = "data/raw/ods001.csv",
    source_policy: str = "restricted",
    input_scope: str = "continuous",
) -> dict:
    return run_local_pipeline(
        csv_path, source_policy=source_policy, input_scope=input_scope
    )


if __name__ == "__main__":
    energy_pipeline()
