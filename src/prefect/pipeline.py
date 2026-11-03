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


@task(name="run-local-pipeline", log_prints=True, retries=0, persist_result=False)
def run_local_pipeline(
    csv_path: str, source_policy: str = "restricted", input_scope: str = "continuous"
) -> dict:
    python = PROJECT_ROOT / ".venv" / "Scripts" / "python.exe"
    script = PROJECT_ROOT / "src" / "local_pipeline.py"

    path = Path(csv_path)
    if not path.is_absolute():
        path = PROJECT_ROOT / path
    path = path.resolve(strict=True)
    if not path.is_file():
        raise ValueError("Le chemin CSV doit désigner un fichier")

    # Fix the child encoding too: Windows' default code page is not always UTF-8.
    env = {**os.environ, "PYTHONIOENCODING": "utf-8"}
    process = subprocess.run(
        [
            str(python),
            "-B",
            str(script),
            str(path),
            "--source-policy",
            source_policy,
            "--input-scope",
            input_scope,
        ],
        cwd=PROJECT_ROOT,
        env=env,
        capture_output=True,
        text=True,
        encoding="utf-8",
        errors="replace",
        check=False,
    )
    result = {
        "status": "TECHNICAL_ERROR",
        "csv_path": str(path),
        "exit_code": process.returncode,
        "stdout": process.stdout,
        "stderr": process.stderr,
    }
    if process.stdout:
        print(process.stdout)
    if process.stderr:
        print(process.stderr)
    print(f"local_pipeline exit_code={process.returncode}")

    try:
        payload = json.loads(process.stdout)
        if not isinstance(payload, dict) or payload.get("status") not in EXIT_CODES:
            raise ValueError("Statut JSON absent ou inconnu")
        if process.returncode != EXIT_CODES[payload["status"]]:
            raise ValueError("Code de sortie incompatible avec le statut JSON")
    except (ValueError, TypeError) as error:
        raise PipelineExecutionError(
            f"Résultat CLI invalide (exit code {process.returncode})", result
        ) from error

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
