"""Shared paths and PostgreSQL environment configuration."""

import os
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
CSV_PATH = PROJECT_ROOT / "data" / "raw" / "ods001.csv"


def get_connection_params() -> dict[str, str]:
    """Read configuration at call time without exposing credential values."""
    variables = {
        "host": "PGHOST",
        "port": "PGPORT",
        "dbname": "PGDATABASE",
        "user": "PGUSER",
        "password": "PGPASSWORD",
    }
    missing = [name for name in variables.values() if not os.environ.get(name)]
    if missing:
        raise ValueError(
            "Missing PostgreSQL environment variables: " + ", ".join(missing)
        )
    return {key: os.environ[name] for key, name in variables.items()}
