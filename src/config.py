"""Shared paths and PostgreSQL environment configuration."""

import os
from pathlib import Path

from dotenv import load_dotenv

PROJECT_ROOT = Path(__file__).resolve().parents[1]
CSV_PATH = PROJECT_ROOT / "data" / "raw" / "ods001.csv"
INGESTION_DATABASE = "belgian_energy"


def get_connection_params() -> dict[str, str | int]:
    """Read configuration at call time without exposing credential values."""
    load_dotenv(PROJECT_ROOT / ".env", override=False)
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
    params = {key: os.environ[name] for key, name in variables.items()}
    params["connect_timeout"] = positive_seconds("PIPELINE_CONNECT_TIMEOUT", 5)
    return params


def verify_database_target(connection, configured_database, expected_database):
    """Reject an unexpected configured or actual database before ingestion writes."""
    if not configured_database or configured_database != expected_database:
        raise RuntimeError(
            "Refusing database writes: configured database is not authorized"
        )
    actual_database = connection.execute("SELECT current_database()").fetchone()[0]
    if actual_database != expected_database:
        raise RuntimeError(
            "Refusing database writes: connected database is not authorized"
        )


def positive_seconds(name: str, default: int) -> int:
    """Bound operational waits; never echo untrusted configuration values."""
    try:
        value = int(os.environ.get(name, str(default)))
    except ValueError:
        raise ValueError(f"{name} must be an integer between 1 and 3600") from None
    if not 1 <= value <= 3600:
        raise ValueError(f"{name} must be an integer between 1 and 3600")
    return value
