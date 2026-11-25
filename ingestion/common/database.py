import os
from pathlib import Path

import psycopg
from dotenv import load_dotenv

PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent

APP_ENV = os.getenv("APP_ENV", "dev")

ENV_FILES = {
    "dev": ".env-dev",
    "prod": ".env",
}

if APP_ENV not in ENV_FILES:
    raise ValueError(f"Unknown environment: {APP_ENV}")

env_file = PROJECT_ROOT / ENV_FILES[APP_ENV]

if not env_file.exists():
    raise FileNotFoundError(env_file)

load_dotenv(env_file, override=True)


def get_connection():
    return psycopg.connect(
        host=os.environ["PGHOST"],
        port=os.environ["PGPORT"],
        dbname=os.environ["PGDATABASE"],
        user=os.environ["PGUSER"],
        password=os.environ["PGPASSWORD"],
    )


if __name__ == "__main__":
    with get_connection() as conn:
        result = conn.execute("SELECT current_database();")
        print(result.fetchone())
