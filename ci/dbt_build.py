"""Synthetic dbt acceptance run, exclusively on the disposable PostgreSQL database.

Executed by .venv (psycopg); dbt itself runs in .venv-dbt. No business imports,
dotenv, ingestion, deployment or production dbt guard bypass is involved.
"""

import json
import os
import subprocess
import sys
import tempfile
import xml.etree.ElementTree as ET
from datetime import UTC, datetime, timedelta
from pathlib import Path

import psycopg

from ci.run_tests import clean_environment

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "tests"))
from postgres_support import (  # noqa: E402
    TEST_DATABASE,
    TEST_LOCK,
    TEST_USER,
    verify_test_database,
)

MODELS = {"dim_date", "dim_time", "fact_load_15m"}
ARTIFACTS = ROOT / "reports/dbt-build"
LOGS = ROOT / "reports/dbt-logs"


def connection_params(env):
    variables = {
        key: "TEST_PG" + suffix
        for key, suffix in (
            ("host", "HOST"),
            ("port", "PORT"),
            ("dbname", "DATABASE"),
            ("user", "USER"),
            ("password", "PASSWORD"),
        )
    }
    if not all(env.get(name) for name in variables.values()):
        raise RuntimeError("Explicit TEST_PG credentials are required")
    params = {key: env[name] for key, name in variables.items()}
    if params["dbname"] != TEST_DATABASE or params["user"] != TEST_USER:
        raise RuntimeError("Unauthorized test database or user")
    params["connect_timeout"] = 5
    return params


def synthetic_rows():
    instants = [datetime(2015, 6, 24, 3, 30, tzinfo=UTC)]
    instants += [
        datetime(year, month, day, tzinfo=UTC)
        for year, month, day in (
            (2016, 1, 1),
            (2018, 12, 31),
            (2021, 1, 1),
            (2025, 11, 2),
        )
    ]
    for start, count in (
        (datetime(2025, 3, 29, 23, tzinfo=UTC), 92),
        (datetime(2025, 10, 25, 22, tzinfo=UTC), 100),
    ):
        instants += [start + timedelta(minutes=15 * i) for i in range(count)]
    return [
        (
            instant,
            "PT15M",
            None if i == 4 else (-1 if i == 2 else 8500),
            846 if i == 0 else 8500,
            8000,
            9000,
            8500,
            8000,
            9000,
            None if i == 4 else 8500,
        )
        for i, instant in enumerate(instants)
    ]


def reset_schemas(connection):
    verify_test_database(connection)
    connection.execute("DROP SCHEMA IF EXISTS staging CASCADE")
    connection.execute("DROP SCHEMA IF EXISTS analytics CASCADE")
    connection.commit()


def seed(connection):
    verify_test_database(connection)
    connection.execute((ROOT / "sql/01_create_schema.sql").read_text(encoding="utf-8"))
    with connection.cursor() as cursor:
        cursor.executemany(
            "INSERT INTO staging.elia_load (timestamp_utc, resolution_code, "
            "total_load_mw, most_recent_forecast_mw, most_recent_p10_mw, "
            "most_recent_p90_mw, day_ahead_forecast_mw, day_ahead_p10_mw, "
            "day_ahead_p90_mw, week_ahead_forecast_mw) "
            "VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)",
            synthetic_rows(),
        )
    connection.commit()


def validate_results(manifest, results):
    nodes = {
        uid: node
        for uid, node in manifest["nodes"].items()
        if node["resource_type"] in {"model", "test"}
    }
    models = {
        node["name"] for node in nodes.values() if node["resource_type"] == "model"
    }
    if models != MODELS or not any(
        n["resource_type"] == "test" for n in nodes.values()
    ):
        raise RuntimeError("dbt must contain all three models and enabled tests")
    actual = {result["unique_id"]: result for result in results["results"]}
    if len(actual) != len(results["results"]) or actual.keys() != nodes.keys():
        raise RuntimeError("dbt did not execute every enabled model and test")
    for uid, node in nodes.items():
        expected = "success" if node["resource_type"] == "model" else "pass"
        if actual[uid]["status"] != expected:
            raise RuntimeError("dbt model or test failed, warned or was skipped")
    return len(nodes) - len(MODELS)


def verify_models(connection):
    verify_test_database(connection)
    for model in sorted(MODELS):
        # Names are fixed above, never supplied by environment or user input.
        count = connection.execute(
            f"SELECT count(*) FROM analytics.{model}"
        ).fetchone()[0]
        if not count or (model == "fact_load_15m" and count != len(synthetic_rows())):
            raise RuntimeError(
                "Analytical model is empty or has an unexpected row count"
            )
    connection.commit()


def write_report(error):
    suite = ET.Element("testsuite", name="dbt")
    case = ET.SubElement(
        suite, "testcase", classname="ci.dbt", name="guard_build_cleanup"
    )
    if error:
        ET.SubElement(case, "failure", message=error)
    path = ARTIFACTS / "run_results.json"
    results = []
    if path.exists():
        try:
            results = json.loads(path.read_text(encoding="utf-8"))["results"]
        except (OSError, ValueError, KeyError, TypeError):
            case = ET.SubElement(
                suite, "testcase", classname="ci.dbt", name="read_results"
            )
            ET.SubElement(case, "failure", message="Cannot read dbt run_results.json")
    for result in results:
        case = ET.SubElement(
            suite,
            "testcase",
            classname="dbt",
            name=result["unique_id"],
            time=str(result.get("execution_time", 0)),
        )
        if result["status"] not in {"pass", "success"}:
            ET.SubElement(case, "failure", message="dbt status: " + result["status"])
    suite.set("tests", str(len(suite)))
    suite.set("failures", str(len(suite.findall(".//failure"))))
    suite.set("errors", "0")
    suite.set("skipped", "0")
    ET.ElementTree(suite).write(
        ROOT / "reports/dbt.xml", encoding="utf-8", xml_declaration=True
    )


def run():
    error = None
    ARTIFACTS.mkdir(parents=True, exist_ok=True)
    LOGS.mkdir(parents=True, exist_ok=True)
    # No stale success can validate a failed command.
    for name in ("manifest.json", "run_results.json"):
        (ARTIFACTS / name).unlink(missing_ok=True)
    try:
        env = clean_environment()
        params = connection_params(env)
        # libpq reads the parent process environment in addition to kwargs.
        # Match the dbt child's explicit target; ignore services and PGOPTIONS.
        for name in list(os.environ):
            if name.upper().startswith("PG"):
                del os.environ[name]
        with psycopg.connect(**params) as connection:
            verify_test_database(connection)
            locked = connection.execute(
                "SELECT pg_try_advisory_lock(%s, %s)", TEST_LOCK
            ).fetchone()[0]
            connection.commit()
            if not locked:
                raise RuntimeError("Another test run holds the database lock")
            print(
                "Verified belgian_energy_test / energy_test_app / PostgreSQL 18",
                flush=True,
            )
            try:
                reset_schemas(connection)
                seed(connection)
                with tempfile.TemporaryDirectory(prefix="energy-dbt-ci-") as temporary:
                    profile = (ROOT / "dbt/profiles.yml.example").read_text(
                        encoding="utf-8"
                    )
                    # dbt scrubs DBT_ENV_SECRET_* values from its own logs.
                    profile = profile.replace(
                        "env_var('PGPASSWORD')", "env_var('DBT_ENV_SECRET_CI_PASSWORD')"
                    )
                    Path(temporary, "profiles.yml").write_text(
                        profile, encoding="utf-8"
                    )
                    env = {
                        k: v
                        for k, v in env.items()
                        if not k.upper().startswith(("TEST_PG", "DBT_"))
                    }
                    env.update(
                        PGHOST=params["host"],
                        PGPORT=params["port"],
                        PGDATABASE=TEST_DATABASE,
                        PGUSER=TEST_USER,
                        DBT_ENV_SECRET_CI_PASSWORD=params["password"],
                        DBT_SEND_ANONYMOUS_USAGE_STATS="false",
                    )
                    result = subprocess.run(
                        [
                            # Launch Python directly so timeout kills the dbt
                            # process, not just a Windows console-script wrapper.
                            str(ROOT / ".venv-dbt/Scripts/python.exe"),
                            "-B",
                            "-c",
                            "from dbt.cli.main import cli; cli()",
                            "build",
                            "--project-dir",
                            str(ROOT / "dbt"),
                            "--profiles-dir",
                            temporary,
                            "--target",
                            "local",
                            "--target-path",
                            str(ARTIFACTS),
                            "--log-path",
                            str(LOGS),
                            "--no-partial-parse",
                            "--vars",
                            '{"quality_as_of_utc": "2025-11-01T00:00:00+00:00"}',
                        ],
                        env=env,
                        cwd=temporary,
                        check=False,
                        timeout=600,
                    )
                    if result.returncode:
                        raise RuntimeError(
                            "dbt build failed; inspect archived dbt logs"
                        )
                manifest = json.loads((ARTIFACTS / "manifest.json").read_text("utf-8"))
                results = json.loads(
                    (ARTIFACTS / "run_results.json").read_text("utf-8")
                )
                count = validate_results(manifest, results)
                verify_models(connection)
                print(f"Verified 3 analytical tables and {count} dbt tests", flush=True)
            finally:
                connection.rollback()
                reset_schemas(connection)
                print(
                    "Test schemas removed; database lock released on close", flush=True
                )
    except Exception as exception:
        # Never echo libpq parameters, subprocess environments or secret-bearing errors.
        error = (
            str(exception)
            if isinstance(exception, RuntimeError)
            else (
                f"dbt CI failed ({type(exception).__name__}); inspect stage logs/artifacts"
            )
        )
        print(error, file=sys.stderr)
    finally:
        write_report(error)
    return int(error is not None)


if __name__ == "__main__":
    sys.exit(run())
