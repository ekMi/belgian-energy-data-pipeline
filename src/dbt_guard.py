"""Read-only dbt target preflight and guarded local build entry point."""

import argparse
import ipaddress
import os
import socket
import subprocess
from contextlib import contextmanager, nullcontext
from dataclasses import dataclass

import psycopg

from config import INGESTION_DATABASE, PROJECT_ROOT, get_connection_params
from operations import run_cli
from pipeline_lock import pipeline_lock

PROFILE_NAME = "belgian_energy_analytics"
TARGET_NAME = "local"
DBT_DIR = PROJECT_ROOT / "dbt"
DBT_PYTHON = PROJECT_ROOT / ".venv-dbt" / "Scripts" / "python.exe"
DBT_ENTRYPOINT = PROJECT_ROOT / "src" / "dbt_entrypoint.py"
PG_VARIABLES = ("PGHOST", "PGPORT", "PGDATABASE", "PGUSER", "PGPASSWORD")


def _expected_profile() -> str:
    """Allow only the project's small, known dbt profile shape."""
    return "\n".join(
        (
            f"{PROFILE_NAME}:",
            f"  target: {TARGET_NAME}",
            "  outputs:",
            f"    {TARGET_NAME}:",
            "      type: postgres",
            "      host: \"{{ env_var('PGHOST') }}\"",
            "      port: \"{{ env_var('PGPORT') | int }}\"",
            "      dbname: \"{{ env_var('PGDATABASE') }}\"",
            "      user: \"{{ env_var('PGUSER') }}\"",
            "      password: \"{{ env_var('PGPASSWORD') }}\"",
            "      schema: analytics",
            "      threads: 1",
            "",
        )
    )


def _verify_profile() -> None:
    """Reject fixed values, extra outputs, and a divergent local profile."""
    expected = _expected_profile()
    for path in (DBT_DIR / "profiles.yml.example", DBT_DIR / "profiles.yml"):
        try:
            contents = path.read_text(encoding="utf-8").replace("\r\n", "\n")
        except (OSError, UnicodeError):
            raise RuntimeError(
                "Refusing dbt build: expected dbt profile is unavailable"
            ) from None
        if contents != expected:
            raise RuntimeError(
                "Refusing dbt build: dbt profile differs from expected target"
            )

    try:
        project = (DBT_DIR / "dbt_project.yml").read_text(encoding="utf-8")
    except (OSError, UnicodeError):
        raise RuntimeError("Refusing dbt build: dbt project is unavailable") from None
    if project.splitlines().count(f"profile: {PROFILE_NAME}") != 1:
        raise RuntimeError("Refusing dbt build: dbt project profile is unexpected")


def dbt_environment(params: dict[str, str | int]) -> dict[str, str]:
    """Pass Python's resolved PG settings explicitly to the separate dbt process."""
    environment = os.environ.copy()
    for variable, key in zip(
        PG_VARIABLES, ("host", "port", "dbname", "user", "password"), strict=True
    ):
        environment[variable] = str(params[key])
    return environment


def preflight(connection=None) -> dict[str, str]:
    """Validate configuration, dbt resolution contract and effective connection."""
    params = get_connection_params()
    if params["dbname"] != INGESTION_DATABASE:
        raise RuntimeError("Refusing dbt build: configured database is not authorized")
    try:
        port = int(params["port"])
    except (TypeError, ValueError):
        raise ValueError("PGPORT must be an integer between 1 and 65535") from None
    if not 1 <= port <= 65535:
        raise ValueError("PGPORT must be an integer between 1 and 65535")
    _verify_profile()

    # Host resolution also covers localhost with either IPv4 or IPv6. Never
    # display addresses or libpq errors, which can include credentials.
    try:
        allowed_addresses = {
            item[4][0]
            for item in socket.getaddrinfo(
                str(params["host"]), port, type=socket.SOCK_STREAM
            )
        }
        with (
            psycopg.connect(**params) if connection is None else nullcontext(connection)
        ) as connection:
            with connection.cursor() as cursor:
                cursor.execute(
                    "SELECT current_database(), current_user, "
                    "inet_server_addr()::text, inet_server_port()"
                )
                database, user, address, actual_port = cursor.fetchone()
        if (
            database != INGESTION_DATABASE
            or user != params["user"]
            or str(ipaddress.ip_interface(address).ip) not in allowed_addresses
            or actual_port != port
        ):
            raise RuntimeError("Refusing dbt build: connected target is not authorized")
    except (OSError, psycopg.Error):
        raise RuntimeError(
            "Refusing dbt build: PostgreSQL connection check failed"
        ) from None
    return dbt_environment(params)


@dataclass
class LockedDbtBuild:
    """Valid only inside locked_build(); do not close its connection early."""

    connection: psycopg.Connection
    environment: dict[str, str]
    active: bool = True

    def build(self) -> None:
        """Wait for dbt while the owning context holds the session lock."""
        if not self.active or self.connection.closed:
            raise RuntimeError("dbt lock context is no longer active")
        if not DBT_PYTHON.is_file() or not DBT_ENTRYPOINT.is_file():
            raise RuntimeError(
                "Refusing dbt build: dbt Python or entry point is unavailable"
            )
        try:
            result = subprocess.run(
                [
                    str(DBT_PYTHON),
                    "-I",  # Keep src/ and PYTHONPATH out of dbt plugin discovery.
                    str(DBT_ENTRYPOINT),
                    "build",
                    "--project-dir",
                    str(DBT_DIR),
                    "--profiles-dir",
                    str(DBT_DIR),
                    "--target",
                    TARGET_NAME,
                ],
                env=self.environment,
                check=False,
            )
        except OSError as error:
            # Do not include OS error text or the environment: either may be sensitive.
            code = getattr(error, "winerror", None)
            detail = f" (WinError {code})" if code is not None else ""
            raise RuntimeError(f"dbt Python subprocess launch failed{detail}") from None
        if result.returncode:
            raise RuntimeError("dbt build failed")


@contextmanager
def locked_build():
    """Expose one protected connection for batch selection, tracking and dbt.

    Commit tracking transactions explicitly before/after build(). Never acquire
    another pipeline lock within this context. Closing the connection releases it.
    """
    environment = preflight()
    params = get_connection_params()
    with psycopg.connect(**params) as connection:
        # Recheck the effective target on the actual lock-owning connection.
        environment = preflight(connection)
        connection.commit()
        with pipeline_lock(connection):
            session = LockedDbtBuild(connection, environment)
            try:
                yield session
            finally:
                session.active = False


def guarded_build(check_only: bool = False) -> None:
    if check_only:
        preflight()
        print("dbt preflight passed")
        return
    with locked_build() as session:
        session.build()


def main() -> None:
    parser = argparse.ArgumentParser(description="Preflight and guarded dbt build")
    parser.add_argument("--check-only", action="store_true")
    args = parser.parse_args()
    guarded_build(check_only=args.check_only)


if __name__ == "__main__":
    raise SystemExit(run_cli(main))
