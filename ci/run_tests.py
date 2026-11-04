"""Strict pytest entry points with isolated settings and separate JUnit reports."""

import os
import subprocess
import sys
import tempfile
import xml.etree.ElementTree as ET
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
PREFECT_TESTS = (
    "tests/unit/test_prefect_orchestration.py",
    "tests/prefect/test_flow_engine.py",
)


def clean_environment():
    # Keep TEST_PG* credentials only for the explicitly selected integration run.
    env = {
        key: value
        for key, value in os.environ.items()
        if not key.upper().startswith(("PG", "PREFECT_", "ENERGY_", "PYTEST_"))
    }
    env.update(PYTHON_DOTENV_DISABLED="1", PYTHONDONTWRITEBYTECODE="1")
    return env


def verify_report(path, expected_modules=()):
    cases = ET.parse(path).getroot().findall(".//testcase")
    if not cases:
        raise RuntimeError("No tests executed")
    if any(
        case.find(tag) is not None
        for case in cases
        for tag in ("skipped", "failure", "error")
    ):
        raise RuntimeError("Tests failed, errored or were skipped")
    for module in expected_modules:
        if not any(module in case.get("classname", "").split(".") for case in cases):
            raise RuntimeError(f"Required test module absent: {module}")
    print(f"Validated {len(cases)} tests, zero skips", flush=True)


def run(suite):
    if suite not in {"unit", "integration", "prefect"}:
        raise ValueError("Expected unit, integration or prefect")
    report = ROOT / "reports" / f"{suite}.xml"
    report.parent.mkdir(exist_ok=True)
    report.unlink(missing_ok=True)
    env = clean_environment()
    if suite != "integration":
        env = {k: v for k, v in env.items() if not k.upper().startswith("TEST_PG")}
    else:
        if (
            env.get("TEST_PGDATABASE") != "belgian_energy_test"
            or env.get("TEST_PGUSER") != "energy_test_app"
            or not all(
                env.get("TEST_PG" + name)
                for name in ("HOST", "PORT", "DATABASE", "USER", "PASSWORD")
            )
        ):
            raise RuntimeError("Explicit test database credentials are required")
    command = [
        sys.executable,
        "-B",
        "-m",
        "pytest",
        "-p",
        "no:cacheprovider",
        "-ra",
        f"--junitxml={report}",
    ]
    expected = ()
    # Prefect's deployment parser computes relative file entrypoints. Keep its
    # temporary working directory on the workspace drive (Windows C: vs D:).
    with tempfile.TemporaryDirectory(
        prefix="energy-ci-", dir=report.parent
    ) as temporary:
        cwd = ROOT
        if suite == "prefect":
            cwd = Path(temporary)
            # Run outside the repository so Prefect cannot read its .env or TOML.
            # Clear inherited profiles/API settings before importing Prefect.
            env.update(
                PREFECT_HOME=str(cwd / "prefect"),
                PREFECT_PROFILES_PATH=str(cwd / "profiles.toml"),
                PREFECT_LOCAL_STORAGE_PATH=str(cwd / "storage"),
                PREFECT_API_URL="",
                PREFECT_API_KEY="",
                PREFECT_SERVER_EPHEMERAL_ENABLED="false",
                PREFECT_SERVER_ANALYTICS_ENABLED="false",
                PYTHONPATH=str(ROOT),
                TEMP=temporary,
                TMP=temporary,
            )
            command += ["-c", str(ROOT / "pyproject.toml")]
            for name in PREFECT_TESTS:
                path = ROOT / name
                if not path.is_file():
                    raise RuntimeError(f"Required test file absent: {name}")
                command.append(str(path))
            expected = ("test_prefect_orchestration", "test_flow_engine")
        elif suite == "unit":
            command += ["tests/unit", f"--ignore={PREFECT_TESTS[0]}"]
        else:
            command += ["tests/integration", "-m", "integration"]
        result = subprocess.run(command, cwd=cwd, env=env, check=False)
        verify_report(report, expected)
        return result.returncode


if __name__ == "__main__":
    try:
        sys.exit(run(sys.argv[1]))
    except (RuntimeError, OSError, ET.ParseError) as error:
        print(f"CI tests: {error}", file=sys.stderr)
        sys.exit(1)
