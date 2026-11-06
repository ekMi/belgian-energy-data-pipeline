"""CI rejects skipped tests, unsafe targets and incomplete dbt executions."""

from unittest.mock import MagicMock
from zoneinfo import ZoneInfo

import pytest
from ci import check_requirements, dbt_build, run_tests


@pytest.mark.parametrize("encoding", ["utf-8", "utf-8-sig", "utf-16"])
def test_requirements_windows_encodings(tmp_path, monkeypatch, encoding):
    path = tmp_path / "requirements.txt"
    path.write_text("# frozen\nexample==1.0\n", encoding=encoding)
    monkeypatch.setattr(
        check_requirements.importlib.metadata, "version", lambda _: "1.0"
    )
    assert check_requirements.mismatches(path) == []


@pytest.mark.parametrize(
    "body",
    [
        "",
        '<testcase classname="test_flow_engine"><skipped/></testcase>',
        '<testcase classname="test_flow_engine"><failure/></testcase>',
        '<testcase classname="test_flow_engine"><error/></testcase>',
        '<testcase classname="test_unrelated"/>',
    ],
)
def test_rejects_empty_skipped_failed_or_missing_modules(tmp_path, body):
    path = tmp_path / "report.xml"
    path.write_text(f"<testsuite>{body}</testsuite>", encoding="utf-8")
    with pytest.raises(RuntimeError):
        run_tests.verify_report(path, ("test_flow_engine",))


def test_isolation_removes_inherited_settings(monkeypatch):
    for key in (
        "PREFECT_API_URL",
        "PREFECT_PROFILE",
        "PGSERVICE",
        "PGPASSWORD",
        "ENERGY_PROJECT_ROOT",
        "PYTEST_ADDOPTS",
    ):
        monkeypatch.setenv(key, "must-not-be-inherited")
    env = run_tests.clean_environment()
    assert not any(
        key.startswith(("PG", "PREFECT_", "ENERGY_", "PYTEST_")) for key in env
    )
    assert env["PYTHON_DOTENV_DISABLED"] == "1"


@pytest.mark.parametrize(
    "database,user",
    [
        ("belgian_energy", "energy_test_app"),
        ("belgian_energy_test", "energy_app"),
        ("belgian_energy_test", "postgres"),
        ("other_test", "energy_test_app"),
    ],
)
def test_dbt_rejects_target_before_connect(database, user):
    env = dict(
        TEST_PGHOST="localhost",
        TEST_PGPORT="5432",
        TEST_PGDATABASE=database,
        TEST_PGUSER=user,
        TEST_PGPASSWORD="secret",
    )
    with pytest.raises(RuntimeError, match="Unauthorized"):
        dbt_build.connection_params(env)


def test_cleanup_checks_identity_before_drop():
    connection = MagicMock()
    cursor = connection.cursor.return_value.__enter__.return_value
    cursor.fetchone.return_value = (
        "belgian_energy",
        "180000",
        "energy_app",
        "energy_app",
    )
    with pytest.raises(RuntimeError):
        dbt_build.reset_schemas(connection)
    connection.execute.assert_not_called()


def dbt_artifacts():
    nodes = {
        f"model.energy.{name}": {"resource_type": "model", "name": name}
        for name in dbt_build.MODELS
    }
    nodes.update(
        {
            f"seed.energy.{name}": {"resource_type": "seed", "name": name}
            for name in dbt_build.SEEDS
        }
    )
    nodes["test.energy.check"] = {"resource_type": "test", "name": "check"}
    results = [
        {
            "unique_id": uid,
            "status": "pass" if node["resource_type"] == "test" else "success",
        }
        for uid, node in nodes.items()
    ]
    return {"nodes": nodes}, {"results": results}


@pytest.mark.parametrize("status", ["skipped", "warn", "fail", "error"])
def test_dbt_rejects_nonpassing_tests(status):
    manifest, results = dbt_artifacts()
    results["results"][-1]["status"] = status
    with pytest.raises(RuntimeError):
        dbt_build.validate_results(manifest, results)


def test_dbt_requires_complete_execution():
    manifest, results = dbt_artifacts()
    assert dbt_build.validate_results(manifest, results) == 1
    results["results"].pop()
    with pytest.raises(RuntimeError, match="every enabled"):
        dbt_build.validate_results(manifest, results)


def test_synthetic_dst_days_are_complete():
    rows = dbt_build.synthetic_rows()
    local_days = [
        row[0].astimezone(ZoneInfo("Europe/Brussels")).strftime("%Y%m%d")
        for row in rows
    ]
    assert local_days.count("20250330") == 92
    assert local_days.count("20251026") == 100
    assert len({row[0] for row in rows}) == len(rows) == 197


@pytest.mark.parametrize("stage", ["identity", "lock", "seed"])
def test_dbt_cleanup_only_after_identity_and_lock(tmp_path, monkeypatch, stage):
    monkeypatch.setattr(dbt_build, "ROOT", tmp_path)
    monkeypatch.setattr(dbt_build, "ARTIFACTS", tmp_path / "reports/dbt-build")
    monkeypatch.setattr(dbt_build, "LOGS", tmp_path / "reports/dbt-logs")
    for key, value in dict(
        TEST_PGHOST="localhost",
        TEST_PGPORT="5432",
        TEST_PGDATABASE="belgian_energy_test",
        TEST_PGUSER="energy_test_app",
        TEST_PGPASSWORD="never-print-this-secret",
    ).items():
        monkeypatch.setenv(key, value)
    connect = MagicMock()
    connection = connect.return_value.__enter__.return_value
    connection.execute.return_value.fetchone.return_value = (stage != "lock",)
    verify = MagicMock(
        side_effect=RuntimeError("Wrong database") if stage == "identity" else None
    )
    reset = MagicMock()
    seed = MagicMock(side_effect=RuntimeError("Synthetic preparation failed"))
    monkeypatch.setattr(dbt_build.psycopg, "connect", connect)
    monkeypatch.setattr(dbt_build, "verify_test_database", verify)
    monkeypatch.setattr(dbt_build, "reset_schemas", reset)
    monkeypatch.setattr(dbt_build, "seed", seed)
    assert dbt_build.run() == 1
    assert reset.call_count == (2 if stage == "seed" else 0)
    report = (tmp_path / "reports/dbt.xml").read_text(encoding="utf-8")
    assert "failure" in report
    assert "never-print-this-secret" not in report


def test_dbt_report_survives_truncated_artifact(tmp_path, monkeypatch):
    monkeypatch.setattr(dbt_build, "ROOT", tmp_path)
    monkeypatch.setattr(dbt_build, "ARTIFACTS", tmp_path / "reports/dbt-build")
    dbt_build.ARTIFACTS.mkdir(parents=True)
    (dbt_build.ARTIFACTS / "run_results.json").write_text(
        '{"results":', encoding="utf-8"
    )
    dbt_build.write_report("Interrupted build")
    report = (tmp_path / "reports/dbt.xml").read_text(encoding="utf-8")
    assert "Interrupted build" in report
    assert "Cannot read dbt run_results.json" in report


@pytest.mark.parametrize("status", ["skipped", "warn", "fail", "error"])
def test_dbt_requires_successful_seed(status):
    manifest, results = dbt_artifacts()
    seed = next(r for r in results["results"] if r["unique_id"].startswith("seed."))
    seed["status"] = status
    with pytest.raises(RuntimeError):
        dbt_build.validate_results(manifest, results)


def test_dbt_requires_holiday_seed():
    manifest, results = dbt_artifacts()
    manifest["nodes"] = {
        uid: node
        for uid, node in manifest["nodes"].items()
        if node["resource_type"] != "seed"
    }
    with pytest.raises(RuntimeError, match="holiday seed"):
        dbt_build.validate_results(manifest, results)
