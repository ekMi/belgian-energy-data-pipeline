from unittest.mock import MagicMock

import pytest

import dbt_guard


@pytest.fixture
def configured(monkeypatch, tmp_path):
    dbt_dir = tmp_path / "dbt"
    dbt_dir.mkdir()
    profile = dbt_guard._expected_profile()
    (dbt_dir / "profiles.yml.example").write_text(profile, encoding="utf-8")
    (dbt_dir / "profiles.yml").write_text(profile, encoding="utf-8")
    (dbt_dir / "dbt_project.yml").write_text(
        "profile: belgian_energy_analytics\n", encoding="utf-8"
    )
    monkeypatch.setattr(dbt_guard, "DBT_DIR", dbt_dir)
    params = {
        "host": "localhost",
        "port": "5432",
        "dbname": "belgian_energy",
        "user": "energy_app",
        "password": "secret-do-not-log",
        "connect_timeout": 5,
    }
    monkeypatch.setattr(dbt_guard, "get_connection_params", lambda: params)
    monkeypatch.setattr(
        dbt_guard.socket,
        "getaddrinfo",
        lambda *args, **kwargs: [(None, None, None, None, ("127.0.0.1", 5432))],
    )
    connection = MagicMock()
    connection.__enter__.return_value = connection
    cursor = connection.cursor.return_value.__enter__.return_value
    cursor.fetchone.return_value = ("belgian_energy", "energy_app", "127.0.0.1", 5432)
    monkeypatch.setattr(dbt_guard.psycopg, "connect", lambda **kwargs: connection)
    return params, dbt_dir, connection


def test_preflight_passes_pg_settings_to_dbt(configured):
    params, _, connection = configured
    env = dbt_guard.preflight()
    assert {key: env[key] for key in dbt_guard.PG_VARIABLES} == {
        "PGHOST": "localhost",
        "PGPORT": "5432",
        "PGDATABASE": "belgian_energy",
        "PGUSER": "energy_app",
        "PGPASSWORD": params["password"],
    }
    assert connection.cursor.return_value.__enter__.return_value.execute.call_count == 1


def test_ipv6_server_address_with_mask_is_accepted(configured, monkeypatch):
    _, _, connection = configured
    monkeypatch.setattr(
        dbt_guard.socket,
        "getaddrinfo",
        lambda *args, **kwargs: [(None, None, None, None, ("::1", 5432))],
    )
    connection.cursor.return_value.__enter__.return_value.fetchone.return_value = (
        "belgian_energy",
        "energy_app",
        "::1/128",
        5432,
    )
    dbt_guard.preflight()


def test_missing_variable_blocks_build(monkeypatch, configured):
    def missing():
        raise ValueError("Missing PostgreSQL environment variables: PGHOST")

    monkeypatch.setattr(dbt_guard, "get_connection_params", missing)
    build = MagicMock()
    monkeypatch.setattr(dbt_guard.subprocess, "run", build)
    with pytest.raises(ValueError, match="PGHOST"):
        dbt_guard.guarded_build()
    build.assert_not_called()


def test_wrong_database_blocks_build(configured, monkeypatch):
    params, _, connection = configured
    params["dbname"] = "postgres"
    build = MagicMock()
    monkeypatch.setattr(dbt_guard.subprocess, "run", build)
    with pytest.raises(RuntimeError, match="not authorized"):
        dbt_guard.guarded_build()
    connection.cursor.assert_not_called()
    build.assert_not_called()


def test_fixed_profile_value_blocks_build(configured, monkeypatch):
    _, dbt_dir, connection = configured
    profile = dbt_dir / "profiles.yml"
    profile.write_text(
        dbt_guard._expected_profile().replace(
            "\"{{ env_var('PGDATABASE') }}\"", "postgres"
        ),
        encoding="utf-8",
    )
    build = MagicMock()
    monkeypatch.setattr(dbt_guard.subprocess, "run", build)
    with pytest.raises(RuntimeError, match="profile differs"):
        dbt_guard.guarded_build()
    connection.cursor.assert_not_called()
    build.assert_not_called()


@pytest.mark.parametrize(
    "row",
    [
        ("postgres", "energy_app", "127.0.0.1", 5432),
        ("belgian_energy", "other_user", "127.0.0.1", 5432),
        ("belgian_energy", "energy_app", "192.0.2.1", 5432),
        ("belgian_energy", "energy_app", "127.0.0.1", 5433),
    ],
)
def test_effective_target_mismatch_blocks_build(configured, monkeypatch, row):
    _, _, connection = configured
    connection.cursor.return_value.__enter__.return_value.fetchone.return_value = row
    build = MagicMock()
    monkeypatch.setattr(dbt_guard.subprocess, "run", build)
    with pytest.raises(RuntimeError, match="connected target"):
        dbt_guard.guarded_build()
    build.assert_not_called()


def test_connection_error_does_not_disclose_secret(configured, monkeypatch, capsys):
    def fail(**kwargs):
        raise dbt_guard.psycopg.OperationalError("secret-do-not-log")

    monkeypatch.setattr(dbt_guard.psycopg, "connect", fail)
    build = MagicMock()
    monkeypatch.setattr(dbt_guard.subprocess, "run", build)
    assert dbt_guard.run_cli(dbt_guard.guarded_build) == 1
    assert "secret-do-not-log" not in capsys.readouterr().err
    build.assert_not_called()


def test_check_only_skips_build(configured, monkeypatch):
    build = MagicMock()
    monkeypatch.setattr(dbt_guard.subprocess, "run", build)
    dbt_guard.guarded_build(check_only=True)
    build.assert_not_called()


def test_build_holds_lock_and_invalidates_session(configured, monkeypatch, tmp_path):
    params, dbt_dir, connection = configured
    connection.closed = False
    exe = tmp_path / "python.exe"
    exe.touch()
    monkeypatch.setattr(dbt_guard, "DBT_PYTHON", exe)
    events = []

    from contextlib import contextmanager

    @contextmanager
    def lock(conn):
        assert conn is connection
        events.append("locked")
        try:
            yield conn
        finally:
            events.append("released")

    def run(*args, **kwargs):
        assert events == ["locked"]
        assert args[0] == [
            str(exe),
            "-I",
            str(dbt_guard.DBT_ENTRYPOINT),
            "build",
            "--project-dir",
            str(dbt_dir),
            "--profiles-dir",
            str(dbt_dir),
            "--target",
            dbt_guard.TARGET_NAME,
        ]
        assert kwargs["env"] == dbt_guard.dbt_environment(params)
        assert kwargs["check"] is False
        events.append("subprocess")
        return MagicMock(returncode=0)

    monkeypatch.setattr(dbt_guard, "pipeline_lock", lock)
    monkeypatch.setattr(dbt_guard.subprocess, "run", run)
    with dbt_guard.locked_build() as session:
        session.build()
        assert session.connection is connection
    assert events == ["locked", "subprocess", "released"]
    with pytest.raises(RuntimeError, match="no longer active"):
        session.build()


@pytest.mark.parametrize("failure", [RuntimeError("simulated launch failure"), 1])
def test_build_failure_releases_context(configured, monkeypatch, tmp_path, failure):
    _, _, connection = configured
    connection.closed = False
    exe = tmp_path / "python.exe"
    exe.touch()
    monkeypatch.setattr(dbt_guard, "DBT_PYTHON", exe)

    def run(*args, **kwargs):
        if isinstance(failure, Exception):
            raise failure
        return MagicMock(returncode=failure)

    monkeypatch.setattr(dbt_guard.subprocess, "run", run)
    with pytest.raises(RuntimeError):
        dbt_guard.guarded_build()
    assert connection.execute.call_args.args[0] == "SELECT pg_advisory_unlock(%s, %s)"


@pytest.mark.parametrize("returncode", [1, 2])
def test_nonzero_exit_is_failure(configured, monkeypatch, returncode):
    connection = configured[2]
    connection.closed = False
    monkeypatch.setattr(
        dbt_guard.subprocess, "run", lambda *a, **k: MagicMock(returncode=returncode)
    )
    with pytest.raises(RuntimeError, match="dbt build failed"):
        dbt_guard.guarded_build()


def test_windows_launch_failure_is_sanitized(configured, monkeypatch):
    configured[2].closed = False
    error = OSError("secret-do-not-log")
    error.winerror = 4551

    def fail(*args, **kwargs):
        raise error

    monkeypatch.setattr(dbt_guard.subprocess, "run", fail)
    with pytest.raises(RuntimeError, match="launch failed.*WinError 4551") as caught:
        dbt_guard.guarded_build()
    assert "secret-do-not-log" not in str(caught.value)
