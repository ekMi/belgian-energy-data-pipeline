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
