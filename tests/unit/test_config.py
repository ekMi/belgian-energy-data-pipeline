import pytest

from config import CSV_PATH, PROJECT_ROOT, get_connection_params

VARIABLES = ["PGHOST", "PGPORT", "PGDATABASE", "PGUSER", "PGPASSWORD"]


def test_connection_params(monkeypatch):
    values = ["localhost", "5432", "test_database", "test_user", "test_secret"]
    for name, value in zip(VARIABLES, values, strict=True):
        monkeypatch.setenv(name, value)
    monkeypatch.delenv("PIPELINE_CONNECT_TIMEOUT", raising=False)
    assert get_connection_params()["connect_timeout"] == 5
    assert {
        k: v for k, v in get_connection_params().items() if k != "connect_timeout"
    } == dict(zip(["host", "port", "dbname", "user", "password"], values, strict=True))


def test_missing_config_does_not_expose_values(monkeypatch):
    for name in VARIABLES:
        monkeypatch.delenv(name, raising=False)
    monkeypatch.setenv("PGPASSWORD", "test_secret")
    with pytest.raises(ValueError) as error:
        get_connection_params()
    assert "PGHOST" in str(error.value)
    assert "test_secret" not in str(error.value)


def test_paths_do_not_depend_on_working_directory(monkeypatch, tmp_path):
    monkeypatch.chdir(tmp_path)
    assert PROJECT_ROOT.is_absolute()
    assert CSV_PATH == PROJECT_ROOT / "data" / "raw" / "ods001.csv"
