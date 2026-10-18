import logging

import pytest

import operations
from config import positive_seconds


@pytest.mark.parametrize("value", ["0", "-1", "3601", "secret-invalid", "1.5"])
def test_invalid_timeout_hides_value(monkeypatch, value):
    monkeypatch.setenv("PIPELINE_CONNECT_TIMEOUT", value)
    with pytest.raises(ValueError, match="PIPELINE_CONNECT_TIMEOUT") as error:
        positive_seconds("PIPELINE_CONNECT_TIMEOUT", 5)
    assert "secret-invalid" not in str(error.value)


def test_configurable_timeout(monkeypatch):
    monkeypatch.setenv("PIPELINE_LOCK_TIMEOUT", "2")
    assert positive_seconds("PIPELINE_LOCK_TIMEOUT", 10) == 2


@pytest.mark.parametrize(
    ("exception", "expected"),
    [(RuntimeError("password=SECRET"), 1), (KeyboardInterrupt(), 130)],
)
def test_exit_codes_and_safe_errors(monkeypatch, caplog, exception, expected):
    monkeypatch.setattr(operations, "configure_logging", lambda: None)

    def fail():
        raise exception

    with caplog.at_level(logging.INFO):
        assert operations.run_cli(fail) == expected
    assert "SECRET" not in caplog.text
    assert str(expected) in caplog.text


def test_success_code(monkeypatch):
    monkeypatch.setattr(operations, "configure_logging", lambda: None)
    assert operations.run_cli(lambda: None) == 0


def test_argparse_exit_preserved(monkeypatch):
    monkeypatch.setattr(operations, "configure_logging", lambda: None)

    def fail():
        raise SystemExit(2)

    with pytest.raises(SystemExit) as result:
        operations.run_cli(fail)
    assert result.value.code == 2
