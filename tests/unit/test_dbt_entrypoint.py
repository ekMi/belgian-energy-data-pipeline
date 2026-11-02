"""Exercise the entry point without importing dbt into the ingestion environment."""

import runpy
import sys
from pathlib import Path
from types import ModuleType, SimpleNamespace
from unittest.mock import Mock

import pytest

import dbt_entrypoint


@pytest.mark.parametrize("success,exit_code", [(True, 0), (False, 1)])
def test_entrypoint_forwards_arguments_and_exit_code(monkeypatch, success, exit_code):
    args = [
        "build",
        "--project-dir",
        "project",
        "--profiles-dir",
        "profiles",
        "--target",
        "local",
    ]
    runner = Mock()
    runner.invoke.return_value = SimpleNamespace(success=success)
    module = ModuleType("dbt.cli.main")
    module.dbtRunner = Mock(return_value=runner)
    monkeypatch.setitem(sys.modules, "dbt.cli.main", module)
    monkeypatch.setattr(sys, "argv", ["dbt_entrypoint", *args])
    with pytest.raises(SystemExit) as caught:
        runpy.run_path(str(Path(dbt_entrypoint.__file__)), run_name="__main__")
    assert caught.value.code == exit_code
    runner.invoke.assert_called_once_with(args)


def test_exception_propagates(monkeypatch):
    module = ModuleType("dbt.cli.main")
    module.dbtRunner = Mock(side_effect=RuntimeError("dbt error"))
    monkeypatch.setitem(sys.modules, "dbt.cli.main", module)
    with pytest.raises(RuntimeError):
        dbt_entrypoint.main()
