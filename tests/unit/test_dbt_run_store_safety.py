"""Destination and category guards without a PostgreSQL connection."""

from unittest.mock import MagicMock, Mock

import pytest

import dbt_run_store as store


def test_configured_destination_rejected_before_connect(monkeypatch):
    connect = Mock()
    monkeypatch.setattr(store, "get_connection_params", lambda: {"dbname": "other"})
    monkeypatch.setattr(store.psycopg, "connect", connect)
    with pytest.raises(RuntimeError, match="configured database"):
        store.start_build(1)
    connect.assert_not_called()


def test_connected_destination_rejected_before_write(monkeypatch):
    connection = MagicMock()
    connection.execute.return_value.fetchone.return_value = ("other",)
    monkeypatch.setattr(
        store, "get_connection_params", lambda: {"dbname": "belgian_energy"}
    )
    monkeypatch.setattr(store.psycopg, "connect", Mock(return_value=connection))
    with pytest.raises(RuntimeError, match="connected database"):
        store.start_build(1)
    assert [call.args[0] for call in connection.execute.call_args_list] == [
        "SELECT current_database()"
    ]


def test_failure_category_rejected_before_connect(monkeypatch):
    connect = Mock()
    monkeypatch.setattr(store.psycopg, "connect", connect)
    with pytest.raises(ValueError, match="approved failure category"):
        store.mark_failed(1, "DBT_BUILD_FAILED; password")
    connect.assert_not_called()
