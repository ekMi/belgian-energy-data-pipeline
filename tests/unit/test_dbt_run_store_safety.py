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


def test_borrowed_connection_is_verified_without_opening_or_closing(monkeypatch):
    connection = MagicMock()
    connection.execute.return_value.fetchone.return_value = ("belgian_energy",)
    connect = Mock()
    monkeypatch.setattr(store.psycopg, "connect", connect)
    monkeypatch.setattr(
        store, "get_connection_params", lambda: {"dbname": "belgian_energy"}
    )
    with store._connection_context(connection=connection) as borrowed:
        assert borrowed is connection
    connect.assert_not_called()
    connection.close.assert_not_called()
    connection.commit.assert_not_called()
    connection.__exit__.assert_not_called()


def test_borrowed_wrong_database_rejected(monkeypatch):
    connection = MagicMock()
    connection.execute.return_value.fetchone.return_value = ("other",)
    monkeypatch.setattr(
        store, "get_connection_params", lambda: {"dbname": "belgian_energy"}
    )
    with pytest.raises(RuntimeError, match="connected database"):
        store.mark_success(1, connection=connection)
    assert connection.execute.call_count == 1
