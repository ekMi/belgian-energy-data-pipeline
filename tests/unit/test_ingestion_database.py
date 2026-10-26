"""The ingestion target is checked before permanent database writes."""

from contextlib import nullcontext
from pathlib import Path
from unittest.mock import MagicMock, Mock

import pytest

import ingest_ods001
from config import INGESTION_DATABASE, verify_database_target


def test_accepts_configured_and_connected_database():
    connection = MagicMock()
    connection.execute.return_value.fetchone.return_value = (INGESTION_DATABASE,)

    verify_database_target(connection, INGESTION_DATABASE, INGESTION_DATABASE)

    connection.execute.assert_called_once_with("SELECT current_database()")


@pytest.mark.parametrize("configured_database", ["postgres", None, ""])
def test_rejects_wrong_or_missing_configuration_without_query(configured_database):
    connection = MagicMock()

    with pytest.raises(RuntimeError, match="configured database is not authorized"):
        verify_database_target(connection, configured_database, INGESTION_DATABASE)

    connection.execute.assert_not_called()


def test_rejects_mismatch_between_configuration_and_connection():
    connection = MagicMock()
    connection.execute.return_value.fetchone.return_value = ("postgres",)

    with pytest.raises(RuntimeError, match="connected database is not authorized"):
        verify_database_target(connection, INGESTION_DATABASE, INGESTION_DATABASE)

    connection.execute.assert_called_once_with("SELECT current_database()")


def test_wrong_configured_database_never_connects(monkeypatch):
    connect = Mock()
    monkeypatch.setattr(
        ingest_ods001, "get_connection_params", lambda: {"dbname": "postgres"}
    )
    monkeypatch.setattr(ingest_ods001.psycopg, "connect", connect)

    with pytest.raises(RuntimeError, match="configured database is not authorized"):
        ingest_ods001.ingest_csv(Path("unused"))

    connect.assert_not_called()


def test_wrong_connected_database_never_starts_batch(monkeypatch):
    connection = MagicMock()
    connection.__enter__.return_value = connection
    connection.execute.return_value.fetchone.return_value = ("postgres",)
    connect = Mock(return_value=connection)
    checksum = Mock()
    monkeypatch.setattr(
        ingest_ods001,
        "get_connection_params",
        lambda: {"dbname": INGESTION_DATABASE},
    )
    monkeypatch.setattr(ingest_ods001.psycopg, "connect", connect)
    monkeypatch.setattr(ingest_ods001, "ingestion_lock", lambda conn: nullcontext())
    monkeypatch.setattr(ingest_ods001, "calculate_sha256", checksum)

    with pytest.raises(RuntimeError, match="connected database is not authorized"):
        ingest_ods001.ingest_csv(Path("unused"))

    connection.execute.assert_called_once_with("SELECT current_database()")
    checksum.assert_not_called()
