from unittest.mock import MagicMock

import pytest

import orchestration_store as store
from config import verify_database_target

PARAMS = {
    "host": "localhost",
    "port": "5432",
    "dbname": "belgian_energy",
    "user": "energy_app",
    "password": "fake_password",
    "connect_timeout": 5,
}

SHA256 = "a" * 64


@pytest.fixture
def fake_db(monkeypatch):
    conn = MagicMock()
    conn.__enter__.return_value = conn

    connect_mock = MagicMock(return_value=conn)
    verify_mock = MagicMock()

    monkeypatch.setattr(store, "get_connection_params", lambda: PARAMS.copy())
    monkeypatch.setattr(store.psycopg, "connect", connect_mock)
    monkeypatch.setattr(store, "verify_database_target", verify_mock)

    return conn, connect_mock, verify_mock


def test_existing_successful_batch(fake_db):
    conn, connect_mock, verify_mock = fake_db

    # Simuler une ligne retournée par PostgreSQL
    conn.execute.return_value.fetchone.return_value = (2,)

    result = store.find_successful_batch(SHA256)

    assert result == 2

    # Vérifier les paramètres utilisés pour la connexion
    connect_mock.assert_called_once_with(**PARAMS)

    # Vérifier l'activation du mode lecture seule
    assert conn.execute.call_args_list[0].args[0] == ("SET TRANSACTION READ ONLY")

    # Vérifier le paramètre transmis à la requête SELECT
    sql_call = conn.execute.call_args_list[1]
    assert "FROM staging.ingestion_batch" in sql_call.args[0]
    assert sql_call.args[1] == (SHA256,)

    # Vérifier que la protection de base est appelée
    verify_mock.assert_called_once_with(conn, "belgian_energy", "belgian_energy")


def test_unknown_file(fake_db):
    conn, _, _ = fake_db

    # Simuler l'absence de résultat
    conn.execute.return_value.fetchone.return_value = None

    result = store.find_successful_batch(SHA256)

    assert result is None


def test_wrong_database_is_rejected(fake_db, monkeypatch):
    _, connect_mock, _ = fake_db

    monkeypatch.setattr(
        store,
        "get_connection_params",
        lambda: {**PARAMS, "dbname": "postgres"},
    )

    with pytest.raises(RuntimeError):
        store.find_successful_batch(SHA256)

    connect_mock.assert_not_called()


def test_connected_to_wrong_database(fake_db, monkeypatch):
    conn, _, _ = fake_db

    # Utiliser la vraie fonction de vérification
    monkeypatch.setattr(
        store,
        "verify_database_target",
        verify_database_target,
    )

    # Simuler la réponse de PostgreSQL
    conn.execute.return_value.fetchone.return_value = ("postgres",)

    with pytest.raises(RuntimeError, match="connected database is not authorized"):
        store.find_successful_batch(SHA256)

    # Vérifier que la recherche du batch n'a jamais été exécutée
    queries = [call.args[0] for call in conn.execute.call_args_list]

    assert not any("FROM staging.ingestion_batch" in query for query in queries)
