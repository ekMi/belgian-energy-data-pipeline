from unittest.mock import MagicMock

import pytest
from postgres_support import verify_test_database


@pytest.mark.parametrize("database", ["belgian_energy", "postgres", "other_test"])
def test_rejects_unauthorized_database(database):
    connection = MagicMock()
    cursor = connection.cursor.return_value.__enter__.return_value
    cursor.fetchone.return_value = (database, "180000")
    with pytest.raises(RuntimeError, match="not authorized"):
        verify_test_database(connection)
    assert cursor.execute.call_count == 1
    assert cursor.execute.call_args.args[0].startswith("SELECT ")


def test_accepts_authorized_postgres18():
    connection = MagicMock()
    connection.cursor.return_value.__enter__.return_value.fetchone.return_value = (
        "belgian_energy_test",
        "180003",
    )
    verify_test_database(connection)


def test_rejects_other_postgres_version():
    connection = MagicMock()
    connection.cursor.return_value.__enter__.return_value.fetchone.return_value = (
        "belgian_energy_test",
        "170005",
    )
    with pytest.raises(RuntimeError, match="PostgreSQL 18"):
        verify_test_database(connection)
