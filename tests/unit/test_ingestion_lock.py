from pathlib import Path
from unittest.mock import MagicMock, Mock

import pytest

import ingest_ods001
from ingest_ods001 import INGESTION_LOCK, ingestion_lock


def test_invalid_scope_does_not_connect(monkeypatch):
    connect = Mock()
    monkeypatch.setattr(ingest_ods001.psycopg, "connect", connect)
    with pytest.raises(ValueError, match="input_scope"):
        ingest_ods001.ingest_csv(Path("unused"), input_scope="guess")
    connect.assert_not_called()


@pytest.mark.parametrize("failed", [False, True])
def test_session_lock_released(failed):
    conn = MagicMock()
    conn.closed = False
    if failed:
        with pytest.raises(ValueError, match="original"), ingestion_lock(conn):
            raise ValueError("original")
    else:
        with ingestion_lock(conn):
            pass
    sql = [call.args[0] for call in conn.execute.call_args_list]
    assert sql[0] == "SET LOCAL lock_timeout = '10s'"
    assert "pg_advisory_lock" in sql[1]
    assert "pg_advisory_unlock" in sql[-1]
    assert conn.execute.call_args.args[1] == INGESTION_LOCK


def test_unlock_failure_preserves_original_error():
    conn = MagicMock()
    conn.closed = False
    conn.rollback.side_effect = RuntimeError("connection lost")
    with pytest.raises(ValueError, match="original") as caught, ingestion_lock(conn):
        raise ValueError("original")
    assert "unlock failed" in caught.value.__notes__[0]
