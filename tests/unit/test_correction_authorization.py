from datetime import UTC, datetime
from pathlib import Path
from unittest.mock import Mock

import pytest

import ingest_ods001
from ingest_ods001 import validate_authorization


@pytest.mark.parametrize(
    ("checksum", "reason"),
    [
        (None, "reason"),
        ("bad", "reason"),
        ("A" * 64, "reason"),
        ("a" * 64, None),
        ("a" * 64, ""),
        ("a" * 64, "   "),
    ],
)
def test_invalid_authorization(checksum, reason):
    with pytest.raises(ValueError):
        validate_authorization(checksum, reason, None)


def test_naive_extraction_date():
    with pytest.raises(ValueError, match="offset"):
        validate_authorization(None, None, datetime(2025, 1, 1))


def test_explicit_authorization_and_legacy_default():
    validate_authorization(None, None, None)
    validate_authorization(
        "a" * 64, "Reviewed correction", datetime(2025, 1, 1, tzinfo=UTC)
    )


def test_bad_authorization_never_connects(monkeypatch):
    connect = Mock()
    monkeypatch.setattr(ingest_ods001.psycopg, "connect", connect)
    with pytest.raises(ValueError):
        ingest_ods001.ingest_csv(
            Path("unused"), authorized_sha256="bad", correction_reason="reviewed"
        )
    connect.assert_not_called()
