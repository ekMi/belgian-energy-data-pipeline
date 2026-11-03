"""Real session locks, simulated dbt only, exclusively on belgian_energy_test."""

from concurrent.futures import ThreadPoolExecutor
from threading import Event

import psycopg
import pytest

import dbt_guard
import ingest_ods001
from pipeline_lock import PIPELINE_LOCK, pipeline_lock

pytestmark = pytest.mark.integration


@pytest.fixture
def guarded_dbt(test_database, monkeypatch, tmp_path):
    params = ingest_ods001.get_connection_params()
    monkeypatch.setattr(dbt_guard, "get_connection_params", lambda: params.copy())
    monkeypatch.setattr(dbt_guard, "INGESTION_DATABASE", "belgian_energy_test")
    monkeypatch.setattr(dbt_guard, "_verify_profile", lambda: None)
    executable = tmp_path / "simulated-python.exe"
    executable.touch()
    monkeypatch.setattr(dbt_guard, "DBT_PYTHON", executable)
    return params


@pytest.mark.parametrize("first_kind", ["ingestion", "dbt"])
def test_no_overlap(guarded_dbt, monkeypatch, first_kind):
    entered, release, attempting, completed = Event(), Event(), Event(), Event()

    def simulated_run(*args, **kwargs):
        entered.set()
        assert release.wait(5)
        return type("Result", (), {"returncode": 0})()

    monkeypatch.setattr(dbt_guard.subprocess, "run", simulated_run)

    def first():
        if first_kind == "dbt":
            dbt_guard.guarded_build()
        else:
            with (
                psycopg.connect(**guarded_dbt) as conn,
                ingest_ods001.ingestion_lock(conn),
            ):
                entered.set()
                assert release.wait(5)

    def second():
        attempting.set()
        with dbt_guard.locked_build() as session:
            # Reuse this connection for SQL without acquiring a second lock.
            assert session.connection.execute("SELECT 1").fetchone() == (1,)
            session.connection.commit()
            completed.set()

    with ThreadPoolExecutor(max_workers=2) as pool:
        one = pool.submit(first)
        try:
            assert entered.wait(5)
            two = pool.submit(second)
            assert attempting.wait(5)
            assert not completed.wait(0.3)
        finally:
            release.set()
        one.result(timeout=5)
        two.result(timeout=5)
    assert completed.is_set()


@pytest.mark.parametrize("fail", [False, True])
def test_release_after_context(guarded_dbt, fail):
    try:
        with dbt_guard.locked_build():
            if fail:
                raise ValueError("simulated failure")
    except ValueError:
        assert fail
    with psycopg.connect(**guarded_dbt) as conn:
        assert conn.execute(
            "SELECT pg_try_advisory_lock(%s, %s)", PIPELINE_LOCK
        ).fetchone()[0]


def test_contention_timeout_and_connection_close(guarded_dbt, monkeypatch):
    monkeypatch.setenv("PIPELINE_LOCK_TIMEOUT", "1")
    with psycopg.connect(**guarded_dbt) as holder:
        holder.execute("SELECT pg_advisory_lock(%s, %s)", PIPELINE_LOCK)
        holder.commit()
        with pytest.raises(psycopg.errors.LockNotAvailable):
            with dbt_guard.locked_build():
                pytest.fail("contended lock acquired")
    with psycopg.connect(**guarded_dbt) as conn, pipeline_lock(conn):
        assert conn.execute("SELECT 1").fetchone() == (1,)
