"""Safety checks shared by PostgreSQL fixtures and their unit tests."""

TEST_DATABASE = "belgian_energy_test"


def verify_test_database(connection):
    """Perform read-only checks before allowing any database mutation."""
    with connection.cursor() as cursor:
        cursor.execute(
            "SELECT current_database(), current_setting('server_version_num')"
        )
        database, version = cursor.fetchone()
    if database != TEST_DATABASE:
        raise RuntimeError(
            "Refusing database writes: connected database is not authorized"
        )
    if not 180000 <= int(version) < 190000:
        raise RuntimeError("Integration tests require PostgreSQL 18")
