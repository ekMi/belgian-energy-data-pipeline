import psycopg

from config import get_connection_params
from operations import run_cli


def main() -> None:
    connection_params = get_connection_params()

    with psycopg.connect(**connection_params) as conn, conn.cursor() as cur:
        cur.execute(
            """
                SELECT
                    current_database(),
                    current_user,
                    current_setting('TimeZone'),
                    version();
                """
        )

        database, user, timezone, version = cur.fetchone()

        print(f"Database: {database}")
        print(f"User: {user}")
        print(f"Timezone: {timezone}")
        print(f"PostgreSQL: {version.split(',')[0]}")


if __name__ == "__main__":
    raise SystemExit(run_cli(main))
