"""Run dbt in its dedicated Python environment and propagate failure to the parent."""

import sys


def main() -> int:
    """Forward CLI arguments to the official API; never turn failure into success."""
    from dbt.cli.main import dbtRunner

    result = dbtRunner().invoke(sys.argv[1:])
    return 0 if result.success else 1


if __name__ == "__main__":
    raise SystemExit(main())
