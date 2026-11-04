r"""Read-only checks of Elia ODS001: 30 days, DST, and latest published data.

Run from the repository root:
    .\.venv\Scripts\python.exe scripts/probe_elia_api.py

Only makes HTTP GET requests. No downloads are saved and no database is accessed.
"""

import csv
import io
import json
import sys
from datetime import UTC, datetime, timedelta
from pathlib import Path
from urllib.error import HTTPError, URLError
from urllib.parse import urlencode
from urllib.request import Request, urlopen

# Reuse the actual ingestion contract, rather than maintaining another CSV schema.
sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))
from validate_ods001 import EXPECTED_COLUMNS, parse_row  # noqa: E402

BASE_URL = (
    "https://external-elia.opendatasoft.com/api/explore/v2.1/catalog/datasets/ods001"
)
START = datetime(2025, 10, 1, tzinfo=UTC)
END = datetime(2025, 10, 31, tzinfo=UTC)  # 30 UTC days, includes autumn DST switch
STEP = timedelta(minutes=15)


def iso_utc(value: datetime) -> str:
    return value.isoformat().replace("+00:00", "Z")


def get(endpoint: str, params: dict[str, str | int], expected_type: str) -> str:
    url = f"{BASE_URL}/{endpoint}?{urlencode(params)}"
    print(f"GET {url}")
    request = Request(url, headers={"User-Agent": "belgian-energy-api-probe/1.1"})
    with urlopen(request, timeout=60) as response:
        content_type = response.headers.get("Content-Type", "")
        print(f"HTTP {response.status} | {content_type}")
        if expected_type not in content_type:
            raise ValueError(f"Unexpected Content-Type: {content_type}")
        return response.read().decode("utf-8-sig")


def get_json(endpoint: str, params: dict[str, str | int]) -> dict:
    payload = json.loads(get(endpoint, params, "application/json"))
    if not isinstance(payload, dict):
        raise ValueError("Expected a JSON object")
    return payload


def check_historical_window() -> bool:
    print("\n=== 1. HISTORIQUE : 30 JOURS EN OCTOBRE 2025 ===")
    where = f"datetime >= '{iso_utc(START)}' AND datetime < '{iso_utc(END)}'"
    common = {"where": where, "timezone": "UTC"}

    # Independent API count helps detect a silently truncated CSV export.
    count_json = get_json("records", {**common, "limit": 1})
    count_api = count_json["total_count"]
    raw = get(
        "exports/csv",
        {**common, "order_by": "datetime ASC", "use_labels": "true"},
        "text/csv",
    )
    reader = csv.reader(io.StringIO(raw), delimiter=";")
    header = next(reader, None)
    if header != EXPECTED_COLUMNS:
        raise ValueError(f"CSV header does not match ingestion contract: {header}")
    timestamps = []
    missing_total_load = 0
    missing_weekahead = 0
    for line_number, fields in enumerate(reader, start=2):
        if not fields:
            continue
        parsed = parse_row(fields, line_number=line_number)
        timestamps.append(parsed[0])
        missing_total_load += parsed[2] is None
        missing_weekahead += parsed[9] is None

    expected_count = int((END - START) / STEP)
    duplicates = len(timestamps) - len(set(timestamps))
    out_of_bounds = sum(not (START <= t < END) for t in timestamps)
    ordered = sorted(set(timestamps))
    discontinuities = sum(b - a != STEP for a, b in zip(ordered, ordered[1:]))
    ok = (
        (
            len(timestamps) == expected_count == count_api
            and not duplicates
            and not out_of_bounds
            and not discontinuities
            and ordered[0] == START
            and ordered[-1] == END - STEP
        )
        if ordered
        else False
    )

    print(
        f"Rows: CSV={len(timestamps)} | API total_count={count_api} | "
        f"expected={expected_count}"
    )
    print(f"First UTC: {iso_utc(ordered[0]) if ordered else 'none'}")
    print(f"Last UTC:  {iso_utc(ordered[-1]) if ordered else 'none'}")
    print(
        f"Duplicates={duplicates} | out_of_bounds={out_of_bounds} | "
        f"gaps={discontinuities}"
    )
    print(f"Missing Total Load={missing_total_load} | Week-ahead={missing_weekahead}")
    print(f"30-day check: {'PASS' if ok else 'FAIL'}")

    print("\n=== 2. CHANGEMENT D'HEURE : 26 OCTOBRE 2025 ===")
    # Belgium's 2025-10-26 local day spans 25 hours (22:00Z on 25th to
    # 23:00Z on 26th). The repeated 02:00 local hour has TWO UTC hours.
    local_day_start = datetime(2025, 10, 25, 22, tzinfo=UTC)
    local_day_end = datetime(2025, 10, 26, 23, tzinfo=UTC)
    repeated_start = datetime(2025, 10, 26, 0, tzinfo=UTC)
    repeated_mid = datetime(2025, 10, 26, 1, tzinfo=UTC)
    repeated_end = datetime(2025, 10, 26, 2, tzinfo=UTC)
    day_count = sum(local_day_start <= t < local_day_end for t in timestamps)
    first_2am = sum(repeated_start <= t < repeated_mid for t in timestamps)
    second_2am = sum(repeated_mid <= t < repeated_end for t in timestamps)
    dst_ok = day_count == 100 and first_2am == 4 and second_2am == 4
    print(f"26 October (Belgian local day): {day_count} intervals; expected 100")
    print(
        f"Repeated 02:00 local hour: {first_2am} + {second_2am} "
        "intervals; expected 4 + 4"
    )
    print(f"DST check: {'PASS' if dst_ok else 'FAIL'}")
    return ok and dst_ok


def check_latest() -> bool:
    print("\n=== 3. DONNEES RECENTES ===")
    latest = get_json(
        "records",
        {"order_by": "datetime DESC", "limit": 5, "timezone": "UTC"},
    )
    results = latest.get("results", [])
    if not results:
        print("No records returned")
        return False
    print("Latest 5 timestamps (Total Load missing?):")
    for record in results:
        print(
            f"  {record['datetime']} | "
            f"Total Load missing: {record.get('totalload') is None}"
        )

    # Most recent record WITH an actual load value; a forecast can be newer.
    measured = get_json(
        "records",
        {
            "where": "totalload IS NOT NULL",
            "order_by": "datetime DESC",
            "limit": 1,
            "timezone": "UTC",
        },
    ).get("results", [])
    if not measured:
        print("No record with Total Load found")
        return False
    latest_measurement = datetime.fromisoformat(measured[0]["datetime"])
    lag_hours = (
        datetime.now(UTC) - latest_measurement.astimezone(UTC)
    ).total_seconds() / 3600
    print(
        f"Latest record with Total Load: {iso_utc(latest_measurement.astimezone(UTC))}"
    )
    print(f"Age relative to local execution time: {lag_hours:.1f} hours")
    print("Note: record timestamp is not the publication/update timestamp.")
    return True


def main() -> None:
    checks = []
    for title, check in (
        ("historical + DST", check_historical_window),
        ("latest", check_latest),
    ):
        try:
            checks.append(check())
        except (
            HTTPError,
            URLError,
            TimeoutError,
            OSError,
            ValueError,
            KeyError,
            TypeError,
            csv.Error,
        ) as error:
            print(f"{title} FAILED: {type(error).__name__}: {error}")
            checks.append(False)

    print(f"\nFINAL: {'PASS' if all(checks) else 'FAIL'}")
    if not all(checks):
        raise SystemExit(1)


if __name__ == "__main__":
    main()
