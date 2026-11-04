"""Read-only probe of the public Elia ODS001 JSON and CSV endpoints."""

import csv
import io
import json
from urllib.error import HTTPError, URLError
from urllib.parse import urlencode
from urllib.request import Request, urlopen

BASE_URL = (
    "https://external-elia.opendatasoft.com/api/explore/v2.1/"
    "catalog/datasets/ods001"
)
WHERE = (
    "datetime >= '2025-10-20T00:00:00Z' "
    "AND datetime < '2025-10-20T01:00:00Z'"
)


def fetch(endpoint: str, params: dict[str, str | int]) -> str:
    url = f"{BASE_URL}/{endpoint}?{urlencode(params)}"
    print(f"\nGET {url}")
    request = Request(url, headers={"User-Agent": "belgian-energy-data-pipeline-probe/1.0"})
    with urlopen(request, timeout=20) as response:
        print(f"HTTP status: {response.status}")
        print(f"Content-Type: {response.headers.get('Content-Type')}")
        return response.read().decode("utf-8-sig")


def main() -> None:
    common_params = {"where": WHERE, "timezone": "UTC"}

    print("=== JSON /records ===")
    try:
        raw_json = fetch(
            "records",
            {**common_params, "limit": 4, "order_by": "datetime ASC"},
        )
        payload = json.loads(raw_json)
        records = payload.get("results", [])
        print(f"total_count: {payload.get('total_count')}")
        print(f"returned records: {len(records)}")
        if records:
            print("First record:")
            print(json.dumps(records[0], indent=2, ensure_ascii=False))
    except (HTTPError, URLError, TimeoutError, ValueError) as exc:
        print(f"JSON probe failed: {type(exc).__name__}: {exc}")

    print("\n=== CSV /exports/csv ===")
    try:
        raw_csv = fetch(
            "exports/csv",
            {**common_params, "order_by": "datetime ASC", "use_labels": "true"},
        )
        rows = list(csv.reader(io.StringIO(raw_csv), delimiter=";"))
        print(f"CSV data rows: {max(0, len(rows) - 1)}")
        if rows:
            print(f"CSV header ({len(rows[0])} columns): {rows[0]}")
        if len(rows) > 1:
            print(f"First CSV row ({len(rows[1])} columns): {rows[1]}")
    except (HTTPError, URLError, TimeoutError, ValueError) as exc:
        print(f"CSV probe failed: {type(exc).__name__}: {exc}")


if __name__ == "__main__":
    main()
