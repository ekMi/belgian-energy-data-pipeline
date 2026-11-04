"""Acquire and retain validated Elia ODS001 exports, without ingestion."""

import argparse
import csv
import io
import json
import math
import os
import ssl
import sys
import time
from dataclasses import asdict, dataclass
from datetime import UTC, datetime, timedelta
from http.client import IncompleteRead
from pathlib import Path
from tempfile import NamedTemporaryFile
from typing import BinaryIO, Callable
from urllib.error import HTTPError, URLError
from urllib.parse import urlencode
from urllib.request import Request, urlopen
from uuid import uuid4
from weakref import WeakValueDictionary

from config import PROJECT_ROOT
from validate_ods001 import EXPECTED_COLUMNS, calculate_sha256, parse_row

BASE_URL = (
    "https://external-elia.opendatasoft.com/api/explore/v2.1/catalog/datasets/ods001"
)
DEFAULT_OUTPUT_DIR = PROJECT_ROOT / "data" / "raw" / "api"
RETRY_STATUSES = {429, 500, 502, 503, 504}
# Process-local receipts, keyed by identity: constructing/copying a dataclass or
# loading its JSON must not grant automatic correction permissions.
_ACQUISITIONS: WeakValueDictionary[int, "AcquisitionResult"] = WeakValueDictionary()


class AcquisitionError(RuntimeError):
    """The acquisition could not produce a validated export."""


@dataclass(frozen=True)
class AcquisitionResult:
    file_path: Path
    window_start_utc: datetime
    window_end_utc: datetime
    extracted_at: datetime
    source_reference: str
    row_count: int
    sha256: str

    def to_dict(self) -> dict:
        result = asdict(self)
        result["file_path"] = str(self.file_path)
        for name in ("window_start_utc", "window_end_utc", "extracted_at"):
            result[name] = iso_utc(result[name])
        return result


def verify_acquisition(
    result: AcquisitionResult | None, path: Path | None = None
) -> None:
    """Require an issued receipt and the original, unmodified CSV in this process.

    This guards application entry points, not malicious code in the same Python
    process or a compromised machine/transport. Receipts are not serialized trust.
    """
    if (
        not isinstance(result, AcquisitionResult)
        or _ACQUISITIONS.get(id(result)) is not result
    ):
        raise ValueError(
            "elia-api requires a validated acquisition issued by acquire_elia() "
            "in this process; use api_pipeline.py"
        )
    if path is not None and Path(path).resolve() != result.file_path.resolve():
        raise ValueError("API acquisition file path does not match the input file")
    if not result.file_path.is_file():
        raise ValueError("API acquisition CSV is missing")
    if calculate_sha256(result.file_path) != result.sha256:
        raise ValueError("API acquisition SHA-256 does not match the input file")


def iso_utc(value: datetime) -> str:
    return value.astimezone(UTC).isoformat().replace("+00:00", "Z")


def acquisition_window(
    start: datetime | None = None,
    end: datetime | None = None,
    *,
    now: datetime | None = None,
) -> tuple[datetime, datetime]:
    """Use UTC midnight, or a pair of explicit UTC instants (end exclusive)."""
    if (start is None) != (end is None):
        raise ValueError("Provide both window start and end, or neither")
    if start is None:
        current = now if now is not None else datetime.now(UTC)
        if current.utcoffset() is None:
            raise ValueError("Current time must have a timezone")
        midnight = current.astimezone(UTC).replace(
            hour=0, minute=0, second=0, microsecond=0
        )
        start, end = midnight - timedelta(days=30), midnight + timedelta(days=8)
    for value in (start, end):
        if value.utcoffset() != timedelta(0):
            raise ValueError("Window timestamps must have an explicit UTC offset")
    start, end = start.astimezone(UTC), end.astimezone(UTC)
    if start >= end:
        raise ValueError("Window start must precede exclusive end")
    return start, end


def build_filter(start: datetime, end: datetime) -> str:
    start, end = acquisition_window(start, end)
    return f"datetime >= '{iso_utc(start)}' AND datetime < '{iso_utc(end)}'"


def _fetch(
    url: str,
    media_type: str,
    destination: BinaryIO,
    *,
    timeout: float,
    max_retries: int,
    backoff: float,
    opener: Callable,
    sleep: Callable,
) -> None:
    request = Request(
        url,
        headers={
            "Accept": media_type,
            "User-Agent": "belgian-energy-data-pipeline/elia-acquisition",
        },
        method="GET",
    )
    for attempt in range(max_retries + 1):
        destination.seek(0)
        destination.truncate()
        try:
            with opener(request, timeout=timeout) as response:
                if response.status != 200:
                    raise HTTPError(
                        url,
                        response.status,
                        "Unexpected status",
                        response.headers,
                        None,
                    )
                actual_type = response.headers.get("Content-Type", "").split(";")[0]
                if actual_type.strip().lower() != media_type:
                    raise AcquisitionError(
                        f"GET {url}: expected {media_type}, got {actual_type!r}"
                    )
                length = response.headers.get("Content-Length")
                if length is not None:
                    try:
                        length = int(length)
                        if length < 0:
                            raise ValueError
                    except ValueError:
                        raise AcquisitionError(
                            f"GET {url}: invalid Content-Length"
                        ) from None
                received = 0
                while chunk := response.read(64 * 1024):
                    destination.write(chunk)
                    received += len(chunk)
                if length is not None and received != length:
                    raise IncompleteRead(b"", max(0, length - received))
            return
        except HTTPError as error:
            status = error.code
            error.close()
            if status not in RETRY_STATUSES:
                raise AcquisitionError(
                    f"GET {url}: non-retryable HTTP {status}"
                ) from error
            reason = f"HTTP {status}"
        except (URLError, TimeoutError, ConnectionError, IncompleteRead) as error:
            if isinstance(error, URLError) and isinstance(
                error.reason, ssl.SSLCertVerificationError
            ):
                raise AcquisitionError(
                    f"GET {url}: TLS certificate verification failed"
                ) from error
            reason = f"{type(error).__name__}: {error}"
        if attempt == max_retries:
            raise AcquisitionError(
                f"GET {url}: failed after {attempt + 1} attempts ({reason})"
            )
        sleep(min(backoff * 2**attempt, 60.0))


def _validate_export(path: Path, start: datetime, end: datetime) -> int:
    count = 0
    previous = None
    seen = set()
    try:
        with path.open("r", encoding="utf-8-sig", newline="") as stream:
            reader = csv.reader(stream, delimiter=";", strict=True)
            if next(reader, None) != EXPECTED_COLUMNS:
                raise AcquisitionError("Line 1: unexpected CSV header or column order")
            for row in reader:
                # Acquisition allows NULL in every measurement column. Supply a
                # finite placeholder only to validate the other fields with the
                # stricter ingestion parser; never change the retained CSV bytes.
                fields = [
                    "0" if index >= 2 and value == "" else value
                    for index, value in enumerate(row)
                ]
                parsed = parse_row(fields, line_number=reader.line_num)
                timestamp, resolution = parsed[:2]
                key = (timestamp, resolution)
                if key in seen:
                    raise AcquisitionError(
                        f"Line {reader.line_num}: duplicate observation"
                    )
                if previous is not None and timestamp <= previous:
                    raise AcquisitionError(
                        f"Line {reader.line_num}: timestamps must be strictly increasing"
                    )
                if timestamp.minute % 15 or timestamp.second or timestamp.microsecond:
                    raise AcquisitionError(
                        f"Line {reader.line_num}: timestamp is not aligned to 15 minutes"
                    )
                if not start <= timestamp < end:
                    raise AcquisitionError(
                        f"Line {reader.line_num}: timestamp outside requested window"
                    )
                seen.add(key)
                previous = timestamp
                count += 1
    except (ValueError, csv.Error) as error:
        raise AcquisitionError(f"Invalid CSV: {error}") from error
    if not count:
        raise AcquisitionError("CSV contains no observations")
    return count


def acquire_elia(
    window_start_utc: datetime | None = None,
    window_end_utc: datetime | None = None,
    *,
    output_dir: Path = DEFAULT_OUTPUT_DIR,
    timeout: float = 60.0,
    max_retries: int = 3,
    backoff: float = 1.0,
    opener: Callable | None = None,
    sleep: Callable | None = None,
) -> AcquisitionResult:
    """Download, validate, and publish an immutable CSV; HTTP/waits are injectable."""
    start, end = acquisition_window(window_start_utc, window_end_utc)
    if not math.isfinite(timeout) or timeout <= 0:
        raise ValueError("timeout must be finite and positive")
    if type(max_retries) is not int or not 0 <= max_retries <= 10:
        raise ValueError("max_retries must be an integer between 0 and 10")
    if not math.isfinite(backoff) or not 0 <= backoff <= 60:
        raise ValueError("backoff must be finite and between 0 and 60 seconds")
    common = {"where": build_filter(start, end), "timezone": "UTC"}
    csv_url = f"{BASE_URL}/exports/csv?" + urlencode(
        {**common, "order_by": "datetime ASC", "use_labels": "true"}
    )
    count_url = f"{BASE_URL}/records?" + urlencode({**common, "limit": 1})
    options = dict(
        timeout=timeout,
        max_retries=max_retries,
        backoff=backoff,
        opener=opener if opener is not None else urlopen,
        sleep=sleep if sleep is not None else time.sleep,
    )
    output_dir = Path(output_dir).resolve()
    output_dir.mkdir(parents=True, exist_ok=True)
    temporary_path = None
    try:
        with NamedTemporaryFile(
            mode="w+b", prefix=".ods001_", suffix=".tmp", dir=output_dir, delete=False
        ) as temporary:
            temporary_path = Path(temporary.name)
            _fetch(csv_url, "text/csv", temporary, **options)
            extracted_at = datetime.now(UTC)
            temporary.flush()
            os.fsync(temporary.fileno())
        row_count = _validate_export(temporary_path, start, end)
        payload = io.BytesIO()
        _fetch(count_url, "application/json", payload, **options)
        try:
            record_count = json.loads(payload.getvalue())["total_count"]
        except (ValueError, KeyError, TypeError) as error:
            raise AcquisitionError(
                "Invalid /records JSON: expected total_count"
            ) from error
        if type(record_count) is not int or record_count < 0:
            raise AcquisitionError(
                "Invalid /records total_count: expected nonnegative integer"
            )
        if row_count != record_count:
            raise AcquisitionError(
                f"Observation count mismatch: CSV={row_count}, API={record_count}"
            )
        checksum = calculate_sha256(temporary_path)

        def stamp(value):
            return value.strftime("%Y%m%dT%H%M%S%fZ")

        name = (
            f"ods001_{stamp(start)}_{stamp(end)}_"
            f"{stamp(extracted_at)}_{uuid4().hex}.csv"
        )
        final_path = output_dir / name
        result = AcquisitionResult(
            final_path, start, end, extracted_at, csv_url, row_count, checksum
        )
        # Atomic creation on the same filesystem. Unlike replace/rename on POSIX,
        # link refuses to overwrite even if another process wins a collision.
        os.link(temporary_path, final_path)
        _ACQUISITIONS[id(result)] = result
        return result
    finally:
        if temporary_path is not None:
            temporary_path.unlink(missing_ok=True)


def _utc_argument(value: str) -> datetime:
    try:
        timestamp = datetime.fromisoformat(value)
        if timestamp.utcoffset() != timedelta(0):
            raise ValueError
        return timestamp.astimezone(UTC)
    except ValueError:
        raise argparse.ArgumentTypeError(
            "expected an ISO 8601 UTC timestamp ending in Z or +00:00"
        ) from None


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--start", type=_utc_argument, help="Inclusive UTC timestamp")
    parser.add_argument("--end", type=_utc_argument, help="Exclusive UTC timestamp")
    parser.add_argument("--output-dir", type=Path, default=DEFAULT_OUTPUT_DIR)
    parser.add_argument("--timeout", type=float, default=60.0)
    parser.add_argument("--max-retries", type=int, default=3)
    args = parser.parse_args(argv)
    try:
        result = acquire_elia(
            args.start,
            args.end,
            output_dir=args.output_dir,
            timeout=args.timeout,
            max_retries=args.max_retries,
        )
    except (AcquisitionError, ValueError, OSError) as error:
        print(f"Elia acquisition failed: {error}", file=sys.stderr)
        return 1
    except KeyboardInterrupt:
        print("Elia acquisition interrupted", file=sys.stderr)
        return 130
    print(json.dumps(result.to_dict()))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
