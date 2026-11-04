"""Offline acquisition tests: no network, database, or real retry waits."""

import csv
import hashlib
import io
import json
import ssl
from datetime import UTC, datetime, timedelta, timezone
from email.message import Message
from http.client import IncompleteRead
from types import SimpleNamespace
from unittest.mock import Mock
from urllib.error import HTTPError, URLError
from urllib.parse import parse_qs, urlsplit

import pytest

import acquire_elia as client
from validate_ods001 import EXPECTED_COLUMNS

START = datetime(2025, 10, 26, tzinfo=UTC)
END = START + timedelta(hours=1)


def row(timestamp="2025-10-26T00:00:00Z"):
    return [timestamp, "PT15M", *["1000"] * 8]


def csv_bytes(rows=None, header=None):
    stream = io.StringIO(newline="")
    csv.writer(stream, delimiter=";").writerows(
        [
            EXPECTED_COLUMNS if header is None else header,
            *(rows if rows is not None else [row()]),
        ]
    )
    return stream.getvalue().encode("utf-8-sig")


class Response(io.BytesIO):
    def __init__(
        self, body, content_type="text/csv; charset=utf-8", status=200, length=None
    ):
        super().__init__(body)
        self.status = status
        self.headers = Message()
        self.headers["Content-Type"] = content_type
        self.headers["Content-Length"] = str(len(body) if length is None else length)


def count_response(count=1):
    return Response(json.dumps({"total_count": count}).encode(), "application/json")


@pytest.fixture(autouse=True)
def forbid_live_http(monkeypatch):
    # Also protect direct use of the public function without an injected opener.
    monkeypatch.setattr(
        client, "urlopen", Mock(side_effect=AssertionError("Real HTTP forbidden"))
    )


@pytest.fixture
def harness(tmp_path):
    opener = Mock(side_effect=[Response(csv_bytes()), count_response()])
    sleep = Mock()

    def acquire(**kwargs):
        return client.acquire_elia(
            START, END, output_dir=tmp_path, opener=opener, sleep=sleep, **kwargs
        )

    return SimpleNamespace(
        acquire=acquire, opener=opener, sleep=sleep, directory=tmp_path
    )


def test_default_window_uses_utc_date_even_across_local_midnight():
    local = datetime(2025, 10, 27, 0, 30, tzinfo=timezone(timedelta(hours=1)))
    start, end = client.acquisition_window(now=local)
    assert start == datetime(2025, 9, 26, tzinfo=UTC)
    assert end == datetime(2025, 11, 3, tzinfo=UTC)
    assert end - start == timedelta(days=38)


@pytest.mark.parametrize("day", [datetime(2025, 3, 30, tzinfo=UTC), START])
def test_default_window_at_dst_changes(day):
    assert client.acquisition_window(now=day) == (
        day - timedelta(days=30),
        day + timedelta(days=8),
    )


@pytest.mark.parametrize(
    "start,end",
    [
        (START, None),
        (None, END),
        (END, START),
        (START, START),
        (START.replace(tzinfo=None), END),
        (START.astimezone(timezone(timedelta(hours=1))), END),
    ],
)
def test_invalid_windows(start, end):
    with pytest.raises(ValueError):
        client.acquisition_window(start, end)


def test_explicit_window_and_filter():
    assert client.acquisition_window(START, END) == (START, END)
    assert client.build_filter(START, END) == (
        "datetime >= '2025-10-26T00:00:00Z' AND datetime < '2025-10-26T01:00:00Z'"
    )


def test_valid_download_metadata_and_encoded_parameters(harness):
    before = datetime.now(UTC)
    result = harness.acquire(timeout=12)
    assert before <= result.extracted_at <= datetime.now(UTC)
    assert result.file_path.read_bytes() == csv_bytes()
    assert result.row_count == 1
    assert result.sha256 == hashlib.sha256(csv_bytes()).hexdigest()
    assert result.window_start_utc == START
    assert result.window_end_utc == END
    assert list(harness.directory.iterdir()) == [result.file_path]
    assert result.file_path.name.startswith("ods001_20251026T000000000000Z_")
    requests = [call.args[0] for call in harness.opener.call_args_list]
    export, records = [
        parse_qs(urlsplit(request.full_url).query) for request in requests
    ]
    assert export == {
        "where": [client.build_filter(START, END)],
        "timezone": ["UTC"],
        "order_by": ["datetime ASC"],
        "use_labels": ["true"],
    }
    assert records == {"where": export["where"], "timezone": ["UTC"], "limit": ["1"]}
    assert result.source_reference == requests[0].full_url
    assert all(request.get_method() == "GET" for request in requests)
    assert all(call.kwargs["timeout"] == 12 for call in harness.opener.call_args_list)
    assert result.to_dict()["extracted_at"].endswith("Z")
    json.dumps(result.to_dict())
    harness.sleep.assert_not_called()


@pytest.mark.parametrize(
    "header", [EXPECTED_COLUMNS[::-1], EXPECTED_COLUMNS[:-1], ["HTML"]]
)
def test_bad_headers_leave_no_file(harness, header):
    harness.opener.side_effect = [Response(csv_bytes(header=header))]
    with pytest.raises(client.AcquisitionError, match="header"):
        harness.acquire()
    assert not list(harness.directory.iterdir())


@pytest.mark.parametrize(
    "body,content_type,message",
    [
        (b"", "text/csv", "header"),
        (csv_bytes([]), "text/csv", "no observations"),
        (b"<html>failure</html>", "text/html", "expected text/csv"),
        (b"<html>failure</html>", "text/csv", "header"),
        (csv_bytes(), "application/octet-stream", "expected text/csv"),
        (csv_bytes(), "", "expected text/csv"),
    ],
)
def test_empty_or_unexpected_response(harness, body, content_type, message):
    harness.opener.side_effect = [Response(body, content_type)]
    with pytest.raises(client.AcquisitionError, match=message):
        harness.acquire()
    assert not list(harness.directory.iterdir())


@pytest.mark.parametrize("status", [400, 401, 403, 404, 422, 501])
def test_non_retryable_http_error_fails_immediately(harness, status):
    harness.opener.side_effect = HTTPError(
        "https://example.test", status, "error", {}, None
    )
    with pytest.raises(client.AcquisitionError, match=f"non-retryable HTTP {status}"):
        harness.acquire()
    assert harness.opener.call_count == 1
    harness.sleep.assert_not_called()
    assert not list(harness.directory.iterdir())


@pytest.mark.parametrize("status", sorted(client.RETRY_STATUSES))
def test_transient_http_retry_then_success(harness, status):
    harness.opener.side_effect = [
        HTTPError("https://example.test", status, "temporary", {}, None),
        Response(csv_bytes()),
        count_response(),
    ]
    assert harness.acquire().row_count == 1
    harness.sleep.assert_called_once_with(1.0)


@pytest.mark.parametrize(
    "error",
    [
        TimeoutError("slow"),
        URLError("temporary DNS failure"),
        ConnectionResetError("reset"),
        IncompleteRead(b"partial", 10),
    ],
)
def test_network_retry_then_success(harness, error):
    harness.opener.side_effect = [error, Response(csv_bytes()), count_response()]
    assert harness.acquire().row_count == 1
    harness.sleep.assert_called_once_with(1.0)


def test_retry_exhaustion_uses_backoff_and_cleans_up(harness):
    harness.opener.side_effect = TimeoutError("slow")
    with pytest.raises(
        client.AcquisitionError, match="failed after 4 attempts.*TimeoutError"
    ):
        harness.acquire()
    assert harness.opener.call_count == 4
    assert [call.args[0] for call in harness.sleep.call_args_list] == [1, 2, 4]
    assert not list(harness.directory.iterdir())


def test_no_retries_means_one_attempt(harness):
    harness.opener.side_effect = TimeoutError()
    with pytest.raises(client.AcquisitionError, match="after 1 attempts"):
        harness.acquire(max_retries=0)
    harness.sleep.assert_not_called()


def test_bad_certificate_is_not_retried(harness):
    harness.opener.side_effect = URLError(ssl.SSLCertVerificationError("invalid"))
    with pytest.raises(client.AcquisitionError, match="TLS certificate"):
        harness.acquire()
    assert harness.opener.call_count == 1


def test_returned_http_status_is_checked(harness):
    harness.opener.side_effect = [
        Response(b"", status=503),
        Response(csv_bytes()),
        count_response(),
    ]
    assert harness.acquire().row_count == 1
    harness.sleep.assert_called_once()


@pytest.mark.parametrize(
    "rows,message",
    [
        ([row(), row()], "duplicate"),
        ([row("2025-10-26T00:15:00Z"), row()], "strictly increasing"),
        ([row("2025-10-26T01:00:00Z")], "outside requested window"),
        ([row("2025-10-25T23:45:00Z")], "outside requested window"),
        ([row("2025-10-26T00:01:00Z")], "aligned"),
        ([row("2025-10-26T00:00:01Z")], "aligned"),
        ([row("2025-10-26T00:00:00.000001Z")], "aligned"),
        ([row("2025-10-26T00:00:00")], "UTC offset"),
        ([row("invalid")], "ISO 8601"),
        ([row()[:-1]], "expected 10 fields"),
        ([row() + ["1"]], "expected 10 fields"),
    ],
)
def test_invalid_observations(harness, rows, message):
    harness.opener.side_effect = [Response(csv_bytes(rows))]
    with pytest.raises(client.AcquisitionError, match=message):
        harness.acquire()
    assert not list(harness.directory.iterdir())


def test_duplicate_offsets_are_normalized(harness):
    harness.opener.side_effect = [
        Response(csv_bytes([row(), row("2025-10-26T02:00:00+02:00")]))
    ]
    with pytest.raises(client.AcquisitionError, match="duplicate"):
        harness.acquire()


def test_offset_timestamps_normalized_for_order_and_window(harness):
    rows = [row("2025-10-26T02:00:00+02:00"), row("2025-10-26T01:15:00+01:00")]
    harness.opener.side_effect = [Response(csv_bytes(rows)), count_response(2)]
    assert harness.acquire().row_count == 2


@pytest.mark.parametrize("index", range(2, 10))
def test_every_measurement_may_be_null(harness, index):
    fields = row()
    fields[index] = ""
    body = csv_bytes([fields])
    harness.opener.side_effect = [Response(body), count_response()]
    assert harness.acquire().file_path.read_bytes() == body


@pytest.mark.parametrize("index", range(2, 10))
@pytest.mark.parametrize("value", ["NaN", "inf", "invalid", " "])
def test_present_measurements_use_existing_parser(harness, index, value):
    fields = row()
    fields[index] = value
    harness.opener.side_effect = [Response(csv_bytes([fields]))]
    with pytest.raises(client.AcquisitionError, match=f"column {index + 1}"):
        harness.acquire()


def test_wrong_resolution(harness):
    fields = row()
    fields[1] = "PT60M"
    harness.opener.side_effect = [Response(csv_bytes([fields]))]
    with pytest.raises(client.AcquisitionError, match="PT15M"):
        harness.acquire()


@pytest.mark.parametrize(
    "body", [csv_bytes() + b'"unterminated', csv_bytes() + b"\xff"]
)
def test_malformed_or_invalid_utf8_csv(harness, body):
    harness.opener.side_effect = [Response(body)]
    with pytest.raises(client.AcquisitionError, match="Invalid CSV"):
        harness.acquire()
    assert not list(harness.directory.iterdir())


def test_partial_transfer_is_restarted_without_appending(harness):
    class BrokenResponse(Response):
        def read(self, size=-1):
            if self.tell():
                raise ConnectionResetError("mid-transfer reset")
            return super().read(40)

    harness.opener.side_effect = [
        BrokenResponse(csv_bytes()),
        Response(csv_bytes()),
        count_response(),
    ]
    result = harness.acquire()
    assert result.file_path.read_bytes() == csv_bytes()
    harness.sleep.assert_called_once()


def test_content_length_truncation_retried_then_fails(harness):
    harness.opener.side_effect = lambda *args, **kwargs: Response(
        csv_bytes(), length=9999
    )
    with pytest.raises(client.AcquisitionError, match="IncompleteRead"):
        harness.acquire(max_retries=1)
    assert harness.opener.call_count == 2
    assert not list(harness.directory.iterdir())


@pytest.mark.parametrize("api_count", [0, 2, 96])
def test_complete_rows_but_inconsistent_count_fail(harness, api_count):
    harness.opener.side_effect = [Response(csv_bytes()), count_response(api_count)]
    with pytest.raises(client.AcquisitionError, match=f"CSV=1, API={api_count}"):
        harness.acquire()
    assert not list(harness.directory.iterdir())


@pytest.mark.parametrize(
    "payload",
    [
        b"",
        b"not json",
        b"[]",
        b"{}",
        b'{"total_count": null}',
        b'{"total_count": true}',
        b'{"total_count": -1}',
        b'{"total_count": 1.0}',
        b'{"total_count": "1"}',
    ],
)
def test_invalid_count_response(harness, payload):
    harness.opener.side_effect = [
        Response(csv_bytes()),
        Response(payload, "application/json"),
    ]
    with pytest.raises(client.AcquisitionError, match="/records"):
        harness.acquire()
    assert not list(harness.directory.iterdir())


def test_count_request_retries_independently(harness):
    harness.opener.side_effect = [
        Response(csv_bytes()),
        TimeoutError(),
        count_response(),
    ]
    assert harness.acquire().row_count == 1
    assert harness.opener.call_count == 3
    harness.sleep.assert_called_once()


def test_count_request_failure_never_publishes(harness):
    harness.opener.side_effect = [
        Response(csv_bytes()),
        Response(b"<html>", "text/html"),
    ]
    with pytest.raises(client.AcquisitionError, match="expected application/json"):
        harness.acquire()
    assert not list(harness.directory.iterdir())


def test_atomic_publication_only_after_validation(harness, monkeypatch):
    real_link = client.os.link

    def publish(source, target):
        assert harness.opener.call_count == 2
        assert source.suffix == ".tmp"
        assert source.read_bytes() == csv_bytes()
        assert not target.exists()
        assert not list(harness.directory.glob("*.csv"))
        real_link(source, target)
        assert target.read_bytes() == csv_bytes()

    link = Mock(side_effect=publish)
    monkeypatch.setattr(client.os, "link", link)
    assert harness.acquire().file_path.exists()
    link.assert_called_once()
    assert not list(harness.directory.glob("*.tmp"))


def test_collision_never_overwrites_existing_file(harness, monkeypatch):
    real_link = client.os.link

    def collision(source, target):
        target.write_bytes(b"existing export")
        real_link(source, target)

    monkeypatch.setattr(client.os, "link", collision)
    with pytest.raises(FileExistsError):
        harness.acquire()
    assert [path.read_bytes() for path in harness.directory.iterdir()] == [
        b"existing export"
    ]


def test_publication_failure_cleans_temporary_file(harness, monkeypatch):
    monkeypatch.setattr(
        client.os, "link", Mock(side_effect=OSError("link unavailable"))
    )
    with pytest.raises(OSError, match="link unavailable"):
        harness.acquire()
    assert not list(harness.directory.iterdir())


def test_repeated_explicit_extraction_preserves_both_files(harness):
    harness.opener.side_effect = [
        Response(csv_bytes()),
        count_response(),
        Response(csv_bytes()),
        count_response(),
    ]
    first, second = harness.acquire(), harness.acquire()
    assert first.file_path != second.file_path
    assert first.sha256 == second.sha256
    assert first.window_start_utc == second.window_start_utc == START
    assert first.window_end_utc == second.window_end_utc == END
    assert first.source_reference == second.source_reference
    assert len(list(harness.directory.glob("*.csv"))) == 2


@pytest.mark.parametrize(
    "options",
    [
        {"timeout": 0},
        {"timeout": float("nan")},
        {"max_retries": -1},
        {"max_retries": 11},
        {"backoff": -1},
        {"backoff": float("inf")},
    ],
)
def test_invalid_options_do_not_request(harness, options):
    with pytest.raises(ValueError):
        harness.acquire(**options)
    harness.opener.assert_not_called()


def test_cli_success_json_only_on_stdout(harness, monkeypatch, capsys):
    result = harness.acquire()
    acquire = Mock(return_value=result)
    monkeypatch.setattr(client, "acquire_elia", acquire)
    assert (
        client.main(
            ["--start", "2025-10-26T00:00:00Z", "--end", "2025-10-26T01:00:00+00:00"]
        )
        == 0
    )
    captured = capsys.readouterr()
    assert json.loads(captured.out) == result.to_dict()
    assert not captured.err
    assert acquire.call_args.args == (START, END)


def test_cli_failure_is_explicit_on_stderr(monkeypatch, capsys):
    monkeypatch.setattr(
        client,
        "acquire_elia",
        Mock(side_effect=client.AcquisitionError("count mismatch")),
    )
    assert client.main([]) == 1
    captured = capsys.readouterr()
    assert captured.out == ""
    assert "count mismatch" in captured.err


@pytest.mark.parametrize(
    "arguments", [["--start", "2025-10-26"], ["--start", "2025-10-26T00:00:00+02:00"]]
)
def test_cli_rejects_non_utc_timestamp(arguments):
    with pytest.raises(SystemExit) as error:
        client.main(arguments)
    assert error.value.code == 2
