"""Small synthetic CSV/HTTP responses shared by offline API pipeline tests."""

import csv
import io
import json
from unittest.mock import Mock

from validate_ods001 import EXPECTED_COLUMNS


def csv_payload(rows):
    stream = io.StringIO(newline="")
    csv.writer(stream, delimiter=";").writerows([EXPECTED_COLUMNS, *rows])
    return stream.getvalue().encode("utf-8-sig")


def http_response(body, content_type="text/csv", status=200):
    response = io.BytesIO(body)
    response.status = status
    response.headers = {"Content-Type": content_type, "Content-Length": str(len(body))}
    return response


def api_transport(rows):
    return Mock(
        side_effect=[
            http_response(csv_payload(rows)),
            http_response(
                json.dumps({"total_count": len(rows)}).encode(), "application/json"
            ),
        ]
    )
