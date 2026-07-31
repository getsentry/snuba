import copy
import logging
from typing import Any

import pytest
import simplejson as json
from flask import Response, jsonify
from flask.testing import FlaskClient

from snuba.query.exceptions import InvalidQueryException
from snuba.query.parser.exceptions import ParsingException
from snuba.web.views import application, dump_payload, handle_invalid_query

invalid_query_exception_test_cases = [
    pytest.param(
        ParsingException("This should be reported at WARNING", should_report=True),
        "WARNING",
        id="Report exception",
    ),
    pytest.param(
        ParsingException("This should be reported at INFO", should_report=False),
        "INFO",
        id="Mute exception",
    ),
]


@pytest.fixture
def snuba_api() -> FlaskClient:
    from snuba.web.views import application

    return application.test_client()


def test_response_dumping() -> None:
    data: dict[str, Any] = {
        "data": [
            {"count": 5181337, "release": "elsa"},
            {"count": 2170, "release": "simba"},
            {"count": 98, "release": "bambi"},
            {"count": 88, "release": b"x;\x83\xc0\x05"},
        ],
        "meta": [],
        "profile": {
            "blocks": 1,
            "bytes": 5698,
            "elapsed": 0.07789874076843262,
            "progress_bytes": 210032515,
            "rows": 10,
        },
        "timing": {
            "duration_ms": 211,
            "marks_ms": {},
            "tags": {},
            "timestamp": 1697823384,
        },
        "trace_output": "",
    }
    dumped_payload = dump_payload(data)

    clean_data = copy.deepcopy(data)
    clean_data["data"][3]["release"] = "RAW_BYTESTRING__" + b"x;\x83\xc0\x05".hex()
    assert json.loads(dumped_payload) == clean_data


def test_response_dumping_sanitizes_bytes_everywhere() -> None:
    """
    When the payload contains invalid-UTF-8 bytes anywhere (nested in lists, in
    the totals row, etc.) every ``bytes`` value is replaced: valid UTF-8 is
    decoded to a string, invalid bytes become a ``RAW_BYTESTRING__<hex>`` marker.
    """
    bad = b"x;\x83\xc0\x05"
    bad_hex = "RAW_BYTESTRING__" + bad.hex()
    data = {
        "data": [
            {"count": 1, "release": b"good-utf8", "tags": ["ok", bad, ["nested", bad]]},
            {"count": 2, "release": bad},
        ],
        "totals": {"count": 0, "release": bad},
        "meta": [],
        "trace_output": "",
    }
    expected = {
        "data": [
            {"count": 1, "release": "good-utf8", "tags": ["ok", bad_hex, ["nested", bad_hex]]},
            {"count": 2, "release": bad_hex},
        ],
        "totals": {"count": 0, "release": bad_hex},
        "meta": [],
        "trace_output": "",
    }
    dumped_payload = dump_payload(data)
    assert json.loads(dumped_payload) == expected


@pytest.mark.parametrize("exception, expected_log_level", invalid_query_exception_test_cases)
def test_handle_invalid_query(
    caplog: Any, exception: InvalidQueryException, expected_log_level: str
) -> None:
    with caplog.at_level(logging.INFO):
        caplog.clear()
        _ = handle_invalid_query(exception)
        for record in caplog.records:
            assert record.levelname == expected_log_level


def test_check_envoy_health(snuba_api: FlaskClient) -> None:
    response = snuba_api.get("/health_envoy")
    assert response.status_code == 200


def test_health_always_ok(snuba_api: FlaskClient) -> None:
    response = snuba_api.get("/health")
    assert response.status_code == 200
    assert json.loads(response.data) == {"status": "ok"}

    # thorough no longer changes the outcome — ClickHouse is not checked
    response = snuba_api.get("/health?thorough=true")
    assert response.status_code == 200
    assert json.loads(response.data) == {"status": "ok"}


COMPRESS_CASES = [
    ({"Accept-Encoding": "zstd"}, "zstd"),
    ({}, None),
]


@pytest.mark.parametrize("accept_header, expected_encoding", COMPRESS_CASES)
def test_json_responses_are_compressed(
    snuba_api: FlaskClient, accept_header: str, expected_encoding: str
) -> None:
    large_resp = snuba_api.get(test_route_large, headers=accept_header)
    # large resps are compressed according to header
    assert large_resp.headers.get("Content-Encoding") == expected_encoding
    assert "Accept-Encoding" in large_resp.headers.get("Vary", "")

    # small resps are not compressed regardless of header
    small_resp = snuba_api.get(test_route_small, headers=accept_header)
    assert small_resp.headers.get("Content-Encoding") is None


# Compression test fixtures
test_route_large = "/_test_data_large"
test_route_small = "/_test_data_small"


@application.route(test_route_large)
def _test_data_large() -> Response:
    return jsonify({"rows": [{"a": i, "b": "x" * 10} for i in range(100)]})


@application.route(test_route_small)
def _test_data_small() -> Response:
    return jsonify({"rows": [{"a": i, "b": "x" * 10} for i in range(10)]})
