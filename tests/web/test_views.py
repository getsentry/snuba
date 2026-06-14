import copy
import logging
from typing import Any

import pytest
import simplejson as json
from flask.testing import FlaskClient

from snuba.query.exceptions import InvalidQueryException
from snuba.query.parser.exceptions import ParsingException
from snuba.web.views import dump_payload, handle_invalid_query

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
    data = {
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
    clean_data["data"][3]["release"] = "RAW_BYTESTRING__" + b"x;\x83\xc0\x05".hex()  # type: ignore
    assert json.loads(dumped_payload) == clean_data


def test_stream_payload() -> None:
    from snuba.web.views import stream_payload

    data = {
        "data": [
            {"count": 5181337, "release": "elsa"},
            {"count": 2170, "release": b"valid-utf8"},
            {"count": 88, "release": b"x;\x83\xc0\x05"},
        ],
        "meta": [],
    }

    chunks = list(stream_payload(data))
    # The whole point is incremental encoding: many small chunks, not one blob.
    assert len(chunks) > 1
    assert all(isinstance(chunk, str) for chunk in chunks)

    parsed = json.loads("".join(chunks))
    assert parsed["data"][0]["release"] == "elsa"
    # valid UTF-8 bytes are decoded to a string
    assert parsed["data"][1]["release"] == "valid-utf8"
    # undecodable bytes are hex-encoded rather than breaking the stream
    assert parsed["data"][2]["release"] == "RAW_BYTESTRING__" + b"x;\x83\xc0\x05".hex()


def test_streamed_response() -> None:
    from flask import Response

    from snuba.web.views import application, stream_payload

    payload = {"data": [{"a": 1}, {"b": b"x;\x83\xc0\x05"}], "meta": []}

    with application.app_context():
        response = Response(stream_payload(payload), 200, {"Content-Type": "application/json"})
        # A generator body is streamed, not buffered into a fixed-length body.
        assert response.is_streamed
        body = response.get_data(as_text=True)

    parsed = json.loads(body)
    assert parsed["data"][0]["a"] == 1
    assert parsed["data"][1]["b"] == "RAW_BYTESTRING__" + b"x;\x83\xc0\x05".hex()


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
