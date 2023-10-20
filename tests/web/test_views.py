import copy
import logging
from typing import Any
from unittest import mock

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
    clean_data["data"][3]["release"] = "RAW_BYTESTRING__" + b"x;\x83\xc0\x05".hex()
    assert json.loads(dumped_payload) == clean_data


@pytest.mark.parametrize(
    "exception, expected_log_level", invalid_query_exception_test_cases
)
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
    with mock.patch("snuba.web.views.check_down_file_exists", return_value=True):
        response = snuba_api.get("/health_envoy")
        assert response.status_code == 503


def test_check_health(snuba_api: FlaskClient) -> None:
    response = snuba_api.get("/health")
    assert response.status_code == 200
    # down file existing does not mean the pod is unhealthy
    with mock.patch("snuba.web.views.check_down_file_exists", return_value=True):
        response = snuba_api.get("/health")
        assert response.status_code == 200
    # don't check clickhouse if not thorough
    with mock.patch("snuba.web.views.check_clickhouse", return_value=False):
        response = snuba_api.get("/health")
        assert response.status_code == 200
    # thorough healthcheck fails on bad clickhouse connection
    with mock.patch("snuba.web.views.check_clickhouse", return_value=False):
        response = snuba_api.get("/health?thorough=true")
        assert response.status_code == 502
    # thorough healthcheck passes on good clickhouse connection
    with mock.patch("snuba.web.views.check_clickhouse", return_value=True):
        response = snuba_api.get("/health?thorough=true")
        assert response.status_code == 200
