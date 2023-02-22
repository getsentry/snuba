import logging
from typing import Any
from unittest import mock

import pytest
from flask.testing import FlaskClient

from snuba.query.exceptions import InvalidQueryException
from snuba.query.parser.exceptions import ParsingException
from snuba.web.views import handle_invalid_query

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
