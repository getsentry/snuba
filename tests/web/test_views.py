import logging
from typing import Any

import pytest

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
