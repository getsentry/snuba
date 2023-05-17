import re

import pytest

from snuba.datasets.factory import get_dataset
from snuba.query.exceptions import InvalidExpressionException
from snuba.query.parser.exceptions import ParsingException
from snuba.query.snql.parser import parse_snql_query

test_cases = [
    pytest.param(
        """
        MATCH (events)
        SELECT event_id
        WHERE timestamp LIKE 'carbonara'
        """,
        ParsingException(
            "Missing >= condition with a datetime literal on column timestamp for entity events. Example: timestamp >= toDateTime('2023-05-16 00:00')"
        ),
        id="Invalid LIKE param",
    ),
    pytest.param(
        "MATCH (discover_events) SELECT arrayMap((`x`) -> identity(`y`), sdk_integrations) AS sdks WHERE project_id = 1 AND timestamp >= toDateTime('2021-01-01') AND timestamp < toDateTime('2021-01-02')",
        InvalidExpressionException("identifier(s) `y` not defined"),
        id="invalid lambda identifier",
    ),
    pytest.param(
        "MATCH (discover_events) SELECT arrayMap((`x`) -> arrayMap((`y`) -> identity(`z`), sdk_integrations), sdk_integrations) AS sdks WHERE project_id = 1 AND timestamp >= toDateTime('2021-01-01') AND timestamp < toDateTime('2021-01-02')",
        InvalidExpressionException("identifier(s) `z` not defined"),
        id="invalid nested lambda identifier",
    ),
]


@pytest.mark.parametrize("query_body, exception", test_cases)
def test_validation(query_body: str, exception: Exception) -> None:
    events = get_dataset("events")
    with pytest.raises(type(exception), match=re.escape(str(exception))):
        parse_snql_query(query_body, events)
