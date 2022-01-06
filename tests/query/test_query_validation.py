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
        ParsingException("missing >= condition on column timestamp for entity events"),
        id="Invalid LIKE param",
    ),
    pytest.param(
        "MATCH (discover_events) SELECT arrayMap((`x`) -> identity(`y`), sdk_integrations) AS sdks WHERE project_id = 1 AND timestamp >= toDateTime('2021-01-01') AND timestamp < toDateTime('2021-01-02')",
        InvalidExpressionException("identifier y not defined"),
        id="invalid lambda identifier",
    ),
]


@pytest.mark.parametrize("query_body, exception", test_cases)
def test_validation(query_body: str, exception: Exception) -> None:
    events = get_dataset("events")
    with pytest.raises(type(exception), match=re.escape(str(exception))):
        parse_snql_query(query_body, events)
