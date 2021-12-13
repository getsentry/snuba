import pytest

from snuba.datasets.factory import get_dataset
from snuba.query.parser.exceptions import ParsingException
from snuba.query.snql.parser import parse_snql_query

test_cases = [
    pytest.param(
        """
        MATCH (events)
        SELECT event_id
        WHERE timestamp LIKE 'carbonara'
        """,
        id="Invalid LIKE param",
    ),
]


@pytest.mark.parametrize("query_body", test_cases)
def test_validation(query_body: str) -> None:
    events = get_dataset("events")
    with pytest.raises(ParsingException):
        parse_snql_query(query_body, events)
