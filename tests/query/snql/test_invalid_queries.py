import pytest
from parsimonious.exceptions import IncompleteParseError

from snuba.datasets.factory import get_dataset
from snuba.query.snql.parser import parse_snql_query

test_cases = [
    # below are case1s that are not parsed completely
    # i.e. the entire string is not consumed
    pytest.param(
        "MATCH(e: Events)SELECT4-5,3*g(c),cBYd,2+7 WHEREa<3ORDERBYfDESC",
        IncompleteParseError,
        id="ORDER BY is two words",
    ),
    pytest.param(
        "MATCH (e: Events) SELECT 4-5, 3*g(c), c BY d,2+7 WHERE a<3  ORDER BYfDESC",
        IncompleteParseError,
        id="Expression before ASC / DESC needs to be separated from ASC / DESC keyword by space",
    ),
    pytest.param(
        "MATCH (e: Events) SELECT 4-5, 3*g(c), c BY d, ,2+7 WHERE a<3  ORDER BY f DESC",
        IncompleteParseError,
        id="In a list, columns are separated by exactly one comma",
    ),
]


@pytest.mark.parametrize("query_body, expected_exception", test_cases)
def test_failures(query_body: str, expected_exception: Exception) -> None:
    with pytest.raises(expected_exception):
        events = get_dataset("events")
        parse_snql_query(query_body, events)
