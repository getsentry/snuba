from typing import Any, MutableMapping, Type

import pytest

from snuba.datasets.factory import get_dataset
from snuba.query.parser.exceptions import (
    AliasShadowingException,
    InvalidQueryException,
    ParsingException,
)
from snuba.query.parser import parse_query

test_cases = [
    pytest.param(
        {"aggregations": [["f(i(am)bad((at(parentheses)+3", None, "alias"]]},
        ParsingException,
        id="Aggregation string cannot be parsed",
    ),
    pytest.param(
        {"orderby": [[[[["column"]]]]]}, ParsingException, id="Nonsensical order by",
    ),
    pytest.param(
        {"conditions": [["timestamp", "=", ""]]},
        ParsingException,
        id="Invalid date in a condition",
    ),
    pytest.param(
        {"conditions": [["timestamp", "IS NOT NULL", "this makes no sense"]]},
        ParsingException,
        id="Binary condition with unary operator",
    ),
    pytest.param(
        {"conditions": [["project_id", "IN", "2"]]},
        ParsingException,
        id="IN condition without a sequence as right hand side",
    ),
    pytest.param(
        {"selected_columns": [["f", [1], "alias"], ["f", [2], "alias"]]},
        AliasShadowingException,
        id="Alias shadowing",
    ),
]


@pytest.mark.parametrize("query_body, expected_exception", test_cases)
def test_failures(
    query_body: MutableMapping[str, Any],
    expected_exception: Type[InvalidQueryException],
) -> None:
    with pytest.raises(expected_exception):
        events = get_dataset("events")
        parse_query(query_body, events)
