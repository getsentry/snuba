from typing import Any, MutableMapping, Type

import pytest

from snuba.datasets.factory import get_dataset
from snuba.query.exceptions import InvalidQueryException
from snuba_sdk.legacy import json_to_snql
from snuba_sdk.query_visitors import InvalidQuery
from snuba.query.snql.parser import parse_snql_query
from snuba.query.parser.exceptions import AliasShadowingException, ParsingException

test_cases = [
    pytest.param(
        {"aggregations": [["f(i(am)bad((at(parentheses)+3", None, "alias"]]},
        ParsingException,
        id="Aggregation string cannot be parsed",
    ),
    pytest.param(
        {"orderby": [[[[["column"]]]]]}, InvalidQuery, id="Nonsensical order by",
    ),
    pytest.param(
        {"conditions": [["timestamp", "IS NOT NULL", "this makes no sense"]]},
        InvalidQuery,
        id="Binary condition with unary operator",
    ),
    pytest.param(
        {"conditions": [["project_id", "IN", "2"]]},
        InvalidQuery,
        id="IN condition without a sequence as right hand side",
    ),
    pytest.param(
        {"selected_columns": [["foo", [1], "alias"], ["bar", [2], "alias"]]},
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
        snql_query = json_to_snql(query_body, "events")
        parse_snql_query(str(snql_query), events)
