from typing import Any, MutableMapping, Type

import pytest
from snuba_sdk.legacy import json_to_snql
from snuba_sdk.query_visitors import InvalidQueryError

from snuba.query.exceptions import InvalidQueryException

test_cases = [
    pytest.param(
        {"orderby": [[[[["column"]]]]]},
        InvalidQueryError,
        id="Nonsensical order by",
    ),
    pytest.param(
        {"conditions": [["timestamp", "IS NOT NULL", "this makes no sense"]]},
        InvalidQueryError,
        id="Binary condition with unary operator",
    ),
    pytest.param(
        {"conditions": [["project_id", "IN", "2"]]},
        InvalidQueryError,
        id="IN condition without a sequence as right hand side",
    ),
]


@pytest.mark.parametrize("query_body, expected_exception", test_cases)
def test_failures(
    query_body: MutableMapping[str, Any],
    expected_exception: Type[InvalidQueryException],
) -> None:
    with pytest.raises(expected_exception):
        json_to_snql(query_body, "events")
