from typing import Any, MutableMapping

import pytest

from snuba.datasets.factory import get_dataset
from snuba.query.exceptions import ValidationException
from snuba.query.parser import parse_query

test_cases = [
    pytest.param(
        {
            "selected_columns": ["event_id"],
            "conditions": [["timestamp", "LIKE", "asdasd"]],
        },
        id="Invalid LIKE param",
    ),
]


@pytest.mark.parametrize("query_body", test_cases)
def test_validation(query_body: MutableMapping[str, Any]) -> None:
    events = get_dataset("events")
    with pytest.raises(ValidationException):
        parse_query(query_body, events)
