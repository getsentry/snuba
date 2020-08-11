from typing import Any, MutableMapping

import pytest

from snuba import state
from snuba.datasets.factory import get_dataset
from snuba.query.parser import parse_query
from snuba.query.parser.exceptions import ValidationException
from snuba.state import set_config

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
    set_config("enforce_expression_validation", 1)
    state.set_config("query_parsing_expand_aliases", 1)
    events = get_dataset("events")
    with pytest.raises(ValidationException):
        parse_query(query_body, events)
