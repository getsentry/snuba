import pytest

from snuba.query.expressions import (
    Column,
    CurriedFunctionCall,
    FunctionCall,
    Literal,
)
from snuba.query.parser.expressions import parse_aggregation

test_data = [
    (
        ["count", "event_id", None],
        FunctionCall(None, "count", (Column(None, "event_id", None),)),
    ),  # Simple aggregation
    (
        ["count()", "", None],
        FunctionCall(None, "count", ()),
    ),  # Common way to provide count()
    (
        ["uniq", "platform", "uniq_platforms"],
        FunctionCall("uniq_platforms", "uniq", (Column(None, "platform", None),)),
    ),  # Use the columns provided as parameters
    (
        ["topK(1)", "platform", "top_platforms"],
        CurriedFunctionCall(
            "top_platforms",
            FunctionCall(None, "topK", (Literal(None, 1),)),
            (Column(None, "platform", None),),
        ),
    ),  # Curried function
    (
        ["apdex(duration, 300)", "", "apdex_score"],
        FunctionCall(
            "apdex_score",
            "apdex",
            (Column(None, "duration", None), Literal(None, 300),),
        ),
    ),  # The most complex function we plan to support in the syntax
]


@pytest.mark.parametrize("aggregation, expected_function", test_data)
def test_aggregation_parsing(aggregation, expected_function):
    function = parse_aggregation(aggregation[0], aggregation[1], aggregation[2])
    assert function == expected_function
