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
        ["count()", None, None],
        FunctionCall(None, "count", ()),
    ),  # Common way to provide count()
    (
        ["count()", "event_id", None],
        CurriedFunctionCall(
            None, FunctionCall(None, "count", ()), (Column(None, "event_id", None),)
        ),
    ),  # This is probably wrong, but we cannot disambiguate it at this level
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
        ["quantile(0.95)(duration)", None, "p95"],
        CurriedFunctionCall(
            "p95",
            FunctionCall(None, "quantile", (Literal(None, 0.95),)),
            (Column(None, "duration", None),),
        ),
    ),  # Curried function
    (
        ["apdex(duration, 300)", "", "apdex_score"],
        FunctionCall(
            "apdex_score",
            "apdex",
            (Column(None, "duration", None), Literal(None, 300),),
        ),
    ),  # apdex formula
    (
        ["f('asd')", "", "apdex_score"],
        FunctionCall("apdex_score", "f", (Literal(None, "asd"),),),
    ),  # string literals
    (
        ["toUInt64(plus(multiply(log(times_seen), 600), last_seen))", None, None],
        FunctionCall(
            None,
            "toUInt64",
            (
                FunctionCall(
                    None,
                    "plus",
                    (
                        FunctionCall(
                            None,
                            "multiply",
                            (
                                FunctionCall(
                                    None, "log", (Column(None, "times_seen", None),),
                                ),
                                Literal(None, 600),
                            ),
                        ),
                        Column(None, "last_seen", None),
                    ),
                ),
            ),
        ),
    ),  # This really happens in the Sentry codebase. Not that we should support it.
]


@pytest.mark.parametrize("aggregation, expected_function", test_data)
def test_aggregation_parsing(aggregation, expected_function):
    function = parse_aggregation(aggregation[0], aggregation[1], aggregation[2])
    assert function == expected_function
