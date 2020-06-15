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
        FunctionCall(None, "count", (Column("event_id", None, "event_id"),)),
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
        ["f(123)", None, None],
        FunctionCall(None, "f", (Literal(None, 123),)),
    ),  # Int literal
    (
        ["f(123e+06)", None, None],
        FunctionCall(None, "f", (Literal(None, float(123e06)),)),
    ),  # Int literal exp
    (
        ["f(123.2)", None, None],
        FunctionCall(None, "f", (Literal(None, 123.2),)),
    ),  # Float literal
    (
        ["f(123.2e-06)", None, None],
        FunctionCall(None, "f", (Literal(None, 123.2e-06),)),
    ),  # Float literal exp
    (
        ["count()", "event_id", None],
        CurriedFunctionCall(
            None,
            FunctionCall(None, "count", ()),
            (Column("event_id", None, "event_id"),),
        ),
    ),  # This is probably wrong, but we cannot disambiguate it at this level
    (
        ["uniq", "platform", "uniq_platforms"],
        FunctionCall("uniq_platforms", "uniq", (Column("platform", None, "platform"),)),
    ),  # Use the columns provided as parameters
    (
        ["topK(1)", "platform", "top_platforms"],
        CurriedFunctionCall(
            "top_platforms",
            FunctionCall(None, "topK", (Literal(None, 1),)),
            (Column("platform", None, "platform"),),
        ),
    ),  # Curried function
    (
        ["quantile(0.95)(duration)", None, "p95"],
        CurriedFunctionCall(
            "p95",
            FunctionCall(None, "quantile", (Literal(None, 0.95),)),
            (Column(None, None, "duration"),),
        ),
    ),  # Curried function
    (
        ["apdex(duration, 300)", "", "apdex_score"],
        FunctionCall(
            "apdex_score",
            "apdex",
            (Column(None, None, "duration"), Literal(None, 300),),
        ),
    ),  # apdex formula
    (
        ["f('asd')", "", "apdex_score"],
        FunctionCall("apdex_score", "f", (Literal(None, "asd"),),),
    ),  # string literals
    (
        ["f('asd', '')", "", "f"],
        FunctionCall("f", "f", (Literal(None, "asd"), Literal(None, ""))),
    ),  # empty string literals
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
                                    None, "log", (Column(None, None, "times_seen"),),
                                ),
                                Literal(None, 600),
                            ),
                        ),
                        Column(None, None, "last_seen"),
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
