import pytest

from snuba.datasets.entities.factory import get_entity, EntityKey
from snuba.query.expressions import (
    Column,
    CurriedFunctionCall,
    FunctionCall,
    Literal,
)
from snuba.query.parser.expressions import parse_aggregation

test_data = [
    (
        ["f('2020/07/16')", None, None],
        FunctionCall(None, "f", (Literal(None, "2020/07/16"),),),
    ),  # String (that looks like arithmetic) nested in function call
    (
        ["f(a/b, g(c*3))", None, None],
        FunctionCall(
            None,
            "f",
            (
                FunctionCall(
                    None, "divide", (Column(None, None, "a"), Column(None, None, "b"))
                ),
                FunctionCall(
                    None,
                    "g",
                    (
                        FunctionCall(
                            None,
                            "multiply",
                            (Column(None, None, "c"), Literal(None, 3)),
                        ),
                    ),
                ),
            ),
        ),
    ),  # Arithmetic expressions nested inside function calls
    (
        ["f(a,b)-3*g(c)", None, None],
        FunctionCall(
            None,
            "minus",
            (
                FunctionCall(
                    None, "f", (Column(None, None, "a"), Column(None, None, "b"))
                ),
                FunctionCall(
                    None,
                    "multiply",
                    (
                        Literal(None, 3),
                        FunctionCall(None, "g", (Column(None, None, "c"),)),
                    ),
                ),
            ),
        ),
    ),  # Arithmetic expressions involving function calls
    (
        ["a+b", None, None],
        FunctionCall(None, "plus", (Column(None, None, "a"), Column(None, None, "b"))),
    ),  # Simple addition with Columns
    (
        ["1+2-3", None, None],
        FunctionCall(
            None,
            "minus",
            (
                FunctionCall(None, "plus", (Literal(None, 1), Literal(None, 2))),
                Literal(None, 3),
            ),
        ),
    ),  # Addition with more than 2 terms
    (
        [" 5 +4 * 3/6  ", None, None],
        FunctionCall(
            None,
            "plus",
            (
                Literal(None, 5),
                FunctionCall(
                    None,
                    "divide",
                    (
                        FunctionCall(
                            None, "multiply", (Literal(None, 4), Literal(None, 3))
                        ),
                        Literal(None, 6),
                    ),
                ),
            ),
        ),
    ),  # Combination of multiplication, addition
    (
        [" 3  +6 * 2 /5*7- 4/1   ", None, None],
        FunctionCall(
            None,
            "minus",
            (
                FunctionCall(
                    None,
                    "plus",
                    (
                        Literal(None, 3),
                        FunctionCall(
                            None,
                            "multiply",
                            (
                                FunctionCall(
                                    None,
                                    "divide",
                                    (
                                        FunctionCall(
                                            None,
                                            "multiply",
                                            (Literal(None, 6), Literal(None, 2)),
                                        ),
                                        Literal(None, 5),
                                    ),
                                ),
                                Literal(None, 7),
                            ),
                        ),
                    ),
                ),
                FunctionCall(None, "divide", (Literal(None, 4), Literal(None, 1))),
            ),
        ),
    ),  # More complicated Combination of operators
    (
        ["count", "event_id", None],
        FunctionCall(None, "count", (Column(None, None, "event_id"),)),
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
            None, FunctionCall(None, "count", ()), (Column(None, None, "event_id"),),
        ),
    ),  # This is probably wrong, but we cannot disambiguate it at this level
    (
        ["uniq", "platform", "uniq_platforms"],
        FunctionCall("uniq_platforms", "uniq", (Column(None, None, "platform"),)),
    ),  # Use the columns provided as parameters
    (
        ["topK(1)", "platform", "top_platforms"],
        CurriedFunctionCall(
            "top_platforms",
            FunctionCall(None, "topK", (Literal(None, 1),)),
            (Column(None, None, "platform"),),
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
    entity = get_entity(EntityKey.EVENTS)
    function = parse_aggregation(
        aggregation[0], aggregation[1], aggregation[2], entity.get_data_model(), set(),
    )
    assert function == expected_function, expected_function
