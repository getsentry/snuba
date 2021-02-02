import pytest

from snuba.clickhouse.query import Expression as ClickhouseExpression
from snuba.clickhouse.translators.snuba.mapping import SnubaClickhouseMappingTranslator
from snuba.datasets.entities.discover import (
    transaction_translation_mappers,
    null_function_translation_mappers,
)
from snuba.query.dsl import identity
from snuba.query.expressions import (
    Column,
    CurriedFunctionCall,
    Expression,
    FunctionCall,
    Literal,
)


test_data = [
    pytest.param(
        Column(None, None, "group_id"),
        Literal(None, None),
        id="basic column translation",
    ),
    pytest.param(
        Column(None, None, "primary_hash"),
        identity(Literal(None, None), "primary_hash"),
        id="basic event translation",
    ),
    pytest.param(
        FunctionCall(None, "isHandled", (Literal(None, 1),)),
        identity(Literal(None, None), None),
        id="none function mapper",
    ),
    pytest.param(
        FunctionCall("alias", "foo", (Column(None, None, "primary_hash"),)),
        identity(Literal(None, None), "alias"),
        id="if null function mapper",
    ),
    pytest.param(
        FunctionCall(
            "alias",
            "foobar",
            (FunctionCall("alias", "foo", (Column(None, None, "primary_hash"),)),),
        ),
        identity(Literal(None, None), "alias"),
        id="if null wrapped function mapper",
    ),
    pytest.param(
        FunctionCall(
            None,
            "plus",
            (
                FunctionCall(
                    None,
                    "multiply",
                    (
                        FunctionCall(
                            None,
                            "floor",
                            (
                                FunctionCall(
                                    None,
                                    "divide",
                                    (
                                        FunctionCall(
                                            None,
                                            "minus",
                                            (
                                                FunctionCall(
                                                    None,
                                                    "multiply",
                                                    (
                                                        FunctionCall(
                                                            None,
                                                            "arrayJoin",
                                                            (
                                                                Column(
                                                                    None,
                                                                    None,
                                                                    "duration",
                                                                ),
                                                            ),
                                                        ),
                                                        Literal(None, 100),
                                                    ),
                                                ),
                                                Literal(None, 0),
                                            ),
                                        ),
                                        Literal(None, 1),
                                    ),
                                ),
                            ),
                        ),
                        Literal(None, 1),
                    ),
                ),
                Literal(None, 0),
            ),
        ),
        FunctionCall(
            None,
            "plus",
            (
                FunctionCall(
                    None,
                    "multiply",
                    (
                        FunctionCall(
                            None,
                            "floor",
                            (
                                FunctionCall(
                                    None,
                                    "divide",
                                    (
                                        FunctionCall(
                                            None,
                                            "minus",
                                            (
                                                FunctionCall(
                                                    None,
                                                    "multiply",
                                                    (
                                                        FunctionCall(
                                                            None,
                                                            "arrayJoin",
                                                            (
                                                                Column(
                                                                    None,
                                                                    None,
                                                                    "duration",
                                                                ),
                                                            ),
                                                        ),
                                                        Literal(None, 100),
                                                    ),
                                                ),
                                                Literal(None, 0),
                                            ),
                                        ),
                                        Literal(None, 1),
                                    ),
                                ),
                            ),
                        ),
                        Literal(None, 1),
                    ),
                ),
                Literal(None, 0),
            ),
        ),
        id="discover craziness with no replacement",
    ),
    pytest.param(
        FunctionCall(
            "alias",
            "plus",
            (
                FunctionCall(
                    None,
                    "multiply",
                    (
                        FunctionCall(
                            None,
                            "floor",
                            (
                                FunctionCall(
                                    None,
                                    "divide",
                                    (
                                        FunctionCall(
                                            None,
                                            "minus",
                                            (
                                                FunctionCall(
                                                    None,
                                                    "multiply",
                                                    (
                                                        FunctionCall(
                                                            None,
                                                            "arrayJoin",
                                                            (
                                                                Column(
                                                                    None,
                                                                    None,
                                                                    "primary_hash",
                                                                ),
                                                            ),
                                                        ),
                                                        Literal(None, 100),
                                                    ),
                                                ),
                                                Literal(None, 0),
                                            ),
                                        ),
                                        Literal(None, 1),
                                    ),
                                ),
                            ),
                        ),
                        Literal(None, 1),
                    ),
                ),
                Literal(None, 0),
            ),
        ),
        identity(Literal(None, None), "alias"),
        id="discover craziness with replacement",
    ),
    pytest.param(
        CurriedFunctionCall(
            None,
            FunctionCall(None, "quantile", (Literal(None, 0.95),)),
            (Column(None, None, "primary_hash"),),
        ),
        CurriedFunctionCall(
            None,
            FunctionCall(None, "quantileOrNull", (Literal(None, 0.95),)),
            (Literal(None, None),),
        ),
        id="curried function call",
    ),
    pytest.param(
        FunctionCall(
            None,
            "plus",
            (
                Literal(None, 0),
                FunctionCall(None, "multiply", (Literal(None, 0.0), Literal(None, 0),)),
            ),
        ),
        FunctionCall(
            None,
            "plus",
            (
                Literal(None, 0),
                FunctionCall(None, "multiply", (Literal(None, 0.0), Literal(None, 0),)),
            ),
        ),
        id="literals aren't cached",
    ),
]


@pytest.mark.parametrize("expression, expected", test_data)
def test_transaction_translation(
    expression: Expression, expected: ClickhouseExpression,
) -> None:
    translator = SnubaClickhouseMappingTranslator(
        transaction_translation_mappers.concat(null_function_translation_mappers)
    )
    translated = expression.accept(translator)

    assert translated == expected
