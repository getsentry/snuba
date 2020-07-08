from snuba import state
from snuba.clickhouse.columns import ColumnSet
from snuba.clickhouse.formatter import ClickhouseExpressionFormatter
from snuba.datasets.schemas.tables import TableSource
from snuba.query.conditions import (
    BooleanFunctions,
    ConditionFunctions,
    binary_condition,
)
from snuba.query.dsl import count, countIf, divide, minus, multiply, plus
from snuba.query.expressions import Column, FunctionCall, Literal
from snuba.query.logical import Query, SelectedExpression
from snuba.query.processors.impact_processor import ImpactProcessor
from snuba.request.request_settings import HTTPRequestSettings


def test_impact_format_expressions() -> None:
    state.set_config("infix_where_format", 1)
    unprocessed = Query(
        {},
        TableSource("events", ColumnSet([])),
        selected_columns=[
            SelectedExpression(name=None, expression=Column(None, None, "column2")),
            SelectedExpression(
                "perf",
                FunctionCall(
                    "perf",
                    "impact",
                    (
                        Column(None, None, "column1"),
                        Literal(None, 300),
                        Column(None, None, "user"),
                    ),
                ),
            ),
        ],
    )
    expected = Query(
        {},
        TableSource("events", ColumnSet([])),
        selected_columns=[
            SelectedExpression(name=None, expression=Column(None, None, "column2")),
            SelectedExpression(
                "perf",
                plus(
                    minus(
                        Literal(None, 1),
                        divide(
                            plus(
                                countIf(
                                    binary_condition(
                                        None,
                                        ConditionFunctions.LTE,
                                        Column(None, None, "column1"),
                                        Literal(None, 300),
                                    ),
                                ),
                                divide(
                                    countIf(
                                        binary_condition(
                                            None,
                                            BooleanFunctions.AND,
                                            binary_condition(
                                                None,
                                                ConditionFunctions.GT,
                                                Column(None, None, "column1"),
                                                Literal(None, 300),
                                            ),
                                            binary_condition(
                                                None,
                                                ConditionFunctions.LTE,
                                                Column(None, None, "column1"),
                                                multiply(
                                                    Literal(None, 300), Literal(None, 4)
                                                ),
                                            ),
                                        ),
                                    ),
                                    Literal(None, 2),
                                ),
                            ),
                            count(),
                        ),
                    ),
                    multiply(
                        minus(
                            Literal(None, 1),
                            divide(
                                Literal(None, 1),
                                FunctionCall(
                                    None,
                                    "sqrt",
                                    (
                                        FunctionCall(
                                            None,
                                            "uniq",
                                            (
                                                Column(
                                                    alias=None,
                                                    column_name="user",
                                                    table_name=None,
                                                ),
                                            ),
                                        ),
                                    ),
                                ),
                            ),
                        ),
                        Literal(None, 3),
                    ),
                    "perf",
                ),
            ),
        ],
    )

    ImpactProcessor().process_query(unprocessed, HTTPRequestSettings())
    assert (
        expected.get_selected_columns_from_ast()
        == unprocessed.get_selected_columns_from_ast()
    )

    ret = unprocessed.get_selected_columns_from_ast()[1].expression.accept(
        ClickhouseExpressionFormatter()
    )
    assert ret == (
        "(plus(minus(1, divide(plus(countIf(lessOrEquals(column1, 300)), "
        "divide(countIf(greater(column1, 300) AND lessOrEquals(column1, "
        "multiply(300, 4))), 2)), count())), "
        "multiply(minus(1, divide(1, sqrt(uniq(user)))), 3)) AS perf)"
    )
