from snuba.clickhouse.columns import ColumnSet
from snuba.clickhouse.formatter import ClickhouseExpressionFormatter
from snuba.datasets.schemas.tables import TableSource
from snuba.query.conditions import (
    BooleanFunctions,
    ConditionFunctions,
    binary_condition,
)
from snuba.query.expressions import Column, FunctionCall, Lambda, Literal, Argument
from snuba.query.logical import Query, SelectedExpression
from snuba.query.processors import handled_functions
from snuba.request.request_settings import HTTPRequestSettings


def test_handled_processor() -> None:
    unprocessed = Query(
        {},
        TableSource("events", ColumnSet([])),
        selected_columns=[
            SelectedExpression(name=None, expression=Column(None, None, "id")),
            SelectedExpression(
                "result",
                FunctionCall(
                    "result",
                    "isHandled",
                    (Column(None, None, "exception_stacks.handled"),),
                ),
            ),
        ],
    )

    expected = Query(
        {},
        TableSource("events", ColumnSet([])),
        selected_columns=[
            SelectedExpression(name=None, expression=Column(None, None, "id")),
            SelectedExpression(
                "result",
                FunctionCall(
                    "result",
                    "arrayExists",
                    (
                        Lambda(
                            None,
                            ("x",),
                            binary_condition(
                                None,
                                BooleanFunctions.OR,
                                FunctionCall(None, "isNull", (Argument(None, "x"),)),
                                binary_condition(
                                    None,
                                    ConditionFunctions.EQ,
                                    FunctionCall(
                                        None, "assumeNotNull", (Argument(None, "x"),)
                                    ),
                                    Literal(None, 1),
                                ),
                            ),
                        ),
                        Column(None, None, "exception_stacks.handled"),
                    ),
                ),
            ),
        ],
    )
    processor = handled_functions.HandledFunctionsProcessor()
    processor.process_query(unprocessed, HTTPRequestSettings())

    assert (
        expected.get_selected_columns_from_ast()
        == unprocessed.get_selected_columns_from_ast()
    )

    ret = unprocessed.get_selected_columns_from_ast()[1].expression.accept(
        ClickhouseExpressionFormatter()
    )
    assert ret == (
        "(arrayExists((x -> (isNull(x) OR equals(assumeNotNull(x), 1))), exception_stacks.handled) AS result)"
    )


def test_not_handled_processor() -> None:
    unprocessed = Query(
        {},
        TableSource("events", ColumnSet([])),
        selected_columns=[
            SelectedExpression(name=None, expression=Column(None, None, "id")),
            SelectedExpression(
                "result",
                FunctionCall(
                    "result",
                    "notHandled",
                    (Column(None, None, "exception_stacks.handled"),),
                ),
            ),
        ],
    )

    expected = Query(
        {},
        TableSource("events", ColumnSet([])),
        selected_columns=[
            SelectedExpression(name=None, expression=Column(None, None, "id")),
            SelectedExpression(
                "result",
                FunctionCall(
                    "result",
                    "arrayExists",
                    (
                        Lambda(
                            None,
                            ("x",),
                            binary_condition(
                                None,
                                BooleanFunctions.AND,
                                FunctionCall(None, "isNotNull", (Argument(None, "x"),)),
                                binary_condition(
                                    None,
                                    ConditionFunctions.EQ,
                                    FunctionCall(
                                        None, "assumeNotNull", (Argument(None, "x"),)
                                    ),
                                    Literal(None, 0),
                                ),
                            ),
                        ),
                        Column(None, None, "exception_stacks.handled"),
                    ),
                ),
            ),
        ],
    )
    processor = handled_functions.HandledFunctionsProcessor()
    processor.process_query(unprocessed, HTTPRequestSettings())

    assert (
        expected.get_selected_columns_from_ast()
        == unprocessed.get_selected_columns_from_ast()
    )

    ret = unprocessed.get_selected_columns_from_ast()[1].expression.accept(
        ClickhouseExpressionFormatter()
    )
    assert ret == (
        "(arrayExists((x -> isNotNull(x) AND equals(assumeNotNull(x), 0)), exception_stacks.handled) AS result)"
    )
