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
from snuba.query.processors import null_array_functions
from snuba.request.request_settings import HTTPRequestSettings


def test_null_array_exists_processor() -> None:
    unprocessed = Query(
        {},
        TableSource("events", ColumnSet([])),
        selected_columns=[
            SelectedExpression(name=None, expression=Column(None, None, "id")),
            SelectedExpression(
                "result",
                FunctionCall(
                    "result",
                    "nullArrayExists",
                    (Column(None, None, "exception_stacks.handled"), Literal(None, 1)),
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
    processor = null_array_functions.NullArrayFunctionsProcessor()
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


def test_not_null_array_exists_processor() -> None:
    unprocessed = Query(
        {},
        TableSource("events", ColumnSet([])),
        selected_columns=[
            SelectedExpression(name=None, expression=Column(None, None, "id")),
            SelectedExpression(
                "result",
                FunctionCall(
                    "result",
                    "notNullArrayExists",
                    (Column(None, None, "exception_stacks.handled"), Literal(None, 0)),
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
    processor = null_array_functions.NullArrayFunctionsProcessor()
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
