import pytest

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
from snuba.query.validation import InvalidFunctionCall
from snuba.request.request_settings import HTTPRequestSettings


def test_handled_processor() -> None:
    columnset = ColumnSet([])
    unprocessed = Query(
        {},
        TableSource("events", columnset),
        selected_columns=[
            SelectedExpression(name=None, expression=Column(None, None, "id")),
            SelectedExpression(
                "result", FunctionCall("result", "isHandled", tuple(),),
            ),
        ],
    )

    expected = Query(
        {},
        TableSource("events", columnset),
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
                        Column(None, None, "exception_stacks.mechanism_handled"),
                    ),
                ),
            ),
        ],
    )
    processor = handled_functions.HandledFunctionsProcessor(
        "exception_stacks.mechanism_handled", columnset
    )
    processor.process_query(unprocessed, HTTPRequestSettings())

    assert (
        expected.get_selected_columns_from_ast()
        == unprocessed.get_selected_columns_from_ast()
    )

    ret = unprocessed.get_selected_columns_from_ast()[1].expression.accept(
        ClickhouseExpressionFormatter()
    )
    assert ret == (
        "(arrayExists((x -> (isNull(x) OR equals(assumeNotNull(x), 1))), exception_stacks.mechanism_handled) AS result)"
    )


def test_handled_processor_invalid() -> None:
    columnset = ColumnSet([])
    unprocessed = Query(
        {},
        TableSource("events", columnset),
        selected_columns=[
            SelectedExpression(
                "result",
                FunctionCall("result", "isHandled", (Column(None, None, "type"),),),
            ),
        ],
    )
    processor = handled_functions.HandledFunctionsProcessor(
        "exception_stacks.mechanism_handled", columnset
    )
    with pytest.raises(InvalidFunctionCall):
        processor.process_query(unprocessed, HTTPRequestSettings())


def test_not_handled_processor() -> None:
    columnset = ColumnSet([])
    unprocessed = Query(
        {},
        TableSource("events", columnset),
        selected_columns=[
            SelectedExpression(name=None, expression=Column(None, None, "id")),
            SelectedExpression(
                "result", FunctionCall("result", "notHandled", tuple(),),
            ),
        ],
    )

    expected = Query(
        {},
        TableSource("events", columnset),
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
                        Column(None, None, "exception_stacks.mechanism_handled"),
                    ),
                ),
            ),
        ],
    )
    processor = handled_functions.HandledFunctionsProcessor(
        "exception_stacks.mechanism_handled", columnset
    )
    processor.process_query(unprocessed, HTTPRequestSettings())

    assert (
        expected.get_selected_columns_from_ast()
        == unprocessed.get_selected_columns_from_ast()
    )

    ret = unprocessed.get_selected_columns_from_ast()[1].expression.accept(
        ClickhouseExpressionFormatter()
    )
    assert ret == (
        "(arrayExists((x -> isNotNull(x) AND equals(assumeNotNull(x), 0)), exception_stacks.mechanism_handled) AS result)"
    )


def test_not_handled_processor_invalid() -> None:
    columnset = ColumnSet([])
    unprocessed = Query(
        {},
        TableSource("events", columnset),
        selected_columns=[
            SelectedExpression(
                "result",
                FunctionCall("result", "notHandled", (Column(None, None, "type"),),),
            ),
        ],
    )
    processor = handled_functions.HandledFunctionsProcessor(
        "exception_stacks.mechanism_handled", columnset
    )
    with pytest.raises(InvalidFunctionCall):
        processor.process_query(unprocessed, HTTPRequestSettings())
