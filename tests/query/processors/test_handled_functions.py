import pytest
from snuba.clickhouse.columns import ColumnSet
from snuba.clickhouse.formatter.expression import ClickhouseExpressionFormatter
from snuba.datasets.entities import EntityKey
from snuba.query import SelectedExpression
from snuba.query.conditions import (
    BooleanFunctions,
    ConditionFunctions,
    binary_condition,
)
from snuba.query.data_source.simple import Entity as QueryEntity
from snuba.query.exceptions import InvalidExpressionException
from snuba.query.expressions import Argument, Column, FunctionCall, Lambda, Literal
from snuba.query.logical import Query
from snuba.query.processors import handled_functions
from snuba.request.request_settings import HTTPRequestSettings


def test_handled_processor() -> None:
    columnset = ColumnSet([])
    unprocessed = Query(
        {},
        QueryEntity(EntityKey.EVENTS, ColumnSet([])),
        selected_columns=[
            SelectedExpression(name=None, expression=Column(None, None, "id")),
            SelectedExpression(
                "result", FunctionCall("result", "isHandled", tuple(),),
            ),
        ],
    )

    expected = Query(
        {},
        QueryEntity(EntityKey.EVENTS, ColumnSet([])),
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
                                BooleanFunctions.OR,
                                FunctionCall(None, "isNull", (Argument(None, "x"),)),
                                binary_condition(
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
        QueryEntity(EntityKey.EVENTS, ColumnSet([])),
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
    with pytest.raises(InvalidExpressionException):
        processor.process_query(unprocessed, HTTPRequestSettings())


def test_not_handled_processor() -> None:
    columnset = ColumnSet([])
    unprocessed = Query(
        {},
        QueryEntity(EntityKey.EVENTS, ColumnSet([])),
        selected_columns=[
            SelectedExpression(name=None, expression=Column(None, None, "id")),
            SelectedExpression(
                "result", FunctionCall("result", "notHandled", tuple(),),
            ),
        ],
    )

    expected = Query(
        {},
        QueryEntity(EntityKey.EVENTS, ColumnSet([])),
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
                                BooleanFunctions.AND,
                                FunctionCall(None, "isNotNull", (Argument(None, "x"),)),
                                binary_condition(
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
        QueryEntity(EntityKey.EVENTS, ColumnSet([])),
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
    with pytest.raises(InvalidExpressionException):
        processor.process_query(unprocessed, HTTPRequestSettings())
