from snuba.clickhouse.columns import ColumnSet
from snuba.clickhouse.formatter import ClickhouseExpressionFormatter
from snuba.datasets.schemas.tables import TableSource
from snuba.query.conditions import (
    binary_condition,
    BooleanFunctions,
    ConditionFunctions,
)
from snuba.query.dsl import div, multiply, plus
from snuba.query.expressions import Column, FunctionCall, Literal
from snuba.query.processors.apdex_processor import ApdexProcessor
from snuba.query.query import Query
from snuba.request.request_settings import HTTPRequestSettings


def test_format_expressions() -> None:
    unprocessed = Query(
        {},
        TableSource("events", ColumnSet([])),
        selected_columns=[
            Column(None, "column2", None),
            FunctionCall(
                "perf", "apdex", (Column(None, "column1", None), Literal(None, 300))
            ),
        ],
    )
    expected = Query(
        {},
        TableSource("events", ColumnSet([])),
        selected_columns=[
            Column(None, "column2", None),
            div(
                plus(
                    FunctionCall(
                        None,
                        "countIf",
                        (
                            binary_condition(
                                None,
                                ConditionFunctions.LTE,
                                Column(None, "column1", None),
                                Literal(None, 300),
                            ),
                        ),
                    ),
                    div(
                        FunctionCall(
                            None,
                            "countIf",
                            (
                                binary_condition(
                                    None,
                                    BooleanFunctions.AND,
                                    binary_condition(
                                        None,
                                        ConditionFunctions.GT,
                                        Column(None, "column1", None),
                                        Literal(None, 300),
                                    ),
                                    binary_condition(
                                        None,
                                        ConditionFunctions.LTE,
                                        Column(None, "column1", None),
                                        multiply(Literal(None, 300), Literal(None, 4)),
                                    ),
                                ),
                            ),
                        ),
                        Literal(None, 2),
                    ),
                ),
                FunctionCall(None, "count", (),),
            ),
        ],
    )

    ApdexProcessor().process_query(unprocessed, HTTPRequestSettings())
    assert (
        expected.get_selected_columns_from_ast()
        == unprocessed.get_selected_columns_from_ast()
    )

    ret = unprocessed.get_selected_columns_from_ast()[1].accept(
        ClickhouseExpressionFormatter()
    )
    assert ret == (
        "div(plus(countIf(lessOrEquals(column1, 300)), "
        "div(countIf(and(greater(column1, 300), "
        "lessOrEquals(column1, multiply(300, 4)))), 2)), count())"
    )
