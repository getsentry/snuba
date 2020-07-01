from copy import deepcopy

import pytest

from snuba.clickhouse.columns import ColumnSet
from snuba.datasets.schemas.tables import TableSource
from snuba.query.expressions import Column, CurriedFunctionCall, FunctionCall, Literal
from snuba.query.logical import Query, SelectedExpression
from snuba.query.processors.basic_functions import BasicFunctionsProcessor
from snuba.request.request_settings import HTTPRequestSettings

test_data = [
    (
        Query(
            {},
            TableSource("events", ColumnSet([])),
            selected_columns=[
                SelectedExpression(
                    "alias",
                    FunctionCall("alias", "uniq", (Column(None, None, "column1"),)),
                ),
                SelectedExpression(
                    "alias2",
                    FunctionCall(
                        "alias2", "emptyIfNull", (Column(None, None, "column2"),)
                    ),
                ),
            ],
        ),
        Query(
            {},
            TableSource("events", ColumnSet([])),
            selected_columns=[
                SelectedExpression(
                    "alias",
                    FunctionCall(
                        "alias",
                        "ifNull",
                        (
                            FunctionCall(
                                None, "uniq", (Column(None, None, "column1"),)
                            ),
                            Literal(None, 0),
                        ),
                    ),
                ),
                SelectedExpression(
                    "alias2",
                    FunctionCall(
                        "alias2",
                        "ifNull",
                        (
                            FunctionCall(
                                None, "emptyIfNull", (Column(None, None, "column2"),)
                            ),
                            Literal(None, ""),
                        ),
                    ),
                ),
            ],
        ),
    ),  # Single simple uniq + emptyIfNull
    (
        Query(
            {},
            TableSource("events", ColumnSet([])),
            selected_columns=[
                SelectedExpression(name=None, expression=Column(None, None, "column1")),
                SelectedExpression(
                    "alias",
                    FunctionCall("alias", "uniq", (Column(None, None, "column1"),)),
                ),
                SelectedExpression(
                    "alias2",
                    FunctionCall(
                        "alias2", "emptyIfNull", (Column(None, None, "column2"),)
                    ),
                ),
            ],
            condition=FunctionCall(
                None, "eq", (Column(None, None, "column1"), Literal(None, "a"))
            ),
            groupby=[
                FunctionCall("alias3", "uniq", (Column(None, None, "column5"),)),
                FunctionCall("alias4", "emptyIfNull", (Column(None, None, "column6"),)),
            ],
        ),
        Query(
            {},
            TableSource("events", ColumnSet([])),
            selected_columns=[
                SelectedExpression(name=None, expression=Column(None, None, "column1")),
                SelectedExpression(
                    "alias",
                    FunctionCall(
                        "alias",
                        "ifNull",
                        (
                            FunctionCall(
                                None, "uniq", (Column(None, None, "column1"),)
                            ),
                            Literal(None, 0),
                        ),
                    ),
                ),
                SelectedExpression(
                    "alias2",
                    FunctionCall(
                        "alias2",
                        "ifNull",
                        (
                            FunctionCall(
                                None, "emptyIfNull", (Column(None, None, "column2"),)
                            ),
                            Literal(None, ""),
                        ),
                    ),
                ),
            ],
            condition=FunctionCall(
                None, "eq", (Column(None, None, "column1"), Literal(None, "a"))
            ),
            groupby=[
                FunctionCall(
                    "alias3",
                    "ifNull",
                    (
                        FunctionCall(None, "uniq", (Column(None, None, "column5"),)),
                        Literal(None, 0),
                    ),
                ),
                FunctionCall(
                    "alias4",
                    "ifNull",
                    (
                        FunctionCall(
                            None, "emptyIfNull", (Column(None, None, "column6"),)
                        ),
                        Literal(None, ""),
                    ),
                ),
            ],
        ),
    ),  # Complex query with both uniq and emptyIfNull
    (
        Query(
            {},
            TableSource("events", ColumnSet([])),
            selected_columns=[
                SelectedExpression(
                    name=None,
                    expression=CurriedFunctionCall(
                        None,
                        FunctionCall(None, "top", (Literal(None, 10),)),
                        (Column(None, None, "column1"),),
                    ),
                )
            ],
        ),
        Query(
            {},
            TableSource("events", ColumnSet([])),
            selected_columns=[
                SelectedExpression(
                    name=None,
                    expression=CurriedFunctionCall(
                        None,
                        FunctionCall(None, "topK", (Literal(None, 10),)),
                        (Column(None, None, "column1"),),
                    ),
                )
            ],
        ),
    ),
]


@pytest.mark.parametrize("pre_format, expected_query", test_data)
def test_format_expressions(pre_format: Query, expected_query: Query) -> None:
    copy = deepcopy(pre_format)
    BasicFunctionsProcessor().process_query(copy, HTTPRequestSettings())
    assert (
        copy.get_selected_columns_from_ast()
        == expected_query.get_selected_columns_from_ast()
    )
    assert copy.get_groupby_from_ast() == expected_query.get_groupby_from_ast()
    assert copy.get_condition_from_ast() == expected_query.get_condition_from_ast()
