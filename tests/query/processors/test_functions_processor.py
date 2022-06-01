from copy import deepcopy

import pytest

from snuba.clickhouse.columns import ColumnSet
from snuba.datasets.entities import EntityKey
from snuba.query import SelectedExpression
from snuba.query.data_source.simple import Entity as QueryEntity
from snuba.query.expressions import Column, CurriedFunctionCall, FunctionCall, Literal
from snuba.query.logical import Query
from snuba.query.processors.basic_functions import BasicFunctionsProcessor
from snuba.query.query_settings import HTTPQuerySettings

test_data = [
    (
        Query(
            QueryEntity(EntityKey.EVENTS, ColumnSet([])),
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
            QueryEntity(EntityKey.EVENTS, ColumnSet([])),
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
            QueryEntity(EntityKey.EVENTS, ColumnSet([])),
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
            QueryEntity(EntityKey.EVENTS, ColumnSet([])),
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
            QueryEntity(EntityKey.EVENTS, ColumnSet([])),
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
            QueryEntity(EntityKey.EVENTS, ColumnSet([])),
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
    (
        Query(
            QueryEntity(EntityKey.EVENTS, ColumnSet([])),
            selected_columns=[
                SelectedExpression(
                    "alias",
                    FunctionCall("alias", "log", (Column(None, None, "column1"),)),
                ),
            ],
        ),
        Query(
            QueryEntity(EntityKey.EVENTS, ColumnSet([])),
            selected_columns=[
                SelectedExpression(
                    "alias",
                    FunctionCall(
                        "alias",
                        "ifNotFinite",
                        (
                            FunctionCall(None, "log", (Column(None, None, "column1"),)),
                            Literal(None, 0),
                        ),
                    ),
                ),
            ],
        ),
    ),
]


@pytest.mark.parametrize("pre_format, expected_query", test_data)
def test_format_expressions(pre_format: Query, expected_query: Query) -> None:
    copy = deepcopy(pre_format)
    BasicFunctionsProcessor().process_query(copy, HTTPQuerySettings())
    assert copy.get_selected_columns() == expected_query.get_selected_columns()
    assert copy.get_groupby() == expected_query.get_groupby()
    assert copy.get_condition() == expected_query.get_condition()
