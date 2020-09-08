from datetime import datetime

import pytest

from snuba.clickhouse.columns import ColumnSet
from snuba.clickhouse.query import Query as ClickhouseQuery
from snuba.clickhouse.query_profiler import generate_profile
from snuba.clickhouse.translators.snuba.mappers import build_mapping_expr
from snuba.datasets.schemas.tables import TableSource
from snuba.query.conditions import (
    BooleanFunctions,
    ConditionFunctions,
    binary_condition,
)
from snuba.query.expressions import Column, FunctionCall, Literal
from snuba.query.logical import Query, SelectedExpression
from snuba.querylog.query_metadata import ClickhouseQueryProfile, FilterProfile

test_cases = [
    pytest.param(
        ClickhouseQuery(
            Query(
                {},
                TableSource("events", ColumnSet([])),
                selected_columns=[
                    SelectedExpression("column2", Column("column2", None, "column2")),
                    SelectedExpression(
                        "something",
                        FunctionCall(
                            "something",
                            "arrayJoin",
                            (Column(None, None, "contexts.key"),),
                        ),
                    ),
                ],
                condition=binary_condition(
                    None,
                    BooleanFunctions.AND,
                    binary_condition(
                        None,
                        ConditionFunctions.GTE,
                        Column(None, None, "timestamp"),
                        Literal(None, datetime(2020, 8, 1)),
                    ),
                    binary_condition(
                        None,
                        BooleanFunctions.AND,
                        binary_condition(
                            None,
                            ConditionFunctions.LT,
                            Column(None, None, "timestamp"),
                            Literal(None, datetime(2020, 9, 1)),
                        ),
                        binary_condition(
                            None,
                            ConditionFunctions.EQ,
                            build_mapping_expr(
                                "tags[asd]", None, "tags", Literal(None, "asd"),
                            ),
                            Literal(None, "sdf"),
                        ),
                    ),
                ),
                groupby=[
                    Column("column2", None, "column2"),
                    Column("column3", None, "column3"),
                ],
            )
        ),
        ClickhouseQueryProfile(
            time_range=31,
            table="events",
            multi_level_condition=False,
            where_profile=FilterProfile(
                columns={"timestamp", "tags.key", "tags.value"},
                mapping_cols={"tags.key", "tags.value"},
            ),
            groupby_cols={"column2", "column3"},
            array_join_cols={"contexts.key"},
        ),
        id="Query with timestamp, tags, and arrayjoin",
    ),
    pytest.param(
        ClickhouseQuery(
            Query(
                {},
                TableSource("events", ColumnSet([])),
                selected_columns=[
                    SelectedExpression("column2", Column("column2", None, "column2")),
                ],
                condition=binary_condition(
                    None,
                    BooleanFunctions.OR,
                    binary_condition(
                        None,
                        ConditionFunctions.GTE,
                        Column(None, None, "timestamp"),
                        Literal(None, datetime(2020, 8, 1)),
                    ),
                    binary_condition(
                        None,
                        ConditionFunctions.LT,
                        Column(None, None, "timestamp"),
                        Literal(None, datetime(2020, 9, 1)),
                    ),
                ),
            )
        ),
        ClickhouseQueryProfile(
            time_range=None,
            table="events",
            multi_level_condition=True,
            where_profile=FilterProfile(columns={"timestamp"}, mapping_cols=set(),),
            groupby_cols=set(),
            array_join_cols=set(),
        ),
        id="Almost empty query with OR",
    ),
]


@pytest.mark.parametrize("query, profile", test_cases)
def test_format_expressions(
    query: ClickhouseQuery, profile: ClickhouseQueryProfile,
) -> None:
    assert generate_profile(query) == profile
