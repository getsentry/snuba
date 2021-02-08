from datetime import datetime

import pytest
from snuba.clickhouse.columns import ColumnSet
from snuba.clickhouse.query import Query as ClickhouseQuery
from snuba.query.composite import CompositeQuery
from snuba.clickhouse.query_profiler import generate_profile
from snuba.clickhouse.translators.snuba.mappers import build_mapping_expr
from snuba.query import SelectedExpression
from snuba.query.conditions import (
    BooleanFunctions,
    ConditionFunctions,
    binary_condition,
)
from snuba.query.data_source.simple import Table
from snuba.query.expressions import Column, FunctionCall, Literal
from snuba.querylog.query_metadata import ClickhouseQueryProfile, FilterProfile
from snuba.state import safe_dumps
from tests.query.joins.join_structures import (
    clickhouse_events_node,
    clickhouse_groups_node,
    events_groups_join,
)


test_cases = [
    pytest.param(
        ClickhouseQuery(
            Table("events", ColumnSet([])),
            selected_columns=[
                SelectedExpression("column2", Column("column2", None, "column2")),
                SelectedExpression(
                    "something",
                    FunctionCall(
                        "something", "arrayJoin", (Column(None, None, "contexts.key"),),
                    ),
                ),
            ],
            condition=binary_condition(
                BooleanFunctions.AND,
                binary_condition(
                    ConditionFunctions.GTE,
                    Column(None, None, "timestamp"),
                    Literal(None, datetime(2020, 8, 1)),
                ),
                binary_condition(
                    BooleanFunctions.AND,
                    binary_condition(
                        ConditionFunctions.LT,
                        Column(None, None, "timestamp"),
                        Literal(None, datetime(2020, 9, 1)),
                    ),
                    binary_condition(
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
        ),
        ClickhouseQueryProfile(
            time_range=31,
            table="events",
            all_columns={
                "events.timestamp",
                "events.column2",
                "events.column3",
                "events.contexts.key",
                "events.tags.key",
                "events.tags.value",
            },
            multi_level_condition=False,
            where_profile=FilterProfile(
                columns={"events.timestamp", "events.tags.key", "events.tags.value"},
                mapping_cols={"events.tags.key", "events.tags.value"},
            ),
            groupby_cols={"events.column2", "events.column3"},
            array_join_cols={"events.contexts.key"},
        ),
        id="Query with timestamp, tags, and arrayjoin",
    ),
    pytest.param(
        ClickhouseQuery(
            Table("events", ColumnSet([])),
            selected_columns=[
                SelectedExpression("column2", Column("column2", None, "column2")),
            ],
            condition=binary_condition(
                BooleanFunctions.OR,
                binary_condition(
                    ConditionFunctions.GTE,
                    Column(None, None, "timestamp"),
                    Literal(None, datetime(2020, 8, 1)),
                ),
                binary_condition(
                    ConditionFunctions.LT,
                    Column(None, None, "timestamp"),
                    Literal(None, datetime(2020, 9, 1)),
                ),
            ),
        ),
        ClickhouseQueryProfile(
            time_range=None,
            table="events",
            all_columns={"events.column2", "events.timestamp"},
            multi_level_condition=True,
            where_profile=FilterProfile(
                columns={"events.timestamp"}, mapping_cols=set(),
            ),
            groupby_cols=set(),
            array_join_cols=set(),
        ),
        id="Almost empty query with OR",
    ),
    pytest.param(
        CompositeQuery(
            from_clause=events_groups_join(
                clickhouse_events_node(
                    [
                        SelectedExpression(
                            "_snuba_group_id",
                            Column("_snuba_group_id", None, "group_id"),
                        ),
                    ],
                    binary_condition(
                        BooleanFunctions.AND,
                        binary_condition(
                            ConditionFunctions.GTE,
                            Column(None, None, "timestamp"),
                            Literal(None, datetime(2020, 8, 1)),
                        ),
                        binary_condition(
                            ConditionFunctions.LT,
                            Column(None, None, "timestamp"),
                            Literal(None, datetime(2020, 9, 1)),
                        ),
                    ),
                ),
                clickhouse_groups_node(
                    [SelectedExpression("_snuba_id", Column("_snuba_id", None, "id"))],
                ),
            ),
            selected_columns=[],
        ),
        ClickhouseQueryProfile(
            time_range=31,
            table="groupedmessage_local,sentry_errors",
            all_columns={
                "sentry_errors.group_id",
                "sentry_errors.timestamp",
                "groupedmessage_local.id",
            },
            multi_level_condition=False,
            where_profile=FilterProfile(
                columns={"sentry_errors.timestamp"}, mapping_cols=set(),
            ),
            groupby_cols=set(),
            array_join_cols=set(),
        ),
        id="Simple join with two tables",
    ),
]


@pytest.mark.parametrize("query, profile", test_cases)
def test_format_expressions(
    query: ClickhouseQuery, profile: ClickhouseQueryProfile,
) -> None:
    generated_profile = generate_profile(query)
    assert generated_profile == profile
    # Ensure that json serialization does not fail.
    safe_dumps(generated_profile.to_dict())


def test_serialization() -> None:
    profile = ClickhouseQueryProfile(
        time_range=10,
        table="events",
        all_columns={"col", "timestamp", "arrayjoin"},
        multi_level_condition=True,
        where_profile=FilterProfile(columns={"timestamp"}, mapping_cols=set(),),
        groupby_cols={"col"},
        array_join_cols={"arrayjoin"},
    )

    assert profile.to_dict() == {
        "time_range": 10,
        "table": "events",
        "all_columns": ["arrayjoin", "col", "timestamp"],
        "multi_level_condition": True,
        "where_profile": {"columns": ["timestamp"], "mapping_cols": []},
        "groupby_cols": ["col"],
        "array_join_cols": ["arrayjoin"],
    }
