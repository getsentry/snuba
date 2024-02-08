from typing import Any, MutableMapping

import pytest
from snuba_sdk.legacy import json_to_snql

from snuba.clickhouse.columns import ColumnSet
from snuba.clickhouse.query import Query
from snuba.datasets.entities.entity_key import EntityKey
from snuba.datasets.entities.factory import get_entity
from snuba.datasets.factory import get_dataset
from snuba.pipeline.processors import execute_all_clickhouse_processors
from snuba.query import OrderBy, OrderByDirection, SelectedExpression
from snuba.query.conditions import ConditionFunctions, binary_condition
from snuba.query.data_source.simple import Table
from snuba.query.expressions import (
    Column,
    Expression,
    FunctionCall,
    Literal,
    SubscriptableReference,
)
from snuba.query.query_settings import HTTPQuerySettings
from snuba.query.snql.parser import parse_snql_query, parse_snql_query_initial


def test_iterate_over_query() -> None:
    """
    Creates a query with the new AST and iterate over all expressions.
    """
    column1 = Column(None, "t1", "c1")
    column2 = Column(None, "t1", "c2")
    function_1 = FunctionCall("alias", "f1", (column1, column2))
    function_2 = FunctionCall("alias", "f2", (column2,))

    condition = binary_condition(ConditionFunctions.EQ, column1, Literal(None, "1"))

    prewhere = binary_condition(ConditionFunctions.EQ, column2, Literal(None, "2"))

    orderby = OrderBy(OrderByDirection.ASC, function_2)

    query = Query(
        Table("my_table", ColumnSet([])),
        selected_columns=[SelectedExpression("alias", function_1)],
        array_join=None,
        condition=condition,
        groupby=[function_1],
        prewhere=prewhere,
        having=None,
        order_by=[orderby],
    )

    expected_expressions = [
        # selected columns
        column1,
        column2,
        function_1,
        # condition
        column1,
        Literal(None, "1"),
        condition,
        # groupby
        column1,
        column2,
        function_1,
        # order by
        column2,
        function_2,
        # prewhere
        column2,
        Literal(None, "2"),
        prewhere,
    ]

    assert list(query.get_all_expressions()) == expected_expressions


def test_replace_expression() -> None:
    """
    Create a query with the new AST and replaces a function with a different function
    replaces f1(...) with tag(f1)
    """
    column1 = Column(None, "t1", "c1")
    column2 = Column(None, "t1", "c2")
    function_1 = FunctionCall("alias", "f1", (column1, column2))
    function_2 = FunctionCall("alias", "f2", (column2,))

    condition = binary_condition(ConditionFunctions.EQ, function_1, Literal(None, "1"))

    prewhere = binary_condition(ConditionFunctions.EQ, function_1, Literal(None, "2"))

    orderby = OrderBy(OrderByDirection.ASC, function_2)

    query = Query(
        Table("my_table", ColumnSet([])),
        selected_columns=[SelectedExpression("alias", function_1)],
        array_join=None,
        condition=condition,
        groupby=[function_1],
        having=None,
        prewhere=prewhere,
        order_by=[orderby],
    )

    def replace(exp: Expression) -> Expression:
        if isinstance(exp, FunctionCall) and exp.function_name == "f1":
            return FunctionCall(exp.alias, "tag", (Literal(None, "f1"),))
        return exp

    query.transform_expressions(replace)

    expected_query = Query(
        Table("my_table", ColumnSet([])),
        selected_columns=[
            SelectedExpression(
                "alias", FunctionCall("alias", "tag", (Literal(None, "f1"),))
            )
        ],
        array_join=None,
        condition=binary_condition(
            ConditionFunctions.EQ,
            FunctionCall("alias", "tag", (Literal(None, "f1"),)),
            Literal(None, "1"),
        ),
        groupby=[FunctionCall("alias", "tag", (Literal(None, "f1"),))],
        prewhere=binary_condition(
            ConditionFunctions.EQ,
            FunctionCall("alias", "tag", (Literal(None, "f1"),)),
            Literal(None, "2"),
        ),
        having=None,
        order_by=[orderby],
    )

    assert query.get_selected_columns() == expected_query.get_selected_columns()
    assert query.get_condition() == expected_query.get_condition()
    assert query.get_groupby() == expected_query.get_groupby()
    assert query.get_having() == expected_query.get_having()
    assert query.get_orderby() == expected_query.get_orderby()

    assert list(query.get_all_expressions()) == list(
        expected_query.get_all_expressions()
    )


def test_get_all_columns_legacy() -> None:
    query_body = {
        "selected_columns": [
            ["f1", ["title", "message"], "f1_alias"],
            ["f2", [], "f2_alias"],
            ["formatDateTime", ["timestamp", "'%Y-%m-%d'"], "formatted_time"],
        ],
        "aggregations": [
            ["count", "platform", "platforms"],
            ["uniq", "platform", "uniq_platforms"],
            ["testF", ["platform", ["anotherF", ["offset"]]], "top_platforms"],
        ],
        "conditions": [
            ["tags[sentry:dist]", "IN", ["dist1", "dist2"]],
            ["timestamp", ">=", "2020-01-01T12:00:00"],
            ["timestamp", "<", "2020-01-02T12:00:00"],
            ["project_id", "=", 1],
        ],
        "having": [["trace_sampled", ">", 1]],
        "groupby": [["format_eventid", ["event_id"]]],
    }
    events = get_dataset("events")
    request = json_to_snql(query_body, "events")
    request.validate()
    query, _ = parse_snql_query(str(request.query), events)

    assert query.get_all_ast_referenced_columns() == {
        Column("_snuba_title", None, "title"),
        Column("_snuba_message", None, "message"),
        Column("_snuba_platform", None, "platform"),
        Column("_snuba_offset", None, "offset"),
        Column("_snuba_tags", None, "tags"),
        Column("_snuba_trace_sampled", None, "trace_sampled"),
        Column("_snuba_event_id", None, "event_id"),
        Column("_snuba_timestamp", None, "timestamp"),
        Column("_snuba_project_id", None, "project_id"),
    }

    assert query.get_all_ast_referenced_subscripts() == {
        SubscriptableReference(
            "_snuba_tags[sentry:dist]",
            Column("_snuba_tags", None, "tags"),
            Literal(None, "sentry:dist"),
        )
    }


def test_get_all_columns() -> None:
    query_body = """
        MATCH (events)
        SELECT f1(partition, release) AS f1_alias,
            f2() AS f2_alias,
            formatDateTime(timestamp, '%Y-%m-%d') AS formatted_time,
            count() AS platforms,
            uniq(platform) AS uniq_platforms,
            testF(platform, anotherF(title)) AS top_platforms
        BY format_eventid(event_id)
        WHERE tags[sentry:dist] IN tuple('dist1', 'dist2')
            AND timestamp >= toDateTime('2020-01-01 12:00:00')
            AND timestamp < toDateTime('2020-01-02 12:00:00')
            AND project_id = 1
        HAVING trace_sampled > 1
        """
    events = get_dataset("events")
    query, _ = parse_snql_query(query_body, events)

    assert query.get_all_ast_referenced_columns() == {
        Column("_snuba_partition", None, "partition"),
        Column("_snuba_release", None, "release"),
        Column("_snuba_platform", None, "platform"),
        Column("_snuba_title", None, "title"),
        Column("_snuba_tags", None, "tags"),
        Column("_snuba_trace_sampled", None, "trace_sampled"),
        Column("_snuba_event_id", None, "event_id"),
        Column("_snuba_timestamp", None, "timestamp"),
        Column("_snuba_project_id", None, "project_id"),
    }

    assert query.get_all_ast_referenced_subscripts() == {
        SubscriptableReference(
            "_snuba_tags[sentry:dist]",
            Column("_snuba_tags", None, "tags"),
            Literal(None, "sentry:dist"),
        )
    }


def test_initial_parsing_snql() -> None:
    # Initial parsing created a map object for groupby clause, should be a list
    body = "MATCH (events) SELECT col BY title"
    query = parse_snql_query_initial(body)
    # casting a map object to a list drains the generator, should be able to cast as much as needed
    assert list(query.get_groupby()) != []
    assert list(query.get_groupby()) != []
    assert isinstance(query.get_groupby(), list)


def test_alias_regex_allows_parentheses() -> None:
    body = (
        "MATCH (metrics_counters) SELECT sumIf(value, equals(metric_id, 0)) "
        "AS `sum(sentry.sessions.session)`, metric_id AS `metric.id`, measurements["
        "lcp.elementSize] AS `good_lcp_stuff` BY tags[44] AS `session.status`"
    )
    query = parse_snql_query_initial(body)
    expressions = query.get_selected_columns()
    assert len(expressions) == 4
    assert sorted([expr.name for expr in expressions]) == [
        "good_lcp_stuff",
        "metric.id",
        "session.status",
        "sum(sentry.sessions.session)",
    ]


def test_alias_regex_allows_for_mri_format() -> None:
    body = (
        "MATCH (metrics_counters) SELECT sumIf(value, equals(metric_id, 0)) "
        "AS `c:sessions/session@none` BY tags[44] AS `session.status`"
    )
    query = parse_snql_query_initial(body)
    expressions = query.get_selected_columns()
    assert len(expressions) == 2
    assert sorted([expr.name for expr in expressions]) == [
        "c:sessions/session@none",
        "session.status",
    ]


def test_quoted_column_regex_allows_for_mri_format() -> None:
    body = (
        "MATCH (metrics_counters) SELECT sumIf(value, equals(c:sessions/session@none, 0)) "
        "BY tags[44] AS `session.status`"
    )
    query = parse_snql_query_initial(body)
    expressions = query.get_selected_columns()
    assert len(expressions) == 2
    assert sorted([expr.name for expr in expressions]) == [
        "session.status",
        "sumIf(value, equals(c:sessions/session@none, 0))",
    ]


VALIDATION_TESTS = [
    pytest.param(
        {
            "selected_columns": ["project_id", "event_id"],
            "conditions": [
                ["event_id", "IN", ["a" * 32, "b" * 32]],
                ["project_id", "=", 1],
                ["timestamp", ">=", "2020-01-01T12:00:00"],
                ["timestamp", "<", "2020-01-02T12:00:00"],
            ],
        },
        True,
        id="No alias references",
    ),
    pytest.param(
        {
            "selected_columns": ["project_id", ["foo", ["event_id"], "not_event"]],
            "conditions": [
                ["not_event", "IN", ["a" * 32, "b" * 32]],
                ["project_id", "=", 1],
                ["timestamp", ">=", "2020-01-01T12:00:00"],
                ["timestamp", "<", "2020-01-02T12:00:00"],
            ],
        },
        True,
        id="Alias declared and referenced",
    ),
    pytest.param(
        {
            "selected_columns": ["project_id", ["foo", ["event_id"], "event_id"]],
            "conditions": [
                ["event_id", "IN", ["a" * 32, "b" * 32]],
                ["project_id", "=", 1],
                ["timestamp", ">=", "2020-01-01T12:00:00"],
                ["timestamp", "<", "2020-01-02T12:00:00"],
            ],
        },
        True,
        id="Alias redefines col and referenced",
    ),
]


@pytest.mark.parametrize("query_body, expected_result", VALIDATION_TESTS)
def test_alias_validation(
    query_body: MutableMapping[str, Any], expected_result: bool
) -> None:
    events = get_dataset("events")
    events_entity = get_entity(EntityKey.EVENTS)
    request = json_to_snql(query_body, "events")
    request.validate()
    query, _ = parse_snql_query(str(request.query), events)
    settings = HTTPQuerySettings()
    query_plan = (
        events_entity.get_query_pipeline_builder().build_planner(query, settings)
    ).build_best_plan()
    execute_all_clickhouse_processors(query_plan, settings)

    assert query_plan.query.validate_aliases() == expected_result
